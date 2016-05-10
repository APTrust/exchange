package tarball

import (
	"archive/tar"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/platform"
	"github.com/APTrust/exchange/util"
	"github.com/satori/go.uuid"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func Untar(tarFilePath string, manifest *models.IngestManifest) {

	// Record that we're starting on this.
	manifest.Untar.Attempted = true
	manifest.Untar.AttemptNumber += 1
	manifest.Untar.FinishedAt = time.Time{}
	manifest.Untar.Start()

	absInputFile, err := filepath.Abs(tarFilePath)
	if err != nil {
		manifest.Untar.AddError("Before untarring, could not determine "+
			"absolute path to file '%s': %v", tarFilePath, err)
		return
	}
	bagName, err := util.BagNameFromTarFileName(absInputFile)
	if err != nil {
		manifest.Untar.AddError("Couldn't get bag name from tar file path '%s': %v", absInputFile, err)
		return
	}

	// Open the tar file for reading.
	file, err := os.Open(manifest.TarPath)
	if file != nil {
		defer file.Close()
	}
	if err != nil {
		manifest.Untar.AddError("Could not open file %s for untarring: %v", manifest.TarPath, err)
		return
	}

	// Record the name of the top-level directory in the tar
	// file. Our spec says that the name of the directory into
	// which the file untars should be the same as the tar file
	// name, minus the .tar extension. So uva-123.tar should
	// untar into a directory called uva-123. This is required
	// so that IntellectualObject and GenericFile identifiers
	// in Fedora can be traced back to the named bag from which
	// they came. Other parts of bagman, such as the file cleanup
	// routines, assume that the untarred directory name will
	// match the tar file name, as the spec demands. When the names
	// do not match, the cleanup routines will not clean up the
	// untarred files, and we'll end up losing a lot of disk space.
	topLevelDir := ""

	// Untar the file and record the results.
	tarReader := tar.NewReader(file)

	for {
		header, err := tarReader.Next()
		if err != nil && err.Error() == "EOF" {
			break // end of archive
		}
		if err != nil {
			manifest.Untar.AddError("Error reading tar file header: %v. " +
				"Either this is not a tar file, or the file is corrupt.", err)
			return
		}

		// Top-level dir will be the first header entry.
		if header.Typeflag == tar.TypeDir && topLevelDir == "" {
			topLevelDir, err = getTopLevelDir(header.Name, tarFilePath)
			if err != nil {
				manifest.Untar.AddError(err.Error())
				return
			}
			manifest.UntarredPath = topLevelDir
		}

		// Get the output path for this file -> Where should we untar it to?
		outputPath := getOutputPath(header.Name, absInputFile)

		// Make sure the directory that we're about to write into exists.
		err = os.MkdirAll(filepath.Dir(outputPath), 0755)
		if err != nil {
			manifest.Untar.AddError("Could not create destination file '%s' "+
				"while unpacking tar archive: %v", outputPath, err)
			return
		}

		// Copy the file, if it's an actual file. Otherwise, ignore it and record
		// a warning. The bag library does not deal with items like symlinks.
		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
			fileName, err := getFileName(header.Name)
			if err != nil {
				manifest.Untar.AddError(err.Error())
				return
			}
			if util.HasSavableName(fileName) {
				fileDir := filepath.Dir(absInputFile)
				gf := models.NewGenericFile()
				manifest.Object.GenericFiles = append(manifest.Object.GenericFiles, gf)
				gf.IngestLocalPath, err = filepath.Abs(filepath.Join(fileDir, header.Name))
				if err != nil {
					gf.IngestErrorMessage = fmt.Sprintf("Path error: %v", err)
					manifest.Untar.AddError(gf.IngestErrorMessage)
					return
				}
				cleanBagName, err := util.CleanBagName(bagName)
				if err != nil {
					gf.IngestErrorMessage = fmt.Sprintf("Path error: %v", err)
					manifest.Untar.AddError(gf.IngestErrorMessage)
					return
				}
				gf.IntellectualObjectIdentifier = cleanBagName
				gf.Identifier = fmt.Sprintf("%s/%s", cleanBagName, gf.IngestLocalPath)
				gf.FileModified = header.ModTime
				gf.Size = header.Size
				gf.IngestFileUid = header.Uid
				gf.IngestFileGid = header.Gid
				gf.IngestFileUname = header.Uname
				gf.IngestFileGname = header.Gname
				gf.IngestUUID = uuid.NewV4().String()
				gf.IngestUUIDGeneratedAt = time.Now().UTC()
				saveWithChecksums(tarReader, gf)
				if gf.IngestErrorMessage != "" {
					manifest.Untar.AddError(gf.IngestErrorMessage)
					return
				}
			} else {
				err = saveFile(outputPath, tarReader)
				if err != nil {
					manifest.Untar.AddError("Error copying file from tar archive to '%s': %v", outputPath, err)
					return
				}
			}
		} else if header.Typeflag != tar.TypeDir {
			// Header item is neither file nor directory.
			// Do nothing, but record that we saw this item.
			manifest.Object.IngestFilesIgnored = append(manifest.Object.IngestFilesIgnored, header.Name)
		}
	}
}

// Saves a file from the tar archive to local disk. This function
// used to save non-data files (manifests, tag files, etc.)
func saveFile(destination string, tarReader *tar.Reader) error {
	// TODO: Save with same permissions as file in tar archive
	outputWriter, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY, 0644)
	if outputWriter != nil {
		defer outputWriter.Close()
	}
	if err != nil {
		return err
	}
	_, err = io.Copy(outputWriter, tarReader)
	if err != nil {
		return err
	}
	return nil
}

func getTopLevelDir(headerName, tarFilePath string) (topLevelDir string, err error) {
	topLevelDir = strings.Replace(headerName, "/", "", 1)
	// Fix for Windows
	systemNormalizedPath := tarFilePath
	if runtime.GOOS == "windows" && strings.Contains(tarFilePath, "\\") {
		systemNormalizedPath = strings.Replace(tarFilePath, "\\", "/", -1)
	}
	expectedDir := path.Base(systemNormalizedPath)
	if strings.HasSuffix(expectedDir, ".tar") {
		expectedDir = expectedDir[0 : len(expectedDir)-4]
	}
	if topLevelDir != expectedDir {
		err = fmt.Errorf("Bag '%s' should untar to a folder named '%s', but "+
			"it untars to '%s'. Please repackage this bag and try again.",
			path.Base(tarFilePath), expectedDir, topLevelDir)
	}
	return topLevelDir, err
}

func getOutputPath(headerName, absInputFile string) (string) {
	// PT #114804083 - Make sure there's a slash in the path before setting
	// tarfile.OutputDir, or we'll set it incorrectly and when we call
	// bagman.ReadBag(tarResult.OutputDir), it will fail with "file not found".
	pathParts := strings.Split(headerName, "/")
	tarDirectory := pathParts[0]
	return filepath.Join(filepath.Dir(absInputFile), tarDirectory)
}

func getFileName(headerName string) (string, error) {
	pathParts := strings.SplitN(headerName, "/", 2)
	if len(pathParts) < 2 {
		err := fmt.Errorf("File %s in tar archive should be in format dir/filename", headerName)
		return "", err
	}
	return pathParts[1], nil
}

// buildFile saves a data file from the tar archive to disk,
// then returns a struct with data we'll need to construct the
// GenericFile object in Fedora later.
func saveWithChecksums(tarReader *tar.Reader, gf *models.GenericFile) {
	// Set up a MultiWriter to stream data ONCE to file,
	// md5 and sha256. We don't want to process the stream
	// three separate times.
	outputWriter, err := os.OpenFile(gf.IngestLocalPath, os.O_CREATE|os.O_WRONLY, 0644)
	if outputWriter != nil {
		defer outputWriter.Close()
	}
	if err != nil {
		gf.IngestErrorMessage = fmt.Sprintf("Error opening writing to %s: %v", gf.IngestLocalPath, err)
		return
	}
	md5Hash := md5.New()
	shaHash := sha256.New()
	multiWriter := io.MultiWriter(md5Hash, shaHash, outputWriter)
	io.Copy(multiWriter, tarReader)
	gf.IngestMd5 = fmt.Sprintf("%x", md5Hash.Sum(nil))
	gf.IngestSha256 = fmt.Sprintf("%x", shaHash.Sum(nil))
	gf.IngestSha256GeneratedAt = time.Now().UTC()
	gf.FileFormat, _ = platform.GuessMimeType(gf.IngestLocalPath)  // on err, defaults to application/binary
	return
}

// Adds a file to a tar archive.
func AddToArchive(tarWriter *tar.Writer, filePath, pathWithinArchive string) (error) {
	finfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("Cannot add '%s' to archive: %v", filePath, err)
	}
	header := &tar.Header{
		Name: pathWithinArchive,
		Size: finfo.Size(),
		Mode: int64(finfo.Mode().Perm()),
		ModTime: finfo.ModTime(),
	}

	// This call adds the owner and group info to the tar file header.
	// When running on *nix systems that support this call, we use
	// the definition in nix.go. On Windows, which does not support
	// the call, we use the no-op definition in windows.go.
	platform.GetOwnerAndGroup(finfo, header)

	// Write the header entry
	if err := tarWriter.WriteHeader(header); err != nil {
		return err
	}

	// Open the file whose data we're going to add.
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		return err
	}

	// Copy the contents of the file into the tarWriter.
	bytesWritten, err := io.Copy(tarWriter, file)
	if bytesWritten != header.Size {
		return fmt.Errorf("addToArchive() copied only %d of %d bytes for file %s",
			bytesWritten, header.Size, filePath)
	}
	if err != nil {
		return fmt.Errorf("Error copying %s into tar archive: %v",
			filePath, err)
	}

	return nil
}
