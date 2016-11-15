package tarfile

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
	"path/filepath"
	"strings"
	"time"
)

type Reader struct {
	Manifest  *models.IngestManifest
	tarReader *tar.Reader
}

func NewReader(manifest *models.IngestManifest) *Reader {
	return &Reader{
		Manifest: manifest,
	}
}

// This is the only method you should be calling.
func (reader *Reader) Untar() {
	reader.recordStartOfWork()
	if !reader.manifestInfoIsValid() {
		reader.Manifest.UntarResult.Finish()
		return
	}

	// Note the tar file's parent directory
	tarFileDir := filepath.Dir(reader.Manifest.Object.IngestTarFilePath)

	// Open the tar file for reading.
	file, err := os.Open(reader.Manifest.Object.IngestTarFilePath)
	if file != nil {
		defer file.Close()
	}
	if err != nil {
		reader.Manifest.UntarResult.AddError(
			"Could not open file %s for untarring: %v",
			reader.Manifest.Object.IngestTarFilePath, err)
		reader.Manifest.UntarResult.Finish()
		return
	}

	// Untar the file and record the results.
	reader.tarReader = tar.NewReader(file)

	for {
		header, err := reader.tarReader.Next()
		if err != nil && err.Error() == "EOF" {
			break // end of archive
		}
		if err != nil {
			reader.Manifest.UntarResult.AddError(
				"Error reading tar file header: %v. "+
					"Either this is not a tar file, or the file is corrupt.",
				err)
			reader.Manifest.UntarResult.Finish()
			return
		}

		// Top-level dir will be the first header entry.
		if reader.Manifest.Object.IngestUntarredPath == "" {
			topLevelDir, err := reader.getTopLevelDir(header.Name)
			if err != nil {
				reader.Manifest.UntarResult.AddError(err.Error())
				reader.Manifest.UntarResult.Finish()
				return
			}
			reader.Manifest.Object.IngestUntarredPath = filepath.Join(tarFileDir, topLevelDir)
		}

		// Get the output path for this file -> Where should we untar it to?
		outputPath := filepath.Join(reader.Manifest.Object.IngestUntarredPath, header.Name)

		// Make sure the directory that we're about to write into exists.
		err = os.MkdirAll(filepath.Dir(outputPath), 0755)
		if err != nil {
			reader.Manifest.UntarResult.AddError("Could not create destination file '%s' "+
				"while unpacking tar archive: %v", outputPath, err)
			return
		}

		// Copy the file, if it's an actual file. Otherwise, ignore it and record
		// a warning. The bag library does not deal with items like symlinks.
		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
			fileName, err := getFileName(header.Name)
			if err != nil {
				reader.Manifest.UntarResult.AddError(err.Error())
				reader.Manifest.UntarResult.Finish()
				return
			}
			if util.HasSavableName(fileName) {
				gf := reader.createAndSaveGenericFile(fileName, header)
				if gf.IngestErrorMessage != "" {
					reader.Manifest.UntarResult.AddError(gf.IngestErrorMessage)
					reader.Manifest.UntarResult.Finish()
					return
				}
			} else {
				// This is probably something like bagit.txt or a manifest,
				// which we must save to disk but won't need to preserve in
				// long-term storage
				reader.Manifest.Object.IngestFilesIgnored = append(
					reader.Manifest.Object.IngestFilesIgnored, outputPath)
				err = reader.saveFile(outputPath)
				if err != nil {
					reader.Manifest.UntarResult.AddError(
						"Error copying file from tar archive to '%s': %v",
						outputPath, err)
					reader.Manifest.UntarResult.Finish()
					return
				}
			}
		} else if header.Typeflag != tar.TypeDir {
			// Header item is neither file nor directory.
			// Do nothing, but record that we saw this item.
			reader.Manifest.Object.IngestFilesIgnored = append(
				reader.Manifest.Object.IngestFilesIgnored,
				header.Name)
		}
	}
	reader.Manifest.UntarResult.Finish()
}

// Record that we're starting on this.
func (reader *Reader) recordStartOfWork() {
	reader.Manifest.UntarResult.Attempted = true
	reader.Manifest.UntarResult.AttemptNumber += 1
	reader.Manifest.UntarResult.FinishedAt = time.Time{}
	reader.Manifest.UntarResult.Start()
}

// Make sure the manifest has enough information
// for us to get started.
func (reader *Reader) manifestInfoIsValid() bool {
	if reader.Manifest.Object == nil {
		reader.Manifest.UntarResult.AddError("IntellectualObject is missing from manifest.")
		return false
	}
	if reader.Manifest.Object.Identifier == "" {
		reader.Manifest.UntarResult.AddError("IntellectualObject has no Identifier.")
	}
	if reader.Manifest.Object.BagName == "" {
		reader.Manifest.UntarResult.AddError("IntellectualObject has no BagName.")
	}
	if reader.Manifest.Object.Institution == "" {
		reader.Manifest.UntarResult.AddError("IntellectualObject has no Institution.")
	}
	tarFilePath := reader.Manifest.Object.IngestTarFilePath
	if tarFilePath == "" {
		reader.Manifest.UntarResult.AddError("IntellectualObject is missing IngestTarFilePath.")
	} else if absPath, _ := filepath.Abs(tarFilePath); absPath != tarFilePath {
		reader.Manifest.UntarResult.AddError("IntellectualObject IngestTarFilePath '%s' does not exist.", tarFilePath)
	}
	if fileStat, err := os.Stat(tarFilePath); os.IsNotExist(err) {
		reader.Manifest.UntarResult.AddError("IngestTarFilePath '%s' does not exist.", tarFilePath)
	} else if fileStat.Mode().IsDir() {
		reader.Manifest.UntarResult.AddError("IngestTarFilePath '%s' is a directory.", tarFilePath)
	}
	return !reader.Manifest.UntarResult.HasErrors()
}

// Saves the file to disk and returns a GenericFile object.
func (reader *Reader) createAndSaveGenericFile(fileName string, header *tar.Header) *models.GenericFile {
	fileDir := filepath.Dir(reader.Manifest.Object.IngestUntarredPath)
	gf := models.NewGenericFile()
	reader.Manifest.Object.GenericFiles = append(reader.Manifest.Object.GenericFiles, gf)
	var err error
	gf.IngestLocalPath, err = filepath.Abs(filepath.Join(fileDir, header.Name))
	if err != nil {
		gf.IngestErrorMessage = fmt.Sprintf("Path error: %v", err)
		reader.Manifest.UntarResult.AddError(gf.IngestErrorMessage)
		return gf
	}
	gf.IntellectualObjectIdentifier = reader.Manifest.Object.Identifier
	gf.Identifier = fmt.Sprintf("%s/%s", reader.Manifest.Object.Identifier, gf.IngestLocalPath)
	gf.FileModified = header.ModTime
	gf.Size = header.Size
	gf.IngestFileUid = header.Uid
	gf.IngestFileGid = header.Gid
	gf.IngestFileUname = header.Uname
	gf.IngestFileGname = header.Gname
	gf.IngestUUID = uuid.NewV4().String()
	gf.IngestUUIDGeneratedAt = time.Now().UTC()
	reader.saveWithChecksums(gf)
	return gf
}

// Saves a file from the tar archive to local disk. This function
// used to save non-data files (manifests, tag files, etc.)
func (reader *Reader) saveFile(destination string) error {
	// TODO: Save with same permissions as file in tar archive
	outputWriter, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY, 0644)
	if outputWriter != nil {
		defer outputWriter.Close()
	}
	if err != nil {
		return err
	}
	_, err = io.Copy(outputWriter, reader.tarReader)
	if err != nil {
		return err
	}
	return nil
}

// Returns the relative path of the top-level directory into which a
// tar file expands. According to APTrust specs, my_bag.tar should
// expand into a directory called my_bag (the dir name should match
// the tar file name, minus the .tar extension). This isn't always the
// case with bags we get from depositors. So this figures out what that
// top-level directory actually is, and lets us know if there's an error.
func (reader *Reader) getTopLevelDir(headerName string) (topLevelDir string, err error) {
	parts := strings.Split(headerName, "/")
	if len(parts) < 1 {
		return "", fmt.Errorf("Top level dir is empty")
	}
	return parts[0], err
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
func (reader *Reader) saveWithChecksums(gf *models.GenericFile) {
	// Set up a MultiWriter to stream data ONCE to file,
	// md5 and sha256. We don't want to process the stream
	// three separate times.
	err := os.MkdirAll(filepath.Dir(gf.IngestLocalPath), 0755)
	if err != nil {
		gf.IngestErrorMessage = err.Error()
		return
	}
	outputWriter, err := os.OpenFile(gf.IngestLocalPath, os.O_CREATE|os.O_WRONLY, 0644)
	if outputWriter != nil {
		defer outputWriter.Close()
	}
	if err != nil {
		gf.IngestErrorMessage = fmt.Sprintf("Error opening %s for writing: %v", gf.IngestLocalPath, err)
		return
	}
	md5Hash := md5.New()
	shaHash := sha256.New()
	multiWriter := io.MultiWriter(md5Hash, shaHash, outputWriter)
	io.Copy(multiWriter, reader.tarReader)
	gf.IngestMd5 = fmt.Sprintf("%x", md5Hash.Sum(nil))
	gf.IngestSha256 = fmt.Sprintf("%x", shaHash.Sum(nil))
	gf.IngestSha256GeneratedAt = time.Now().UTC()
	gf.FileFormat, _ = platform.GuessMimeType(gf.IngestLocalPath) // on err, defaults to application/binary
	return
}
