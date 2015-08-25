package fileutil

import (
	"archive/tar"
	"crypto/md5"
	"crypto/sha256"
	"github.com/APTrust/exchange/platform"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

// BagmanHome returns the absolute path to the bagman root directory,
// which contains source, config and test files. This will usually be
// something like /home/xxx/go/src/github.com/APTrust/bagman. You can
// set this explicitly by defining an environment variable called
// BAGMAN_HOME. Otherwise, this function will try to infer the value
// by appending to the environment variable GOPATH. If neither of
// those variables is set, this returns an error.
func BagmanHome() (bagmanHome string, err error) {
	bagmanHome = os.Getenv("BAGMAN_HOME")
	if bagmanHome == "" {
		goHome := os.Getenv("GOPATH")
		if goHome != "" {
			bagmanHome = filepath.Join(goHome, "src", "github.com", "APTrust", "bagman")
		} else {
			err = fmt.Errorf("Cannot determine bagman home because neither " +
				"BAGMAN_HOME nor GOPATH is set in environment.")
		}
	}
	if bagmanHome != "" {
		bagmanHome, err = filepath.Abs(bagmanHome)
	}
	return bagmanHome, err
}

// LoadRelativeFile reads the file at the specified path
// relative to BAGMAN_HOME and returns the contents as a byte array.
func LoadRelativeFile(relativePath string) ([]byte, error) {
	absPath, err := RelativeToAbsPath(relativePath)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadFile(absPath)
}

// Converts a relative path within the bagman directory tree
// to an absolute path.
func RelativeToAbsPath(relativePath string) (string, error) {
	bagmanHome, err := BagmanHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(bagmanHome, relativePath), nil
}


// Returns true if the file at path exists, false if not.
func FileExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

// Expands the tilde in a directory path to the current
// user's home directory. For example, on Linux, ~/data
// would expand to something like /home/josie/data
func ExpandTilde(filePath string) (string, error) {
	if strings.Index(filePath, "~") < 0 {
		return filePath, nil
	}
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	homeDir := usr.HomeDir + "/"
	expandedDir := strings.Replace(filePath, "~/", homeDir, 1)
	return expandedDir, nil
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

// RecursiveFileList returns a list of all files in path dir
// and its subfolders. It does not return directories.
func RecursiveFileList(dir string) ([]string, error) {
    files := make([]string, 0)
    err := filepath.Walk(dir, func(filePath string, f os.FileInfo, err error) error {
		if f.IsDir() == false {
			files = append(files, filePath)
		}
        return nil
    })
	return files, err
}

type FileDigest struct {
	PathToFile     string
	Md5Digest      string
	Sha256Digest   string
	Size           int64
}

// Returns a FileDigest structure with the md5 and sha256 digests
// of the specified file as hex-enconded strings, along with the
// file's size.
//
// TODO: Rename?
func CalculateDigests(pathToFile string) (*FileDigest, error) {
	md5Hash := md5.New()
	shaHash := sha256.New()
	multiWriter := io.MultiWriter(md5Hash, shaHash)
	reader, err := os.Open(pathToFile)
	defer reader.Close()

	if err != nil {
		detailedError := fmt.Errorf("Error opening file '%s': %v", pathToFile, err)
		return nil, detailedError
	}
	fileInfo, err := reader.Stat()
	if err != nil {
		detailedError := fmt.Errorf("Cannot stat file '%s': %v", pathToFile, err)
		return nil, detailedError
	}
	// Calculate md5 and sha256 checksums in one read
	bytesWritten, err := io.Copy(multiWriter, reader)
	if err != nil {
		detailedError := fmt.Errorf("Error running md5 checksum on file '%s': %v",
			pathToFile, err)
		return nil, detailedError
	}
	if bytesWritten != fileInfo.Size() {
		detailedError := fmt.Errorf("Error running md5 checksum on file '%s': " +
			"read only %d of %d bytes.",
			pathToFile, bytesWritten, fileInfo.Size())
		return nil, detailedError
	}
	fileDigest := &FileDigest{
		PathToFile: pathToFile,
		Md5Digest: fmt.Sprintf("%x", md5Hash.Sum(nil)),
		Sha256Digest: fmt.Sprintf("%x", shaHash.Sum(nil)),
		Size: fileInfo.Size(),
	}
	return fileDigest, nil
}
