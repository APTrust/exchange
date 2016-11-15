package fileutil

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

// ExchangeHome returns the absolute path to the exchange root directory,
// which contains source, config and test files. This will usually be
// something like /home/xxx/go/src/github.com/APTrust/exchange. You can
// set this explicitly by defining an environment variable called
// EXCHANGE_HOME. Otherwise, this function will try to infer the value
// by appending to the environment variable GOPATH. If neither of
// those variables is set, this returns an error.
func ExchangeHome() (exchangeHome string, err error) {
	exchangeHome = os.Getenv("EXCHANGE_HOME")
	if exchangeHome == "" {
		goHome := os.Getenv("GOPATH")
		if goHome != "" {
			exchangeHome = filepath.Join(goHome, "src", "github.com", "APTrust", "exchange")
		} else {
			err = fmt.Errorf("Cannot determine exchange home because neither " +
				"EXCHANGE_HOME nor GOPATH is set in environment.")
		}
	}
	if exchangeHome != "" {
		exchangeHome, err = filepath.Abs(exchangeHome)
	}
	return exchangeHome, err
}

// LoadRelativeFile reads the file at the specified path
// relative to EXCHANGE_HOME and returns the contents as a byte array.
func LoadRelativeFile(relativePath string) ([]byte, error) {
	absPath, err := RelativeToAbsPath(relativePath)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadFile(absPath)
}

// Reads data from the file at absPath (an absolute path)
// and coverts it to an object of whatever type param obj
// is. Returns an error if there's a problem reading the
// file or unmarshalling the data into the type you passed in.
// On success, this returns nil and your object will contain
// the data from the file.
func JsonFileToObject(absPath string, obj interface{}) error {
	data, err := ioutil.ReadFile(absPath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, obj)
	if err != nil {
		return err
	}
	return nil
}

// Converts a relative path within the exchange directory tree
// to an absolute path.
func RelativeToAbsPath(relativePath string) (string, error) {
	absPath, _ := filepath.Abs(relativePath)
	if absPath == relativePath {
		return relativePath, nil // it already is absolute
	}
	exchangeHome, err := ExchangeHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(exchangeHome, relativePath), nil
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

// RecursiveFileList returns a list of all files in path dir
// and its subfolders. It does not return directories.
func RecursiveFileList(dir string) ([]string, error) {
	files := make([]string, 0)
	err := filepath.Walk(dir, func(filePath string, f os.FileInfo, err error) error {
		if f != nil && f.IsDir() == false {
			files = append(files, filePath)
		}
		return nil
	})
	return files, err
}

// Returns true if the path specified by dir has at least minLength
// characters and at least minSeparators path separators. This is
// for testing paths you want pass into os.RemoveAll(), so you don't
// wind up deleting "/" or "/etc" or something catastrophic like that.
func LooksSafeToDelete(dir string, minLength, minSeparators int) bool {
	separator := string(os.PathSeparator)
	separatorCount := (len(dir) - len(strings.Replace(dir, separator, "", -1)))
	return len(dir) >= minLength && separatorCount >= minSeparators
}
