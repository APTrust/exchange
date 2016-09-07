package testhelper

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"
	"runtime"
)

func VbagGetPath(fileName string) (string) {
	_, filename, _, _ := runtime.Caller(0)
	dir, _ := filepath.Abs(filepath.Dir(filename))
	testDataPath := filepath.Join(dir, "..", "testdata", "unit_test_bags", fileName)
	return testDataPath
}

// Assumes nameOfTarFile is a file name with no path that ends in .tar.
// E.g. "example.edu.tagsample_good.tar"
// Also assumes name of untarred bag will match name of tar file,
// which is true for our APTrust requirements and our test data.
// Neither of these assumptions are safe in production. This is a
// convenience method for testing only.
func UntarTestBag(nameOfTarFile string) (tempDir string, bagPath string, err error) {
	tarFilePath := VbagGetPath(nameOfTarFile)
	tempDir, err = ioutil.TempDir("", "test")
	if err != nil {
		return "", "", fmt.Errorf("Cannot create temp dir: %v", err)
	}
	cmd := exec.Command("tar", "xf", tarFilePath, "--directory", tempDir)
	err = cmd.Run()
	if err != nil {
		return "", "", fmt.Errorf("Cannot untar test bag into temp dir: %v", err)
	}
	nameOfOutputDir := nameOfTarFile
	index := strings.LastIndex(nameOfTarFile, ".tar")
	if index > -1 {
		nameOfOutputDir = nameOfTarFile[0:index]
	}
	pathToUntarredBag := filepath.Join(tempDir, nameOfOutputDir)
	return tempDir, pathToUntarredBag, nil
}
