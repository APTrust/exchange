package testhelper

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"runtime"
)

func VbagGetPath(fileName string) (string) {
	_, filename, _, _ := runtime.Caller(0)
	dir, _ := filepath.Abs(filepath.Dir(filename))
	testDataPath := filepath.Join(dir, "..", "testdata", fileName)
	return testDataPath
}

func UntarTestBag() (tempDir string, bagPath string, err error) {
	tarFilePath := VbagGetPath("example.edu.tagsample_good.tar")
	tempDir, err = ioutil.TempDir("", "test")
	if err != nil {
		return "", "", fmt.Errorf("Cannot create temp dir: %v", err)
	}
	cmd := exec.Command("tar", "xf", tarFilePath, "--directory", tempDir)
	err = cmd.Run()
	if err != nil {
		return "", "", fmt.Errorf("Cannot untar test bag into temp dir: %v", err)
	}
	pathToUntarredBag := filepath.Join(tempDir, "example.edu.tagsample_good")
	return tempDir, pathToUntarredBag, nil
}
