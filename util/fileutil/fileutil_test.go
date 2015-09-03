package fileutil_test

import (
	"archive/tar"
	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExchangeHome(t *testing.T) {
	exchangeHome := os.Getenv("EXCHANGE_HOME")
	goHome := os.Getenv("GOPATH")
	defer os.Setenv("EXCHANGE_HOME", exchangeHome)
	defer os.Setenv("GOPATH", goHome)

	// Should use EXCHANGE_HOME, if it's set...
	os.Setenv("EXCHANGE_HOME", "/bagman_home")
	exchangeHome, err := fileutil.ExchangeHome()
	if err != nil {
		t.Error(err)
	}
	if exchangeHome != "/bagman_home" {
		t.Errorf("ExchangeHome returned '%s', expected '%s'",
			exchangeHome,
			"/bagman_home")
	}
	os.Setenv("EXCHANGE_HOME", "")

	// Otherwise, should use GOPATH
	os.Setenv("GOPATH", "/go_home")
	exchangeHome, err = fileutil.ExchangeHome()
	if err != nil {
		t.Error(err)
	}
	if exchangeHome != "/go_home/src/github.com/APTrust/exchange" {
		t.Errorf("ExchangeHome returned '%s', expected '%s'",
			exchangeHome,
			"/go_home")
	}
	os.Setenv("GOPATH", "")

	// Without EXCHANGE_HOME and GOPATH, we should get an error
	exchangeHome, err = fileutil.ExchangeHome()
	if err == nil {
		t.Error("ExchangeHome should have an thrown exception.")
	}
}

func TestLoadRelativeFile(t *testing.T) {
	path := filepath.Join("testdata", "result_good.json")
	data, err := fileutil.LoadRelativeFile(path)
	if err != nil {
		t.Error(err)
	}
	if data == nil || len(data) == 0 {
		t.Errorf("Read no data out of file '%s'", path)
	}
}

func TestFileExists(t *testing.T) {
	if fileutil.FileExists("fileutil_test.go") == false {
		t.Errorf("FileExists returned false for fileutil_test.go")
	}
	if fileutil.FileExists("NonExistentFile.xyz") == true {
		t.Errorf("FileExists returned true for NonExistentFile.xyz")
	}
}

func TestExpandTilde(t *testing.T) {
	expanded, err := fileutil.ExpandTilde("~/tmp")
	if err != nil {
		t.Error(err)
	}
	// Testing this cross-platform is pain. Different home dirs
	// on Windows, Linux, Mac. Different separators ("/" vs "\").
	if len(expanded) <= 5 || !strings.HasSuffix(expanded, "tmp") {
		t.Errorf("~/tmp expanded to unexpected value %s", expanded)
	}

	expanded, err = fileutil.ExpandTilde("/nothing/to/expand")
	if err != nil {
		t.Error(err)
	}
	if expanded != "/nothing/to/expand" {
		t.Errorf("/nothing/to/expand expanded to unexpected value %s", expanded)
	}
}

func TestAddToArchive(t *testing.T) {
	tarFile, err := ioutil.TempFile("", "util_test.tar")
	if err != nil {
		t.Errorf("Error creating temp file for tar archive: %v", err)
	}
	defer os.Remove(tarFile.Name())
	tarWriter := tar.NewWriter(tarFile)
	exchangeHome, _ := fileutil.ExchangeHome()
	testfilePath := filepath.Join(exchangeHome, "testdata")
	files, _ := filepath.Glob(filepath.Join(testfilePath, "*.json"))
	for _, filePath := range files {
		pathWithinArchive := fmt.Sprintf("data/%s", filePath)
		err = fileutil.AddToArchive(tarWriter, filePath, pathWithinArchive)
		if err != nil {
			t.Errorf("Error adding %s to tar file: %v", filePath, err)
		}
	}
}

func getPath(filename string) (string) {
	exchangeHome, _ := fileutil.ExchangeHome()
	return filepath.Join(exchangeHome, filename)
}

func TestRecursiveFileList(t *testing.T) {
	exchangeHome, _ := fileutil.ExchangeHome()
	files, err := fileutil.RecursiveFileList(exchangeHome)
	if err != nil {
		t.Errorf("RecursiveFileList() returned error: %v", err)
	}
	// Make a map for quick lookup & check for a handful
	// of files at different levels.
	fileMap := make(map[string]string, 0)
	for _, f := range files {
		fileMap[f] = f
	}
	sampleFiles := []string{
		getPath("README.md"),
		getPath("config/config.go"),
		getPath("constants/constants.go"),
		getPath("config/config.go"),
		getPath("models/generic_file.go"),
		getPath("testdata/intel_obj.json"),
		getPath("util/logger/logger.go"),
	}
	for _, filePath := range sampleFiles {
		_, present := fileMap[filePath]
		if present == false {
			t.Errorf("File '%s' is missing from recursive file list", filePath)
		}
	}
}

func TestCalculateDigests(t *testing.T) {
	exchangeHome, _ := fileutil.ExchangeHome()
	absPath := filepath.Join(exchangeHome, "testdata", "result_good.json")
	fileDigest, err := fileutil.CalculateDigests(absPath)
	if err != nil {
		t.Errorf("CalculateDigests returned unexpected error: %v", err)
	}
	expectedMd5 := "9cd263b67bad7ae264fda8987fd221e7"
	if fileDigest.Md5Digest != expectedMd5 {
		t.Errorf("Expected digest '%s', got '%s'", expectedMd5, fileDigest.Md5Digest)
	}
	expectedSha := "3c04086d429b4dcba91891dad54759a465869d381f180908203a73b9e3120a87"
	if fileDigest.Sha256Digest != expectedSha {
		t.Errorf("Expected digest '%s', got '%s'", expectedSha, fileDigest.Sha256Digest)
	}
	if fileDigest.Size != 7718 {
		t.Errorf("Expected file size 7718, got %d", fileDigest.Size)
	}
}
