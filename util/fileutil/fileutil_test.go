package fileutil_test

import (
	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
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
	path := filepath.Join("testdata", "ingest_result.json")
	data, err := fileutil.LoadRelativeFile(path)
	if err != nil {
		t.Error(err)
	}
	if data == nil || len(data) == 0 {
		t.Errorf("Read no data out of file '%s'", path)
	}
}

func TestJsonFileToObject(t *testing.T) {
	fmt.Println("TODO: Rewrite test for JsonFileToObject")
	// relativePath := filepath.Join("testdata", "ingest_result.json")
	// absPath, err := fileutil.RelativeToAbsPath(relativePath)
	// if err != nil {
	// 	t.Errorf("Can't get AbsPath for %s: %v", relativePath, err)
	// }
    // ingestResult := &results.IngestResult{}
    // err = fileutil.JsonFileToObject(absPath, ingestResult)
	// if err != nil {
	// 	t.Errorf("JsonFileToObject returned error %v", err)
	// }
	// // Test one nested item in the struct to see if it parsed OK.
	// if ingestResult.TarResult.LocalFiles[0].Uuid != "b21fdb34-1f79-4101-62c5-56918f4782fc" {
	// 	t.Errorf("JSON parsing didn't get first file UUID.")
	// }
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
		getPath("constants/constants.go"),
		getPath("models/config.go"),
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
