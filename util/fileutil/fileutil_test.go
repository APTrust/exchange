package fileutil_test

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	_, err = fileutil.ExchangeHome()
	if err == nil {
		t.Error("ExchangeHome should have an thrown exception.")
	}
}

func TestLoadRelativeFile(t *testing.T) {
	path := filepath.Join("testdata", "json_objects", "ingest_result.json")
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
	//	t.Errorf("Can't get AbsPath for %s: %v", relativePath, err)
	// }
	// ingestResult := &results.IngestResult{}
	// err = fileutil.JsonFileToObject(absPath, ingestResult)
	// if err != nil {
	//	t.Errorf("JsonFileToObject returned error %v", err)
	// }
	// // Test one nested item in the struct to see if it parsed OK.
	// if ingestResult.TarResult.LocalFiles[0].Uuid != "b21fdb34-1f79-4101-62c5-56918f4782fc" {
	//	t.Errorf("JSON parsing didn't get first file UUID.")
	// }
}

func TestRelativeToAbsPath(t *testing.T) {
	// If path is already absolute, it should come back unchanged
	relPath, err := fileutil.RelativeToAbsPath("/usr/local/config/test.json")
	if err != nil {
		t.Errorf("RelativeToAbsPath() returned unexpected error %v", err)
	} else if relPath != "/usr/local/config/test.json" {
		t.Errorf("RelativeToAbsPath() altered a path that was already absolute")
	}

	// Otherwise, we should get an absolute path that assumes
	// relPath is relative to ExchangeHome.
	exHome, err := fileutil.ExchangeHome()
	if err == nil {
		relPath, err := fileutil.RelativeToAbsPath("config/test.json")
		if err != nil {
			t.Errorf("RelativeToAbsPath() returned unexpected error %v", err)
			return
		}
		expected := filepath.Join(exHome, "config/test.json")
		if relPath != expected {
			t.Errorf("Expected path %s, got %s", expected, relPath)
		}
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

func getPath(filename string) string {
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
		getPath("testdata/json_objects/intel_obj.json"),
		getPath("util/logger/logger.go"),
	}
	for _, filePath := range sampleFiles {
		_, present := fileMap[filePath]
		if present == false {
			t.Errorf("File '%s' is missing from recursive file list", filePath)
		}
	}
}

func TestLooksSafeToDelete(t *testing.T) {
	assert.True(t, fileutil.LooksSafeToDelete("/mnt/apt/data/some_dir", 15, 3))
	assert.False(t, fileutil.LooksSafeToDelete("/usr/local", 12, 3))
}

func TestGetChecksum(t *testing.T) {
	filePath, _ := fileutil.RelativeToAbsPath("testdata/unit_test_bags/example.edu.sample_good.tar")
	md5, err := fileutil.CalculateChecksum(filePath, constants.AlgMd5)
	require.Nil(t, err)
	assert.Equal(t, "05e68e69767c772d36bd8a2baf693428", md5)

	sha256, err := fileutil.CalculateChecksum(filePath, constants.AlgSha256)
	require.Nil(t, err)
	assert.Equal(t, "24f4ea194115efa3e8a9bd229cbfa7ac23ded35917af6bd2ec24ffcb1a067f55", sha256)

	_, err = fileutil.CalculateChecksum(filePath, "fake_algorithm")
	require.NotNil(t, err)

	_, err = fileutil.CalculateChecksum("file/does/not/exist", constants.AlgMd5)
	require.NotNil(t, err)
}
