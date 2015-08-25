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

func TestBagmanHome(t *testing.T) {
	bagmanHome := os.Getenv("BAGMAN_HOME")
	goHome := os.Getenv("GOPATH")
	defer os.Setenv("BAGMAN_HOME", bagmanHome)
	defer os.Setenv("GOPATH", goHome)

	// Should use BAGMAN_HOME, if it's set...
	os.Setenv("BAGMAN_HOME", "/bagman_home")
	bagmanHome, err := fileutil.BagmanHome()
	if err != nil {
		t.Error(err)
	}
	if bagmanHome != "/bagman_home" {
		t.Errorf("BagmanHome returned '%s', expected '%s'",
			bagmanHome,
			"/bagman_home")
	}
	os.Setenv("BAGMAN_HOME", "")

	// Otherwise, should use GOPATH
	os.Setenv("GOPATH", "/go_home")
	bagmanHome, err = fileutil.BagmanHome()
	if err != nil {
		t.Error(err)
	}
	if bagmanHome != "/go_home/src/github.com/APTrust/bagman" {
		t.Errorf("BagmanHome returned '%s', expected '%s'",
			bagmanHome,
			"/go_home")
	}
	os.Setenv("GOPATH", "")

	// Without BAGMAN_HOME and GOPATH, we should get an error
	bagmanHome, err = fileutil.BagmanHome()
	if err == nil {
		t.Error("BagmanHome should have an thrown exception.")
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
	if fileutil.FileExists("file_test.go") == false {
		t.Errorf("FileExists returned false for util_test.go")
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
	bagmanHome, _ := fileutil.BagmanHome()
	testfilePath := filepath.Join(bagmanHome, "testdata")
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
	bagmanHome, _ := fileutil.BagmanHome()
	return filepath.Join(bagmanHome, filename)
}

func TestRecursiveFileList(t *testing.T) {
	bagmanHome, _ := fileutil.BagmanHome()
	files, err := fileutil.RecursiveFileList(bagmanHome)
	if err != nil {
		t.Errorf("RecursiveFileList() returned error: %v", err)
	}
	// Make a map for quick lookup & check for a handful
	// of files at different levels.
	fileMap := make(map[string]string, 0)
	for _, f := range files {
		fileMap[f] = f
	}
	// TODO: This list of files will need to change during the rewrite.
	sampleFiles := []string{
		getPath("README.md"),
		getPath("apps/apt_fixity/apt_fixity.go"),
		getPath("bagman/bucketsummary.go"),
		getPath("config/config.json"),
		getPath("partner-apps/apt_upload/apt_upload.go"),
		getPath("testdata/intel_obj.json"),
		getPath("workers/fixitychecker.go"),
		getPath("testdata/example.edu.sample_good/data/datastream-DC"),
	}
	for _, filePath := range sampleFiles {
		_, present := fileMap[filePath]
		if present == false {
			t.Errorf("File '%s' is missing from recursive file list", filePath)
		}
	}
}

func TestCalculateDigests(t *testing.T) {
	bagmanHome, _ := fileutil.BagmanHome()
	absPath := filepath.Join(bagmanHome, "testdata", "result_good.json")
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
