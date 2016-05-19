package tarfile_test

import (
	"archive/tar"
//	"fmt"
	"github.com/APTrust/exchange/tarfile"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
)

func TestNewWriter(t *testing.T) {
	dir, err := ioutil.TempDir("", "tarwriter_test")
	if err != nil {
		assert.FailNow(t, "Cannot create temp dir", err.Error())
	}
	tempFilePath := filepath.Join(dir, "test_file.tar")
	defer os.RemoveAll(dir)
	w := tarfile.NewWriter(tempFilePath)
	assert.NotNil(t, w)
	assert.Equal(t, tempFilePath, w.PathToTarFile)
}

func TestAndCloseOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "tarwriter_test")
	if err != nil {
		assert.FailNow(t, "Cannot create temp dir", err.Error())
	}
	tempFilePath := filepath.Join(dir, "test_file.tar")
	defer os.RemoveAll(dir)
	w := tarfile.NewWriter(tempFilePath)
	defer w.Close()
	err = w.Open()
	assert.Nil(t, err)
	if _, err := os.Stat(w.PathToTarFile); os.IsNotExist(err) {
		assert.Fail(t, "Tar file does not exist at %s", w.PathToTarFile)
	}
	err = w.Close()
	assert.Nil(t, err)
}

func TestAddToArchive(t *testing.T) {
	dir, err := ioutil.TempDir("", "tarwriter_test")
	if err != nil {
		assert.FailNow(t, "Cannot create temp dir", err.Error())
	}
	tempFilePath := filepath.Join(dir, "test_file.tar")
	defer os.RemoveAll(dir)
	w := tarfile.NewWriter(tempFilePath)
	defer w.Close()
	err = w.Open()
	assert.Nil(t, err)
	if _, err := os.Stat(w.PathToTarFile); os.IsNotExist(err) {
		assert.Fail(t, "Tar file does not exist at %s", w.PathToTarFile)
	}
	err = w.AddToArchive(pathToTestFile("cleanup_result.json"), "file1.json")
	assert.Nil(t, err)
	err = w.AddToArchive(pathToTestFile("ingest_result.json"), "data/subdir/file2.json")
	assert.Nil(t, err)
	w.Close()

	file, err := os.Open(w.PathToTarFile)
	if file != nil {
		defer file.Close()
	}
	if err != nil {
		assert.FailNow(t, "Could not open tar file", err.Error())
	}
	filesInArchive := make([]string, 0)
	reader := tar.NewReader(file)
	for {
		header, err := reader.Next()
		if err != nil {
			break
		}
		filesInArchive = append(filesInArchive, header.Name)
	}

	assert.Equal(t, "file1.json", filesInArchive[0])
	assert.Equal(t, "data/subdir/file2.json", filesInArchive[1])
}

func TestAddToArchiveWithClosedWriter(t *testing.T) {

}

func TestAddToArchiveWithBadFilePath(t *testing.T) {

}

func pathToTestFile(name string) (string) {
	_, filename, _, _ := runtime.Caller(0)
	testDataPath, _ := filepath.Abs(path.Join(filepath.Dir(filename), "..", "testdata"))
	return path.Join(testDataPath, name)
}
