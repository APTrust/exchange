package fileutil_test

import (
//	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/stretchr/testify/assert"
//	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNewFileSystemIterator(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testDataPath, _ := filepath.Abs(path.Join(filepath.Dir(filename), "..", "..", "testdata"))
	fsi, err := fileutil.NewFileSystemIterator(testDataPath)
	assert.Nil(t, err)
	assert.NotNil(t, fsi)

	badPath := path.Join(testDataPath, "path", "does", "not", "exist")
	fsi, err = fileutil.NewFileSystemIterator(badPath)
	assert.NotNil(t, err)
	assert.Nil(t, fsi)
	assert.True(t, strings.Contains(err.Error(), "does not exist"))

	badPath = "not/an/absolute/path"
	fsi, err = fileutil.NewFileSystemIterator(badPath)
	assert.NotNil(t, err)
	assert.Nil(t, fsi)
	assert.True(t, strings.Contains(err.Error(), "must be absolute"))

	fsi, err = fileutil.NewFileSystemIterator(filename)
	assert.NotNil(t, err)
	assert.Nil(t, fsi)
	assert.True(t, strings.Contains(err.Error(), "is not a directory"))
}

func TextNext(t *testing.T) {

}
