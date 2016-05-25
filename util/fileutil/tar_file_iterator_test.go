package fileutil_test

import (
//	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/stretchr/testify/assert"
//	"io"
//	"os"
	"path"
	"path/filepath"
	"runtime"
//	"strings"
	"testing"
)

func TestNewTarFileIterator(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	tarFilePath, _ := filepath.Abs(path.Join(filepath.Dir(filename),
		"..", "..", "testdata", "example.edu.tagsample_good.tar"))
	tfi, err := fileutil.NewTarFileIterator(tarFilePath)
	assert.NotNil(t, tfi)
	assert.Nil(t, err)
}

func TestNext(t *testing.T) {

}

func TestTarFileIteratorClose(t *testing.T) {

}

func TestRead(t *testing.T) {

}

func TestTarReaderCloserClose(t *testing.T) {

}
