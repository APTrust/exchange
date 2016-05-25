package fileutil_test

import (
//	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/stretchr/testify/assert"
	"io"
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
	_, filename, _, _ := runtime.Caller(0)
	tarFilePath, _ := filepath.Abs(path.Join(filepath.Dir(filename),
		"..", "..", "testdata", "example.edu.tagsample_good.tar"))
	tfi, err := fileutil.NewTarFileIterator(tarFilePath)
	if tfi != nil {
		defer tfi.Close()
	}
	assert.NotNil(t, tfi)
	assert.Nil(t, err)

	for {
		reader, fileSummary, err := tfi.Next()
		if err == io.EOF {
			break
		}
		if reader == nil {
			assert.Fail(t, "Reader is nil")
		}
		if fileSummary == nil {
			assert.Fail(t, "FileSummary is nil")
		}

		assert.NotEmpty(t, fileSummary.Name)
		assert.Empty(t, fileSummary.AbsPath)
		assert.NotNil(t, fileSummary.Mode)
		if fileSummary.IsRegularFile {
			assert.True(t, fileSummary.Size > int64(0))
		}
		assert.False(t, fileSummary.ModTime.IsZero())

		buf := make([]byte, 1024)
		_, err = reader.Read(buf)
		if err != nil {
			assert.Equal(t, io.EOF, err)
		}
	}
}

func TestTarFileIteratorClose(t *testing.T) {

}

func TestRead(t *testing.T) {

}

func TestTarReaderCloserClose(t *testing.T) {

}
