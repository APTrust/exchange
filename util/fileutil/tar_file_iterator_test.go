package fileutil_test

import (
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/stretchr/testify/assert"
	"io"
	"path"
	"path/filepath"
	"runtime"
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

// Should be able to close repeatedly without panic.
func TestTarFileIteratorClose(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	tarFilePath, _ := filepath.Abs(path.Join(filepath.Dir(filename),
		"..", "..", "testdata", "example.edu.tagsample_good.tar"))
	tfi, _ := fileutil.NewTarFileIterator(tarFilePath)
	if tfi == nil {
		assert.Fail(t, "Could not get TarFileIterator")
	}
	assert.NotPanics(t, tfi.Close, "TarFileIterator.Close() freaked out")
	assert.NotPanics(t, tfi.Close, "TarFileIterator.Close() freaked out")
}

func TestRead(t *testing.T) {
	// This is tested above, in the call to reader.Read(buf)
}

// Should be able to close repeatedly without error.
func TestTarReaderCloserClose(t *testing.T) {
	trc := fileutil.TarReadCloser{}
	err := trc.Close()
	assert.Nil(t, err)
	err = trc.Close()
	assert.Nil(t, err)
}
