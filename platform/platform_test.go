package platform_test

import (
	"archive/tar"
	"github.com/APTrust/exchange/platform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
)

func setupMimeTest(t *testing.T) string {
	tempfile, err := ioutil.TempFile("", "mime_test")
	require.Nil(t, err)
	_, err = io.WriteString(tempfile, "This is a text file.")
	require.Nil(t, err)
	tempfile.Close()
	return tempfile.Name()
}

func teardownMimeTest(pathToTempFile string) {
	os.Remove(pathToTempFile)
}

// For partner build, we just want to make sure this doesn't throw
// an execption. For our own server build, we also want to make sure
// it gets the mime type right. GuessMimeType is defined in mime.go,
// with a dummy version in partner builds in nomime.go.
func TestGuessMimeType(t *testing.T) {
	pathToTempFile := setupMimeTest(t)
	defer teardownMimeTest(pathToTempFile)
	mimetype, err := platform.GuessMimeType(pathToTempFile)
	if err != nil {
		t.Error(err)
	}
	if !platform.IsPartnerBuild {
		assert.Equal(t, "text/plain", mimetype)
	}
}

func TestGuessMimeTypeByBuffer(t *testing.T) {
	pathToTempFile := setupMimeTest(t)
	defer teardownMimeTest(pathToTempFile)
	file, err := os.Open(pathToTempFile)
	require.Nil(t, err)

	defer file.Close()
	buf := make([]byte, 256)
	_, _ = file.Read(buf)
	mimetype, err := platform.GuessMimeTypeByBuffer(buf)
	require.Nil(t, err)
	if !platform.IsPartnerBuild {
		assert.Equal(t, "text/plain", mimetype)
	}
}

// GetOwnerAndGroup should fill in the Uid and Gid fields of
// the tar header on Posix systems. On windows, it won't fill in
// anything, but it should not cause any errors.
func TestGetOwnerAndGroup(t *testing.T) {
	pathToTempFile := setupMimeTest(t)
	defer teardownMimeTest(pathToTempFile)
	tempfile, err := os.Open(pathToTempFile)
	require.Nil(t, err)

	finfo, err := tempfile.Stat()
	require.Nil(t, err)

	tarHeader := &tar.Header{}
	platform.GetOwnerAndGroup(finfo, tarHeader)
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" ||
		runtime.GOOS == "unix" || runtime.GOOS == "bsd" {
		// We just wrote these files, so their uid and gid
		// should match ours.
		assert.EqualValues(t, os.Getuid(), tarHeader.Uid)
		assert.EqualValues(t, os.Getgid(), tarHeader.Gid)
	}
}

func TestGetMountPointFromPath(t *testing.T) {
	tempfile, err := ioutil.TempFile("", "platform_test")
	require.Nil(t, err)
	mountpoint, err := platform.GetMountPointFromPath(tempfile.Name())
	assert.Nil(t, err)
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" ||
		runtime.GOOS == "unix" || runtime.GOOS == "bsd" {
		assert.Equal(t, "/", mountpoint)
	} else if runtime.GOOS == "windows" {
		assert.Equal(t, tempfile.Name(), mountpoint)
	}
}
