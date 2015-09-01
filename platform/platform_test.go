package platform_test

import (
	"archive/tar"
	"github.com/APTrust/exchange/platform"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
)


func setupMimeTest(t *testing.T) (string) {
	tempfile, err := ioutil.TempFile("", "mime_test")
	if err != nil {
		t.Error(err)
	}
	_, err = io.WriteString(tempfile, "This is a text file.")
	if err != nil {
		t.Error(err)
	}
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
		if mimetype != "text/plain" {
			t.Errorf("Got mime type %s for file %s. Expected text/plain",
				mimetype, pathToTempFile)
		}
	}
}

// GetOwnerAndGroup should fill in the Uid and Gid fields of
// the tar header on Posix systems. On windows, it won't fill in
// anything, but it should not cause any errors.
func TestGetOwnerAndGroup(t *testing.T) {
	pathToTempFile := setupMimeTest(t)
	defer teardownMimeTest(pathToTempFile)
	tempfile, err := os.Open(pathToTempFile)
	if err != nil {
		t.Errorf("Could not open temp file: %v", err)
	}
	finfo, err := tempfile.Stat()
	if err != nil {
		t.Errorf("Could not stat file %s: %v", pathToTempFile, err)
	}
	tarHeader := &tar.Header{}
	platform.GetOwnerAndGroup(finfo, tarHeader)
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" ||
		runtime.GOOS == "unix" || runtime.GOOS == "bsd" {
		if tarHeader.Uid == 0 {
			t.Errorf("GetOwnerAndGroup should have gotten a UID on posix system.")
		}
		if tarHeader.Gid == 0 {
			t.Errorf("GetOwnerAndGroup should have gotten a UID on posix system.")
		}
	}
}
