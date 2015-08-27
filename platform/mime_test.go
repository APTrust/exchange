package platform_test

import (
	"github.com/APTrust/exchange/platform"
	"io"
	"io/ioutil"
	"os"
	"testing"
)


// runtime.GOOS == "windows"

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
// it gets the mime type right.
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
