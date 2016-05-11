package tarfile_test

import (
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Return a manifest pointing to our test tar file.
func getManifest(tarFileName string) (*models.IngestManifest) {
	_, filename, _, _ := runtime.Caller(1)
	tarFilePath, _ := filepath.Abs(path.Join(filepath.Dir(filename), "..", "testdata", tarFileName))
	objIdentifier := strings.Replace(tarFileName, ".tar", "", 1)
	parts := strings.Split(objIdentifier, ".")
	institution := fmt.Sprintf("%s.%s", parts[0], parts[1])
	bagName := strings.Replace(objIdentifier, institution + ".", "", 1)

	manifest := models.NewIngestManifest()
	manifest.Object.Identifier = objIdentifier
	manifest.Object.Institution = institution
	manifest.Object.BagName = bagName
	manifest.Object.IngestTarFilePath = tarFilePath

	return manifest
}

func TestNewReader(t *testing.T) {
	m := getManifest("virginia.edu.uva-lib_2278801.tar")
	if m.Object == nil {
		assert.Fail(t, "Manifest object should not be nil")
	}
	assert.Equal(t, "virginia.edu.uva-lib_2278801", m.Object.Identifier)
	assert.Equal(t, "virginia.edu", m.Object.Institution)
	assert.Equal(t, "uva-lib_2278801", m.Object.BagName)
	assert.True(t, strings.HasPrefix(m.Object.IngestTarFilePath, "/"))
	assert.True(t, strings.HasSuffix(m.Object.IngestTarFilePath, "testdata/virginia.edu.uva-lib_2278801.tar"))
}

func TestRecordStartOfWork(t *testing.T) {

}

func TestManifestInfoIsValid(t *testing.T) {

}

func TestCreateAndSaveGenericFile(t *testing.T) {

}

func TestSaveFile(t *testing.T) {

}

func GetTopLevelDir(t *testing.T) {

}

func TestGetFileName(t *testing.T) {

}

func TestSaveWithChecksums(t *testing.T) {

}

func TestUntar(t *testing.T) {

}
