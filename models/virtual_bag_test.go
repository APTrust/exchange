package models_test

import (
	//"encoding/json"
	//"fmt"
	// "github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	// "os"
	// "path"
	"path/filepath"
	"runtime"
	"testing"
	// "time"
)

func TestNewVirtualBag(t *testing.T) {
	tarFilePath := vbagGetPath("example.edu.tagsample_good.tar")
	vbag := models.NewVirtualBag(tarFilePath, nil, false, false)
	assert.NotNil(t, vbag)
}

func TestVirtualBagRead_FromDirectory(t *testing.T) {

}

// ----------------------------------------------------------------
// TODO: Update example.edu.tagsample_good.tar or create a new
// version of it to include 1) checksums for all tag files,
// 2) Description tag, 3) Internal-Sender-Identifier tag.
// ----------------------------------------------------------------
func TestVirtualBagRead_FromTarFile(t *testing.T) {
	tarFilePath := vbagGetPath("example.edu.tagsample_good.tar")
	files := []string {"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	vbag := models.NewVirtualBag(tarFilePath, files, true, true)
	assert.NotNil(t, vbag)
	obj, summary := vbag.Read()

	assert.False(t, summary.StartedAt.IsZero())
	assert.False(t, summary.FinishedAt.IsZero())
	assert.Empty(t, summary.Errors)
	assert.Equal(t, 33, len(obj.GenericFiles))
	for _, gf := range obj.GenericFiles {
		assert.NotEmpty(t, gf.Identifier)
		assert.NotEmpty(t, gf.IntellectualObjectIdentifier)
		assert.NotEmpty(t, gf.FileFormat)
		assert.NotEmpty(t, gf.IngestFileType)
		assert.NotEmpty(t, gf.IngestMd5)
		assert.NotEmpty(t, gf.IngestSha256)
		assert.Empty(t, gf.IngestLocalPath)
		assert.Empty(t, gf.IngestStorageURL)
		assert.Empty(t, gf.IngestReplicationURL)
		assert.True(t, gf.Size > 0)
	}
	// Spot check generic file aptrust-info.txt
	gf := obj.GenericFiles[1]
	assert.Equal(t, "example.edu.tagsample_good/aptrust-info.txt", gf.Identifier)
	assert.Equal(t, 0, gf.IntellectualObjectId)
	assert.Equal(t, "example.edu.tagsample_good", gf.IntellectualObjectIdentifier)
	assert.Equal(t, "application/binary", gf.FileFormat)
	assert.Empty(t, gf.URI)
	assert.EqualValues(t, 67, gf.Size)
	assert.False(t, gf.FileModified.IsZero())
	assert.Equal(t, "tag_file", gf.IngestFileType)
	assert.Equal(t, "300e936e622605f9f7a846d261d53093", gf.IngestManifestMd5)
	assert.Equal(t, "300e936e622605f9f7a846d261d53093", gf.IngestMd5)
	assert.False(t, gf.IngestMd5GeneratedAt.IsZero())
	assert.True(t, gf.IngestMd5VerifiedAt.IsZero())
	assert.Equal(t, "a2b6c5a713af771c5e4edde8d5be25fbcad86e45ea338f43a5bb769347e7c8bb", gf.IngestManifestSha256)
	assert.Equal(t, "a2b6c5a713af771c5e4edde8d5be25fbcad86e45ea338f43a5bb769347e7c8bb", gf.IngestSha256)
	assert.False(t, gf.IngestSha256GeneratedAt.IsZero())
	assert.True(t, gf.IngestSha256VerifiedAt.IsZero())
	assert.NotEmpty(t, gf.IngestUUID)
	assert.False(t, gf.IngestUUIDGeneratedAt.IsZero())
	assert.Empty(t, gf.IngestStorageURL)
	assert.True(t, gf.IngestStoredAt.IsZero())
	assert.Empty(t, gf.IngestReplicationURL)
	assert.True(t, gf.IngestReplicatedAt.IsZero())
	assert.False(t, gf.IngestPreviousVersionExists)
	assert.True(t, gf.IngestNeedsSave)
	assert.Empty(t, gf.IngestErrorMessage)
}

func TestVirtualBagRead_ChecksumOptions(t *testing.T) {

}

// With md5 manifest only, sha256 only, and both
func TestVirtualBagRead_ManifestOptions(t *testing.T) {

}

func vbagGetPath(fileName string) (string) {
	_, filename, _, _ := runtime.Caller(0)
	dir, _ := filepath.Abs(filepath.Dir(filename))
	testDataPath := filepath.Join(dir, "..", "testdata", fileName)
	return testDataPath
}
