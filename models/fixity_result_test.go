package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var md5sum = "1234567890"
var sha256sum = "fedcba9876543210"

func getGenericFile() (*models.GenericFile) {
	checksums := make([]*models.Checksum, 2)
	checksums[0] = &models.Checksum{
		Algorithm: "md5",
		DateTime: time.Date(2014,11,11,12,0,0,0,time.UTC),
		Digest: md5sum,
	}
	checksums[1] = &models.Checksum{
		Algorithm: "sha256",
		DateTime: time.Date(2014,11,11,12,0,0,0,time.UTC),
		Digest: sha256sum,
	}
	return &models.GenericFile{
		URI: "https://s3.amazonaws.com/aptrust.preservation.storage/52a928da-89ef-48c6-4627-826d1858349b",
		Checksums: checksums,
	}
}

func TestBucketAndKey(t *testing.T) {
	result := models.NewFixityResult(getGenericFile())
	bucket, key, err := result.BucketAndKey()
	if err != nil {
		t.Errorf("BucketAndKey() returned error: %v", err)
		return
	}
	assert.Equal(t, "aptrust.preservation.storage", bucket)
	assert.Equal(t, "52a928da-89ef-48c6-4627-826d1858349b", key)
}

func TestBucketAndKeyWithBadUri(t *testing.T) {
	result := models.NewFixityResult(getGenericFile())
	result.GenericFile.URI = "http://example.com"
	_, _, err := result.BucketAndKey()
	if err == nil {
		t.Errorf("BucketAndKey() should have returned an error for invalid URI")
		return
	}
	assert.Equal(t, "GenericFile URI 'http://example.com' is invalid", result.WorkSummary.FirstError())
	assert.False(t, result.WorkSummary.Retry, "Retry should be false after fatal error.")
}


func TestSha256Matches(t *testing.T) {
	result := models.NewFixityResult(getGenericFile())
	result.Sha256 = sha256sum
	matches, err := result.Sha256Matches()
	if err != nil {
		t.Error(err)
	}
	assert.True(t, matches)


	result.Sha256 = "some random string"
	matches, err = result.Sha256Matches()
	if err != nil {
		t.Error(err)
	}
	assert.False(t, matches)
}

func TestMissingChecksums(t *testing.T) {
	result := models.NewFixityResult(getGenericFile())
	_, err := result.Sha256Matches()
	assert.NotNil(t, err, "Sha256Matches should have returned a usage error")

	result.Sha256 = sha256sum
	result.GenericFile.Checksums = make([]*models.Checksum, 2)
	_, err = result.Sha256Matches()
	assert.NotNil(t, err, "Sha256Matches should have returned a usage error")

}

func TestGotDigestFromPreservationFile(t *testing.T) {
	result := models.NewFixityResult(getGenericFile())
	assert.False(t, result.GotDigestFromPreservationFile())
	result.Sha256 = sha256sum
	assert.True(t, result.GotDigestFromPreservationFile())
}

func TestGenericFileHasDigest(t *testing.T) {
	result := models.NewFixityResult(getGenericFile())
	assert.True(t, result.GenericFileHasDigest())

	// Make the SHA256 checksum disappear
	for i := range result.GenericFile.Checksums {
		result.GenericFile.Checksums[i].Algorithm = "Md five and a half"
	}
	assert.False(t, result.GenericFileHasDigest())
}

func TestFedoraSha256(t *testing.T) {
	result := models.NewFixityResult(getGenericFile())
	if result.FedoraSha256() != sha256sum {
		t.Errorf("FedoraSha256() should have returned", sha256sum)
	}
}

func TestFixityCheckPossible(t *testing.T) {
	result := models.NewFixityResult(getGenericFile())
	result.Sha256 = sha256sum
	assert.True(t, result.FixityCheckPossible())

	result.Sha256 = ""
	assert.False(t, result.FixityCheckPossible())

	result.Sha256 = sha256sum
	// Make the SHA256 checksum disappear
	for i := range result.GenericFile.Checksums {
		result.GenericFile.Checksums[i].Algorithm = "Md five and a half"
	}
	assert.False(t, result.FixityCheckPossible())
}
