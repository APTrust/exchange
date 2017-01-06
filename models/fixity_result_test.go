package models_test

import (
	"github.com/APTrust/exchange/dpn/util/testutil"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var md5sum = "1234567890"
var sha256sum = "fedcba9876543210"

func getGenericFile() *models.GenericFile {
	checksums := make([]*models.Checksum, 2)
	checksums[0] = &models.Checksum{
		Algorithm: "md5",
		DateTime:  time.Date(2014, 11, 11, 12, 0, 0, 0, time.UTC),
		Digest:    md5sum,
	}
	checksums[1] = &models.Checksum{
		Algorithm: "sha256",
		DateTime:  time.Date(2014, 11, 11, 12, 0, 0, 0, time.UTC),
		Digest:    sha256sum,
	}
	return &models.GenericFile{
		URI:       "https://s3.amazonaws.com/aptrust.preservation.storage/52a928da-89ef-48c6-4627-826d1858349b",
		Checksums: checksums,
	}
}

func TestBucketAndKey(t *testing.T) {
	result := models.NewFixityResult(testutil.MakeNsqMessage("999"))
	result.GenericFile = getGenericFile()
	bucket, key, err := result.BucketAndKey()
	if err != nil {
		t.Errorf("BucketAndKey() returned error: %v", err)
		return
	}
	assert.Equal(t, "aptrust.preservation.storage", bucket)
	assert.Equal(t, "52a928da-89ef-48c6-4627-826d1858349b", key)
}

func TestBucketAndKeyWithBadUri(t *testing.T) {
	result := models.NewFixityResult(testutil.MakeNsqMessage("999"))
	result.GenericFile = getGenericFile()
	result.GenericFile.URI = "http://example.com"
	_, _, err := result.BucketAndKey()
	if err == nil {
		t.Errorf("BucketAndKey() should have returned an error for invalid URI")
		return
	}
}

func TestBucketAndKeyWithNilFile(t *testing.T) {
	result := models.NewFixityResult(testutil.MakeNsqMessage("999"))
	_, _, err := result.BucketAndKey()
	if err == nil {
		t.Errorf("BucketAndKey() should have returned an error for missing GenericFile")
		return
	}
}

func TestPharosSha256(t *testing.T) {
	result := models.NewFixityResult(testutil.MakeNsqMessage("999"))
	result.GenericFile = getGenericFile()
	if result.PharosSha256() != sha256sum {
		t.Errorf("FedoraSha256() should have returned", sha256sum)
	}
}
