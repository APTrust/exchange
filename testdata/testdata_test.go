package testdata_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/testdata"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestMakeChecksum(t *testing.T) {
	cs := testdata.MakeChecksum()
	if cs == nil {
		t.Errorf("MakeChecksum() returned nil")
		return
	}
	assert.NotEqual(t, 0, cs.Id)
	assert.NotEqual(t, 0, cs.GenericFileId)
	assert.NotEqual(t, "", cs.Algorithm)
	assert.False(t, cs.DateTime.IsZero())
	assert.NotEqual(t, "", cs.Digest)
}

func TestMakeGenericFile(t *testing.T) {
	objIdentifier := "virginia.edu/some_object"
	gf := testdata.MakeGenericFile(2, 3, objIdentifier)
	if gf == nil {
		t.Errorf("MakeGenericFile() returned nil")
		return
	}
	assert.NotEqual(t, 0, gf.Id)
	assert.True(t, strings.HasPrefix(gf.Identifier, objIdentifier))
	assert.NotEqual(t, 0, gf.IntellectualObjectId)
	assert.Equal(t, objIdentifier, gf.IntellectualObjectIdentifier)
	assert.NotEqual(t, "", gf.FileFormat)
	assert.True(t, strings.HasPrefix(gf.URI, constants.S3UriPrefix))
	assert.NotEqual(t, 0, gf.Size)
	assert.False(t, gf.Created.IsZero())
	assert.False(t, gf.Modified.IsZero())
	assert.Equal(t, 2, len(gf.Checksums))
	for _, cs := range(gf.Checksums) {
		assert.NotNil(t, cs)
	}
	assert.Equal(t, 3, len(gf.PremisEvents))
	for _, event := range(gf.PremisEvents) {
		assert.NotNil(t, event)
	}
	assert.NotEqual(t, "", gf.IngestLocalPath)
	assert.NotEqual(t, "", gf.IngestMd5)
	assert.False(t, gf.IngestMd5VerifiedAt.IsZero())
	assert.NotEqual(t, "", gf.IngestSha256)
	assert.False(t, gf.IngestSha256GeneratedAt.IsZero())
	assert.NotEqual(t, "", gf.IngestUUID)
	assert.False(t, gf.IngestUUIDGeneratedAt.IsZero())
	assert.NotEqual(t, "", gf.IngestStorageURL)
	assert.False(t, gf.IngestStoredAt.IsZero())
	assert.False(t, gf.IngestPreviousVersionExists)
	assert.True(t, gf.IngestNeedsSave)
	assert.Equal(t, "", gf.IngestErrorMessage)
}
