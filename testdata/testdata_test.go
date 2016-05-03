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
	assert.Equal(t, 3, len(gf.Checksums))
	for _, cs := range(gf.Checksums) {
		assert.NotNil(t, cs)
	}
	assert.Equal(t, 2, len(gf.PremisEvents))
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

func TestMakeInstitution(t *testing.T) {
	inst := testdata.MakeInstitution()
	if inst == nil {
		t.Errorf("MakeInstitution() returned nil")
		return
	}
	assert.NotEqual(t, 0, inst.Id)
	assert.NotEqual(t, "", inst.Name)
	assert.NotEqual(t, "", inst.BriefName)
	assert.NotEqual(t, "", inst.Identifier)
}

func TestMakeIntellectualObject(t *testing.T) {
	obj := testdata.MakeIntellectualObject(2,4,6,8)
	if obj == nil {
		t.Errorf("MakeIntellectualObject() returned nil")
		return
	}
	assert.NotEqual(t, 0, obj.Id)
	assert.NotEqual(t, "", obj.Identifier)
	assert.NotEqual(t, "", obj.BagName)
	assert.NotEqual(t, "", obj.Institution)
	assert.NotEqual(t, 0, obj.InstitutionId)
	assert.NotEqual(t, "", obj.Title)
	assert.NotEqual(t, "", obj.Description)
	assert.NotEqual(t, "", obj.Access)
	assert.NotEqual(t, "", obj.AltIdentifier)

	assert.Equal(t, 2, len(obj.GenericFiles))
	for _, gf := range obj.GenericFiles {
		if gf == nil {
			t.Errorf("GenericFile should not be nil")
		} else {
			assert.Equal(t, obj.Identifier, gf.IntellectualObjectIdentifier)
			assert.Equal(t, 4, len(gf.PremisEvents))
			assert.Equal(t, 6, len(gf.Checksums))
		}
	}

	assert.Equal(t, 4, len(obj.PremisEvents))
	for _, event := range obj.PremisEvents {
		assert.NotNil(t, event)
	}

	assert.Equal(t, 8, len(obj.IngestTags))
	for _, tag := range obj.IngestTags {
		assert.NotNil(t, tag)
		if tag != nil {
			assert.NotEqual(t, "", tag.Label)
			assert.NotEqual(t, "", tag.Value)
		}
	}

	assert.NotEqual(t, "", obj.Institution)
	assert.NotEqual(t, "", obj.IngestS3Bucket)
	assert.NotEqual(t, "", obj.IngestS3Key)
	assert.NotEqual(t, "", obj.IngestTarFilePath)
	assert.NotEqual(t, "", obj.IngestUntarredPath)
	assert.NotEqual(t, "", obj.IngestRemoteMd5)
	assert.NotEqual(t, "", obj.IngestLocalMd5)
	assert.False(t, obj.IngestMd5Verified)
	assert.False(t, obj.IngestMd5Verifiable)
	assert.Equal(t, "", obj.IngestErrorMessage)

	if obj.IngestSummary == nil {
		t.Errorf("IngestSummary should not be nil")
		return
	}
	assert.True(t, obj.IngestSummary.Attempted)
	assert.Equal(t, 1, obj.IngestSummary.AttemptNumber)
	assert.Equal(t, 0, len(obj.IngestSummary.Errors))
	assert.False(t, obj.IngestSummary.StartedAt.IsZero())
	assert.False(t, obj.IngestSummary.FinishedAt.IsZero())
	assert.True(t, obj.IngestSummary.Retry)
}

func TestMakePremisEvent(t *testing.T) {
	event := testdata.MakePremisEvent()
	if event == nil {
		t.Errorf("MakePremisEvent() returned nil")
		return
	}
	assert.NotEqual(t, 0, event.Id)
	assert.NotEqual(t, "", event.Identifier)
	assert.NotEqual(t, "", event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.NotEqual(t, "", event.Detail)
	assert.NotEqual(t, "", event.Outcome)
	assert.NotEqual(t, "", event.OutcomeDetail)
	assert.NotEqual(t, "", event.Object)
	assert.NotEqual(t, "", event.Agent)
	assert.NotEqual(t, "", event.OutcomeInformation)
}

func TestMakeS3File(t *testing.T) {
	s3File := testdata.MakeS3File()
	if s3File == nil {
		t.Errorf("MakeS3File() returned nil")
		return
	}
	assert.NotEqual(t, "", s3File.BucketName)
	assert.NotEqual(t, "", s3File.Key.Key)
	assert.NotEqual(t, "", s3File.Key.LastModified)
	assert.NotEqual(t, 0, s3File.Key.Size)
	assert.NotEqual(t, "", s3File.Key.ETag)
	assert.NotEqual(t, "", s3File.Key.StorageClass)
	assert.NotNil(t, s3File.Key.Owner)
	assert.Equal(t, "", s3File.ErrorMessage)
	assert.True(t, s3File.DeletedAt.IsZero())
	assert.False(t, s3File.DeleteSkippedPerConfig)
}

func TestMakeTag(t *testing.T) {
	tag := testdata.MakeTag()
	if tag == nil {
		t.Errorf("MakeTag() returned nil")
		return
	}
	assert.NotEqual(t, "", tag.Label)
	assert.NotEqual(t, "", tag.Value)
}

func TestMakeWorkSummary(t *testing.T) {
	ws := testdata.MakeWorkSummary()
	if ws == nil {
		t.Errorf("MakeWorkSummary() returned nil")
		return
	}
	assert.True(t, ws.Attempted)
	assert.Equal(t, 1, ws.AttemptNumber)
	assert.Equal(t, 0, len(ws.Errors))
	assert.False(t, ws.StartedAt.IsZero())
	assert.False(t, ws.FinishedAt.IsZero())
	assert.True(t, ws.Retry)
}
