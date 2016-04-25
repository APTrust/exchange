package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/crowdmob/goamz/s3"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewS3FileWithKey(t *testing.T) {
	key := s3.Key{
		Key: "yadda.yadda",
		Size: 54321,
		ETag: "81726354afdc",
	}
	s3File := models.NewS3FileWithKey("bucket1", key)
	assert.Equal(t, "bucket1", s3File.BucketName)
	assert.Equal(t, "yadda.yadda", s3File.Key.Key)
	assert.EqualValues(t, 54321, s3File.Key.Size)
	assert.Equal(t, "81726354afdc", s3File.Key.ETag)
}

func TestNewS3FileWithName(t *testing.T) {
	s3File := models.NewS3FileWithName("bucket1", "Key Wee")
	assert.Equal(t, "bucket1", s3File.BucketName)
	assert.Equal(t, "Key Wee", s3File.Key.Key)
}


func TestDeleteAttempted(t *testing.T) {
	cf := models.S3File {
		BucketName: "charley",
		Key: s3.Key{ Key: "horse"},
		ErrorMessage: "",
	}
	assert.False(t, cf.DeleteAttempted())
	cf.ErrorMessage = "Oopsie!"
	assert.True(t, cf.DeleteAttempted())
	cf.ErrorMessage = ""
	cf.DeletedAt = time.Now()
	assert.True(t, cf.DeleteAttempted())
}

func testFile() (*models.S3File) {
	return &models.S3File{
		BucketName: "aptrust.receiving.uc.edu",
		Key: s3.Key{
			Key: "cin.675812.tar",
		},
	}
}

func TestS3BagName(t *testing.T) {
	s3File := testFile()
	assert.Equal(t, "uc.edu/cin.675812.tar", s3File.BagName())
}

func TestObjectName(t *testing.T) {
	s3File := testFile()

	// Test with single-part bag
	objname, err := s3File.ObjectName()
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, "uc.edu/cin.675812", objname)

	// Test with multi-part bag
	s3File.Key.Key = "cin.1234.b003.of191.tar"
	objname, err = s3File.ObjectName()
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, "uc.edu/cin.1234", objname)
}

func TestKeyIsComplete(t *testing.T) {
	s3file := models.NewS3FileWithName("buckey-dent", "file-in-a-cake.xml")
	assert.False(t, s3file.KeyIsComplete())
	s3file.Key.Size = 4800
	s3file.Key.ETag = "aec157cfbc1a34d52"
	assert.True(t, s3file.KeyIsComplete())
}
