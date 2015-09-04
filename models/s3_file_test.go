package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/crowdmob/goamz/s3"
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
	if s3File.BucketName != "bucket1" {
		t.Errorf("Expected bucket 'bucket1', got '%s'", s3File.BucketName)
	}
	if s3File.Key.Key != key.Key {
		t.Errorf("Expected key '%s', got '%s'", key.Key, s3File.Key.Key)
	}
	if s3File.Key.Size != key.Size {
		t.Errorf("Expected size %d, got %d", key.Size, s3File.Key.Size)
	}
	if s3File.Key.ETag != key.ETag {
		t.Errorf("Expected etag '%s', got '%s'", key.ETag, s3File.Key.ETag)
	}
}

func TestNewS3FileWithName(t *testing.T) {
	s3File := models.NewS3FileWithName("bucket1", "Key Wee")
	if s3File.BucketName != "bucket1" {
		t.Errorf("Expected bucket 'bucket1', got '%s'", s3File.BucketName)
	}
	if s3File.Key.Key != "Key Wee" {
		t.Errorf("Expected key 'Key Wee', got '%s'", s3File.Key.Key)
	}
}


func TestDeleteAttempted(t *testing.T) {
	cf := models.S3File {
		BucketName: "charley",
		Key: s3.Key{ Key: "horse"},
		ErrorMessage: "",
	}
	if cf.DeleteAttempted() == true {
		t.Errorf("DeleteAttempted() should have returned false")
	}
	cf.ErrorMessage = "Oopsie!"
	if cf.DeleteAttempted() == false {
		t.Errorf("DeleteAttempted() should have returned true")
	}
	cf.ErrorMessage = ""
	cf.DeletedAt = time.Now()
	if cf.DeleteAttempted() == false {
		t.Errorf("DeleteAttempted() should have returned true")
	}
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
	bagname := s3File.BagName()
	if bagname != "uc.edu/cin.675812.tar" {
		t.Errorf("BagName returned '%s'; expected 'uc.edu/cin.675812.tar'", bagname)
	}
}

func TestObjectName(t *testing.T) {
	s3File := testFile()

	// Test with single-part bag
	objname, err := s3File.ObjectName()
	if err != nil {
		t.Error(err)
		return
	}
	if objname != "uc.edu/cin.675812" {
		t.Errorf("BagName returned '%s'; expected 'uc.edu/cin.675812'", objname)
	}

	// Test with multi-part bag
	s3File.Key.Key = "cin.1234.b003.of191.tar"
	objname, err = s3File.ObjectName()
	if err != nil {
		t.Error(err)
		return
	}
	if objname != "uc.edu/cin.1234" {
		t.Errorf("BagName returned '%s'; expected 'uc.edu/cin.1234'", objname)
	}
}

func TestKeyIsComplete(t *testing.T) {
	s3file := models.NewS3FileWithName("buckey-dent", "file-in-a-cake.xml")
	if s3file.KeyIsComplete() {
		t.Errorf("KeyIsComplete should have returned false")
	}
	s3file.Key.Size = 4800
	s3file.Key.ETag = "aec157cfbc1a34d52"
	if !s3file.KeyIsComplete() {
		t.Errorf("KeyIsComplete should have returned true")
	}
}
