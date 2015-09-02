package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/crowdmob/goamz/s3"
	"testing"
	"time"
)

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
