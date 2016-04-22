package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"path/filepath"
	"testing"
)

func TestInstitutionIdentifier(t *testing.T) {
	genericFile := models.GenericFile{}
	genericFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	instId, err := genericFile.InstitutionIdentifier()
	if err != nil {
		t.Errorf(err.Error())
	}
	if instId != "uc.edu" {
		t.Errorf("BagName returned '%s'; expected 'uc.edu'", instId)
	}
}

func TestOriginalPath(t *testing.T) {
	genericFile := models.GenericFile{}

	// Top-level custom tag file
	genericFile.Identifier = "uc.edu/cin.675812/tagmanifest-sha256.txt"
	origPath, err := genericFile.OriginalPath()
	if err != nil {
		t.Errorf(err.Error())
	}
	if origPath != "tagmanifest-sha256.txt" {
		t.Errorf("Expected 'tagmanifest-sha256.txt', got '%s'",
			origPath)
	}

	// Payload file
	genericFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	origPath, err = genericFile.OriginalPath()
	if err != nil {
		t.Errorf(err.Error())
	}
	if origPath != "data/object.properties" {
		t.Errorf("Expected 'data/object.properties', got '%s'",
			origPath)
	}

	// Nested custom tag file
	genericFile.Identifier = "uc.edu/cin.675812/custom/tag/dir/special_info.xml"
	origPath, err = genericFile.OriginalPath()
	if err != nil {
		t.Errorf(err.Error())
	}
	if origPath != "custom/tag/dir/special_info.xml" {
		t.Errorf("Expected 'custom/tag/dir/special_info.xml', got '%s'",
			origPath)
	}

}

func TestGetChecksum(t *testing.T) {
	filename := filepath.Join("testdata", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	if intelObj == nil {
		return
	}
	genericFile := intelObj.GenericFiles[1]

	// MD5
	md5Checksum := genericFile.GetChecksum("md5")
	if md5Checksum == nil {
		t.Errorf("GetChecksum did not return md5 sum")
	}
	if md5Checksum.Digest != "c6d8080a39a0622f299750e13aa9c200" {
		t.Errorf("GetChecksum did not return md5 sum")
	}

	// SHA256
	sha256Checksum := genericFile.GetChecksum("sha256")
	if sha256Checksum == nil {
		t.Errorf("GetChecksum did not return sha256 sum")
	}
	if sha256Checksum.Digest != "a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790" {
		t.Errorf("GetChecksum did not return sha256 sum")
	}

	// bogus checksum
	bogusChecksum := genericFile.GetChecksum("bogus")
	if bogusChecksum != nil {
		t.Errorf("GetChecksum returned something it shouldn't have")
	}
}

func TestPreservationStorageFileName(t *testing.T) {
	genericFile := models.GenericFile{}
	genericFile.URI = ""
	fileName, err := genericFile.PreservationStorageFileName()
	if err == nil {
		t.Errorf("PreservationStorageFileName() should have returned an error")
	}
	genericFile.URI = "https://s3.amazonaws.com/aptrust.test.preservation/a58a7c00-392f-11e4-916c-0800200c9a66"
	fileName, err = genericFile.PreservationStorageFileName()
	if err != nil {
		t.Errorf("PreservationStorageFileName() returned an error: %v", err)
	}
	expected := "a58a7c00-392f-11e4-916c-0800200c9a66"
	if fileName != expected {
		t.Errorf("PreservationStorageFileName() returned '%s', expected '%s'",
			fileName, expected)
	}
}

func TestFindEventsByType(t *testing.T) {
	filename := filepath.Join("testdata", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	if intelObj == nil {
		return
	}

	genericFile := intelObj.GenericFiles[1]

	// Typical generic file will have one ingest event,
	// but our fixture data shows multiple ingests.
	if len(genericFile.FindEventsByType("ingest")) != 2 {
		t.Errorf("Should have found 1 ingest event")
	}
	// Typical generic file will have two identifier assignments,
	// but our fixture data shows multiple ingests.
	if len(genericFile.FindEventsByType("identifier_assignment")) != 4 {
		t.Errorf("Should have found 2 identifier assignment events")
	}

}
