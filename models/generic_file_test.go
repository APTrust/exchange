package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"path/filepath"
	"testing"
	"time"
)

func TestBagName(t *testing.T) {
	genericFile := models.GenericFile{}
	genericFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	bagname, err := genericFile.BagName()
	if err != nil {
		t.Error(err)
	}
	if bagname != "cin.675812" {
		t.Errorf("BagName returned '%s'; expected 'cin.675812'", bagname)
	}
}

func TestInstitutionId(t *testing.T) {
	genericFile := models.GenericFile{}
	genericFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	instId, err := genericFile.InstitutionId()
	if err != nil {
		t.Errorf(err.Error())
	}
	if instId != "uc.edu" {
		t.Errorf("BagName returned '%s'; expected 'uc.edu'", instId)
	}
}

func TestOriginalPath(t *testing.T) {
	genericFile := models.GenericFile{}
	genericFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	origPath, err := genericFile.OriginalPath()
	if err != nil {
		t.Errorf(err.Error())
	}
	if origPath != "data/object.properties" {
		t.Errorf("OriginalPath returned some kinda shizzle. Expected 'data/object.properties', got '%s'",
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

func TestTotalFileSize(t *testing.T) {
	filepath := filepath.Join("testdata", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	if obj.TotalFileSize() != 686 {
		t.Errorf("TotalFileSize() returned '%d', expected 686", obj.TotalFileSize())
	}
}

func TestGenericFilesToFluctusMap(t *testing.T) {
	filepath := filepath.Join("testdata", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	gfMap := obj.GenericFiles[0].ToMapForBulkSave()
	if gfMap["identifier"] != "uc.edu/cin.675812/data/object.properties" {
		t.Errorf("identifier expected %s, got %s", "uc.edu/cin.675812/data/object.properties", gfMap["identifier"])
	}
	if gfMap["file_format"] != "text/plain" {
		t.Errorf("file_format expected %s, got %s", "text/plain", gfMap["file_format"])
	}
	if gfMap["uri"] != "https://s3.amazonaws.com/aptrust.test.fixtures/restore_test/data/object.properties" {
		t.Errorf("uri expected %s, got %s", "https://s3.amazonaws.com/aptrust.test.fixtures/restore_test/data/object.properties", gfMap["uri"])
	}
	if gfMap["size"] != int64(80) {
		t.Errorf("size expected %d, got %d", 80, gfMap["size"])
	}

	expectedTime := "2014-04-25T18:05:51-05:00"
	created := gfMap["created"].(time.Time).Format(time.RFC3339)
	if created != expectedTime {
		t.Errorf("created expected %v, got %v", expectedTime, created)
	}
	modified := gfMap["modified"].(time.Time).Format(time.RFC3339)
	if modified != expectedTime {
		t.Errorf("modified expected %v, got %v", expectedTime, modified)
	}

	if len(gfMap["checksum"].([]*models.ChecksumAttribute)) != 2 {
		t.Errorf("expected 2 checksums, found only %d", len(gfMap["checksum"].([]*models.ChecksumAttribute)))
	}
	if len(gfMap["premisEvents"].([]*models.PremisEvent)) != 10 {
		t.Errorf("expected 10 premis events, found only %d", len(gfMap["premisEvents"].([]*models.PremisEvent)))
	}
}

func TestGenericFilesToMaps(t *testing.T) {
	filepath := filepath.Join("testdata", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	gfMaps := obj.GenericFilesToBulkSaveMaps(-1)
	if len(gfMaps) != 2 {
		t.Errorf("Error converting generic files to maps: %v", err)
	}
	for _, gfMap := range gfMaps {
		if len(gfMap["checksum"].([]*models.ChecksumAttribute)) != 2 {
			t.Errorf("GenericFile should have 2 checksum attributes, found %d",
				len(gfMap["checksum"].([]*models.ChecksumAttribute)))
		}
		if len(gfMap["premisEvents"].([]*models.PremisEvent)) != 10 {
			t.Errorf("GenericFile should have 10 premis events, found %d",
				len(gfMap["premisEvents"].([]*models.PremisEvent)))
		}
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
