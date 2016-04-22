package models_test

import (
	"encoding/json"
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

func TestSerializeForPharos(t *testing.T) {
	filename := filepath.Join("testdata", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	genericFile := intelObj.GenericFiles[1]
	data, err := genericFile.SerializeForPharos()
	if err != nil {
		t.Errorf("Error serializing for Pharos: %v", err)
		return
	}
	hash := make(map[string]interface{})
	err = json.Unmarshal(data, &hash)
	if err != nil {
		t.Errorf("Error unmarshalling data: %v", err)
	}

	// Convert int and int64 to float64, because that's what JSON uses
	assertValue(t, "TestSerializeForPhars", hash, "modified", "2014-04-25T18:05:51-05:00")
	assertValue(t, "TestSerializeForPhars", hash, "size", float64(606))
	assertValue(t, "TestSerializeForPhars", hash, "uri", "https://s3.amazonaws.com/aptrust.test.fixtures/restore_test/data/metadata.xml")
	assertValue(t, "TestSerializeForPhars", hash, "created", "2014-04-25T18:05:51-05:00")
	assertValue(t, "TestSerializeForPhars", hash, "intellectual_object_identifier", "uc.edu/cin.675812")
	assertValue(t, "TestSerializeForPhars", hash, "intellectual_object_id", float64(741))
	assertValue(t, "TestSerializeForPhars", hash, "file_format", "application/xml")
	assertValue(t, "TestSerializeForPhars", hash, "identifier", "uc.edu/cin.675812/data/metadata.xml")

	checksums := hash["checksums"].([]interface{})
	checksum0 := checksums[0].(map[string]interface{})
	assertValue(t, "TestSerializeForPhars Checksum 0", checksum0, "id", float64(0))
	assertValue(t, "TestSerializeForPhars Checksum 0", checksum0, "algorithm", "md5")
	assertValue(t, "TestSerializeForPhars Checksum 0", checksum0, "datetime", "2014-04-25T18:05:51-05:00")
	assertValue(t, "TestSerializeForPhars Checksum 0", checksum0, "digest", "c6d8080a39a0622f299750e13aa9c200")

	checksum1 := checksums[1].(map[string]interface{})
	assertValue(t, "TestSerializeForPhars Checksum 1", checksum1, "id", float64(0))
	assertValue(t, "TestSerializeForPhars Checksum 1", checksum1, "algorithm", "sha256")
	assertValue(t, "TestSerializeForPhars Checksum 1", checksum1, "datetime", "2014-08-12T20:51:20Z")
	assertValue(t, "TestSerializeForPhars Checksum 1", checksum1, "digest", "a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790")

}
