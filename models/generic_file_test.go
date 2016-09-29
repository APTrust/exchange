package models_test

import (
	"encoding/json"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestNewGenericFile(t *testing.T) {
	gf := models.NewGenericFile()
	assert.NotNil(t, gf.Checksums)
	assert.NotNil(t, gf.PremisEvents)
	assert.False(t, gf.IngestPreviousVersionExists)
	assert.True(t, gf.IngestNeedsSave)
}

func TestInstitutionIdentifier(t *testing.T) {
	genericFile := models.GenericFile{}
	genericFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	instId, err := genericFile.InstitutionIdentifier()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	assert.Equal(t, "uc.edu", instId)
}

func TestOriginalPath(t *testing.T) {
	genericFile := models.GenericFile{}
	genericFile.IntellectualObjectIdentifier = "uc.edu/cin.675812"

	// Top-level custom tag file
	genericFile.Identifier = "uc.edu/cin.675812/tagmanifest-sha256.txt"
	origPath := genericFile.OriginalPath()
	assert.Equal(t, "tagmanifest-sha256.txt", origPath)

	// Payload file
	genericFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	origPath = genericFile.OriginalPath()
	assert.Equal(t, "data/object.properties", origPath)

	// Nested custom tag file
	genericFile.Identifier = "uc.edu/cin.675812/custom/tag/dir/special_info.xml"
	origPath = genericFile.OriginalPath()
	assert.Equal(t, "custom/tag/dir/special_info.xml", origPath)
}

func TestGetChecksum(t *testing.T) {
	filename := filepath.Join("testdata", "json_objects", "intel_obj.json")
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
	assert.Equal(t, "c6d8080a39a0622f299750e13aa9c200", md5Checksum.Digest)

	// SHA256
	sha256Checksum := genericFile.GetChecksum("sha256")
	assert.Equal(t, "a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790", sha256Checksum.Digest)

	// bogus checksum
	bogusChecksum := genericFile.GetChecksum("bogus")
	assert.Nil(t, bogusChecksum, "GetChecksum returned something it shouldn't have")
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
		return
	}
	assert.Equal(t, "a58a7c00-392f-11e4-916c-0800200c9a66", fileName)
}

func TestFindEventsByType(t *testing.T) {
	filename := filepath.Join("testdata", "json_objects", "intel_obj.json")
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

func TestSerializeFileForPharos(t *testing.T) {
	filename := filepath.Join("testdata", "json_objects", "intel_obj.json")
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
	assert.Equal(t, "2014-04-25T18:05:51-05:00", hash["file_modified"])
	assert.EqualValues(t, 606, hash["size"])
	assert.Equal(t, "https://s3.amazonaws.com/aptrust.test.fixtures/restore_test/data/metadata.xml", hash["uri"])
	assert.Equal(t, "2014-04-25T18:05:51-05:00", hash["file_created"])
	assert.Equal(t, "uc.edu/cin.675812", hash["intellectual_object_identifier"])
	assert.EqualValues(t, 741, hash["intellectual_object_id"])
	assert.Equal(t, "application/xml", hash["file_format"])
	assert.Equal(t, "uc.edu/cin.675812/data/metadata.xml", hash["identifier"])

	checksums := hash["checksums"].([]interface{})
	checksum0 := checksums[0].(map[string]interface{})
	assert.EqualValues(t, 0, checksum0["id"])
	assert.Equal(t, "md5", checksum0["algorithm"])
	assert.Equal(t, "2014-04-25T18:05:51-05:00", checksum0["datetime"])
	assert.Equal(t, "c6d8080a39a0622f299750e13aa9c200", checksum0["digest"])

	checksum1 := checksums[1].(map[string]interface{})
	assert.EqualValues(t, 0, checksum1["id"])
	assert.Equal(t, "sha256", checksum1["algorithm"])
	assert.Equal(t, "2014-08-12T20:51:20Z", checksum1["datetime"])
	assert.Equal(t, "a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790", checksum1["digest"])
}

func TestBuildIngestEvents(t *testing.T) {
	gf := testutil.MakeGenericFile(0, 0, "test.edu/test_bag/file.txt")
	assert.Equal(t, 0, len(gf.PremisEvents))
	err := gf.BuildIngestEvents()
	assert.Nil(t, err)
	assert.Equal(t, 6, len(gf.PremisEvents))
	assert.Equal(t, 1, len(gf.FindEventsByType(constants.EventFixityCheck)))
	assert.Equal(t, 1, len(gf.FindEventsByType(constants.EventDigestCalculation)))
	assert.Equal(t, 2, len(gf.FindEventsByType(constants.EventIdentifierAssignment)))
	assert.Equal(t, 1, len(gf.FindEventsByType(constants.EventReplication)))
	assert.Equal(t, 1, len(gf.FindEventsByType(constants.EventIngestion)))

	// Calling this function again should not generate new events
	// if all the events are there.
	err = gf.BuildIngestEvents()
	assert.Nil(t, err)
	assert.Equal(t, 6, len(gf.PremisEvents))
}
