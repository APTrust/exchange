package models_test

import (
	"encoding/json"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestOriginalPathWithBagName(t *testing.T) {
	genericFile := models.GenericFile{}
	genericFile.IntellectualObjectIdentifier = "uc.edu/cin.675812"

	// Top-level custom tag file
	genericFile.Identifier = "uc.edu/cin.675812/tagmanifest-sha256.txt"
	origPath, err := genericFile.OriginalPathWithBagName()
	require.Nil(t, err)
	assert.Equal(t, "cin.675812/tagmanifest-sha256.txt", origPath)

	// Payload file
	genericFile.Identifier = "uc.edu/cin.675812/data/object.properties"
	origPath, err = genericFile.OriginalPathWithBagName()
	require.Nil(t, err)
	assert.Equal(t, "cin.675812/data/object.properties", origPath)

	// Nested custom tag file
	genericFile.Identifier = "uc.edu/cin.675812/custom/tag/dir/special_info.xml"
	origPath, err = genericFile.OriginalPathWithBagName()
	require.Nil(t, err)
	assert.Equal(t, "cin.675812/custom/tag/dir/special_info.xml", origPath)
}

func TestGetChecksumByAlgorithm(t *testing.T) {
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
	md5Checksum := genericFile.GetChecksumByAlgorithm("md5")
	assert.Equal(t, "c6d8080a39a0622f299750e13aa9c200", md5Checksum.Digest)

	// SHA256
	sha256Checksum := genericFile.GetChecksumByAlgorithm("sha256")
	assert.Equal(t, "a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790", sha256Checksum.Digest)

	// bogus checksum
	bogusChecksum := genericFile.GetChecksumByAlgorithm("bogus")
	assert.Nil(t, bogusChecksum, "GetChecksum returned something it shouldn't have")
}

func TestGetChecksumByDigest(t *testing.T) {
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
	md5Checksum := genericFile.GetChecksumByDigest("c6d8080a39a0622f299750e13aa9c200")
	assert.Equal(t, "md5", md5Checksum.Algorithm)

	// SHA256
	sha256Checksum := genericFile.GetChecksumByDigest("a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790")
	assert.Equal(t, "sha256", sha256Checksum.Algorithm)

	// bogus checksum
	bogusChecksum := genericFile.GetChecksumByDigest("bogus")
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

func TestFindEventByIdentifier(t *testing.T) {
	filename := filepath.Join("testdata", "json_objects", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	if intelObj == nil {
		return
	}

	gf := intelObj.GenericFiles[0]
	event1 := gf.FindEventByIdentifier("6c705682-73ed-4609-6c11-30cb0cd1dcd9")
	event2 := gf.FindEventByIdentifier("bc78e9d9-a860-4ef1-5ae9-216151303c6a")
	require.NotNil(t, event1)
	require.NotNil(t, event2)
	assert.Equal(t, "identifier_assignment", event1.EventType)
	assert.Equal(t, "ingest", event2.EventType)
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
	topLevelHash := make(map[string]interface{})
	err = json.Unmarshal(data, &topLevelHash)
	if err != nil {
		t.Errorf("Error unmarshalling data: %v", err)
	}

	require.NotNil(t, topLevelHash["generic_file"])

	hash := topLevelHash["generic_file"].(map[string]interface{})

	// Convert int and int64 to float64, because that's what JSON uses

	// TODO: Add this back when it's part of the Rails model.
	// assert.Equal(t, "2014-04-25T18:05:51-05:00", hash["file_modified"])
	// assert.Equal(t, "2014-04-25T18:05:51-05:00", hash["file_created"])
	assert.EqualValues(t, 606, hash["size"])
	assert.Equal(t, "https://s3.amazonaws.com/aptrust.test.fixtures/restore_test/data/metadata.xml", hash["uri"])
	assert.EqualValues(t, 741, hash["intellectual_object_id"])
	assert.Equal(t, "application/xml", hash["file_format"])
	assert.Equal(t, "uc.edu/cin.675812/data/metadata.xml", hash["identifier"])

	// Note the Rails 4 naming convention
	checksums := hash["checksums_attributes"].([]interface{})
	checksum0 := checksums[0].(map[string]interface{})
	assert.EqualValues(t, nil, checksum0["id"]) // Don't serialize 0 ids. Pharos pukes.
	assert.Equal(t, "md5", checksum0["algorithm"])
	assert.Equal(t, "2014-04-25T18:05:51-05:00", checksum0["datetime"])
	assert.Equal(t, "c6d8080a39a0622f299750e13aa9c200", checksum0["digest"])

	checksum1 := checksums[1].(map[string]interface{})
	assert.EqualValues(t, nil, checksum1["id"]) // Don't serialize 0 ids. Pharos pukes.
	assert.Equal(t, "sha256", checksum1["algorithm"])
	assert.Equal(t, "2014-08-12T20:51:20Z", checksum1["datetime"])
	assert.Equal(t, "a418d61067718141d7254d7376d5499369706e3ade27cb84c4d5519f7cfed790", checksum1["digest"])

	// Note the Rails 4 naming convention
	events := hash["premis_events_attributes"].([]interface{})
	event0 := events[0].(map[string]interface{})
	assert.EqualValues(t, nil, event0["id"]) // Don't serialize 0 ids. Pharos pukes.
	assert.EqualValues(t, 0, event0["intellectual_object_id"])
	assert.Equal(t, "Success", event0["outcome"])
	assert.Equal(t, "http://golang.org/pkg/crypto/md5/", event0["agent"])
	assert.Equal(t, "2014-08-13T11:04:41-04:00", event0["date_time"])
	assert.Equal(t, "Go crypto/md5", event0["object"])
	assert.Equal(t, "Fixity matches", event0["outcome_information"])
	assert.Equal(t, "md5:c6d8080a39a0622f299750e13aa9c200", event0["outcome_detail"])
	assert.Equal(t, "fixity_check", event0["event_type"])
	assert.Equal(t, "Fixity check against registered hash", event0["detail"])
	assert.Equal(t, "uc.edu/cin.675812", event0["intellectual_object_identifier"])
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

	for _, event := range gf.PremisEvents {
		assert.Equal(t, gf.IntellectualObjectId, event.IntellectualObjectId)
		assert.Equal(t, gf.IntellectualObjectIdentifier, event.IntellectualObjectIdentifier)
		assert.Equal(t, gf.Id, event.GenericFileId)
		assert.Equal(t, gf.Identifier, event.GenericFileIdentifier)
	}

	// Calling this function again should not generate new events
	// if all the events are there.
	err = gf.BuildIngestEvents()
	assert.Nil(t, err)
	assert.Equal(t, 6, len(gf.PremisEvents))
}

func TestBuildIngestChecksums(t *testing.T) {
	gf := testutil.MakeGenericFile(0, 0, "test.edu/test_bag/file.txt")
	assert.Equal(t, 0, len(gf.Checksums))
	err := gf.BuildIngestChecksums()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(gf.Checksums))
	md5 := gf.GetChecksumByAlgorithm(constants.AlgMd5)
	sha256 := gf.GetChecksumByAlgorithm(constants.AlgSha256)
	require.NotNil(t, md5)
	require.NotNil(t, sha256)

	assert.Equal(t, md5.GenericFileId, gf.Id)
	assert.Equal(t, constants.AlgMd5, md5.Algorithm)
	assert.False(t, md5.DateTime.IsZero())
	assert.Equal(t, 32, len(md5.Digest))

	assert.Equal(t, sha256.GenericFileId, gf.Id)
	assert.Equal(t, constants.AlgSha256, sha256.Algorithm)
	assert.False(t, sha256.DateTime.IsZero())
	assert.Equal(t, 64, len(sha256.Digest))

	// Calling this function again should not generate new checksums
	// when all the checksums are already present.
	err = gf.BuildIngestChecksums()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(gf.Checksums))
}

func TestPropagateIdsToChildren(t *testing.T) {
	// Make a generic file with 6 events and 2 checksums
	gf := testutil.MakeGenericFile(6, 2, "test.edu/test_bag/file.txt")
	assert.Equal(t, 6, len(gf.PremisEvents))
	assert.Equal(t, 2, len(gf.Checksums))

	// Check pre-condition before actual test.
	for _, event := range gf.PremisEvents {
		assert.NotEqual(t, gf.Id, event.GenericFileId)
		assert.NotEqual(t, gf.Identifier, event.GenericFileIdentifier)
		assert.NotEqual(t, gf.IntellectualObjectId, event.IntellectualObjectId)
		assert.NotEqual(t, gf.IntellectualObjectIdentifier, event.IntellectualObjectIdentifier)
	}
	for _, checksum := range gf.Checksums {
		assert.NotEqual(t, gf.Id, checksum.GenericFileId)
	}

	gf.PropagateIdsToChildren()
	for _, event := range gf.PremisEvents {
		assert.Equal(t, gf.Id, event.GenericFileId)
		assert.Equal(t, gf.Identifier, event.GenericFileIdentifier)
		assert.Equal(t, gf.IntellectualObjectId, event.IntellectualObjectId)
		assert.Equal(t, gf.IntellectualObjectIdentifier, event.IntellectualObjectIdentifier)
	}
	for _, checksum := range gf.Checksums {
		assert.Equal(t, gf.Id, checksum.GenericFileId)
	}
}

func TestGenericFileMergeAttributes(t *testing.T) {
	gf1 := testutil.MakeGenericFile(3, 3, "test.edu/file1.txt")
	gf2 := testutil.MakeGenericFile(3, 3, "test.edu/file1.txt")

	// Match up identifiers, so copy assignment will work.
	for i, event := range gf1.PremisEvents {
		gf2.PremisEvents[i].Identifier = event.Identifier
	}
	for i, cs := range gf1.Checksums {
		gf2.Checksums[i].Algorithm = cs.Algorithm
		gf2.Checksums[i].Digest = cs.Digest
	}

	errors := gf1.MergeAttributes(gf2)
	require.Empty(t, errors)

	for i, event := range gf1.PremisEvents {
		assert.Equal(t, gf2.PremisEvents[i].Id, event.Id)
		assert.Equal(t, gf2.PremisEvents[i].CreatedAt, event.CreatedAt)
		assert.Equal(t, gf2.PremisEvents[i].UpdatedAt, event.UpdatedAt)
		// Ids and identifiers should propagate
		assert.Equal(t, gf1.Id, event.GenericFileId)
		assert.Equal(t, gf1.Identifier, event.GenericFileIdentifier)
		assert.Equal(t, gf1.IntellectualObjectId, event.IntellectualObjectId)
		assert.Equal(t, gf1.IntellectualObjectIdentifier, event.IntellectualObjectIdentifier)
	}
	for i, cs := range gf1.Checksums {
		assert.Equal(t, gf2.Checksums[i].Id, cs.Id)
		assert.Equal(t, gf2.Checksums[i].CreatedAt, cs.CreatedAt)
		assert.Equal(t, gf2.Checksums[i].UpdatedAt, cs.UpdatedAt)
		// Ids should propagate too
		assert.Equal(t, gf1.Id, cs.GenericFileId)
	}

}
