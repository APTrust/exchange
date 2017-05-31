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
	"time"
)

func TestNewIntellectualObject(t *testing.T) {
	obj := models.NewIntellectualObject()
	assert.NotNil(t, obj.GenericFiles)
	assert.NotNil(t, obj.PremisEvents)
	assert.NotNil(t, obj.IngestFilesIgnored)
	assert.NotNil(t, obj.IngestTags)
}

func TestTotalFileSize(t *testing.T) {
	filepath := filepath.Join("testdata", "json_objects", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	if obj.TotalFileSize() != 686 {
		t.Errorf("TotalFileSize() returned '%d', expected 686", obj.TotalFileSize())
	}
}

func TestSerializeObjectForPharos(t *testing.T) {
	filename := filepath.Join("testdata", "json_objects", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filename, err)
	}
	intelObj.Access = "Institution" // Make sure this is lower-cased below
	intelObj.ETag = "12345678"
	data, err := intelObj.SerializeForPharos()
	if err != nil {
		t.Errorf("Error serializing for Pharos: %v", err)
		return
	}
	hash := make(map[string]interface{})
	err = json.Unmarshal(data, &hash)
	if err != nil {
		t.Errorf("Error unmarshalling data: %v", err)
	}
	objHash := hash["intellectual_object"]
	assert.NotNil(t, objHash)

	pharosObj := objHash.(map[string]interface{})

	assert.Equal(t, "uc.edu/cin.675812", pharosObj["identifier"])
	assert.Equal(t, "cin.675812", pharosObj["bag_name"])
	assert.EqualValues(t, 12, pharosObj["institution_id"])
	assert.Equal(t, "Notes from the Oesper Collections", pharosObj["title"])
	assert.Equal(t, "A collection from Cincinnati", pharosObj["description"])
	assert.Equal(t, "Photo Collection", pharosObj["alt_identifier"])
	assert.Equal(t, "institution", pharosObj["access"])
	assert.Equal(t, "12345678", pharosObj["etag"])
}

func TestFindGenericFile(t *testing.T) {
	filepath := filepath.Join("testdata", "json_objects", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}

	gf1 := obj.FindGenericFile("data/object.properties")
	assert.NotNil(t, gf1)
	assert.Equal(t, "uc.edu/cin.675812/data/object.properties", gf1.Identifier)

	gf2 := obj.FindGenericFile("data/metadata.xml")
	assert.NotNil(t, gf2)
	assert.Equal(t, "uc.edu/cin.675812/data/metadata.xml", gf2.Identifier)

	// Make sure we don't get an error here
	assert.NotPanics(t, func() { obj.FindGenericFile("file/does/not/exist") })
	gf3 := obj.FindGenericFile("file/does/not/exist")
	assert.Nil(t, gf3)
}

func TestFindTag(t *testing.T) {
	obj := models.NewIntellectualObject()
	obj.IngestTags = append(obj.IngestTags, models.NewTag("file1", "label1", "value1"))
	obj.IngestTags = append(obj.IngestTags, models.NewTag("file2", "label2", "value2"))
	obj.IngestTags = append(obj.IngestTags, models.NewTag("file3", "label3", "value3.0"))
	obj.IngestTags = append(obj.IngestTags, models.NewTag("file3", "label3", "value3.1"))
	obj.IngestTags = append(obj.IngestTags, models.NewTag("file4", "label3", "value3.2"))

	tags1 := obj.FindTag("label1")
	tags2 := obj.FindTag("label2")
	tags3 := obj.FindTag("label3")
	tagsx := obj.FindTag("Elmer Fudd")

	require.NotNil(t, tags1)
	assert.Equal(t, "value1", tags1[0].Value)
	require.NotNil(t, tags2)
	assert.Equal(t, "value2", tags2[0].Value)
	require.NotNil(t, tags3)
	assert.Equal(t, 3, len(tags3))
	assert.Equal(t, "value3.0", tags3[0].Value)
	assert.Equal(t, "value3.1", tags3[1].Value)
	assert.Equal(t, "value3.2", tags3[2].Value)
	assert.Nil(t, tagsx)
}

func TestAllFilesSaved(t *testing.T) {
	filepath := filepath.Join("testdata", "json_objects", "intel_obj.json")
	obj, err := testutil.LoadIntelObjFixture(filepath)
	if err != nil {
		t.Errorf("Error loading test data file '%s': %v", filepath, err)
	}
	assert.True(t, obj.AllFilesSaved())

	gf := obj.FindGenericFile("data/object.properties")
	gf.IngestNeedsSave = true
	assert.False(t, obj.AllFilesSaved())

	gf.IngestStorageURL = "https://example.com/primary"
	gf.IngestReplicationURL = "https://example.com/secondary"
	gf.IngestStoredAt = time.Now().UTC()
	gf.IngestReplicatedAt = time.Now().UTC()
	assert.True(t, obj.AllFilesSaved())
}

func TestObjFindEventsByType(t *testing.T) {
	obj := models.NewIntellectualObject()

	// Add a creation event
	creationEvent := testutil.MakePremisEvent()
	creationEvent.EventType = constants.EventCreation
	obj.PremisEvents = append(obj.PremisEvents, creationEvent)

	// Add identifier assignment event
	idEvent := testutil.MakePremisEvent()
	idEvent.EventType = constants.EventIdentifierAssignment
	obj.PremisEvents = append(obj.PremisEvents, idEvent)

	// Add an ingest event
	ingestEvent := testutil.MakePremisEvent()
	ingestEvent.EventType = constants.EventIngestion
	obj.PremisEvents = append(obj.PremisEvents, ingestEvent)

	creationEvents := obj.FindEventsByType(constants.EventCreation)
	idEvents := obj.FindEventsByType(constants.EventIdentifierAssignment)
	ingestEvents := obj.FindEventsByType(constants.EventIngestion)

	require.Equal(t, 1, len(creationEvents))
	require.Equal(t, 1, len(idEvents))
	require.Equal(t, 1, len(ingestEvents))

	assert.Equal(t, creationEvent.Identifier, creationEvents[0].Identifier)
	assert.Equal(t, idEvent.Identifier, idEvents[0].Identifier)
	assert.Equal(t, ingestEvent.Identifier, ingestEvents[0].Identifier)
}

func TestObjBuildIngestEvents(t *testing.T) {
	// Make intel obj with 5 files, no events, checksums or tags
	obj := testutil.MakeIntellectualObject(5, 0, 0, 0)
	assert.Equal(t, 5, len(obj.GenericFiles))
	assert.Equal(t, 0, len(obj.PremisEvents))

	err := obj.BuildIngestEvents(len(obj.GenericFiles))
	assert.Nil(t, err)

	// Expecting four PREMIS events total for IntelObj, each with
	// correct IntelObj.Id and IntelObj.Identifier.
	assert.Equal(t, 4, len(obj.PremisEvents))
	assert.Equal(t, 1, len(obj.FindEventsByType(constants.EventCreation)))
	assert.Equal(t, 1, len(obj.FindEventsByType(constants.EventIdentifierAssignment)))
	assert.Equal(t, 1, len(obj.FindEventsByType(constants.EventAccessAssignment)))
	assert.Equal(t, 1, len(obj.FindEventsByType(constants.EventIngestion)))

	for _, event := range obj.PremisEvents {
		assert.Equal(t, obj.Id, event.IntellectualObjectId)
		assert.Equal(t, obj.Identifier, event.IntellectualObjectIdentifier)
	}

	// PREMIS events should be set correctly for all of this
	// object's GenericFiles
	for _, gf := range obj.GenericFiles {
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
	}

	// Calling this function again should not generate new events
	// if all the events are there.
	err = obj.BuildIngestEvents(len(obj.GenericFiles))
	assert.Nil(t, err)
	assert.Equal(t, 4, len(obj.PremisEvents))
	for _, gf := range obj.GenericFiles {
		assert.Equal(t, 6, len(gf.PremisEvents))
	}
}

func TestObjBuildIngestChecksums(t *testing.T) {
	// Make intel obj with 5 files, no events, checksums or tags
	obj := testutil.MakeIntellectualObject(5, 0, 0, 0)
	assert.Equal(t, 5, len(obj.GenericFiles))
	assert.Equal(t, 0, len(obj.PremisEvents))

	err := obj.BuildIngestChecksums()
	assert.Nil(t, err)

	for _, gf := range obj.GenericFiles {
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
	}

	// Calling this function again should not generate new checksums
	// when all the checksums are already present.
	err = obj.BuildIngestChecksums()
	assert.Nil(t, err)
	for _, gf := range obj.GenericFiles {
		assert.Equal(t, 2, len(gf.Checksums))
	}
}

func TestObjPropagateIdsToChildren(t *testing.T) {
	// Make intel obj with 5 files, 5 events, 2 checksums, 0 tags
	// Obj will have 5 events, as will all GenericFiles
	obj := testutil.MakeIntellectualObject(5, 5, 2, 0)
	assert.Equal(t, 5, len(obj.GenericFiles))
	assert.Equal(t, 5, len(obj.PremisEvents))

	obj.PropagateIdsToChildren()

	for _, event := range obj.PremisEvents {
		assert.Equal(t, obj.Id, event.IntellectualObjectId)
		assert.Equal(t, obj.Identifier, event.IntellectualObjectIdentifier)
	}

	for _, gf := range obj.GenericFiles {
		assert.Equal(t, obj.Id, gf.IntellectualObjectId)
		assert.Equal(t, obj.Identifier, gf.IntellectualObjectIdentifier)
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
}
