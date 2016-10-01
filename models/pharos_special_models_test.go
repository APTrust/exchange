package models_test

import (
	"encoding/json"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)


func TestNewGenericFileForPharos(t *testing.T) {
	filename := filepath.Join("testdata", "json_objects", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	require.Nil(t, err)
	gf := intelObj.GenericFiles[1]
	pharosGf := models.NewGenericFileForPharos(gf)
	assert.Equal(t, gf.Identifier, pharosGf.Identifier)
	assert.Equal(t, gf.IntellectualObjectId, pharosGf.IntellectualObjectId)
	assert.Equal(t, gf.IntellectualObjectIdentifier, pharosGf.IntellectualObjectIdentifier)
	assert.Equal(t, gf.FileFormat, pharosGf.FileFormat)
	assert.Equal(t, gf.URI, pharosGf.URI)
	assert.Equal(t, gf.Size, pharosGf.Size)
	assert.Equal(t, gf.FileCreated, pharosGf.FileCreated)
	assert.Equal(t, gf.FileModified, pharosGf.FileModified)
	assert.Equal(t, len(gf.Checksums), len(pharosGf.Checksums))
	assert.Equal(t, len(gf.PremisEvents), len(pharosGf.PremisEvents))
	for i := range gf.Checksums {
		assert.Equal(t, gf.Checksums[i].Digest, pharosGf.Checksums[i].Digest)
	}
	for i := range gf.PremisEvents {
		assert.Equal(t, gf.PremisEvents[i].EventType, pharosGf.PremisEvents[i].EventType)
	}
}

func TestGenericFileBatchForPharos(t *testing.T) {
	filename := filepath.Join("testdata", "json_objects", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	require.Nil(t, err)

	// Make a batch of files, and make sure all files got in
	batch := models.NewGenericFileBatchForPharos(intelObj.GenericFiles)
	assert.Equal(t, len(intelObj.GenericFiles), len(batch.Files))

	// Convert the batch to the JSON that Pharos expects
	actualJson, err := batch.ToJson()
	require.Nil(t, err)

	// Now read that JSON back into a data structure,
	// and do a spot check on some of the data
	data := make(map[string]interface{})
	err = json.Unmarshal(actualJson, &data)
	require.Nil(t, err)
	require.NotNil(t, data["generic_files"])

	// Make sure all generic files made it into the JSON
	genericFiles := data["generic_files"].(map[string]interface{})
	assert.NotNil(t, genericFiles["files"])
	files := genericFiles["files"].([]interface{})
	assert.Equal(t, len(intelObj.GenericFiles), len(files))

	// Spot check a few attributes
	file0 := files[0].(map[string]interface{})
	assert.NotEmpty(t, file0["identifier"])
	assert.NotEmpty(t, file0["checksums_attributes"])
	assert.NotEmpty(t, file0["premis_events_attributes"])
}
