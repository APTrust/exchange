package models_test

import (
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
