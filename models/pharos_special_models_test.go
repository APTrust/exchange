package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
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
	assert.Equal(t, gf.FileFormat, pharosGf.FileFormat)
	assert.Equal(t, gf.URI, pharosGf.URI)
	assert.Equal(t, gf.Size, pharosGf.Size)
	// TODO: Add these back when they're part of the Rails model
	//assert.Equal(t, gf.FileCreated, pharosGf.FileCreated)
	//assert.Equal(t, gf.FileModified, pharosGf.FileModified)
	assert.Equal(t, len(gf.Checksums), len(pharosGf.Checksums))
	assert.Equal(t, len(gf.PremisEvents), len(pharosGf.PremisEvents))
	for i := range gf.Checksums {
		assert.Equal(t, gf.Checksums[i].Digest, pharosGf.Checksums[i].Digest)
	}
	for i := range gf.PremisEvents {
		assert.Equal(t, gf.PremisEvents[i].EventType, pharosGf.PremisEvents[i].EventType)
	}
}

func TestNewIntellectualObjectForPharos(t *testing.T) {
	filename := filepath.Join("testdata", "json_objects", "intel_obj.json")
	intelObj, err := testutil.LoadIntelObjFixture(filename)
	require.Nil(t, err)
	intelObj.Access = "INSTITUTION" // Just so we can test lowercase
	intelObj.DPNUUID = "a0903223-e956-40d8-a6e1-4d09edf7cea2"
	intelObj.ETag = "12345678"
	intelObj.SourceOrganization = "Popeye's Fried Chicken"
	intelObj.BagItProfileIdentifier = "https://example.com/profile.json"
	pharosObj := models.NewIntellectualObjectForPharos(intelObj)
	assert.Equal(t, intelObj.Identifier, pharosObj.Identifier)
	assert.Equal(t, intelObj.BagName, pharosObj.BagName)
	assert.Equal(t, intelObj.InstitutionId, pharosObj.InstitutionId)
	assert.Equal(t, intelObj.Title, pharosObj.Title)
	assert.Equal(t, intelObj.Description, pharosObj.Description)
	assert.Equal(t, intelObj.AltIdentifier, pharosObj.AltIdentifier)
	assert.Equal(t, strings.ToLower(intelObj.Access), pharosObj.Access)
	assert.Equal(t, intelObj.DPNUUID, pharosObj.DPNUUID)
	assert.Equal(t, intelObj.ETag, pharosObj.ETag)
	assert.Equal(t, intelObj.State, pharosObj.State)
	assert.Equal(t, "US Photos, 1940-1945", pharosObj.BagGroupIdentifier)
	assert.Equal(t, "A", pharosObj.State)
	assert.Equal(t, intelObj.SourceOrganization, pharosObj.SourceOrganization)
	assert.Equal(t, intelObj.BagItProfileIdentifier, pharosObj.BagItProfileIdentifier)
}

func TestNewPremisEventForPharos(t *testing.T) {
	event := testutil.MakePremisEvent()
	pharosEvent := models.NewPremisEventForPharos(event)
	assert.Equal(t, event.Id, pharosEvent.Id)
	assert.Equal(t, event.Identifier, pharosEvent.Identifier)
	assert.Equal(t, event.EventType, pharosEvent.EventType)
	assert.Equal(t, event.DateTime, pharosEvent.DateTime)
	assert.Equal(t, event.Detail, pharosEvent.Detail)
	assert.Equal(t, event.Outcome, pharosEvent.Outcome)
	assert.Equal(t, event.OutcomeDetail, pharosEvent.OutcomeDetail)
	assert.Equal(t, event.Object, pharosEvent.Object)
	assert.Equal(t, event.Agent, pharosEvent.Agent)
	assert.Equal(t, event.OutcomeInformation, pharosEvent.OutcomeInformation)
	assert.Equal(t, event.IntellectualObjectId, pharosEvent.IntellectualObjectId)
	assert.Equal(t, event.IntellectualObjectIdentifier, pharosEvent.IntellectualObjectIdentifier)
	assert.Equal(t, event.GenericFileId, pharosEvent.GenericFileId)
	assert.Equal(t, event.GenericFileIdentifier, pharosEvent.GenericFileIdentifier)
}

func TestNewChecksumForPharos(t *testing.T) {
	cs := testutil.MakeChecksum()
	pharosChecksum := models.NewChecksumForPharos(cs)
	assert.Equal(t, cs.Id, pharosChecksum.Id)
	assert.Equal(t, cs.GenericFileId, pharosChecksum.GenericFileId)
	assert.Equal(t, cs.Algorithm, pharosChecksum.Algorithm)
	assert.Equal(t, cs.DateTime, pharosChecksum.DateTime)
	assert.Equal(t, cs.Digest, pharosChecksum.Digest)
}

func TestNewWorkItemStateForPharos(t *testing.T) {
	workItemState := testutil.MakeWorkItemState()
	pharosItem := models.NewWorkItemStateForPharos(workItemState)
	assert.Equal(t, workItemState.Id, pharosItem.Id)
	assert.Equal(t, workItemState.WorkItemId, pharosItem.WorkItemId)
	assert.Equal(t, workItemState.Action, pharosItem.Action)
	assert.Equal(t, workItemState.State, pharosItem.State)
}
