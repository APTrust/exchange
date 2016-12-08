package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	pharosObj := models.NewIntellectualObjectForPharos(intelObj)
	assert.Equal(t, intelObj.Identifier, pharosObj.Identifier)
	assert.Equal(t, intelObj.BagName, pharosObj.BagName)
	assert.Equal(t, intelObj.InstitutionId, pharosObj.InstitutionId)
	assert.Equal(t, intelObj.Title, pharosObj.Title)
	assert.Equal(t, intelObj.Description, pharosObj.Description)
	assert.Equal(t, intelObj.AltIdentifier, pharosObj.AltIdentifier)
	assert.Equal(t, strings.ToLower(intelObj.Access), pharosObj.Access)
	assert.Equal(t, intelObj.DPNUUID, pharosObj.DPNUUID)
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

func TestNewDPNWorkItemForPharos(t *testing.T) {
	timestamp, _ := time.Parse(time.RFC3339, "2016-11-15T15:33:00+00:00")
	note := "All done"
	state := "Nebraska"
	item := &models.DPNWorkItem{
		Id:          1000,
		RemoteNode:  "hathi",
		Task:        "Replication",
		Identifier:  "1234-5678",
		QueuedAt:    &timestamp,
		CompletedAt: &timestamp,
		Note:        &note,
		State:       &state,
		CreatedAt:   timestamp,
		UpdatedAt:   timestamp,
	}
	pharosItem := models.NewDPNWorkItemForPharos(item)
	require.NotNil(t, pharosItem)
	assert.Equal(t, item.RemoteNode, pharosItem.RemoteNode)
	assert.Equal(t, item.Task, pharosItem.Task)
	assert.Equal(t, item.Identifier, pharosItem.Identifier)
	assert.Equal(t, item.QueuedAt, pharosItem.QueuedAt)
	assert.Equal(t, item.CompletedAt, pharosItem.CompletedAt)
	assert.Equal(t, item.Note, pharosItem.Note)
	assert.Equal(t, item.State, pharosItem.State)
}

func TestNewWorkItemStateForPharos(t *testing.T) {
	workItemState := testutil.MakeWorkItemState()
	pharosItem := models.NewWorkItemStateForPharos(workItemState)
	assert.Equal(t, workItemState.Id, pharosItem.Id)
	assert.Equal(t, workItemState.WorkItemId, pharosItem.WorkItemId)
	assert.Equal(t, workItemState.Action, pharosItem.Action)
	assert.Equal(t, workItemState.State, pharosItem.State)
}
