package models_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewWorkItemState(t *testing.T) {
	jsonSnippet := `{"key": "value"}`
	state := models.NewWorkItemState(999, constants.ActionIngest, jsonSnippet)
	assert.NotNil(t, state)
	assert.Equal(t, 0, state.Id) // We didn't set this
	assert.Equal(t, 999, state.WorkItemId)
	assert.Equal(t, constants.ActionIngest, state.Action)
	assert.Equal(t, jsonSnippet, state.State)
}

func TestNewWorkItemState_IngestManifest(t *testing.T) {
	manifest := models.NewIngestManifest()
	manifest.WorkItemId = 999
	state := models.NewWorkItemState(999, constants.ActionIngest, "")
	require.NotNil(t, state)
	assert.False(t, state.HasData())
	err := state.SetStateFromIngestManifest(manifest)
	require.Nil(t, err)
	assert.True(t, state.HasData())
	newManifest, err := state.IngestManifest()
	assert.Nil(t, err)
	assert.Equal(t, manifest.WorkItemId, newManifest.WorkItemId)
}
