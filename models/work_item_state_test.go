package models_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
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
