package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewDeleteState(t *testing.T) {
	deleteState := models.NewDeleteState(testutil.MakeNsqMessage("999"))
	require.NotNil(t, deleteState)
	assert.NotNil(t, deleteState.DeleteSummary)
}
