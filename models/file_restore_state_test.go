package models_test

import (
	"github.com/APTrust/exchange/dpn/util/testutil"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewFileRestoreState(t *testing.T) {
	restoreState := models.NewFileRestoreState(testutil.MakeNsqMessage("999"))
	assert.NotNil(t, restoreState.RestoreSummary)
}
