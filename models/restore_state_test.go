package models_test

import (
	"github.com/APTrust/exchange/dpn/util/testutil"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewRestoreState(t *testing.T) {
	restoreState := models.NewRestoreState(testutil.MakeNsqMessage("999"))
	assert.NotNil(t, restoreState.PackageSummary)
	assert.NotNil(t, restoreState.ValidateSummary)
	assert.NotNil(t, restoreState.CopySummary)
	assert.NotNil(t, restoreState.RecordSummary)
}

func TestRestoreState_HasErrors(t *testing.T) {
	restoreState := models.NewRestoreState(testutil.MakeNsqMessage("999"))
	assert.False(t, restoreState.HasErrors())

	restoreState.PackageSummary.AddError("error")
	assert.True(t, restoreState.HasErrors())
	restoreState.PackageSummary.ClearErrors()
	assert.False(t, restoreState.HasErrors())

	restoreState.ValidateSummary.AddError("error")
	assert.True(t, restoreState.HasErrors())
	restoreState.ValidateSummary.ClearErrors()
	assert.False(t, restoreState.HasErrors())

	restoreState.CopySummary.AddError("error")
	assert.True(t, restoreState.HasErrors())
	restoreState.CopySummary.ClearErrors()
	assert.False(t, restoreState.HasErrors())

	restoreState.RecordSummary.AddError("error")
	assert.True(t, restoreState.HasErrors())
	restoreState.RecordSummary.ClearErrors()
	assert.False(t, restoreState.HasErrors())
}

func TestRestoreState_HasFatalErrors(t *testing.T) {
	restoreState := models.NewRestoreState(testutil.MakeNsqMessage("999"))
	assert.False(t, restoreState.HasFatalErrors())

	restoreState.PackageSummary.ErrorIsFatal = true
	assert.True(t, restoreState.HasFatalErrors())
	restoreState.PackageSummary.ClearErrors()
	assert.False(t, restoreState.HasFatalErrors())

	restoreState.ValidateSummary.ErrorIsFatal = true
	assert.True(t, restoreState.HasFatalErrors())
	restoreState.ValidateSummary.ClearErrors()
	assert.False(t, restoreState.HasFatalErrors())

	restoreState.CopySummary.ErrorIsFatal = true
	assert.True(t, restoreState.HasFatalErrors())
	restoreState.CopySummary.ClearErrors()
	assert.False(t, restoreState.HasFatalErrors())

	restoreState.RecordSummary.ErrorIsFatal = true
	assert.True(t, restoreState.HasFatalErrors())
	restoreState.RecordSummary.ClearErrors()
	assert.False(t, restoreState.HasFatalErrors())
}

func TestRestoreState_AllErrorsAsString(t *testing.T) {
	restoreState := models.NewRestoreState(testutil.MakeNsqMessage("999"))
	assert.False(t, restoreState.HasErrors())

	restoreState.PackageSummary.AddError("error 1")
	restoreState.PackageSummary.AddError("error 2")
	restoreState.ValidateSummary.AddError("error 3")
	restoreState.RecordSummary.AddError("error 4")
	restoreState.RecordSummary.AddError("error 5")
	restoreState.CopySummary.AddError("error 6")

	expected := "error 1\nerror 2\nerror 3\nerror 4\nerror 5\nerror 6\n"
	assert.Equal(t, expected, restoreState.AllErrorsAsString())
}
