package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewIngestManifest(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.NotNil(t, manifest.FetchResult)
	assert.NotNil(t, manifest.ValidateResult)
	assert.NotNil(t, manifest.StoreResult)
	assert.NotNil(t, manifest.RecordResult)
	assert.NotNil(t, manifest.CleanupResult)
	assert.NotNil(t, manifest.Object)
}

func TestIngestManifest_HasErrors(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.False(t, manifest.HasErrors())

	manifest.FetchResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.FetchResult.ClearErrors()
	assert.False(t, manifest.HasErrors())

	manifest.UntarResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.UntarResult.ClearErrors()
	assert.False(t, manifest.HasErrors())

	manifest.ValidateResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.ValidateResult.ClearErrors()
	assert.False(t, manifest.HasErrors())

	manifest.StoreResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.StoreResult.ClearErrors()
	assert.False(t, manifest.HasErrors())

	manifest.RecordResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.RecordResult.ClearErrors()
	assert.False(t, manifest.HasErrors())

	manifest.CleanupResult.AddError("error")
	assert.True(t, manifest.HasErrors())
	manifest.CleanupResult.ClearErrors()
	assert.False(t, manifest.HasErrors())
}

func TestIngestManifest_HasFatalErrors(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.False(t, manifest.HasFatalErrors())

	manifest.FetchResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.FetchResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())

	manifest.UntarResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.UntarResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())

	manifest.ValidateResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.ValidateResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())

	manifest.StoreResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.StoreResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())

	manifest.RecordResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.RecordResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())

	manifest.CleanupResult.ErrorIsFatal = true
	assert.True(t, manifest.HasFatalErrors())
	manifest.CleanupResult.ClearErrors()
	assert.False(t, manifest.HasFatalErrors())
}

func TestIngestManifest_AllErrorsAsString(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.False(t, manifest.HasErrors())

	manifest.FetchResult.AddError("error 1")
	manifest.FetchResult.AddError("error 2")
	manifest.ValidateResult.AddError("error 3")
	manifest.StoreResult.AddError("error 4")
	manifest.RecordResult.AddError("error 5")
	manifest.CleanupResult.AddError("error 6")

	expected := "error 1\nerror 2\nerror 3\nerror 4\nerror 5\nerror 6\n"
	assert.Equal(t, expected, manifest.AllErrorsAsString())
}
