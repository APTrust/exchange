package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewIngestManifest(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.NotNil(t, manifest.FetchResult)
	assert.NotNil(t, manifest.UntarResult)
	assert.NotNil(t, manifest.ValidateResult)
	assert.NotNil(t, manifest.StoreResult)
	assert.NotNil(t, manifest.RecordResult)
	assert.NotNil(t, manifest.ReplicateResult)
	assert.NotNil(t, manifest.CleanupResult)
	assert.NotNil(t, manifest.Object)
}
