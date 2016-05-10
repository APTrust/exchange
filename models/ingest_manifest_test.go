package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewIngestManifest(t *testing.T) {
	manifest := models.NewIngestManifest()
	assert.NotNil(t, manifest.Fetch)
	assert.NotNil(t, manifest.Untar)
	assert.NotNil(t, manifest.Validate)
	assert.NotNil(t, manifest.Store)
	assert.NotNil(t, manifest.Record)
	assert.NotNil(t, manifest.Replicate)
	assert.NotNil(t, manifest.Cleanup)
	assert.NotNil(t, manifest.Object)
}
