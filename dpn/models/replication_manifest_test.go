package models_test

import (
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/util/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)


func TestNewReplicationManifest(t *testing.T) {
	nsqMessage := testutil.MakeNsqMessage("999")
	manifest := models.NewReplicationManifest(nsqMessage)
	assert.Equal(t, nsqMessage, manifest.NsqMessage)
	assert.NotNil(t, manifest.CopySummary)
	assert.NotNil(t, manifest.ValidateSummary)
	assert.NotNil(t, manifest.StoreSummary)
}
