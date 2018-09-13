package models_test

import (
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

const DRMExpectedJson = `{"DPNWorkItem":null,"DPNBag":null,"TaskType":"","GlacierBucket":"","RequestedFromGlacierAt":"0001-01-01T00:00:00Z","GlacierRequestAccepted":false,"EstimatedDeletionFromS3":"0001-01-01T00:00:00Z","IsAvailableInS3":false,"LocalPath":"","RestorationURL":"","S3Bucket":"","ExpectedFixityValue":"","ActualFixityValue":"","GlacierRestoreSummary":{"Attempted":false,"AttemptNumber":0,"ErrorIsFatal":false,"Errors":[],"StartedAt":"0001-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z","Retry":true},"LocalCopySummary":{"Attempted":false,"AttemptNumber":0,"ErrorIsFatal":false,"Errors":[],"StartedAt":"0001-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z","Retry":true},"ValidationSummary":{"Attempted":false,"AttemptNumber":0,"ErrorIsFatal":false,"Errors":[],"StartedAt":"0001-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z","Retry":true},"RecordSummary":{"Attempted":false,"AttemptNumber":0,"ErrorIsFatal":false,"Errors":[],"StartedAt":"0001-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z","Retry":true},"FixityCheck":null,"FixityCheckSavedAt":"0001-01-01T00:00:00Z"}`

func TestNewDPNRetrievalManifest(t *testing.T) {
	message := testutil.MakeNsqMessage("1234")
	manifest := models.NewDPNRetrievalManifest(message)
	require.NotNil(t, manifest)
	require.NotNil(t, manifest.GlacierRestoreSummary)
	require.NotNil(t, manifest.LocalCopySummary)
	require.NotNil(t, manifest.ValidationSummary)
	require.NotNil(t, manifest.RecordSummary)
}

func TestDPNRetrievalManifest_ToJson(t *testing.T) {
	message := testutil.MakeNsqMessage("1234")
	manifest := models.NewDPNRetrievalManifest(message)
	require.NotNil(t, manifest)
	jsonString, err := manifest.ToJson()
	require.Nil(t, err)
	assert.Equal(t, DRMExpectedJson, jsonString)
}

func TestDPNRetrievalManifest_FromJson(t *testing.T) {
	manifest, err := models.DPNRetrievalManifestFromJson(DRMExpectedJson)
	require.Nil(t, err)
	require.NotNil(t, manifest)
}

func TestDPNRetrievalManifest_GetSummary(t *testing.T) {
	message := testutil.MakeNsqMessage("1234")
	manifest := models.NewDPNRetrievalManifest(message)
	require.NotNil(t, manifest)
	assert.Equal(t, manifest.GlacierRestoreSummary, manifest.GetSummary("GlacierRestoreSummary"))
	assert.Equal(t, manifest.LocalCopySummary, manifest.GetSummary("LocalCopySummary"))
	assert.Equal(t, manifest.ValidationSummary, manifest.GetSummary("ValidationSummary"))
	assert.Equal(t, manifest.RecordSummary, manifest.GetSummary("RecordSummary"))
	assert.Nil(t, manifest.GetSummary("NoSuchSummary"))
}
