package integration_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	dpn_testutil "github.com/APTrust/exchange/dpn/util/testutil"
	apt_testutil "github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
)

// TestIngestRecordWorkItems checks to see if Pharos WorkItems
// were updated correctly.
func TestIngestRecordWorkItems(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_, workItems, err := dpn_testutil.GetDPNWorkItems()
	require.Nil(t, err)
	for _, item := range workItems {
		assert.Equal(t, constants.StageRecord, item.Stage)
		assert.Equal(t, constants.StatusSuccess, item.Status)
		assert.Equal(t, "DPN ingest complete", item.Note)
		assert.Equal(t, "", item.Node)
		assert.Equal(t, 0, item.Pid)
		assert.True(t, item.Retry)
	}
}

// TestIngestRecordWorkItemState checks to see if Pharos WorkItemState
// records were updated correctly.
func TestIngestRecordWorkItemState(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	// This actually retrieves DPN related WorkItems, not DPNWorkItems. Hmm...
	_context, workItems, err := dpn_testutil.GetDPNWorkItems()
	require.Nil(t, err)
	for _, item := range workItems {
		require.NotNil(t, item.WorkItemStateId)
		resp := _context.PharosClient.WorkItemStateGet(*item.WorkItemStateId)
		require.Nil(t, resp.Error)
		workItemState := resp.WorkItemState()
		require.NotNil(t, workItemState)
		assert.Equal(t, constants.ActionDPN, workItemState.Action)
		assert.False(t, workItemState.CreatedAt.IsZero())
		assert.False(t, workItemState.UpdatedAt.IsZero())
		detail := fmt.Sprintf("%s from Pharos", item.ObjectIdentifier)
		testIngestRecordWorkItemState(t, _context, workItemState.State, detail)
	}
}

// TestIngestRecordJsonLog checks that all expected entries are present
// in the dpn_ingest_record.json log.
func TestIngestRecordJsonLog(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err)
	pathToLogFile := filepath.Join(_context.Config.LogDirectory, "dpn_ingest_record.json")
	for _, s3Key := range apt_testutil.INTEGRATION_GOOD_BAGS[0:7] {
		parts := strings.Split(s3Key, "/")
		tarFileName := parts[1]
		manifest, err := apt_testutil.FindDPNIngestManifestInLog(pathToLogFile, tarFileName)
		require.Nil(t, err)
		require.NotNil(t, manifest)
		detail := fmt.Sprintf("%s from JSON log", tarFileName)
		testIngestRecordManifest(t, _context, manifest, detail)
	}
}

// Test the JSON serialized WorkItemState. Param WorkItemState is a
// string of JSON data. Param detail describes which object we're
// testing and where the JSON came from, so failure messages can be
// more informative.
func testIngestRecordWorkItemState(t *testing.T, _context *context.Context, workItemState, detail string) {
	dpnIngestManifest := models.NewDPNIngestManifest(nil)
	err := json.Unmarshal([]byte(workItemState), dpnIngestManifest)
	require.Nil(t, err, "Could not unmarshal state")
	testIngestRecordManifest(t, _context, dpnIngestManifest, detail)
}

func testIngestRecordManifest(t *testing.T, _context *context.Context, dpnIngestManifest *models.DPNIngestManifest, detail string) {
	require.NotNil(t, dpnIngestManifest.PackageSummary, detail)
	require.NotNil(t, dpnIngestManifest.ValidateSummary, detail)
	require.NotNil(t, dpnIngestManifest.RecordSummary, detail)

	assert.False(t, dpnIngestManifest.PackageSummary.StartedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.PackageSummary.FinishedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.PackageSummary.HasErrors(), detail)

	assert.False(t, dpnIngestManifest.ValidateSummary.StartedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.ValidateSummary.FinishedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.ValidateSummary.HasErrors(), detail)

	assert.False(t, dpnIngestManifest.StoreSummary.StartedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.StoreSummary.FinishedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.StoreSummary.HasErrors(), detail)

	assert.False(t, dpnIngestManifest.RecordSummary.StartedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.RecordSummary.FinishedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.RecordSummary.HasErrors(), detail)

	// The items below were covered in prior post-tests, but we want to make
	// sure no data is wiped out by dpn_ingest_record.

	assert.NotNil(t, dpnIngestManifest.WorkItem, detail)
	require.NotNil(t, dpnIngestManifest.DPNBag, detail)
	assert.NotEmpty(t, dpnIngestManifest.DPNBag.UUID, detail)
	assert.NotEmpty(t, dpnIngestManifest.DPNBag.LocalId, detail)
	assert.NotEmpty(t, dpnIngestManifest.DPNBag.Member, detail)
	assert.Equal(t, dpnIngestManifest.DPNBag.UUID, dpnIngestManifest.DPNBag.FirstVersionUUID, detail)
	assert.EqualValues(t, 1, dpnIngestManifest.DPNBag.Version, detail)
	assert.Equal(t, _context.Config.DPN.LocalNode, dpnIngestManifest.DPNBag.IngestNode, detail)
	assert.Equal(t, _context.Config.DPN.LocalNode, dpnIngestManifest.DPNBag.AdminNode, detail)
	assert.Equal(t, "D", dpnIngestManifest.DPNBag.BagType, detail)
	assert.NotEqual(t, 0, dpnIngestManifest.DPNBag.Size, detail)
	assert.False(t, dpnIngestManifest.DPNBag.CreatedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.DPNBag.UpdatedAt.IsZero(), detail)

	require.NotEmpty(t, dpnIngestManifest.DPNBag.MessageDigests, detail)
	messageDigest := dpnIngestManifest.DPNBag.MessageDigests[0]
	assert.Equal(t, dpnIngestManifest.DPNBag.UUID, messageDigest.Bag, detail)
	assert.Equal(t, constants.AlgSha256, messageDigest.Algorithm, detail)
	assert.Equal(t, _context.Config.DPN.LocalNode, messageDigest.Node, detail)
	assert.Equal(t, 64, len(messageDigest.Value), detail)
	assert.False(t, messageDigest.CreatedAt.IsZero(), detail)

	assert.NotEmpty(t, dpnIngestManifest.LocalDir, detail)
	assert.NotEmpty(t, dpnIngestManifest.LocalTarFile, detail)
	assert.True(t, strings.HasSuffix(dpnIngestManifest.LocalTarFile, ".tar"), detail)

	// Bag has been stored in Glacier, so this should be set to
	// "https://s3.amazonaws.com/aptrust.dpn.test/<UUID>.tar"
	expectedURL := fmt.Sprintf("https://s3.amazonaws.com/%s/%s.tar",
		_context.Config.DPN.DPNPreservationBucket,
		dpnIngestManifest.DPNBag.UUID)
	assert.Equal(t, expectedURL, dpnIngestManifest.StorageURL, detail)
}
