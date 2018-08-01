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

// TestPackageWorkItems checks to see if Pharos WorkItems
// were updated correctly.
func TestPackageWorkItems(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_, workItems, err := dpn_testutil.GetDPNWorkItems()
	require.Nil(t, err)
	for _, item := range workItems {
		assert.Equal(t, constants.StageStore, item.Stage)
		assert.Equal(t, constants.StatusPending, item.Status)
		assert.Equal(t, "Packaging completed, awaiting storage", item.Note)
		assert.Equal(t, "", item.Node)
		assert.Equal(t, 0, item.Pid)
		assert.True(t, item.Retry)
	}
}

// TestPackageWorkItemState checks to see if Pharos WorkItemState
// records were updated correctly.
func TestPackageWorkItemState(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
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
		testPackageWorkItemState(t, _context, workItemState.State, detail)
	}

}

// TestPackageJsonLog checks that all expected entries are present
// in the dpn_package.json log.
func TestPackageJsonLog(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err)
	pathToLogFile := filepath.Join(_context.Config.LogDirectory, "dpn_package.json")
	for _, s3Key := range apt_testutil.INTEGRATION_GOOD_BAGS[0:7] {
		parts := strings.Split(s3Key, "/")
		tarFileName := parts[1]
		manifest, err := apt_testutil.FindDPNIngestManifestInLog(pathToLogFile, tarFileName)
		require.Nil(t, err)
		require.NotNil(t, manifest)

		detail := fmt.Sprintf("%s from JSON log", tarFileName)
		testPackageManifest(t, _context, manifest, detail)
	}
}

// TestPackageTarFilesPresent tests whether all expected DPN bags
// (tar files) are present in the staging area.
func TestPackageTarFilesPresent(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err)
	pattern := filepath.Join(_context.Config.DPN.StagingDirectory, "test.edu", "*.tar")
	files, err := filepath.Glob(pattern)
	require.Nil(t, err)
	assert.Equal(t, 7, len(files))
}

// TestPackageCleanup checks to see whether dpn_package cleaned up
// all of the intermediate files created during the bag building
// process. Those are directories containing untarred bags.
func TestPackageCleanup(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err)
	pattern := filepath.Join(_context.Config.DPN.StagingDirectory, "test.edu", "*")
	files, err := filepath.Glob(pattern)
	require.Nil(t, err)

	// Only the 7 tar file should remain. The 7 working directories
	// should have been deleted. If anything other than a tar file
	// remains, some part of cleanup failed.
	assert.Equal(t, 7, len(files))
	for _, file := range files {
		assert.True(t, strings.HasSuffix(file, ".tar"))
	}
}

// TestPackageItemsQueued checks to see if dpn_package pushed items
// into the dpn_ingest_store NSQ topic.
func TestPackageItemsQueued(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	stats, err := _context.NSQClient.GetStats()
	require.Nil(t, err)
	foundTopic := false
	for _, topic := range stats.Topics {
		if topic.TopicName == _context.Config.DPN.DPNIngestStoreWorker.NsqTopic {
			// All 7 packaged bags should show up in the storage queue
			foundTopic = true
			assert.EqualValues(t, uint64(7), topic.MessageCount)
		}
	}
	assert.True(t, foundTopic, "Nothing was queued in %s",
		_context.Config.DPN.DPNIngestStoreWorker.NsqTopic)
}

// Test the JSON serialized WorkItemState. Param WorkItemState is a
// string of JSON data. Param detail describes which object we're
// testing and where the JSON came from, so failure messages can be
// more informative.
func testPackageWorkItemState(t *testing.T, _context *context.Context, workItemState, detail string) {
	dpnIngestManifest := models.NewDPNIngestManifest(nil)
	err := json.Unmarshal([]byte(workItemState), dpnIngestManifest)
	require.Nil(t, err, "Could not unmarshal state")
	testPackageManifest(t, _context, dpnIngestManifest, detail)
}

func testPackageManifest(t *testing.T, _context *context.Context, dpnIngestManifest *models.DPNIngestManifest, detail string) {
	require.NotNil(t, dpnIngestManifest.PackageSummary, detail)
	require.NotNil(t, dpnIngestManifest.ValidateSummary, detail)

	assert.False(t, dpnIngestManifest.PackageSummary.StartedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.PackageSummary.FinishedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.PackageSummary.HasErrors(), detail)

	assert.False(t, dpnIngestManifest.ValidateSummary.StartedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.ValidateSummary.FinishedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.ValidateSummary.HasErrors(), detail)

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

	// Bag has not yet been stored in Glacier, so this should be empty.
	assert.Empty(t, dpnIngestManifest.StorageURL, detail)
}
