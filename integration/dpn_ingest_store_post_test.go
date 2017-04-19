package integration_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	dpn_testutil "github.com/APTrust/exchange/dpn/util/testutil"
	"github.com/APTrust/exchange/network"
	apt_testutil "github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
)

// TestIngestStoreWorkItems checks to see if Pharos WorkItems
// were updated correctly.
func TestIngestStoreWorkItems(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_, workItems, err := dpn_testutil.GetDPNWorkItems()
	require.Nil(t, err)
	for _, item := range workItems {
		assert.Equal(t, constants.StageRecord, item.Stage)
		assert.Equal(t, constants.StatusPending, item.Status)
		assert.Equal(t, "Bag copied to long-term storage", item.Note)
		assert.Equal(t, "", item.Node)
		assert.Equal(t, 0, item.Pid)
		assert.True(t, item.Retry)
	}
}

// TestIngestStoreWorkItemState checks to see if Pharos WorkItemState
// records were updated correctly.
func TestIngestStoreWorkItemState(t *testing.T) {
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
		testIngestStoreWorkItemState(t, _context, workItemState.State, detail)
	}

}

// TestIngestStoreJsonLog checks that all expected entries are present
// in the dpn_ingest_store.json log.
func TestIngestStoreJsonLog(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err)
	pathToLogFile := filepath.Join(_context.Config.LogDirectory, "dpn_ingest_store.json")
	for _, s3Key := range apt_testutil.INTEGRATION_GOOD_BAGS[0:7] {
		parts := strings.Split(s3Key, "/")
		tarFileName := parts[1]
		manifest, err := apt_testutil.FindDPNIngestManifestInLog(pathToLogFile, tarFileName)
		require.Nil(t, err)
		require.NotNil(t, manifest)

		detail := fmt.Sprintf("%s from JSON log", tarFileName)
		testIngestStoreManifest(t, _context, manifest, detail)
	}
}

// TestIngestStoreItemsQueued checks to see if dpn_ingest_store pushed items
// into the dpn_ingest_record NSQ topic.
func TestIngestStoreItemsQueued(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	stats, err := _context.NSQClient.GetStats()
	require.Nil(t, err)
	foundTopic := false
	for _, topic := range stats.Data.Topics {
		if topic.TopicName == _context.Config.DPN.DPNIngestRecordWorker.NsqTopic {
			// All 7 stored bags should show up in the record queue
			foundTopic = true
			assert.EqualValues(t, uint64(7), topic.MessageCount)
		}
	}
	assert.True(t, foundTopic, "Nothing was queued in %s",
		_context.Config.DPN.DPNIngestRecordWorker.NsqTopic)
}

// TestIngestStoreItemsAreInStorage makes sure that the items we sent off
// to long-term storage in AWS actually made it there.
func TestIngestStoreItemsAreInStorage(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	maxItemsToList := int64(1)
	// s3List lists bucket contents.
	s3List := network.NewS3ObjectList(
		constants.AWSVirginia,
		_context.Config.DPN.DPNPreservationBucket,
		maxItemsToList)
	// s3Head gets metadata about specific objects in S3/Glacier.
	s3Head := network.NewS3Head(_context.Config.APTrustS3Region,
		_context.Config.DPN.DPNPreservationBucket)

	pathToLogFile := filepath.Join(_context.Config.LogDirectory, "dpn_ingest_store.json")
	for _, s3Key := range apt_testutil.INTEGRATION_GOOD_BAGS[0:7] {
		parts := strings.Split(s3Key, "/")
		localTarFileName := parts[1] // APTrust bag name. E.g. "test.edu.test_123.tar"
		manifest, err := apt_testutil.FindDPNIngestManifestInLog(pathToLogFile, localTarFileName)
		require.Nil(t, err, "Could not find JSON record for %s", localTarFileName)
		parts = strings.Split(manifest.StorageURL, "/")
		dpnTarFileName := parts[len(parts)-1] // DPN bag name: <uuid>.tar
		s3List.GetList(dpnTarFileName)
		require.Empty(t, s3List.ErrorMessage)
		require.EqualValues(t, 1, len(s3List.Response.Contents), "Nothing in S3 for %s", dpnTarFileName)
		obj := s3List.Response.Contents[0]
		assert.Equal(t, dpnTarFileName, *obj.Key)

		// Make sure each item has the expected metadata.
		// s3Head.Response.Metadata is map[string]*string.
		s3Head.Head(dpnTarFileName)
		require.Empty(t, s3Head.ErrorMessage)
		metadata := s3Head.Response.Metadata
		require.NotNil(t, metadata, dpnTarFileName)

		dpnStoredFile := s3Head.DPNStoredFile()
		assert.NotEmpty(t, dpnStoredFile.Member)
		assert.NotEmpty(t, dpnStoredFile.FromNode)
		assert.NotEmpty(t, dpnStoredFile.TransferId)
		assert.NotEmpty(t, dpnStoredFile.LocalId)
		assert.NotEmpty(t, dpnStoredFile.Version)
	}
}

// Test the JSON serialized WorkItemState. Param WorkItemState is a
// string of JSON data. Param detail describes which object we're
// testing and where the JSON came from, so failure messages can be
// more informative.
func testIngestStoreWorkItemState(t *testing.T, _context *context.Context, workItemState, detail string) {
	dpnIngestManifest := models.NewDPNIngestManifest(nil)
	err := json.Unmarshal([]byte(workItemState), dpnIngestManifest)
	require.Nil(t, err, "Could not unmarshal state")
	testIngestStoreManifest(t, _context, dpnIngestManifest, detail)
}

func testIngestStoreManifest(t *testing.T, _context *context.Context, dpnIngestManifest *models.DPNIngestManifest, detail string) {
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

	// Make sure the ReplicationTransfer.Link looks good.
	for _, xfer := range dpnIngestManifest.ReplicationTransfers {
		instAndBagName := strings.Split(dpnIngestManifest.DPNBag.LocalId, "/")
		require.True(t, len(instAndBagName) > 0)
		institutionDomain := instAndBagName[0]
		// Link should begin with dpn.chron@ (or dpn.hathi, dpn.tdr, etc,
		// depending on to_node)
		expectedPrefix := fmt.Sprintf("dpn.%s@", xfer.ToNode)
		assert.True(t, strings.HasPrefix(xfer.Link, expectedPrefix))
		// Link should end with umich.edu/uuid.tar, where umich.edu
		// is the domain name of the APTrust depositor, and uuid is
		// the uuid of the DPN bag.
		expectedSuffix := fmt.Sprintf("%s/%s.tar", institutionDomain, dpnIngestManifest.DPNBag.UUID)
		assert.True(t, strings.HasSuffix(xfer.Link, expectedSuffix))
	}

	// Bag has been stored in Glacier, so this should be set to
	// "https://s3.amazonaws.com/aptrust.dpn.test/<UUID>.tar"
	expectedURL := fmt.Sprintf("https://s3.amazonaws.com/%s/%s.tar",
		_context.Config.DPN.DPNPreservationBucket,
		dpnIngestManifest.DPNBag.UUID)
	assert.Equal(t, expectedURL, dpnIngestManifest.StorageURL, detail)
}
