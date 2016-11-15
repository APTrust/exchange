package integration_test

import (
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"strings"
	"testing"
)

// getObjectIdentifiers returns the identifiers of objects that
// apps/test_push_to_dpn marks as "Push to DPN". These objects
// should wind up in the dpn_copy queue.
func identifiersPushedToDPN() []string {
	identifiers := make([]string, 7)
	for index, s3Key := range testutil.INTEGRATION_GOOD_BAGS[0:7] {
		identifier := strings.Replace(s3Key, "aptrust.receiving.test.", "", 1)
		identifier = strings.Replace(identifier, ".tar", "", 1)
		identifiers[index] = identifier
	}
	return identifiers
}

// We should have created one WorkItem for each DPN ingest request.
func TestWorkItemsCreatedAndQueued(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	expectedIdentifiers := identifiersPushedToDPN()
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	params := url.Values{}
	params.Set("item_action", "DPN")
	params.Set("page", "1")
	params.Set("per_page", "100")
	resp := _context.PharosClient.WorkItemList(params)
	require.Nil(t, resp.Error)
	assert.Equal(t, len(expectedIdentifiers), resp.Count)
	for _, workItem := range resp.WorkItems() {
		found := false
		queued := false
		currentIdentifier := ""
		for _, identifier := range expectedIdentifiers {
			currentIdentifier = identifier
			if workItem.ObjectIdentifier == identifier {
				found = true
				if workItem.QueuedAt != nil && !workItem.QueuedAt.IsZero() {
					queued = true
				}
				break
			}
		}
		assert.True(t, found, "No WorkItem for object %s", currentIdentifier)
		assert.True(t, queued, "Object %s was not queued", currentIdentifier)
	}

	// In addition to checking whether Pharos thinks the items are queued,
	// let's ask NSQ as well.
	stats, err := _context.NSQClient.GetStats()
	require.Nil(t, err)
	foundFetchTopic := false
	foundStoreTopic := false
	foundRecordTopic := false
	for _, topic := range stats.Data.Topics {
		if topic.TopicName == _context.Config.FetchWorker.NsqTopic {
			// We fetch 16 bags in our integration tests.
			// They're not all valid, but we should have that many in the queue.
			foundFetchTopic = true
			assert.EqualValues(t, uint64(16), topic.MessageCount)
		} else if topic.TopicName == _context.Config.StoreWorker.NsqTopic {
			// All of the 11 valid bags should have made it into the store topic.
			foundStoreTopic = true
			assert.EqualValues(t, uint64(11), topic.MessageCount)
		} else if topic.TopicName == _context.Config.RecordWorker.NsqTopic {
			// All of the 11 valid bags should have made it into the record topic.
			foundRecordTopic = true
			assert.EqualValues(t, uint64(11), topic.MessageCount)
		}
	}
	assert.True(t, foundFetchTopic, "Nothing was queued in %s",
		_context.Config.FetchWorker.NsqTopic)
	assert.True(t, foundStoreTopic, "Nothing was queued in %s",
		_context.Config.StoreWorker.NsqTopic)
	assert.True(t, foundRecordTopic, "Nothing was queued in %s",
		_context.Config.RecordWorker.NsqTopic)
}

// We should have created one DPNWorkItem for each replication request
// that we synched from the other nodes.
func TestDPNWorkItemsCreatedAndQueued(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	// Use local DPN REST client to check for all replication requests
	// where ToNode is our LocalNode.
	//
	// Then check Pharos for a DPNWorkItem for each of these replications.
	// The DPNWorkItem should exist, and should have a QueuedAt timestamp.
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")

	// Check DPNWorkItems for ReplicationTransfers
	dpnClient, err := network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	require.Nil(t, err)
	xferParams := url.Values{}
	xferParams.Set("to_node", _context.Config.DPN.LocalNode)
	dpnResp := dpnClient.ReplicationTransferList(xferParams)
	require.Nil(t, dpnResp.Error)
	for _, xfer := range dpnResp.ReplicationTransfers() {
		params := url.Values{}
		params.Set("identifier", xfer.ReplicationId)
		params.Set("task", "replication")
		pharosResp := _context.PharosClient.DPNWorkItemList(params)
		require.Nil(t, pharosResp.Error)
		require.Equal(t, 1, pharosResp.Count)
		dpnWorkItem := pharosResp.DPNWorkItem()
		require.NotNil(t, dpnWorkItem.QueuedAt)
		assert.False(t, dpnWorkItem.QueuedAt.IsZero())
	}

	// Check DPNWorkItems RestoreTransfers
	xferParams.Set("from_node", _context.Config.DPN.LocalNode)
	dpnResp = dpnClient.RestoreTransferList(xferParams)
	require.Nil(t, dpnResp.Error)
	for _, xfer := range dpnResp.RestoreTransfers() {
		params := url.Values{}
		params.Set("identifier", xfer.RestoreId)
		params.Set("task", "restore")
		pharosResp := _context.PharosClient.DPNWorkItemList(params)
		require.Nil(t, pharosResp.Error)
		require.Equal(t, 1, pharosResp.Count)
		dpnWorkItem := pharosResp.DPNWorkItem()
		require.NotNil(t, dpnWorkItem.QueuedAt)
		assert.False(t, dpnWorkItem.QueuedAt.IsZero())
	}

	// Check NSQ as well.
	stats, err := _context.NSQClient.GetStats()
	require.Nil(t, err)
	foundPackageTopic := false
	foundCopyTopic := false
	foundRestoreTopic := false
	for _, topic := range stats.Data.Topics {
		if topic.TopicName == _context.Config.DPN.DPNPackageWorker.NsqTopic {
			// apps/test_push_to_dpn.go requests that items
			// testutil.INTEGRATION_GOOD_BAGS[0:7] be sent to DPN,
			// so we should find seven items in the package queue
			foundPackageTopic = true
			assert.EqualValues(t, uint64(7), topic.MessageCount)
		} else if topic.TopicName == _context.Config.DPN.DPNCopyWorker.NsqTopic {
			// Fixture data has 4 replications: one from each remote node
			foundCopyTopic = true
			assert.EqualValues(t, uint64(4), topic.MessageCount)
		} else if topic.TopicName == _context.Config.DPN.DPNRestoreWorker.NsqTopic {
			// Fixture data has 4 restores: one from each remote node
			foundRestoreTopic = true
			assert.EqualValues(t, uint64(4), topic.MessageCount)
		}
	}
	assert.True(t, foundPackageTopic, "Nothing was queued in %s",
		_context.Config.DPN.DPNPackageWorker.NsqTopic)
	assert.True(t, foundCopyTopic, "Nothing was queued in %s",
		_context.Config.DPN.DPNCopyWorker.NsqTopic)
	assert.True(t, foundRestoreTopic, "Nothing was queued in %s",
		_context.Config.DPN.DPNRestoreWorker.NsqTopic)
}
