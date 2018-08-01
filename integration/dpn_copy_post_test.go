package integration_test

import (
	"fmt"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"path/filepath"
	"testing"
)

func TestItemsCopiedToStaging(t *testing.T) {
	// Make sure that each of the expected bags has shown
	// up in our test staging area.
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	for i := 2; i <= 5; i++ {
		_context, err := testutil.GetContext("integration.json")
		require.Nil(t, err, "Could not create context")
		filename := fmt.Sprintf("00000000-0000-4000-a000-00000000000%d.tar", i)
		path := filepath.Join(_context.Config.DPN.StagingDirectory, filename)
		assert.True(t, fileutil.FileExists(path), "File %s was not copied", path)
	}
}

func TestReplicationDPNWorkItems(t *testing.T) {
	// Make sure that our code updated the DPNWorkItem record for each
	// ReplicationTransfer.
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	params := url.Values{}
	params.Set("task", "replication")
	params.Set("page", "1")
	params.Set("per_page", "100")
	resp := _context.PharosClient.DPNWorkItemList(params)
	require.Nil(t, resp.Error)
	for _, item := range resp.DPNWorkItems() {
		require.NotNil(t, item.QueuedAt, "QueuedAt nil for %s", item.Identifier)
		require.NotNil(t, item.Note, "Note nil for %s", item.Identifier)
		require.NotNil(t, item.State, "State nil for %s", item.Identifier)
		assert.False(t, item.QueuedAt.IsZero(), "QueuedAt is zero for %s", item.Identifier)
		assert.NotEmpty(t, item.Note, "Note is empty for %s", item.Identifier)
		assert.True(t, len(*item.State) > 250, "State JSON too short for %s", item.Identifier)
	}
}

func TestCopyResultSentToRemoteNodes(t *testing.T) {
	// Query the FromNode of each copied bag to make sure that
	// we sent a fixity value back to the ingest node.
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	dpnClient, err := network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	require.Nil(t, err, "Couldn't build DPN REST client: %v", err)

	remoteClients, err := dpnClient.GetRemoteClients()
	require.Nil(t, err, "Couldn't build remote DPN clients: %v", err)

	// These identifiers are in the fixture data for dpn-server.
	// Key is the FromNode, value is the ReplicationId
	xferIdentifiers := map[string]string{
		"chron": "20000000-0000-4000-a000-000000000007",
		"hathi": "30000000-0000-4000-a000-000000000001",
		"sdr":   "40000000-0000-4000-a000-000000000013",
		"tdr":   "50000000-0000-4000-a000-000000000019",
	}

	for fromNode, identifier := range xferIdentifiers {
		client := remoteClients[fromNode]
		require.NotNil(t, client, "No DPN REST client for %s", fromNode)
		resp := client.ReplicationTransferGet(identifier)
		require.Nil(t, resp.Error)
		xfer := resp.ReplicationTransfer()
		require.NotNil(t, xfer, "ReplicationTransfer %s is missing", identifier)
		assert.NotEmpty(t, xfer.FixityValue, "Empty fixity for %s", identifier)
		assert.True(t, xfer.StoreRequested, "StoreRequested should not be false for %s", identifier)
	}
}

func TestValidationQueue(t *testing.T) {
	// Make sure the copied DPN bags made it into the validation queue.
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	stats, err := _context.NSQClient.GetStats()
	require.Nil(t, err)
	foundTopic := false
	for _, topic := range stats.Topics {
		if topic.TopicName == _context.Config.DPN.DPNValidationWorker.NsqTopic {
			// Should have 4 items. One from each remote node.
			foundTopic = true
			assert.EqualValues(t, uint64(4), topic.MessageCount)
		}
	}
	assert.True(t, foundTopic, "Nothing was queued in %s",
		_context.Config.DPN.DPNValidationWorker.NsqTopic)
}
