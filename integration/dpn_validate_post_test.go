package integration_test

import (
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

func TestValidationDPNWorkItems(t *testing.T) {
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
		assert.Equal(t, "Bag passed validation", *item.Note, "Note is incorrect for %s", item.Identifier)
		assert.True(t, len(*item.State) > 250, "State JSON too short for %s", item.Identifier)
	}
}

func TestValidItemInStorageQueue(t *testing.T) {
	// Make sure the copied DPN bags made it into the validation queue.
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	stats, err := _context.NSQClient.GetStats()
	require.Nil(t, err)
	foundTopic := false
	for _, topic := range stats.Data.Topics {
		if topic.TopicName == _context.Config.DPN.DPNReplicationStoreWorker.NsqTopic {
			// All 4 of the valid bags should appear in the store queue.
			foundTopic = true
			assert.EqualValues(t, uint64(4), topic.MessageCount)
		}
	}
	assert.True(t, foundTopic, "Nothing was queued in %s",
		_context.Config.DPN.DPNReplicationStoreWorker.NsqTopic)
}
