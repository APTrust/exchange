package integration_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/stats"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

/*
These tests check the results of the integration tests for
the app apt_queue. See the ingest_test.rb script in
the scripts folder, which sets up an integration context, runs
apt_queue, and then runs this program to check the
stats output of apt_queue to make sure all went well.
*/

// Returns two Stats objects: the expected stats, from our test data dir,
// and the actual stats, from the JSON file that the bucket reader dumped
// out last time it ran
func getQueueOutputs(t *testing.T) (expected *stats.APTQueueStats, actual *stats.APTQueueStats) {
	configFile := filepath.Join("config", "integration.json")
	appConfig, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)

	// This JSON file is part of our code repo.
	// It contains the output we expect from a success test run.
	pathToExpectedJson, err := fileutil.RelativeToAbsPath(filepath.Join("testdata",
		"integration_results", "apt_queue_stats.json"))
	require.Nil(t, err)
	expected, err = stats.APTQueueStatsLoadFromFile(pathToExpectedJson)
	require.Nil(t, err)

	// This JSON file is recreated every time we run apt_bucket_reader
	// as part of the integration tests. It contains the output of the
	// actual test run.
	pathToActualJson, err := fileutil.ExpandTilde(filepath.Join(appConfig.LogDirectory, "apt_queue_stats.json"))
	require.Nil(t, err)
	actual, err = stats.APTQueueStatsLoadFromFile(pathToActualJson)
	require.Nil(t, err)

	return expected, actual
}

func TestQueueStats(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	expected, actual := getQueueOutputs(t)
	testItemsQueued(t, expected, actual)
	testItemsMarkedAsQueued(t, expected, actual)
	testQueueErrors(t, expected, actual)
	testQueueWarnings(t, expected, actual)
}

func TestRestorationQueue(t *testing.T) {
	// Make sure items marked for restoration made it into the restoration queue.
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	stats, err := _context.NSQClient.GetStats()
	require.Nil(t, err)
	foundTopic := false
	for _, topic := range stats.Data.Topics {
		if topic.TopicName == _context.Config.RestoreWorker.NsqTopic {
			// Should have 7 items. See apt_mark_for_restore_test.go
			foundTopic = true
			assert.EqualValues(t, uint64(8), topic.MessageCount,
				"NSQ restore topic should have 8 items")
		}
	}
	assert.True(t, foundTopic, "Nothing was queued in %s",
		_context.Config.RestoreWorker.NsqTopic)
}

func testItemsQueued(t *testing.T, expected *stats.APTQueueStats, actual *stats.APTQueueStats) {
	for _, itemList := range expected.ItemsQueued {
		for _, item := range itemList {
			matchingItem, topic := actual.FindQueuedItemByName(item.Name)
			assert.NotNil(t, matchingItem, "WorkItem %s missing from ItemsQueued", item.Name)
			assert.Equal(t, "apt_restore_topic", topic)
		}
	}
}

func testItemsMarkedAsQueued(t *testing.T, expected *stats.APTQueueStats, actual *stats.APTQueueStats) {
	for _, item := range expected.ItemsMarkedAsQueued {
		matchingItem := actual.FindMarkedItemByName(item.Name)
		assert.NotNil(t, matchingItem, "WorkItem %s missing from ItemsMarkedAsQueued", item.Name)
	}
}

func testQueueErrors(t *testing.T, expected *stats.APTQueueStats, actual *stats.APTQueueStats) {
	assert.Equal(t, 0, len(actual.Errors))
}

func testQueueWarnings(t *testing.T, expected *stats.APTQueueStats, actual *stats.APTQueueStats) {
	assert.Equal(t, 0, len(actual.Warnings))
}
