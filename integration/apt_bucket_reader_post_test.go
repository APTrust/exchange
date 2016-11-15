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
the app apt_bucket_reader. See the ingest_test.sh script in
the scripts folder, which sets up an integration context, runs
the bucket reader, and then runs this program to check the
stats output of the bucket reader to make sure all went well.
*/

// Returns two Stats objects: the expected stats, from our test data dir,
// and the actual stats, from the JSON file that the bucket reader dumped
// out last time it ran
func getBucketReaderOutputs(t *testing.T) (expected *stats.APTBucketReaderStats, actual *stats.APTBucketReaderStats) {
	configFile := filepath.Join("config", "integration.json")
	appConfig, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)

	// This JSON file is part of our code repo.
	// It contains the output we expect from a success test run.
	pathToExpectedJson, err := fileutil.RelativeToAbsPath(filepath.Join("testdata",
		"integration_results", "bucket_reader_stats.json"))
	require.Nil(t, err)
	expected, err = stats.APTBucketReaderStatsLoadFromFile(pathToExpectedJson)
	require.Nil(t, err)

	// This JSON file is recreated every time we run apt_bucket_reader
	// as part of the integration tests. It contains the output of the
	// actual test run.
	pathToActualJson, err := fileutil.ExpandTilde(filepath.Join(appConfig.LogDirectory, "bucket_reader_stats.json"))
	require.Nil(t, err)
	actual, err = stats.APTBucketReaderStatsLoadFromFile(pathToActualJson)
	require.Nil(t, err)

	return expected, actual
}

func TestInstutionsCached(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	expected, actual := getBucketReaderOutputs(t)
	testInstCache(t, expected, actual)
	testWorkItemsCached(t, expected, actual)
	testWorkItemsFetched(t, expected, actual)
	testWorkItemsCreated(t, expected, actual)
	testWorkItemsQueued(t, expected, actual)
	testWorkItemsMarkedAsQueued(t, expected, actual)
	testS3Items(t, expected, actual)
	testErrors(t, expected, actual)
	testWarnings(t, expected, actual)
}

func testInstCache(t *testing.T, expected *stats.APTBucketReaderStats, actual *stats.APTBucketReaderStats) {
	for _, inst := range expected.InstitutionsCached {
		assert.True(t, actual.InstitutionsCachedContains(inst.Identifier),
			"Institution %s missing from inst cache", inst.Identifier)
	}
}

func testWorkItemsCached(t *testing.T, expected *stats.APTBucketReaderStats, actual *stats.APTBucketReaderStats) {
	for _, item := range expected.WorkItemsCached {
		matchingItem, _ := actual.FindWorkItemByNameAndEtag("WorkItemsCached", item.Name, item.ETag)
		assert.NotNil(t, matchingItem,
			"WorkItem %s missing from WorkItemsCache", item.Name)
	}
}

func testWorkItemsFetched(t *testing.T, expected *stats.APTBucketReaderStats, actual *stats.APTBucketReaderStats) {
	for _, item := range expected.WorkItemsFetched {
		matchingItem, _ := actual.FindWorkItemByNameAndEtag("WorkItemsFetched", item.Name, item.ETag)
		assert.NotNil(t, matchingItem,
			"WorkItem %s missing from WorkItemsFetched", item.Name)
	}
}

func testWorkItemsCreated(t *testing.T, expected *stats.APTBucketReaderStats, actual *stats.APTBucketReaderStats) {
	for _, item := range expected.WorkItemsCreated {
		matchingItem, _ := actual.FindWorkItemByNameAndEtag("WorkItemsCreated", item.Name, item.ETag)
		assert.NotNil(t, matchingItem,
			"WorkItem %s missing from WorkItemsCreated", item.Name)
	}
}

func testWorkItemsQueued(t *testing.T, expected *stats.APTBucketReaderStats, actual *stats.APTBucketReaderStats) {
	for _, item := range expected.WorkItemsQueued {
		matchingItem, _ := actual.FindWorkItemByNameAndEtag("WorkItemsQueued", item.Name, item.ETag)
		assert.NotNil(t, matchingItem,
			"WorkItem %s missing from WorkItemsQueued", item.Name)
	}
}

func testWorkItemsMarkedAsQueued(t *testing.T, expected *stats.APTBucketReaderStats, actual *stats.APTBucketReaderStats) {
	for _, item := range expected.WorkItemsMarkedAsQueued {
		matchingItem, _ := actual.FindWorkItemByNameAndEtag("WorkItemsMarkedAsQueued", item.Name, item.ETag)
		assert.NotNil(t, matchingItem,
			"WorkItem %s missing from WorkItemsMarkedAsQueued", item.Name)
	}
}

func testS3Items(t *testing.T, expected *stats.APTBucketReaderStats, actual *stats.APTBucketReaderStats) {
	for _, item := range expected.S3Items {
		assert.True(t, actual.S3ItemWasFound(item))
	}
}

func testErrors(t *testing.T, expected *stats.APTBucketReaderStats, actual *stats.APTBucketReaderStats) {
	assert.Equal(t, 0, len(actual.Errors))
}

func testWarnings(t *testing.T, expected *stats.APTBucketReaderStats, actual *stats.APTBucketReaderStats) {
	assert.Equal(t, 0, len(actual.Warnings))
}
