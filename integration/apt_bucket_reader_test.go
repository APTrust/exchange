package integration_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/stats"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

/*
These tests check the results of the integration tests for
the app apt_bucket_reader. See the process_items.sh script in
the scripts folder, which sets up an integration context, runs
the bucket reader, and then runs this program to check the
stats output of the bucket reader to make sure all went well.
*/

func shouldRunIntegrationTests() (bool) {
	return os.Getenv("RUN_EXCHANGE_INTEGRATION") == "true"
}


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
	if !shouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	expected, actual := getBucketReaderOutputs(t)
	for _, inst := range expected.InstitutionsCached {
		assert.True(t, actual.InstitutionsCachedContains(inst.Identifier),
			"Institution %s missing from inst cache", inst.Identifier)
	}
}
