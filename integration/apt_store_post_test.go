package integration_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
)

/*
These tests check the results of the integration tests for
the app apt_store. See the ingest_test.sh script in
the scripts folder, which sets up an integration context, runs
the apt_store.
*/

func TestStoreResults(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	// Load config
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()

	// Find the log file that apt_store created when it was running
	// with the "config/integration.json" config options. We'll read
	// that file.
	pathToJsonLog := filepath.Join(config.LogDirectory, "apt_store.json")
	bagNames := append(testutil.INTEGRATION_GOOD_BAGS, testutil.INTEGRATION_GLACIER_BAGS...)
	for _, bagName := range bagNames {
		ingestManifest, err := testutil.FindIngestManifestInLog(pathToJsonLog, bagName)
		assert.Nil(t, err)
		if err != nil {
			continue
		}
		// TODO: Test WorkItem (stage, status, etc.) below
		storeTestCommon(t, bagName, ingestManifest, config)
	}
}

func storeTestCommon(t *testing.T, bagName string, ingestManifest *models.IngestManifest, config *models.Config) {
	// Test some basic object properties
	assert.NotEmpty(t, ingestManifest.WorkItemId, "WorkItemId should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.S3Bucket, "S3Bucket should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.S3Key, "S3Key should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.ETag, "ETag should not be empty for %s", bagName)

	// Make sure the result has some basic info
	assert.True(t, ingestManifest.StoreResult.Attempted,
		"StoreResult.Attempted should be true for %s", bagName)
	assert.True(t, ingestManifest.StoreResult.AttemptNumber > 0,
		"StoreResult.AttemptNumber should be > 0 %s", bagName)
	assert.NotEmpty(t, ingestManifest.StoreResult.StartedAt,
		"StoreResult.StartedAt should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.StoreResult.FinishedAt,
		"StoreResult.FinishedAt should not be empty for %s", bagName)
	assert.Empty(t, ingestManifest.StoreResult.Errors,
		"StoreResult.Errors should be empty for %s", bagName)
	assert.True(t, ingestManifest.StoreResult.Retry,
		"StoreResult.Retry should be true for %s", bagName)

	// Make sure the GenericFiles include info about where we put them.
	for _, gf := range ingestManifest.Object.GenericFiles {
		// Test only files that we're SUPPOSED to store.
		if !util.HasSavableName(gf.OriginalPath()) && !isSpecialJunkFile(gf) {
			continue
		}
		assert.True(t, strings.HasPrefix(gf.URI, "https://s3.amazonaws.com/"),
			"URI missing or invalid for %s", gf.Identifier)
		assert.True(t, strings.HasSuffix(gf.URI, gf.IngestUUID),
			"URI should end with '%s' for %s", gf.IngestUUID, gf.Identifier)
		assert.True(t, gf.URI == gf.IngestStorageURL,
			"URI does not match IngestStorageUrl for %s", gf.Identifier)
		assert.True(t, strings.HasSuffix(gf.IngestReplicationURL, gf.IngestUUID),
			"IngestReplicationURL should end with '%s' for %s", gf.IngestUUID, gf.Identifier)
		assert.True(t, strings.Contains(gf.URI, config.PreservationBucket),
			"URI does not point to perservation bucket %s for %s",
			config.PreservationBucket, gf.Identifier)
		assert.True(t, strings.Contains(gf.IngestStorageURL, config.PreservationBucket),
			"IngestStorageURL does not point to perservation bucket %s for %s",
			config.PreservationBucket, gf.Identifier)
		assert.True(t, strings.Contains(gf.IngestReplicationURL, config.ReplicationBucket),
			"IngestReplicationURL does not point to replication bucket %s for %s",
			config.PreservationBucket, gf.Identifier)
		assert.False(t, gf.IngestStoredAt.IsZero())
		assert.False(t, gf.IngestReplicatedAt.IsZero())
	}
}

// Special test for bug https://www.pivotaltracker.com/story/show/151265762
// We want to make sure we actually did save this special junk file.
func isSpecialJunkFile(gf *models.GenericFile) bool {
	return gf.Identifier == "test.edu/example.edu.sample_ds_store_and_empty/data/._DS_StoreTest"
}
