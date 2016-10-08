package integration_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

/*
These tests check the results of the integration tests for
the app apt_record. See the ingest_test.sh script in
the scripts folder, which sets up an integration context, runs
the apt_record.
*/

func TestRecordResults(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	// Load config
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()

	// Find the log file that apt_record created when it was running
	// with the "config/integration.json" config options. We'll read
	// that file.
	pathToJsonLog := filepath.Join(config.LogDirectory, "apt_record.json")
	for _, bagName := range testutil.INTEGRATION_GOOD_BAGS {
		ingestManifest, err := testutil.FindResultInLog(pathToJsonLog, bagName)
		assert.Nil(t, err)
		if err != nil {
			continue
		}
		// TODO: Test WorkItem (stage, status, etc.) below.
		recordTestCommon(t, bagName, ingestManifest)
	}
}

func recordTestCommon(t *testing.T, bagName string, ingestManifest *models.IngestManifest) {
	// Test some basic object properties
	assert.NotEmpty(t, ingestManifest.WorkItemId, "WorkItemId should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.S3Bucket, "S3Bucket should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.S3Key, "S3Key should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.ETag, "ETag should not be empty for %s", bagName)

	// Make sure the result has some basic info in RecordResult
	assert.True(t, ingestManifest.RecordResult.Attempted,
		"RecordResult.Attempted should be true for %s", bagName)
	assert.True(t, ingestManifest.RecordResult.AttemptNumber > 0,
		"RecordResult.AttemptNumber should be > 0 %s", bagName)
	assert.NotEmpty(t, ingestManifest.RecordResult.StartedAt,
		"RecordResult.StartedAt should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.RecordResult.FinishedAt,
		"RecordResult.FinishedAt should not be empty for %s", bagName)
	assert.Empty(t, ingestManifest.RecordResult.Errors,
		"RecordResult.Errors should be empty for %s", bagName)
	assert.True(t, ingestManifest.RecordResult.Retry,
		"RecordResult.Retry should be true for %s", bagName)

	// Make sure the result has some basic info in CleanupResult
	assert.True(t, ingestManifest.CleanupResult.Attempted,
		"CleanupResult.Attempted should be true for %s", bagName)
	assert.True(t, ingestManifest.CleanupResult.AttemptNumber > 0,
		"CleanupResult.AttemptNumber should be > 0 %s", bagName)
	assert.NotEmpty(t, ingestManifest.CleanupResult.StartedAt,
		"CleanupResult.StartedAt should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.CleanupResult.FinishedAt,
		"CleanupResult.FinishedAt should not be empty for %s", bagName)
	assert.Empty(t, ingestManifest.CleanupResult.Errors,
		"CleanupResult.Errors should be empty for %s", bagName)
	assert.True(t, ingestManifest.CleanupResult.Retry,
		"CleanupResult.Retry should be true for %s", bagName)


	// Make sure our IntellectualObject got all of its PremisEvents
	obj := ingestManifest.Object
	require.Equal(t, 4, len(obj.PremisEvents))

	// Make sure this item was deleted from the receiving bucket
	// after ingest completed.
	assert.False(t, obj.IngestDeletedFromReceivingAt.IsZero(),
		"Object %s was not deleted from receiving bucket", bagName)
	assert.Empty(t, obj.IngestErrorMessage)

	// Check the object-level events
	creationEvents := obj.FindEventsByType(constants.EventCreation)
	idEvents := obj.FindEventsByType(constants.EventIdentifierAssignment)
	ingestEvents := obj.FindEventsByType(constants.EventIngestion)
	accessEvents := obj.FindEventsByType(constants.EventAccessAssignment)
	assert.Equal(t, 1, len(accessEvents), "Missing access event for %s", bagName)
	assert.Equal(t, 1, len(creationEvents), "Missing creation event for %s", bagName)
	assert.Equal(t, 1, len(idEvents), "Missing identifier assignment event for %s", bagName)
	assert.Equal(t, 1, len(ingestEvents), "Missing ingest event for %s", bagName)

	for _, event := range obj.PremisEvents {
		assert.True(t, event.Id > 0, "Event %s was not saved for %s", event.EventType, obj.Identifier)
		assert.True(t, event.IntellectualObjectId > 0,
			"event.IntellectualObjectId not set for %s %s", event.EventType, obj.Identifier)
		assert.False(t, event.DateTime.IsZero(),
			"event.DateTime was not set for %s %s", event.EventType, obj.Identifier)
		assert.False(t, event.CreatedAt.IsZero(),
			"event.CreatedAt was not set for %s %s", event.EventType, obj.Identifier)
		assert.False(t, event.UpdatedAt.IsZero(),
			"event.UpdatedAt was not set for %s %s", event.EventType, obj.Identifier)

		assert.True(t, util.LooksLikeUUID(event.Identifier),
			"Identifier for %s %s doesn't look like a UUID", event.EventType, obj.Identifier)
		assert.NotEmpty(t, event.EventType, "EventType missing for %s %s", obj.Identifier, event.Identifier)
		assert.NotEmpty(t, event.Detail, "Detail is empty for %s %s", event.EventType, obj.Identifier)
		assert.NotEmpty(t, event.Outcome, "Outcome is empty for %s %s", event.EventType, obj.Identifier)
		assert.NotEmpty(t, event.OutcomeDetail,
			"OutcomeDetail is empty for %s %s", event.EventType, obj.Identifier)
		assert.NotEmpty(t, event.Object, "Object is empty for %s %s", event.EventType, obj.Identifier)
		assert.NotEmpty(t, event.Agent, "Agent is empty for %s %s", event.EventType, obj.Identifier)
		assert.NotEmpty(t, event.OutcomeInformation,
			"OutcomeInformation is empty for %s %s", event.EventType, obj.Identifier)
		assert.Equal(t, obj.Identifier, event.IntellectualObjectIdentifier,
			"IntellectualObjectIdentifier is wrong for %s %s", event.EventType, obj.Identifier)
	}

	for _, gf := range obj.GenericFiles {
		// Make sure checksums are present
		require.Equal(t, 2, len(gf.Checksums),
			"Checksums should be %s, found %d for %s", 2, len(gf.Checksums), gf.Identifier)
		md5 := gf.GetChecksumByDigest(constants.AlgMd5)
		sha256 := gf.GetChecksumByDigest(constants.AlgSha256)
		require.NotNil(t, md5, "Missing md5 digest for for %s", gf.Identifier)
		require.NotNil(t, sha256, "Missing sha256 digest for for %s", gf.Identifier)

		// Make sure that these checksums were saved
		assert.True(t, md5.Id > 0, "md5 was not saved for %s", gf.Identifier)
		assert.True(t, md5.GenericFileId > 0, "md5.GenericFileId not set for %s", gf.Identifier)
		assert.False(t, md5.CreatedAt.IsZero(), "md5.CreatedAt was not set for %s", gf.Identifier)
		assert.False(t, md5.UpdatedAt.IsZero(), "md5.UpdatedAt was not set for %s", gf.Identifier)

		assert.True(t, sha256.Id > 0, "sha256 was not saved for %s", gf.Identifier)
		assert.True(t, sha256.GenericFileId > 0, "sha256.GenericFileId not set for %s", gf.Identifier)
		assert.False(t, sha256.CreatedAt.IsZero(), "sha256.CreatedAt was not set for %s", gf.Identifier)
		assert.False(t, sha256.UpdatedAt.IsZero(), "sha256.UpdatedAt was not set for %s", gf.Identifier)

		// Make sure PremisEvents are present
		require.Equal(t, 6, len(gf.PremisEvents),
			"PremisEvents count should be %s, found %d for %s", 6, len(gf.PremisEvents), gf.Identifier)
		assert.Equal(t, 1, len(gf.FindEventsByType(constants.EventFixityCheck)),
			"Missing fixity check event for %s", gf.Identifier)
		assert.Equal(t, 1, len(gf.FindEventsByType(constants.EventDigestCalculation)),
			"Missing digest calculation event for %s", gf.Identifier)
		assert.Equal(t, 2, len(gf.FindEventsByType(constants.EventIdentifierAssignment)),
			"Missing identifier assignment event(s) for %s", gf.Identifier)
		assert.Equal(t, 1, len(gf.FindEventsByType(constants.EventReplication)),
			"Missing replication event for %s", gf.Identifier)
		assert.Equal(t, 1, len(gf.FindEventsByType(constants.EventIngestion)),
			"Missing ingestion event for %s", gf.Identifier)

		for _, event := range gf.PremisEvents {
			assert.True(t, event.Id > 0, "Event %s was not saved for %s", event.EventType, gf.Identifier)
			assert.True(t, event.IntellectualObjectId > 0,
				"event.IntellectualObjectId not set for %s %s", event.EventType, gf.Identifier)
			assert.True(t, event.GenericFileId > 0,
				"event.GenericFileId not set for %s %s", event.EventType, gf.Identifier)
			assert.False(t, event.DateTime.IsZero(),
				"event.DateTime was not set for %s %s", event.EventType, gf.Identifier)
			assert.False(t, event.CreatedAt.IsZero(),
				"event.CreatedAt was not set for %s %s", event.EventType, gf.Identifier)
			assert.False(t, event.UpdatedAt.IsZero(),
				"event.UpdatedAt was not set for %s %s", event.EventType, gf.Identifier)

			assert.True(t, util.LooksLikeUUID(event.Identifier),
				"Identifier for %s %s doesn't look like a UUID", event.EventType, gf.Identifier)
			assert.NotEmpty(t, event.EventType, "EventType missing for %s %s", gf.Identifier, event.Identifier)
			assert.NotEmpty(t, event.Detail, "Detail is empty for %s %s", event.EventType, gf.Identifier)
			assert.NotEmpty(t, event.Outcome, "Outcome is empty for %s %s", event.EventType, gf.Identifier)
			assert.NotEmpty(t, event.OutcomeDetail,
				"OutcomeDetail is empty for %s %s", event.EventType, gf.Identifier)
			assert.NotEmpty(t, event.Object, "Object is empty for %s %s", event.EventType, gf.Identifier)
			assert.NotEmpty(t, event.Agent, "Agent is empty for %s %s", event.EventType, gf.Identifier)
			assert.NotEmpty(t, event.OutcomeInformation,
				"OutcomeInformation is empty for %s %s", event.EventType, gf.Identifier)
			assert.Equal(t, obj.Identifier, event.IntellectualObjectIdentifier,
				"IntellectualObjectIdentifier is wrong for %s %s", event.EventType, gf.Identifier)
			assert.Equal(t, gf.Identifier, event.GenericFileIdentifier,
				"GenericFileIdentifier is wrong for %s %s", event.EventType, gf.Identifier)
		}
	}
}
