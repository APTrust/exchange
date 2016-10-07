package integration_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

/*
These tests check the results of the integration tests for
the app apt_fetch. See the ingest_test.sh script in
the scripts folder, which sets up an integration context, runs
the apt_fetch.
*/

func TestFetchResults(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	// Load config
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()

	// Find the log file that apt_fetch created when it was running
	// with the "config/integration.json" config options. We'll read
	// that file.
	pathToJsonLog := filepath.Join(config.LogDirectory, "apt_fetch.json")
	for _, bagName := range testutil.INTEGRATION_GOOD_BAGS {
		ingestManifest, err := testutil.FindResultInLog(pathToJsonLog, bagName)
		assert.Nil(t, err)
		if err != nil {
			continue
		}
		fetcherTestCommon(t, bagName, ingestManifest)
		fetcherTestGoodBagResult(t, bagName, ingestManifest)
	}
	for _, bagName := range testutil.INTEGRATION_BAD_BAGS {
		ingestManifest, err := testutil.FindResultInLog(pathToJsonLog, bagName)
		assert.Nil(t, err)
		if err != nil {
			continue
		}
		fetcherTestCommon(t, bagName, ingestManifest)
		fetcherTestBadBagResult(t, bagName, ingestManifest)
	}
}

func fetcherTestGoodBagResult(t *testing.T, bagName string, ingestManifest *models.IngestManifest) {
	// These bags are valid and should have no validation errors.
	assert.Empty(t, ingestManifest.ValidateResult.Errors,
		"ValidateResult.Errors should be empty for %s", bagName)
	assert.False(t, ingestManifest.ValidateResult.ErrorIsFatal,
		"ValidateResult.ErrorIsFatal should be false for %s", bagName)
	assert.True(t, ingestManifest.ValidateResult.Retry,
		"ValidateResult.Retry should be true for %s", bagName)

	// We should have a valid IntellectualObject and files.
	obj := ingestManifest.Object
	assert.NotEmpty(t, obj.Identifier, "obj.Identifier should not be empty for %s", bagName)
	assert.NotEmpty(t, obj.BagName, "obj.BagName should not be empty for %s", bagName)
	assert.NotEmpty(t, obj.Institution, "obj.Institution should not be empty for %s", bagName)
	assert.NotEmpty(t, obj.InstitutionId, "obj.InstitutionId should not be empty for %s", bagName)
	assert.NotEmpty(t, obj.Title, "obj.Title should not be empty for %s", bagName)

	// Make sure that if these two tags were present, we copied their
	// values to the right places. These are important!
	descriptionTag := obj.FindTag("Internal-Sender-Description")
	if len(descriptionTag) > 0 && descriptionTag[0].Value != "" {
		assert.NotEmpty(t, obj.Description, "obj.Description should not be empty for %s", bagName)
	}
	altIdTag := obj.FindTag("Internal-Sender-Identifier")
	if len(altIdTag) > 0 && altIdTag[0].Value != "" {
		assert.NotEmpty(t, obj.AltIdentifier, "obj.AltIdentifier should not be empty for %s", bagName)
	}

	// Check the GenericFiles
	assert.True(t, len(obj.GenericFiles) > 0, "obj.GenericFiles should not be empty for %s", bagName)
	for i, gf := range obj.GenericFiles {
		assert.NotEmpty(t, gf.Identifier, "Bag %s file %s Identifier is missing", bagName, i)
		assert.NotEmpty(t, gf.IntellectualObjectIdentifier,
			"Bag %s file %s IntellectualObjectIdentifier is missing", bagName, i)
		assert.NotEmpty(t, gf.FileFormat, "Bag %s file %s FileFormat is missing", bagName, i)
		assert.True(t, gf.Size > 0, "Bag %s file %s Size is missing", bagName, i)
		assert.NotEmpty(t, gf.IngestFileType, "Bag %s file %s IngestFileType is missing", bagName, i)
		assert.NotEmpty(t, gf.IngestMd5, "Bag %s file %s IngestMd5 is missing", bagName, i)
		assert.NotEmpty(t, gf.IngestMd5GeneratedAt, "Bag %s file %s IngestMd5GeneratedAt is missing", bagName, i)
		assert.NotEmpty(t, gf.IngestMd5VerifiedAt, "Bag %s file %s IngestMd5VerifiedAt is missing", bagName, i)
		assert.NotEmpty(t, gf.IngestSha256, "Bag %s file %s IngestSha256 is missing", bagName, i)
		assert.NotEmpty(t, gf.IngestSha256GeneratedAt, "Bag %s file %s IngestSha256GeneratedAt is missing", bagName, i)
		assert.NotEmpty(t, gf.IngestSha256VerifiedAt, "Bag %s file %s IngestSha256VerifiedAt is missing", bagName, )
		assert.NotEmpty(t, gf.IngestUUID, "Bag %s file %s UUID is missing", bagName, i)
		assert.NotEmpty(t, gf.IngestUUIDGeneratedAt, "Bag %s file %s UUIDGeneratedAt is missing", bagName, i)
		assert.True(t, gf.IngestNeedsSave, "Bag %s file %s IngestNeedsSave should be true", bagName, i)
	}
}

func fetcherTestBadBagResult(t *testing.T, bagName string, ingestManifest *models.IngestManifest) {
	// These files are invalid, and should show fatal validation errors.
	assert.NotEmpty(t, ingestManifest.ValidateResult.Errors,
		"ValidateResult.Errors should not be empty for %s", bagName)
	assert.True(t, ingestManifest.ValidateResult.ErrorIsFatal,
		"ValidateResult.ErrorIsFatal should be true for %s", bagName)
	assert.False(t, ingestManifest.ValidateResult.Retry,
		"ValidateResult.Retry should be false for %s", bagName)
}

func fetcherTestCommon(t *testing.T, bagName string, ingestManifest *models.IngestManifest) {
	// Show that we recorded basic data.
	assert.NotEmpty(t, ingestManifest.WorkItemId, "WorkItemId should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.S3Bucket, "S3Bucket should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.S3Key, "S3Key should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.ETag, "ETag should not be empty for %s", bagName)

	assert.True(t, ingestManifest.FetchResult.Attempted,
		"FetchResult.Attempted should be true for %s", bagName)
	assert.True(t, ingestManifest.FetchResult.AttemptNumber > 0,
		"FetchResult.AttemptNumber should be > 0 %s", bagName)
	assert.NotEmpty(t, ingestManifest.FetchResult.StartedAt,
		"FetchResult.StartedAt should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.FetchResult.FinishedAt,
		"FetchResult.FinishedAt should not be empty for %s", bagName)

	assert.True(t, ingestManifest.ValidateResult.Attempted,
		"ValidateResult.Attempted should be true for %s", bagName)
	assert.True(t, ingestManifest.ValidateResult.AttemptNumber > 0,
		"ValidateResult.AttemptNumber should be > 0 %s", bagName)
	assert.NotEmpty(t, ingestManifest.ValidateResult.StartedAt,
		"ValidateResult.StartedAt should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.ValidateResult.FinishedAt,
		"ValidateResult.FinishedAt should not be empty for %s", bagName)

	assert.NotEmpty(t, ingestManifest.Object.Identifier,
		"Object.Identifier should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.Object.BagName,
		"Object.BagName should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.Object.Institution,
		"Object.Institution should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.Object.InstitutionId,
		"Object.InstitutionId should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.Object.IngestS3Bucket,
		"Object.IngestS3Bucket should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.Object.IngestS3Key,
		"Object.IngestS3Key should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.Object.IngestTarFilePath,
		"Object.IngestTarFilePath should not be empty for %s", bagName)
}
