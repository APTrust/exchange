package integration_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/storage"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
		ingestManifest, err := testutil.FindIngestManifestInLog(pathToJsonLog, bagName)
		require.Nil(t, err, bagName)
		require.NotNil(t, ingestManifest, bagName)
		fetcherTestCommon(t, bagName, ingestManifest)
		// TODO: Test WorkItem (stage, status, etc.) below
		fetcherTestGoodBagResult(t, bagName, ingestManifest)
		if bagName == "aptrust.integration.test/example.edu.tagsample_good.tar" {
			fetcherTestSpecifics(t, ingestManifest)
		}
	}
	for _, bagName := range testutil.INTEGRATION_GLACIER_BAGS {
		ingestManifest, err := testutil.FindIngestManifestInLog(pathToJsonLog, bagName)
		require.Nil(t, err, bagName)
		require.NotNil(t, ingestManifest, bagName)
		fetcherTestCommon(t, bagName, ingestManifest)
		// TODO: Test WorkItem (stage, status, etc.) below
		fetcherTestGoodBagResult(t, bagName, ingestManifest)
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
	objIdentifier, err := ingestManifest.ObjectIdentifier()
	require.Nil(t, err)
	db, err := storage.NewBoltDB(ingestManifest.DBPath)
	require.Nil(t, err)
	defer db.Close()
	obj, err := db.GetIntellectualObject(objIdentifier)
	require.Nil(t, err, objIdentifier)
	require.NotNil(t, obj, objIdentifier)

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
	// One key in the db is for the Intellectual Object,
	// all the rest are for GenericFiles.
	gfIdentifiers := db.FileIdentifiers()
	assert.True(t, len(gfIdentifiers) > 1, "obj.GenericFiles should not be empty for %s", bagName)
	for _, gfIdentifier := range gfIdentifiers {
		gf, err := db.GetGenericFile(gfIdentifier)
		require.Nil(t, err, gfIdentifier)
		assert.NotEmpty(t, gf.Identifier, "Bag %s file %s Identifier is missing", bagName, gfIdentifier)
		assert.NotEmpty(t, gf.IntellectualObjectIdentifier,
			"Bag %s file %s IntellectualObjectIdentifier is missing", bagName, gfIdentifier)
		assert.NotEmpty(t, gf.FileFormat, "Bag %s file %s FileFormat is missing", bagName, gfIdentifier)

		// Two files actually do have zero size
		if strings.HasSuffix(gf.OriginalPath(), "empty-file.txt") || strings.HasSuffix(gf.OriginalPath(), "empty_dir/.keep") {
			assert.EqualValues(t, gf.Size, 0, "Bag %s file %s Size should be zero", bagName, gfIdentifier)
		} else {
			assert.True(t, gf.Size > 0, "Bag %s file %s Size is missing", bagName, gfIdentifier)
		}

		assert.NotEmpty(t, gf.IngestFileType, "Bag %s file %s IngestFileType is missing", bagName, gfIdentifier)
		assert.NotEmpty(t, gf.IngestMd5, "Bag %s file %s IngestMd5 is missing", bagName, gfIdentifier)
		assert.NotEmpty(t, gf.IngestMd5GeneratedAt, "Bag %s file %s IngestMd5GeneratedAt is missing", bagName, gfIdentifier)
		assert.NotEmpty(t, gf.IngestMd5VerifiedAt, "Bag %s file %s IngestMd5VerifiedAt is missing", bagName, gfIdentifier)
		assert.NotEmpty(t, gf.IngestSha256, "Bag %s file %s IngestSha256 is missing", bagName, gfIdentifier)
		assert.NotEmpty(t, gf.IngestSha256GeneratedAt, "Bag %s file %s IngestSha256GeneratedAt is missing", bagName, gfIdentifier)
		assert.NotEmpty(t, gf.IngestSha256VerifiedAt, "Bag %s file %s IngestSha256VerifiedAt is missing", bagName)
		assert.NotEmpty(t, gf.IngestUUID, "Bag %s file %s UUID is missing", bagName, gfIdentifier)
		assert.NotEmpty(t, gf.IngestUUIDGeneratedAt, "Bag %s file %s UUIDGeneratedAt is missing", bagName, gfIdentifier)
	}
}

func fetcherTestCommon(t *testing.T, bagName string, ingestManifest *models.IngestManifest) {
	// Show that we recorded basic data.
	assert.NotEmpty(t, ingestManifest.WorkItemId, "WorkItemId should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.S3Bucket, "S3Bucket should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.S3Key, "S3Key should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.ETag, "ETag should not be empty for %s", bagName)

	require.NotNil(t, ingestManifest.FetchResult, bagName)
	assert.True(t, ingestManifest.FetchResult.Attempted,
		"FetchResult.Attempted should be true for %s", bagName)
	assert.True(t, ingestManifest.FetchResult.AttemptNumber > 0,
		"FetchResult.AttemptNumber should be > 0 %s", bagName)
	assert.NotEmpty(t, ingestManifest.FetchResult.StartedAt,
		"FetchResult.StartedAt should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.FetchResult.FinishedAt,
		"FetchResult.FinishedAt should not be empty for %s", bagName)

	require.NotNil(t, ingestManifest.ValidateResult, bagName)
	assert.True(t, ingestManifest.ValidateResult.Attempted,
		"ValidateResult.Attempted should be true for %s", bagName)
	assert.True(t, ingestManifest.ValidateResult.AttemptNumber > 0,
		"ValidateResult.AttemptNumber should be > 0 %s", bagName)
	assert.NotEmpty(t, ingestManifest.ValidateResult.StartedAt,
		"ValidateResult.StartedAt should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.ValidateResult.FinishedAt,
		"ValidateResult.FinishedAt should not be empty for %s", bagName)

	assert.NotEmpty(t, ingestManifest.BagPath,
		"BagPath should not be empty for %s", bagName)
	assert.NotEmpty(t, ingestManifest.DBPath,
		"DBPath should not be empty for %s", bagName)
}

func fetcherTestSpecifics(t *testing.T, ingestManifest *models.IngestManifest) {
	db, err := storage.NewBoltDB(ingestManifest.DBPath)
	require.Nil(t, err)
	defer db.Close()
	obj, err := db.GetIntellectualObject("test.edu/example.edu.tagsample_good")
	require.Nil(t, err)
	assert.Equal(t, "test.edu/example.edu.tagsample_good", obj.Identifier)
	assert.Equal(t, "example.edu.tagsample_good", obj.BagName)

	assert.NotEqual(t, 0, obj.InstitutionId)
	assert.Equal(t, "Tag Sample (Good)", obj.Title)
	assert.Equal(t, "Bag of goodies", obj.Description)
	assert.Equal(t, "Institution", obj.Access)
	assert.Equal(t, "uva-internal-id-0001", obj.AltIdentifier)
	assert.Equal(t, "aptrust.integration.test", obj.IngestS3Bucket)
	assert.Equal(t, "example.edu.tagsample_good.tar", obj.IngestS3Key)
	assert.Equal(t, "test.edu", obj.Institution)
	assert.Equal(t, "manifest-md5.txt", obj.IngestManifests[0])
	assert.Equal(t, "manifest-sha256.txt", obj.IngestManifests[1])
	assert.Equal(t, "tagmanifest-md5.txt", obj.IngestTagManifests[0])
	assert.Equal(t, "tagmanifest-sha256.txt", obj.IngestTagManifests[1])
	assert.Empty(t, obj.IngestFilesIgnored)
	assert.Equal(t, "example.edu.tagsample_good", obj.IngestTopLevelDirNames[0])
	assert.Empty(t, obj.IngestErrorMessage)

	assert.Equal(t, 10, len(obj.IngestTags))
	assert.Equal(t, "bag-info.txt", obj.IngestTags[5].SourceFile)
	assert.Equal(t, "Bag-Group-Identifier", obj.IngestTags[5].Label)
	assert.Equal(t, "Charley Horse", obj.IngestTags[5].Value)

	assert.NotEmpty(t, obj.PremisEvents, "Failed object id: %s", obj.Identifier)

	// -----------------------------------------------------------------------
	// Iterate through keys...
	// -----------------------------------------------------------------------

	gfIdentifiers := db.FileIdentifiers()
	assert.Equal(t, 16, len(gfIdentifiers))

	gf, err := db.GetGenericFile("test.edu/example.edu.tagsample_good/aptrust-info.txt")
	require.Nil(t, err)
	require.NotNil(t, gf)
	assert.NotEqual(t, 0, gf.Id)
	assert.Equal(t, "test.edu/example.edu.tagsample_good/aptrust-info.txt", gf.Identifier)
	assert.NotEqual(t, 0, gf.IntellectualObjectId)
	assert.Equal(t, "test.edu/example.edu.tagsample_good", gf.IntellectualObjectIdentifier)
	assert.Equal(t, "text/plain", gf.FileFormat)
	assert.EqualValues(t, 45, gf.Size)
	assert.EqualValues(t, "0001-01-01T00:00:00Z", gf.FileCreated.Format(time.RFC3339))
	assert.Equal(t, "2016-03-21T11:01:51-04:00", gf.FileModified.Format(time.RFC3339))
	assert.Equal(t, 2, len(gf.Checksums))
	assert.Equal(t, 6, len(gf.PremisEvents))
	assert.Equal(t, "tag_file", gf.IngestFileType)
	assert.Equal(t, "bd8be664c790a9175e9d2fe90b40d502", gf.IngestMd5)
	assert.False(t, gf.IngestMd5GeneratedAt.IsZero())
	assert.False(t, gf.IngestMd5VerifiedAt.IsZero())
	assert.Equal(t, "49dd23cdaf644e60629f01d6ebea770cd1c6229ff89f14a6c030a50c48b6ba27", gf.IngestSha256)
	assert.False(t, gf.IngestSha256GeneratedAt.IsZero())
	assert.False(t, gf.IngestSha256VerifiedAt.IsZero())
	assert.True(t, util.LooksLikeUUID(gf.IngestUUID))
	assert.False(t, gf.IngestUUIDGeneratedAt.IsZero())
	// After fetch, the following two timestamps should be empty.
	// However, apt_fetch_post_test runs after fetch, store,
	// and record are complete.
	assert.False(t, gf.IngestStoredAt.IsZero(), gf.Identifier)
	assert.False(t, gf.IngestReplicatedAt.IsZero(), gf.Identifier)
	assert.True(t, gf.IngestNeedsSave)
	assert.EqualValues(t, 502, gf.IngestFileUid)
	assert.EqualValues(t, 20, gf.IngestFileGid)
}

func fetcherTestWorkItem(t *testing.T, bagName string, ingestManifest *models.IngestManifest) {
	// Show that we recorded basic data.
	require.NotEmpty(t, ingestManifest.WorkItemId, "WorkItemId should not be empty for %s", bagName)

	// Load config
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()
	_context := context.NewContext(config)

	resp := _context.PharosClient.WorkItemGet(ingestManifest.WorkItemId)
	assert.Nil(t, resp.Error)
	workItem := resp.WorkItem()
	assert.NotNil(t, workItem)

	// This test runs after apt_store and apt_record,
	// so the WorkItem will be in a completed state.
	// All items should have run to completion (Success or Failure).
	// Nothing should be stuck in an intermediate state.
	assert.Equal(t, constants.ActionIngest, workItem.Action)
	if util.StringListContains(testutil.INTEGRATION_BAD_BAGS, bagName) {
		assert.Equal(t, constants.StatusFailed, workItem.Status)
	} else {
		assert.Equal(t, constants.StatusSuccess, workItem.Status)
	}
}
