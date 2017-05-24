package integration_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/storage"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	// "time"
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
		if bagName == "aptrust.receiving.test.test.edu/example.edu.tagsample_good.tar" {
			fetcherTestSpecifics(t, ingestManifest)
		}
		// TODO: Validate the IntelObj record in valdb.
	}
	for _, bagName := range testutil.INTEGRATION_BAD_BAGS {
		ingestManifest, err := testutil.FindIngestManifestInLog(pathToJsonLog, bagName)
		require.Nil(t, err, bagName)
		require.NotNil(t, ingestManifest, bagName)
		fetcherTestCommon(t, bagName, ingestManifest)
		// TODO: Test WorkItem (stage, status, etc.) below
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
		assert.NotEmpty(t, gf.IngestSha256VerifiedAt, "Bag %s file %s IngestSha256VerifiedAt is missing", bagName)
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

	// For good bags, both the tar file and the .valdb file should
	// remain on disk after fetch. For bad bags, both should be gone.
	if util.StringListContains(testutil.INTEGRATION_GOOD_BAGS, bagName) {
		assert.True(t, fileutil.FileExists(ingestManifest.BagPath), bagName)
		assert.True(t, fileutil.FileExists(ingestManifest.DBPath), bagName)
	} else {
		assert.False(t, fileutil.FileExists(ingestManifest.BagPath), bagName)
		assert.False(t, fileutil.FileExists(ingestManifest.DBPath), bagName)
	}
}

func fetcherTestSpecifics(t *testing.T, ingestManifest *models.IngestManifest) {
	//obj := ingestManifest.Object
	db, err := storage.NewBoltDB(ingestManifest.DBPath)
	require.Nil(t, err)
	defer db.Close()
	obj, err := db.GetIntellectualObject("test.edu/example.edu.tagsample_good")
	require.Nil(t, err)
	assert.Equal(t, "test.edu/example.edu.tagsample_good", obj.Identifier)
	assert.Equal(t, "example.edu.tagsample_good", obj.BagName)
	assert.Equal(t, "test.edu", obj.Institution)
	assert.NotEqual(t, 0, obj.InstitutionId)
	assert.Equal(t, "Tag Sample (Good)", obj.Title)
	assert.Equal(t, "Bag of goodies", obj.Description)
	assert.Equal(t, "Institution", obj.Access)
	assert.Equal(t, "uva-internal-id-0001", obj.AltIdentifier)
	assert.Equal(t, "aptrust.receiving.test.test.edu", obj.IngestS3Bucket)
	assert.Equal(t, "example.edu.tagsample_good.tar", obj.IngestS3Key)

	// -----------------------------------------------------------------------
	// Iterate through keys...
	// -----------------------------------------------------------------------

	// assert.Equal(t, "manifest-md5.txt", obj.IngestManifests[0])
	// assert.Equal(t, "manifest-sha256.txt", obj.IngestManifests[1])
	// assert.Equal(t, "tagmanifest-md5.txt", obj.IngestTagManifests[0])
	// assert.Equal(t, "tagmanifest-sha256.txt", obj.IngestTagManifests[1])
	// assert.Empty(t, obj.IngestFilesIgnored)
	// assert.Equal(t, "example.edu.tagsample_good", obj.IngestTopLevelDirNames[0])
	// assert.Empty(t, obj.IngestErrorMessage)

	// assert.Equal(t, 10, len(obj.IngestTags))
	// assert.Equal(t, "bag-info.txt", obj.IngestTags[5].SourceFile)
	// assert.Equal(t, "Bag-Group-Identifier", obj.IngestTags[5].Label)
	// assert.Equal(t, "Charley Horse", obj.IngestTags[5].Value)

	// assert.Empty(t, obj.PremisEvents)

	// assert.Equal(t, 16, len(obj.GenericFiles))

	// gf := obj.GenericFiles[0]
	// assert.Equal(t, 0, gf.Id)
	// assert.Equal(t, "test.edu/example.edu.tagsample_good/aptrust-info.txt", gf.Identifier)
	// assert.Equal(t, 0, gf.IntellectualObjectId)
	// assert.Equal(t, "test.edu/example.edu.tagsample_good", gf.IntellectualObjectIdentifier)
	// assert.Equal(t, "text/plain", gf.FileFormat)
	// assert.EqualValues(t, 45, gf.Size)
	// assert.EqualValues(t, "0001-01-01T00:00:00Z", gf.FileCreated.Format(time.RFC3339))
	// assert.Equal(t, "2016-03-21T11:01:51-04:00", gf.FileModified.Format(time.RFC3339))
	// assert.Empty(t, gf.Checksums)
	// assert.Empty(t, gf.PremisEvents)
	// assert.Equal(t, "tag_file", gf.IngestFileType)
	// assert.Equal(t, "bd8be664c790a9175e9d2fe90b40d502", gf.IngestMd5)
	// assert.False(t, gf.IngestMd5GeneratedAt.IsZero())
	// assert.False(t, gf.IngestMd5VerifiedAt.IsZero())
	// assert.Equal(t, "49dd23cdaf644e60629f01d6ebea770cd1c6229ff89f14a6c030a50c48b6ba27", gf.IngestSha256)
	// assert.False(t, gf.IngestSha256GeneratedAt.IsZero())
	// assert.False(t, gf.IngestSha256VerifiedAt.IsZero())
	// assert.True(t, util.LooksLikeUUID(gf.IngestUUID))
	// assert.False(t, gf.IngestUUIDGeneratedAt.IsZero())
	// assert.True(t, gf.IngestStoredAt.IsZero())
	// assert.True(t, gf.IngestReplicatedAt.IsZero())
	// assert.True(t, gf.IngestNeedsSave)
	// assert.EqualValues(t, 502, gf.IngestFileUid)
	// assert.EqualValues(t, 20, gf.IngestFileGid)
}
