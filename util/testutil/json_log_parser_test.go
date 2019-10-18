package testutil_test

import (
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestFindIngestManifestInLog(t *testing.T) {
	pathToLogFile, _ := fileutil.RelativeToAbsPath(
		filepath.Join("testdata", "integration_results", "apt_fetch.json"))

	// Should get a manifest if the item exists.
	// Identifier is the S3 bucket/key.
	manifest, err := testutil.FindIngestManifestInLog(pathToLogFile,
		"aptrust.integration.test/ncsu.1840.16-10.tar")
	assert.Nil(t, err)
	assert.NotNil(t, manifest)
	assert.NotNil(t, manifest.Object)

	// Should get an error if the item does not exist
	manifest, err = testutil.FindIngestManifestInLog(pathToLogFile,
		"aptrust.receiving.x/does_not_exist.tar")
	assert.NotNil(t, err)
	assert.Nil(t, manifest)

	// Should get the LAST copy of the item, if it appears
	// more than once in the logs. This one appears twice.
	// The first version has a zero timestamp for FetchResult.StartedAt,
	// while the second one has a non-zero timestamp.
	manifest, err = testutil.FindIngestManifestInLog(pathToLogFile,
		"aptrust.integration.test/example.edu.tagsample_good.tar")
	assert.Nil(t, err)
	require.NotNil(t, manifest)
	assert.False(t, manifest.FetchResult.StartedAt.IsZero())
}

func TestFindRestoreStateInLog(t *testing.T) {
	pathToLogFile, _ := fileutil.RelativeToAbsPath(
		filepath.Join("testdata", "integration_results", "apt_restore.json"))

	// Should get a RestoreState object
	restoreState, err := testutil.FindRestoreStateInLog(pathToLogFile,
		"test.edu/ncsu.1840.16-1004")
	assert.Nil(t, err)
	assert.NotNil(t, restoreState)
	assert.NotNil(t, restoreState.ValidateSummary)

	// Should get an error if the item does not exist
	restoreState, err = testutil.FindRestoreStateInLog(pathToLogFile,
		"aptrust.receiving.x/does_not_exist.tar")
	assert.NotNil(t, err)
	assert.Nil(t, restoreState)
}

func TestFindFileRestoreStateInLog(t *testing.T) {
	pathToLogFile, _ := fileutil.RelativeToAbsPath(
		filepath.Join("testdata", "integration_results", "apt_file_restore.json"))

	// Should get a RestoreState object
	restoreState, err := testutil.FindFileRestoreStateInLog(pathToLogFile,
		"test.edu/example.edu.tagsample_good/data/datastream-MARC")
	assert.Nil(t, err)
	assert.NotNil(t, restoreState)
	assert.NotNil(t, restoreState.RestoreSummary)

	// Should get an error if the item does not exist
	restoreState, err = testutil.FindFileRestoreStateInLog(pathToLogFile,
		"test.edu/example.edu.tagsample_good/data/does_not_exist.txt")
	assert.NotNil(t, err)
	assert.Nil(t, restoreState)
}

func TestExtractJson(t *testing.T) {
	// APTrust Ingest - tests apt_fetch.json, but apt_store.json and
	// apt_record.json record the same JSON structure. We don't have
	// an IntellectualObject.Identifier at this point in processing,
	// so we have to use S3 bucket/key.
	pathToIngestLog, _ := fileutil.RelativeToAbsPath(
		filepath.Join("testdata", "integration_results", "apt_fetch.json"))
	jsonString, err := testutil.ExtractJson(pathToIngestLog,
		"aptrust.integration.test/ncsu.1840.16-10.tar")
	assert.Nil(t, err)
	assert.Equal(t, 2488, len(jsonString))

	// APTrust Restore - finds by IntellectualObject.Identifier
	pathToRestoreLog, _ := fileutil.RelativeToAbsPath(
		filepath.Join("testdata", "integration_results", "apt_restore.json"))
	jsonString, err = testutil.ExtractJson(pathToRestoreLog, "test.edu/ncsu.1840.16-1004")
	assert.Nil(t, err)
	assert.Equal(t, 1162, len(jsonString))
}
