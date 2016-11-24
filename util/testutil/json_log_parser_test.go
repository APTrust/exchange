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

	// Should get a manifest if the item exists
	ingestManifest, err := testutil.FindIngestManifestInLog(pathToLogFile,
		"aptrust.receiving.test.test.edu/ncsu.1840.16-10.tar")
	assert.Nil(t, err)
	assert.NotNil(t, ingestManifest)

	// Should get an error if the item does not exist
	ingestManifest, err = testutil.FindIngestManifestInLog(pathToLogFile,
		"aptrust.receiving.x/does_not_exist.tar")
	assert.NotNil(t, err)
	assert.Nil(t, ingestManifest)

	// Should get the LAST copy of the item, if it appears
	// more than once in the logs. This one appears twice.
	// The first version has a zero timestamp for FetchResult.StartedAt,
	// while the second one has a non-zero timestamp.
	ingestManifest, err = testutil.FindIngestManifestInLog(pathToLogFile,
		"aptrust.receiving.test.test.edu/example.edu.tagsample_good.tar")
	assert.Nil(t, err)
	require.NotNil(t, ingestManifest)
	assert.False(t, ingestManifest.FetchResult.StartedAt.IsZero())
}
