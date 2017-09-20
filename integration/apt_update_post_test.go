package integration_test

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests verify the outcomes of a bag update.
// The bag in testdata/unit_test_bags/updated/example.edu.tagsample_good.tar
// is an altered version of testdata/unit_test_bags/example.edu.tagsample_good.tar,
// with one new file, one deleted file, and one changed file.
// We want to make sure all operations were peformed correctly in
// Pharos, S3, and Glacier.
//
// 1. Ensure unchanged files are not changed in Pharos, S3, Glacier.
// 2. Ensure new file is present in Pharos, S3, Glacier,
// 3. Ensure updated file is actually update in S3 & Glacier.
// 4. Ensure updated file has new checksum in Pharos.
// 5. Ensure updated file has new ingest & fixity calculation events in Pharos.
// 6. Ensure ingest work item was marked complete in Pharos.

const (
	UPDATED_BAG_IDENTIFIER = "test.edu/example.edu.tagsample_good"
	UPDATED_BAG_ETAG       = "a08e2b0b3ba3490434c23a8d78b1760f"
)

func TestUpdatedItemsInPharos(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err)
	testUpdatedWorkItem(t, _context)
	testUpdatedObject(t, _context)
}

func testUpdatedWorkItem(t *testing.T, _context *context.Context) {
	params := url.Values{}
	params.Set("item_action", constants.ActionIngest)
	params.Set("object_identifier", UPDATED_BAG_IDENTIFIER)
	params.Set("etag", UPDATED_BAG_ETAG)
	params.Set("page", "1")
	params.Set("per_page", "1")

	// There will be two ingest work items for this
	// bag, but only one will match the etag we specified.
	resp := _context.PharosClient.WorkItemList(params)
	require.Nil(t, resp.Error)
	workItem := resp.WorkItem()

	require.NotNil(t, workItem, "WorkItem is missing for %s", UPDATED_BAG_IDENTIFIER)
	assert.Equal(t, constants.StatusSuccess, workItem.Status)
	assert.Equal(t, constants.StageCleanup, workItem.Stage)
	assert.Equal(t, "Item was successfully ingested", workItem.Note)
	assert.Empty(t, workItem.Node)
	assert.Equal(t, 0, workItem.Pid)
}

func testUpdatedObject(t *testing.T, _context *context.Context) {
	resp := _context.PharosClient.IntellectualObjectGet(UPDATED_BAG_IDENTIFIER, true, true)
	obj := resp.IntellectualObject()
	require.NotNil(t, obj)

	ingestEvents := obj.FindEventsByType(constants.EventIngestion)
	fileLevelEventCount := 0
	objectLevelEventCount := 0
	for _, event := range ingestEvents {
		if strings.HasPrefix(event.Detail, "Copied all files to perservation bucket") {
			objectLevelEventCount++
		} else if strings.HasPrefix(event.Detail, "Completed copy to S3") {
			fileLevelEventCount++
		}
	}
	assert.Equal(t, 2, objectLevelEventCount)
	assert.Equal(t, 15, fileLevelEventCount)
	assert.Empty(t, obj.FindEventsByType(constants.EventDeletion))
	require.Equal(t, 12, len(obj.GenericFiles))

	testUpdatedFilesInPharos(t, _context, obj)
}

func testUpdatedFilesInPharos(t *testing.T, _context *context.Context, obj *models.IntellectualObject) {
	// The updated file is different in the new version of the bag.
	// We have to make sure we've stored this and have the new
	// checksum attached to it. It should have two events for ingest,
	// and fixity generation.
	updatedFile := obj.FindGenericFile("data/datastream-DC")
	require.NotNil(t, updatedFile)
	assert.Equal(t, 2, len(updatedFile.FindEventsByType(constants.EventIngestion)))
	assert.Equal(t, 2, len(updatedFile.FindEventsByType(constants.EventDigestCalculation)))
	assert.Equal(t, 2, len(updatedFile.FindEventsByType(constants.EventReplication)))
	require.Equal(t, 4, len(updatedFile.Checksums))
	require.Equal(t, "44d85cf4810d6c6fe87750117633e461", updatedFile.Checksums[0].Digest)
	require.Equal(t, "248fac506a5c46b3c760312b99827b6fb5df4698d6cf9a9cdc4c54746728ab99",
		updatedFile.Checksums[1].Digest)
	require.Equal(t, "7a31f705fc1a16571374406c5a9b7681", updatedFile.Checksums[2].Digest)
	require.Equal(t, "baf8752080187b1e401ae952047029ae4e16b5f54c5daf9d97bc0c7598772326",
		updatedFile.Checksums[3].Digest)
	testUpdatedFileInStorage(t, _context, updatedFile)

	// The new file is in the new version of the bag, but was not
	// in the original version.
	newFile := obj.FindGenericFile("data/new_file.txt")
	require.NotNil(t, newFile)
	testUpdatedFileInStorage(t, _context, newFile)

	// The deleted file was in the original version of the bag, but
	// is not in the new version. We DO NOT delete this, since only
	// the depositing institution can delete an item. Make sure
	// it's still there.
	deletedFile := obj.FindGenericFile("data/datastream-RELS-EXT")
	require.NotNil(t, deletedFile)
	testUpdatedFileInStorage(t, _context, deletedFile)
}

func testUpdatedFileInStorage(t *testing.T, _context *context.Context, gf *models.GenericFile) {
	// Set up an S3 client to look at the S3 bucket, and a Glacier client
	// to look at the Glacier bucket.
	clients := make(map[string]*network.S3Head)
	clients["s3"] = network.NewS3Head(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		_context.Config.APTrustS3Region,
		_context.Config.PreservationBucket)
	clients["glacier"] = network.NewS3Head(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		_context.Config.APTrustGlacierRegion,
		_context.Config.ReplicationBucket)

	// This is the name of the file in S3/Glacier
	uuid, err := gf.PreservationStorageFileName()
	require.Nil(t, err, gf.Identifier)

	// Make sure file is present and all metadata is set correctly.
	for _, client := range clients {
		idAndUuid := fmt.Sprintf("%s (%s)", gf.Identifier, uuid)
		client.Head(uuid)
		require.Empty(t, client.ErrorMessage, idAndUuid)
		storedFile := client.StoredFile()
		require.NotNil(t, storedFile, idAndUuid)

		institution, _ := gf.InstitutionIdentifier()
		timeSinceLastModified := time.Since(storedFile.LastModified)
		assert.True(t, timeSinceLastModified < (10*time.Minute))
		assert.EqualValues(t, gf.Size, storedFile.Size, idAndUuid)
		assert.Equal(t, institution, storedFile.Institution, idAndUuid)
		assert.Equal(t, gf.IntellectualObjectIdentifier, storedFile.BagName, idAndUuid)
		assert.Equal(t, gf.OriginalPath(), storedFile.PathInBag, idAndUuid)
		assert.Equal(t, gf.GetChecksumByAlgorithm(constants.AlgMd5).Digest, storedFile.Md5, idAndUuid)
		assert.Equal(t, gf.GetChecksumByAlgorithm(constants.AlgSha256).Digest, storedFile.Sha256, idAndUuid)
	}
}
