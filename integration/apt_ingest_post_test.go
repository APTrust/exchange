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
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestIngestedItemsInPharos(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err)
	params := url.Values{}
	params.Set("item_action", constants.ActionIngest)
	params.Set("page", "1")
	params.Set("per_page", "10")

	defer deleteValDBForUpdateFiles(t, _context)

	for _, bagName := range testutil.INTEGRATION_GOOD_BAGS {
		tarFileName := strings.Split(bagName, "/")[1]
		params.Set("name", tarFileName)
		resp := _context.PharosClient.WorkItemList(params)
		require.Nil(t, resp.Error)
		items := resp.WorkItems()
		require.Equal(t, 1, len(items))
		item := items[0]
		assert.Equal(t, constants.StageCleanup, item.Stage)
		assert.Equal(t, constants.StatusSuccess, item.Status)
		assert.Empty(t, item.Node)
		assert.Equal(t, 0, item.Pid)
		testObject(t, _context, item.ObjectIdentifier)
	}
}

func testObject(t *testing.T, _context *context.Context, objIdentifier string) {
	resp := _context.PharosClient.IntellectualObjectGet(objIdentifier, true, true)
	require.Nil(t, resp.Error, objIdentifier)
	obj := resp.IntellectualObject()
	require.NotNil(t, obj, objIdentifier)

	// TODO: Test object attributes

	require.NotEmpty(t, obj.GenericFiles, objIdentifier)
	for _, gf := range obj.GenericFiles {
		testFile(t, _context, gf)
	}

	// Ingest produces 4 events for the object, and 6 for each GenericFile
	expectedEventCount := 4 + (6 * len(obj.GenericFiles))
	assert.Equal(t, expectedEventCount, len(obj.PremisEvents), objIdentifier)
}

func testFile(t *testing.T, _context *context.Context, gf *models.GenericFile) {
	// TODO: Test file properties.
	assert.Equal(t, 6, len(gf.PremisEvents))
	assert.Equal(t, 2, len(gf.Checksums))
	testFileIsInStorage(t, _context, gf)
}

func testFileIsInStorage(t *testing.T, _context *context.Context, gf *models.GenericFile) {
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

// We're going to re-ingest two bags for the update test.
// Delete the .valdb files for those bags, so the ingest
// workers don't try to reuse them. The delete would happen
// on demo and production automatically. We keep the .valdb files
// around for integration tests only so we can inspect them
// after the tests run.
func deleteValDBForUpdateFiles(t *testing.T, _context *context.Context) {
	// These constants are defined in apt_update_post_test.go
	updateBags := []string{
		testutil.UPDATED_BAG_IDENTIFIER,
		testutil.UPDATED_GLACIER_BAG_IDENTIFIER,
	}
	for _, bagName := range updateBags {
		valdbFile := filepath.Join(_context.Config.TarDirectory, bagName+".valdb")
		err := os.Remove(valdbFile)
		assert.Nil(t, err, "Error removing %s: %v", valdbFile, err)
	}
}
