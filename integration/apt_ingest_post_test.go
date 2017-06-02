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
	// Make sure the object is present.
	// Make sure its attributes are correct
	// Make sure the object-level ingest events are present.
	// Make sure the right number of generic files were preserved.
	// Test all of the object's generic files.
	resp := _context.PharosClient.IntellectualObjectGet(objIdentifier, true, true)
	require.Nil(t, resp.Error, objIdentifier)
	obj := resp.IntellectualObject()
	require.NotNil(t, obj, objIdentifier)

	// TODO ... test attributes ...

	require.NotEmpty(t, obj.GenericFiles, objIdentifier)
	for _, gf := range obj.GenericFiles {
		testFile(t, _context, gf)
	}

	// Ingest produces 4 events for the object, and 6 for each GenericFile
	expectedEventCount := 4 + (6 * len(obj.GenericFiles))
	assert.Equal(t, expectedEventCount, len(obj.PremisEvents), objIdentifier)
}

func testFile(t *testing.T, _context *context.Context, gf *models.GenericFile) {
	// Test file properties.
	// Make sure all ingest events are present.
	// Make sure checksums are present.
	// Call testFileIsInStore for S3 and Glacier
	assert.Equal(t, 6, len(gf.PremisEvents))
	assert.Equal(t, 2, len(gf.Checksums))
	testFileIsInStorage(t, _context, gf)
}

func testFileIsInStorage(t *testing.T, _context *context.Context, gf *models.GenericFile) {
	s3Url := gf.URI
	glacierUrl := strings.Replace(s3Url, _context.Config.PreservationBucket,
		_context.Config.ReplicationBucket, 1)

	// Set up an S3 client to look at the S3 bucket, and a Glacier client
	// to look at the Glacier bucket.
	urls := make(map[string]*network.S3Head)
	urls[s3Url] = network.NewS3Head(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		_context.Config.APTrustS3Region,
		_context.Config.PreservationBucket)
	urls[glacierUrl] = network.NewS3Head(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		_context.Config.APTrustGlacierRegion,
		_context.Config.ReplicationBucket)

	// Make sure file is present and all metadata is set correctly.
	for url, client := range urls {
		idAndUrl := fmt.Sprintf("%s (%s)", gf.Identifier, url)
		client.Head(url)
		require.Empty(t, client.ErrorMessage, idAndUrl)
		storedFile := client.StoredFile()
		require.NotNil(t, storedFile, idAndUrl)

		institution, _ := gf.InstitutionIdentifier()
		assert.InDelta(t, time.Now().UTC(), storedFile.LastModified, float64(10*time.Minute), idAndUrl)
		assert.EqualValues(t, gf.Size, storedFile.Size, idAndUrl)
		assert.Equal(t, institution, storedFile.Institution, idAndUrl)
		assert.Equal(t, gf.IntellectualObjectIdentifier, storedFile.BagName, idAndUrl)
		assert.Equal(t, gf.OriginalPath(), storedFile.PathInBag, idAndUrl)
		assert.Equal(t, gf.GetChecksumByAlgorithm(constants.AlgMd5), storedFile.Md5, idAndUrl)
		assert.Equal(t, gf.GetChecksumByAlgorithm(constants.AlgSha256), storedFile.Sha256, idAndUrl)
	}
}
