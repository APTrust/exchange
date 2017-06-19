package integration_test

import (
	//"fmt"
	"github.com/APTrust/exchange/constants"
	//"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	//"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	//"os"
	//"strings"
	"testing"
	//"time"
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
	testUpdatedWorkItem(t, workItem)

	resp = _context.PharosClient.IntellectualObjectGet(UPDATED_BAG_IDENTIFIER, true, true)
	obj := resp.IntellectualObject()
	require.NotNil(t, obj)
	testUpdatedObject(t, obj)
}

func testUpdatedWorkItem(t *testing.T, workItem *models.WorkItem) {
	assert.Equal(t, constants.StatusSuccess, workItem.Status)
	assert.Equal(t, constants.StageCleanup, workItem.Stage)
	assert.Equal(t, "Item was successfully ingested", workItem.Note)
	assert.Nil(t, workItem.Node)
	assert.Equal(t, 0, workItem.Pid)
}

func testUpdatedObject(t *testing.T, obj *models.IntellectualObject) {
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
	assert.Equal(t, 2, fileLevelEventCount)

	require.NotEmpty(t, obj.GenericFiles)
	//for _, gf := range obj.GenericFiles {
	//	testFile(t, _context, gf)
	//}

}
