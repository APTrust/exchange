package integration_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	apt_models "github.com/APTrust/exchange/models"
	apt_testutil "github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"strings"
	"testing"
)

func packageGetWorkItems(t *testing.T) (*context.Context, []*apt_models.WorkItem) {
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	params := url.Values{}
	params.Set("item_action", "DPN")
	params.Set("page", "1")
	params.Set("per_page", "100")
	resp := _context.PharosClient.WorkItemList(params)
	require.Nil(t, resp.Error)
	return _context, resp.WorkItems()
}

// TestPackageWorkItems checks to see if Pharos WorkItems
// were updated correctly.
func TestPackageWorkItems(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_, workItems := packageGetWorkItems(t)
	for _, item := range workItems {
		assert.Equal(t, constants.StageStore, item.Stage)
		assert.Equal(t, constants.StatusPending, item.Status)
		assert.Equal(t, "Packaging completed, awaiting storage", item.Note)
		assert.Equal(t, "", item.Node)
		assert.Equal(t, 0, item.Pid)
		assert.True(t, item.Retry)
	}
}

// TestPackageWorkItemState checks to see if Pharos WorkItemState
// records were updated correctly.
func TestPackageWorkItemState(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, workItems := packageGetWorkItems(t)
	for _, item := range workItems {
		require.NotNil(t, item.WorkItemStateId)
		resp := _context.PharosClient.WorkItemStateGet(*item.WorkItemStateId)
		require.Nil(t, resp.Error)
		workItemState := resp.WorkItemState()
		require.NotNil(t, workItemState)
		assert.Equal(t, constants.ActionDPN, workItemState.Action)
		assert.False(t, workItemState.CreatedAt.IsZero())
		assert.False(t, workItemState.UpdatedAt.IsZero())

		detail := fmt.Sprintf("%s from Pharos", item.ObjectIdentifier)
		testPackageWorkItemState(t, _context, workItemState.State, detail)
	}

}

// TestPackageJsonLog checks that all expected entries are present
// in the dpn_package.json log.
func TestPackageJsonLog(t *testing.T) {

}

// TestPackageTarFilesPresent tests whether all expected DPN bags
// (tar files) are present in the staging area.
func TestPackageTarFilesPresent(t *testing.T) {

}

// TestPackageCleanup checks to see whether dpn_package cleaned up
// all of the intermediate files created during the bag building
// process. Those are directories containing untarred bags.
func TestPackageCleanup(t *testing.T) {

}

// TestPackageItemsQueued checks to see if dpn_package pushed items
// into the dpn_store NSQ topic.
func TestPackageItemsQueued(t *testing.T) {

}

// Test the JSON serialized WorkItemState. Param WorkItemState is a
// string of JSON data. Param detail describes which object we're
// testing and where the JSON came from, so failure messages can be
// more informative.
func testPackageWorkItemState(t *testing.T, _context *context.Context, workItemState, detail string) {
	dpnIngestManifest := models.NewDPNIngestManifest(nil)
	err := json.Unmarshal([]byte(workItemState), dpnIngestManifest)
	require.Nil(t, err, "Could not unmarshal state")

	require.NotNil(t, dpnIngestManifest.PackageSummary, detail)
	require.NotNil(t, dpnIngestManifest.ValidateSummary, detail)
	assert.False(t, dpnIngestManifest.PackageSummary.StartedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.PackageSummary.FinishedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.ValidateSummary.StartedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.ValidateSummary.FinishedAt.IsZero(), detail)

	assert.NotNil(t, dpnIngestManifest.WorkItem, detail)
	require.NotNil(t, dpnIngestManifest.DPNBag, detail)
	assert.NotEmpty(t, dpnIngestManifest.DPNBag.UUID, detail)
	assert.NotEmpty(t, dpnIngestManifest.DPNBag.LocalId, detail)
	assert.NotEmpty(t, dpnIngestManifest.DPNBag.Member, detail)
	assert.Equal(t, dpnIngestManifest.DPNBag.UUID, dpnIngestManifest.DPNBag.FirstVersionUUID, detail)
	assert.EqualValues(t, 1, dpnIngestManifest.DPNBag.Version, detail)
	assert.Equal(t, _context.Config.DPN.LocalNode, dpnIngestManifest.DPNBag.IngestNode, detail)
	assert.Equal(t, _context.Config.DPN.LocalNode, dpnIngestManifest.DPNBag.AdminNode, detail)
	assert.Equal(t, "D", dpnIngestManifest.DPNBag.BagType, detail)
	assert.NotEqual(t, 0, dpnIngestManifest.DPNBag.Size, detail)
	assert.False(t, dpnIngestManifest.DPNBag.CreatedAt.IsZero(), detail)
	assert.False(t, dpnIngestManifest.DPNBag.UpdatedAt.IsZero(), detail)

	require.NotEmpty(t, dpnIngestManifest.DPNBag.MessageDigests, detail)
	messageDigest := dpnIngestManifest.DPNBag.MessageDigests[0]
	assert.Equal(t, dpnIngestManifest.DPNBag.UUID, messageDigest.Bag, detail)
	assert.Equal(t, constants.AlgSha256, messageDigest.Algorithm, detail)
	assert.Equal(t, _context.Config.DPN.LocalNode, messageDigest.Node, detail)
	assert.Equal(t, 64, len(messageDigest.Value), detail)
	assert.False(t, messageDigest.CreatedAt.IsZero(), detail)

	assert.NotEmpty(t, dpnIngestManifest.LocalDir, detail)
	assert.NotEmpty(t, dpnIngestManifest.LocalTarFile, detail)
	assert.True(t, strings.HasSuffix(dpnIngestManifest.LocalTarFile, ".tar"), detail)

	// Bag has not yet been stored in Glacier, so this should be empty.
	assert.Empty(t, dpnIngestManifest.StorageURL, detail)
}
