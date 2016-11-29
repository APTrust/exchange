package integration_test

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/dpn/network"
	dpn_testutil "github.com/APTrust/exchange/dpn/util/testutil"
	apt_network "github.com/APTrust/exchange/network"
	apt_testutil "github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
	"time"
)

// TestDPNWorkItemsCompleted - make sure DPNWorkItems are marked as
// completed in Pharos.
func TestDPNWorkItemsCompleted(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")
	params := url.Values{}
	params.Set("task", "replication")
	params.Set("page", "1")
	params.Set("per_page", "100")
	resp := _context.PharosClient.DPNWorkItemList(params)
	require.Nil(t, resp.Error)
	for _, item := range resp.DPNWorkItems() {
		require.NotNil(t, item.QueuedAt, "QueuedAt nil for %s", item.Identifier)
		require.NotNil(t, item.QueuedAt, "CompletedAt nil for %s", item.Identifier)
		require.NotNil(t, item.State, "State nil for %s", item.Identifier)
		assert.False(t, item.QueuedAt.IsZero(), "QueuedAt is zero for %s", item.Identifier)
		assert.False(t, item.CompletedAt.IsZero(), "CompletedAt is zero for %s", item.Identifier)
		assert.Equal(t, "Bag copied to long-term storage", *item.Note, "Note is incorrect for %s", item.Identifier)
		assert.True(t, len(*item.State) > 250, "State JSON too short for %s", item.Identifier)
	}
}

// TestReplicationsMarkedAsStored - make sure that the ReplicationTransfer
// records are marked with stored = true on the remote nodes.
func TestReplicationsMarkedAsStored(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")

	// Get a list of ReplicationTransfers where our local node
	// is the ToNode, and make sure we marked them all as stored
	// on the FromNode.
	localClient, err := network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	require.Nil(t, err)
	remoteClients, err := localClient.GetRemoteClients()
	require.Nil(t, err)

	xferParams := url.Values{}
	xferParams.Set("to_node", _context.Config.DPN.LocalNode)
	dpnResp := localClient.ReplicationTransferList(xferParams)
	require.Nil(t, dpnResp.Error)
	for _, xfer := range dpnResp.ReplicationTransfers() {
		remoteClient := remoteClients[xfer.FromNode]
		require.NotNil(t, remoteClient)
		resp := remoteClient.ReplicationTransferGet(xfer.ReplicationId)
		require.Nil(t, resp.Error)
		remoteXfer := resp.ReplicationTransfer()
		require.NotNil(t, remoteXfer)
		assert.True(t, remoteXfer.Stored)
	}
}

// TestItemsAreInLongTermStorage - make sure that each tar file is
// stored in our S3 test storage bucket, with correct metadata.
func TestItemsAreInLongTermStorage(t *testing.T) {
	if !apt_testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	_context, err := apt_testutil.GetContext("integration.json")
	require.Nil(t, err, "Could not create context")

	localClient, err := network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	require.Nil(t, err)

	// s3List lists bucket contents.
	s3List := apt_network.NewS3ObjectList(
		constants.AWSVirginia,
		_context.Config.DPN.DPNPreservationBucket,
		int64(100),
	)
	// s3Head gets metadata about specific objects in S3/Glacier.
	s3Head := apt_network.NewS3Head(_context.Config.APTrustS3Region,
		_context.Config.DPN.DPNPreservationBucket)

	for _, identifier := range dpn_testutil.BAG_IDS {
		resp := localClient.DPNBagGet(identifier)
		dpnBag := resp.Bag()
		require.NotNil(t, dpnBag)
		if dpnBag.IngestNode == _context.Config.DPN.LocalNode {
			continue // we would not have replicated our own bag
		}
		tarFileName := fmt.Sprintf("%s.tar", identifier)
		s3List.GetList(tarFileName)
		require.NotEmpty(t, s3List.Response.Contents, "%s not found in S3", tarFileName)
		object := s3List.Response.Contents[0]
		fiveMinutesAgo := time.Now().UTC().Add(-5 * time.Minute)
		require.NotNil(t, object.LastModified)
		assert.True(t, object.LastModified.After(fiveMinutesAgo))

		// Make sure each item has the expected metadata.
		// s3Head.Response.Metadata is map[string]*string.
		s3Head.Head(tarFileName)
		require.Empty(t, s3Head.ErrorMessage)
		metadata := s3Head.Response.Metadata
		require.NotNil(t, metadata)
		// Amazon library transforms first letters of keys to CAPS
		require.NotNil(t, metadata["From_node"])
		require.NotNil(t, metadata["Transfer_id"])
		require.NotNil(t, metadata["Member"])
		require.NotNil(t, metadata["Local_id"])
		require.NotNil(t, metadata["Version"])

		assert.NotEmpty(t, *metadata["From_node"])
		assert.NotEmpty(t, *metadata["Transfer_id"])
		assert.NotEmpty(t, *metadata["Member"])
		assert.NotEmpty(t, *metadata["Local_id"])
		assert.NotEmpty(t, *metadata["Version"])
	}
}
