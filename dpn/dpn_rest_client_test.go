package dpn_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/dpn"
	"github.com/APTrust/exchange/models"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

/*
This file contains integration that rely on a locally-running instance
of the DPN REST service. The tests will not run if runRestTests()
determines that the DPN REST server is unreachable.

The dpn-server respository includes a set of test fixures under
test/fixtures/integration that contains the test data we're expecting
to find in these tests.

See the data/README.md file in that repo for information about how to
load that test data into your DPN instance.
*/

var configFile = "config/test.json"
var skipRestMessagePrinted = false
var aptrustBagIdentifier = "00000000-0000-4000-a000-000000000001"
var replicationIdentifier = "10000000-0000-4111-a000-000000000001"
var restoreIdentifier = "11000000-0000-4111-a000-000000000001"
var memberIdentifier = "9a000000-0000-4000-a000-000000000001"

// This is the fixity value for replicationIdentifier above
var fixityForReplication = "e39a201a88bc3d7803a5e375d9752439d328c2e85b4f1ba70a6d984b6c5378bd"

func runRestTests(t *testing.T) bool {
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	_, err = http.Get(config.DPN.RestClient.LocalServiceURL)
	if err != nil {
		if skipRestMessagePrinted == false {
			skipRestMessagePrinted = true
			fmt.Printf("Skipping DPN REST integration tests: "+
				"DPN REST server is not running at %s\n",
				config.DPN.RestClient.LocalServiceURL)
		}
		return false
	}
	return true
}

func getClient(t *testing.T) (*dpn.DPNRestClient) {
	// If you want to debug, change ioutil.Discard to os.Stdout
	// to see log output from the client.
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	client, err := dpn.NewDPNRestClient(
		config.DPN.RestClient.LocalServiceURL,
		config.DPN.RestClient.LocalAPIRoot,
		config.DPN.RestClient.LocalAuthToken,
		config.DPN.LocalNode,
		config.DPN)
	if err != nil {
		t.Errorf("Error constructing DPN REST client: %v", err)
	}
	return client
}

func getRemoteClient(t *testing.T, namespace string) (*dpn.DPNRestClient) {
	// If you want to debug, change ioutil.Discard to os.Stdout
	// to see log output from the client.
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	client, err := dpn.NewDPNRestClient(
		config.DPN.RestClient.LocalServiceURL,
		config.DPN.RestClient.LocalAPIRoot,
		config.DPN.RestClient.LocalAuthToken,
		config.DPN.LocalNode,
		config.DPN)
	if err != nil {
		t.Errorf("Error constructing DPN REST client: %v", err)
	}
	remoteClient, err := client.GetRemoteClient(namespace, config.DPN)
	if err != nil {
		t.Errorf("Error constructing remote DPN REST client for node %s: %v",
			namespace, err)
	}
	return remoteClient
}

func TestBuildUrl(t *testing.T) {
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	client := getClient(t)
	require.NotNil(t, client)
	relativeUrl := "/api-v1/popeye/olive/oyl/"
	expectedUrl := config.DPN.RestClient.LocalServiceURL + relativeUrl
	if client.BuildUrl(relativeUrl, nil) != expectedUrl {
		t.Errorf("BuildUrl returned '%s', expected '%s'",
			client.BuildUrl(relativeUrl, nil), expectedUrl)
	}
	params := url.Values{}
	params.Set("color", "blue")
	params.Set("material", "cotton")
	params.Set("size", "extra medium")
	actualUrl := client.BuildUrl(relativeUrl, &params)
	expectedUrl = expectedUrl + "?color=blue&material=cotton&size=extra+medium"
	assert.Equal(t, expectedUrl, actualUrl)
}

func TestNodeGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.NodeGet("aptrust")
	require.Nil(t, resp.Error)
	node := resp.Node()
	require.NotNil(t, node)
	assert.NotNil(t, resp.Request)
	assert.NotNil(t, resp.Response)
	assert.NotEmpty(t, node.Name)
	assert.NotEmpty(t, node.Namespace)
	// In test and local integration environments,
	// we're running on HTTP, not HTTPS.
	assert.True(t, strings.HasPrefix(node.APIRoot, "http://"))
}

func TestNodeList(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	nodeList := client.NodeList(nil)
	require.Nil(t, nodeList.Error)
	require.NotEmpty(t, nodeList.Nodes())
	assert.EqualValues(t, 5, nodeList.Count)
	assert.Equal(t, 5, len(nodeList.Nodes()))
	assert.NotNil(t, nodeList.Request)
	assert.NotNil(t, nodeList.Response)
}

// ---------------------------------------------------------------------
// TODO: Figure out what's wrong with the DPN server.
// This test submits valid JSON to update a node, but the server
// responds with a 500/Internal Server Error. Here's the full error:
//
// ActiveRecord::StatementInvalid (SQLite3::ConstraintException:
// nodes.storage_region_id may not be NULL: UPDATE "nodes"
// SET "name" = ?, "ssh_pubkey" = ?, "storage_region_id" = ?,
// "storage_type_id" = ?, "updated_at" = ? WHERE "nodes"."id"= ?):
// app/controllers/nodes_controller.rb:52:in `update'
// ---------------------------------------------------------------------
func TestNodeUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.NodeGet("sdr")
	require.Nil(t, resp.Error)
	node := resp.Node()
	require.NotNil(t, node)

	origName := node.Name
	if origName == "" {
		origName = "No Name"
	}
	// Reverse the name.
	newName := make([]rune, utf8.RuneCountInString(origName));
	i := len(origName);
	for _, c := range origName {
		i--;
		newName[i] = c;
	}
	node.Name = string(newName)
	savedNodeResult := client.NodeUpdate(node)
	require.Nil(t, savedNodeResult.Error)
	require.NotNil(t, savedNodeResult.Node())
	assert.NotNil(t, savedNodeResult.Request)
	assert.NotNil(t, savedNodeResult.Response)

	// This is broken on the server, causing our test to fail.
	// Uncomment when the server is fixed.
	assert.Equal(t, string(newName), savedNodeResult.Node().Name)
}

func TestNodeGetLastPullDate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	// In reality, we would not try to get the last pull date
	// for our own node, because we don't sync data with ourselves.
	// However, our fixture data starts with just one bag, of
	// which we are the admin node. The last pull date should
	// match the updated_at time on that bag.
	lastPull, err := client.NodeGetLastPullDate("aptrust")
	assert.Nil(t, err)
	assert.False(t, lastPull.IsZero())
}

func TestMemberList(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	memberList := client.MemberList(nil)
	assert.Nil(t, memberList.Error)
	assert.EqualValues(t, 5, memberList.Count)
	assert.EqualValues(t, 5, len(memberList.Members()))
	params := url.Values{}
	params.Set("name", "Faber College")
	memberList  = client.MemberList(&params)
	assert.Nil(t, memberList.Error)
	assert.NotNil(t, memberList.Request)
	assert.NotNil(t, memberList.Response)
	assert.EqualValues(t, 1, memberList.Count)
	assert.Equal(t, 1, len(memberList.Members()))
}

func TestMemberGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.MemberGet(memberIdentifier)
	require.Nil(t, resp.Error)
	require.NotNil(t, resp.Member)
	assert.NotNil(t, resp.Request)
	assert.NotNil(t, resp.Response)
	assert.Equal(t, memberIdentifier, resp.Member().MemberId)
}

func TestMemberCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	id := uuid.NewV4().String()
	member := dpn.Member{
		MemberId: id,
		Name: fmt.Sprintf("GO-TEST-MEMBER-%s", id),
		Email: fmt.Sprintf("%s@example.com", id),
	}
	resp := client.MemberCreate(&member)
	require.Nil(t, resp.Error)
	require.NotNil(t, resp.Member)
	assert.NotNil(t, resp.Request)
	assert.NotNil(t, resp.Response)
	newMember := resp.Member()
	require.NotNil(t, newMember)
	assert.Equal(t, member.MemberId, newMember.MemberId)
	assert.Equal(t, member.Name, newMember.Name)
	assert.Equal(t, member.Email, newMember.Email)
}

func TestMemberUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.MemberGet(memberIdentifier)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	member := resp.Member()
	require.NotNil(t, member)
	newName := fmt.Sprintf("GO-UPDATED-%s", uuid.NewV4().String())
	member.Name = newName
	newMemberResponse := client.MemberUpdate(member)
	require.NotNil(t, newMemberResponse)
	require.Nil(t, newMemberResponse.Error)
	assert.Equal(t, newName, newMemberResponse.Member().Name)
}

func TestDPNBagGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.DPNBagGet(aptrustBagIdentifier)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	bag := resp.Bag()
	require.NotNil(t, bag)
	assert.Equal(t, aptrustBagIdentifier, bag.UUID)
	assert.Equal(t, "APTrust Bag 1", bag.LocalId)
	assert.EqualValues(t, 71680, bag.Size)
	assert.Equal(t, aptrustBagIdentifier, bag.FirstVersionUUID)
	assert.Equal(t, "D", bag.BagType)
	assert.EqualValues(t, 1, bag.Version)
	assert.Equal(t, "aptrust", bag.IngestNode)
	assert.Equal(t, "aptrust", bag.AdminNode)
	assert.Equal(t, "2015-09-15T17:56:03Z", bag.CreatedAt.Format(time.RFC3339))
	assert.Equal(t, "2015-09-15T17:56:03Z", bag.UpdatedAt.Format(time.RFC3339))
	assert.Equal(t, 2, len(bag.ReplicatingNodes))
	require.True(t, len(bag.ReplicatingNodes) > 1)
	assert.Equal(t, "chron", bag.ReplicatingNodes[0])
	assert.Equal(t, "hathi", bag.ReplicatingNodes[1])
}

func TestDPNBagList(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.DPNBagList(nil)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	unfilteredCount := resp.Count
	if unfilteredCount == 0 {
		t.Errorf("DPNBagList returned zero results. Are there any bags in the registry?")
		return
	}
	aptrustCount := 0
	bags := resp.Bags()
	for i := range bags {
		if bags[i].IngestNode == "aptrust" {
			aptrustCount++
		}
	}

	// Test filters
	// Get all bags updated after December 31, 1969
	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params := url.Values{}
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	resp = client.DPNBagList(&params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, unfilteredCount, resp.Count)

	// Get all bags updated after 1 hour from now
	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	resp = client.DPNBagList(&params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.EqualValues(t, 0, resp.Count)
}

func TestDPNBagCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bag := MakeDPNBag()
	resp := client.DPNBagCreate(bag)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	newBag := resp.Bag()
	require.NotNil(t, newBag)
	assert.Equal(t, bag.UUID, newBag.UUID)
	assert.Equal(t, bag.LocalId, newBag.LocalId)
	assert.Equal(t, bag.Size, newBag.Size)
	assert.Equal(t, bag.FirstVersionUUID, newBag.FirstVersionUUID)
	assert.Equal(t, bag.Version, newBag.Version)
	assert.Equal(t, bag.BagType, newBag.BagType)
	assert.NotEmpty(t, newBag.IngestNode)
	assert.Equal(t, newBag.IngestNode, newBag.AdminNode)
	assert.NotEmpty(t, newBag.CreatedAt)
	assert.NotEmpty(t, newBag.UpdatedAt)
}

func TestDPNBagUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bag := MakeDPNBag()
	resp := client.DPNBagCreate(bag)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	// We have to set UpdatedAt ahead, or the server won't update
	// record we're sending.
	newTimestamp := time.Now().UTC().Add(1 * time.Second).Truncate(time.Second)
	newLocalId := fmt.Sprintf("GO-TEST-BAG-%s", uuid.NewV4().String())

	dpnBag := resp.Bag()
	dpnBag.UpdatedAt = newTimestamp
	dpnBag.LocalId = newLocalId

	updateResp := client.DPNBagUpdate(dpnBag)
	require.NotNil(t, updateResp)
	require.Nil(t, updateResp.Error)
	updatedBag := updateResp.Bag()
	require.NotNil(t, updatedBag)
	assert.InDelta(t, newTimestamp.Unix(), updatedBag.UpdatedAt.Unix(), float64(2.0))
	assert.Equal(t, newLocalId, updatedBag.LocalId)
}

func TestReplicationTransferGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.ReplicationTransferGet(replicationIdentifier)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	xfer := resp.ReplicationTransfer()
	require.NotNil(t, xfer)

	assert.Equal(t, "aptrust", xfer.FromNode)
	assert.Equal(t, "hathi", xfer.ToNode)
	assert.Equal(t, aptrustBagIdentifier, xfer.Bag)
	assert.Equal(t, replicationIdentifier, xfer.ReplicationId)

	if xfer.FixityNonce != nil && *xfer.FixityNonce != "" {
		t.Errorf("FixityNonce: expected '', got '%s'", *xfer.FixityNonce)
	}
	if xfer.FixityValue == nil || *xfer.FixityValue != fixityForReplication {
		t.Errorf("FixityValue: expected '%s', got '%s'", fixityForReplication, *xfer.FixityValue)
	}

	assert.Equal(t, "sha256", xfer.FixityAlgorithm)
	assert.False(t, xfer.Cancelled)
	assert.True(t, xfer.Stored)
	assert.True(t, xfer.StoreRequested)
	assert.Equal(t, "rsync", xfer.Protocol)

	expectedTarName := fmt.Sprintf("%s.tar", aptrustBagIdentifier)
	assert.True(t, strings.HasSuffix(xfer.Link, expectedTarName))
	assert.Equal(t, "2015-09-15T19:38:31Z", xfer.CreatedAt.Format(time.RFC3339))
	assert.Equal(t, "2015-09-15T19:38:31Z", xfer.UpdatedAt.Format(time.RFC3339))
}

func TestReplicationList(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.ReplicationList(nil)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.True(t, resp.Count > 0)
	assert.True(t, len(resp.ReplicationTransfers()) > 0)

	totalRecordCount := resp.Count

	params := &url.Values{}
	params.Set("bag_valid", "true")
	resp = client.ReplicationList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	params.Set("bag_valid", "false")
	resp = client.ReplicationList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	params.Del("bag_valid")
	params.Set("fixity_accept", "true")
	resp = client.ReplicationList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	params.Set("fixity_accept", "false")
	resp  = client.ReplicationList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	params.Del("fixity_accept")

	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	resp = client.ReplicationList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	assert.Equal(t, totalRecordCount, resp.Count)

	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	resp = client.ReplicationList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.EqualValues(t, 0, resp.Count)
}

func TestReplicationTransferCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	resp := client.DPNBagCreate(bag)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	// Make sure we can create a transfer request.
	xfer := MakeXferRequest("aptrust", "chron", resp.Bag().UUID)
	xferResp := client.ReplicationTransferCreate(xfer)
	require.NotNil(t, xferResp)
	require.Nil(t, xferResp.Error)

	newXfer := xferResp.ReplicationTransfer()
	require.NotNil(t, newXfer)

	assert.Equal(t, xfer.FromNode, newXfer.FromNode)
	assert.Equal(t, xfer.ToNode, newXfer.ToNode)
	assert.Equal(t, xfer.Bag, newXfer.Bag)
	assert.NotEmpty(t, newXfer.ReplicationId)
	assert.Equal(t, xfer.FixityAlgorithm, newXfer.FixityAlgorithm)
	assert.Equal(t, xfer.FixityNonce, newXfer.FixityNonce)
	assert.Equal(t, xfer.FixityValue, newXfer.FixityValue)
	assert.Equal(t, xfer.Stored, newXfer.Stored)
	assert.Equal(t, xfer.StoreRequested, newXfer.StoreRequested)
	assert.Equal(t, xfer.Cancelled, newXfer.Cancelled)
	assert.Equal(t, xfer.CancelReason, newXfer.CancelReason)
	assert.Equal(t, xfer.Protocol, newXfer.Protocol)
	assert.Equal(t, xfer.Link, newXfer.Link)
	assert.NotEmpty(t, newXfer.CreatedAt)
	assert.NotEmpty(t, newXfer.UpdatedAt)
}

func TestReplicationTransferUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	resp := client.DPNBagCreate(bag)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	// Make sure we can create a transfer request.
	xfer := MakeXferRequest("chron", "aptrust", bag.UUID)

	// Null out the fixity value, because once it's set, we can't change
	// it. And below, we want to set a bad fixity value to see what happens.
	xfer.FixityValue = nil
	xferResp := client.ReplicationTransferCreate(xfer)
	require.NotNil(t, xferResp)
	require.Nil(t, xferResp.Error)

	// Mark as received, with a bad fixity.
	newXfer := xferResp.ReplicationTransfer()
	newFixityValue :=  "1234567890"
	newXfer.UpdatedAt = newXfer.UpdatedAt.Add(1 * time.Second)
	newXfer.FixityValue = &newFixityValue

	updateResp := client.ReplicationTransferUpdate(newXfer)
	require.NotNil(t, updateResp)
	require.Nil(t, updateResp.Error)
	updatedXfer := updateResp.ReplicationTransfer()
	require.NotNil(t, updatedXfer)
	assert.False(t, updatedXfer.StoreRequested)
}

func TestRestoreTransferGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.RestoreTransferGet(restoreIdentifier)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	xfer := resp.RestoreTransfer()
	require.NotNil(t, xfer)
	assert.Equal(t, "hathi", xfer.FromNode)
	assert.Equal(t, "aptrust", xfer.ToNode)
	assert.Equal(t, aptrustBagIdentifier, xfer.Bag)
	assert.Equal(t, restoreIdentifier, xfer.RestoreId)
	assert.Equal(t, "rsync", xfer.Protocol)
	assert.Equal(t, "2015-09-15T19:38:31Z", xfer.CreatedAt.Format(time.RFC3339))
	assert.Equal(t, "2015-09-15T19:38:31Z", xfer.UpdatedAt.Format(time.RFC3339))
	expectedTarName := fmt.Sprintf("%s.tar", aptrustBagIdentifier)
	assert.True(t, strings.HasSuffix(xfer.Link, expectedTarName))
}

func TestRestoreTransferList(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.RestoreTransferList(nil)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.NotEmpty(t, resp.RestoreTransfers())
	assert.False(t, resp.Count == 0)

	totalRecordCount := resp.Count

	params := &url.Values{}
	params.Set("bag_valid", "true")
	resp  = client.RestoreTransferList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	params.Set("bag_valid", "false")
	resp = client.RestoreTransferList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	params.Del("bag_valid")
	params.Set("fixity_accept", "true")
	resp = client.RestoreTransferList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	params.Set("fixity_accept", "false")
	resp = client.RestoreTransferList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	params.Del("fixity_accept")

	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	resp = client.RestoreTransferList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, totalRecordCount, resp.Count)

	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	resp = client.RestoreTransferList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.EqualValues(t, 0, resp.Count)
	assert.Empty(t, resp.RestoreTransfers())
}

func TestRestoreTransferCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	resp := client.DPNBagCreate(bag)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	// Make sure we can create a transfer request.
	xfer := MakeRestoreRequest("tdr", "aptrust", bag.UUID)
	createResp := client.RestoreTransferCreate(xfer)
	require.NotNil(t, createResp)
	require.Nil(t, createResp.Error)

	createdXfer := createResp.RestoreTransfer()
	require.NotNil(t, createdXfer)
	assert.Equal(t, xfer.FromNode, createdXfer.FromNode)
	assert.Equal(t, xfer.ToNode, createdXfer.ToNode)
	assert.Equal(t, xfer.Bag, createdXfer.Bag)
	assert.NotEmpty(t, createdXfer.RestoreId)
	assert.Equal(t, xfer.Protocol, createdXfer.Protocol)
	assert.Equal(t, xfer.Link, createdXfer.Link)
	assert.NotEmpty(t, createdXfer.CreatedAt)
	assert.NotEmpty(t, createdXfer.UpdatedAt)
}

func TestRestoreTransferUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	resp := client.DPNBagCreate(bag)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)

	// Make sure we can create a transfer request.
	xfer := MakeRestoreRequest("chron", "aptrust", bag.UUID)
	createResp := client.RestoreTransferCreate(xfer)
	require.NotNil(t, createResp)
	require.Nil(t, createResp.Error)

	newXfer := createResp.RestoreTransfer()
	require.NotNil(t, newXfer)

	// Accept this one...
	newXfer.Accepted = true

	updateResp := client.RestoreTransferUpdate(newXfer)
	require.NotNil(t, updateResp)
	require.Nil(t, updateResp.Error)

	updatedXfer := updateResp.RestoreTransfer()
	require.NotNil(t, updatedXfer)

	assert.True(t, updatedXfer.Accepted)
}

func TestGetRemoteClient(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	client := getClient(t)
	nodes := []string { "chron", "hathi", "sdr", "tdr" }
	for _, node := range nodes {
		_, err := client.GetRemoteClient(node, config.DPN)
		assert.Nil(t, err)
	}
}

func TestGetRemoteClients(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	remoteClients, err := client.GetRemoteClients()
	require.Nil(t, err)
	nodes := []string { "chron", "hathi", "sdr", "tdr" }
	for _, node := range nodes {
		remoteClient := remoteClients[node]
		assert.NotNil(t, remoteClient)
		assert.Equal(t, node, remoteClient.Node)
	}
}

func TestHackNullDates(t *testing.T) {
	jsonString := `{ "id": 5, "last_pull_date": null }`
	testHackNullDates(jsonString, t)
	jsonString = `{"id":5,"last_pull_date":null}`
	testHackNullDates(jsonString, t)
	jsonString = `{
					 "id": 5,
					 "last_pull_date": null
				   }`
	testHackNullDates(jsonString, t)
}

func testHackNullDates(jsonString string, t *testing.T) {
	data := make(map[string]interface{})
	jsonBytes := []byte(jsonString)
	hackedBytes := dpn.HackNullDates(jsonBytes)
	json.Unmarshal(hackedBytes, &data)
	assert.Equal(t, "1980-01-01T00:00:00Z", data["last_pull_date"])
}

func TestDigestGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	resp := client.DigestGet(aptrustBagIdentifier, "sha256")
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.NotNil(t, resp.Digest())

	// Make sure no error for digest that doesn't exist.
	resp = client.DigestGet("00000000-0000-0000-0000-000000000000", "sha256")
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, 0, resp.Count)
	assert.Nil(t, resp.Digest())
}

func TestDigestList(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	params := &url.Values{}
	params.Set("page", "1")
	params.Set("per_page", "10")
	params.Set("order_by", "created_at")
	params.Set("uuid", aptrustBagIdentifier)
	resp := client.DigestList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.True(t, resp.Count > 0)
	assert.True(t, len(resp.Digests()) > 0)
}

func TestDigestCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// We have to make a new bag first, because
	// the existing bag already has a sha256 digest.
	bag := MakeDPNBag()
	resp := client.DPNBagCreate(bag)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	newBag := resp.Bag()
	require.NotNil(t, newBag)

	digest := MakeMessageDigest(newBag.UUID, "aptrust")
	resp = client.DigestCreate(digest)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.NotNil(t, resp.Digest())
}

func TestFixityCheckList(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	params := &url.Values{}
	params.Set("page", "1")
	params.Set("per_page", "10")
	params.Set("order_by", "created_at")
	params.Set("uuid", aptrustBagIdentifier)
	client := getClient(t)
	resp := client.FixityCheckList(nil)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.True(t, resp.Count > 0)
	assert.True(t, len(resp.FixityChecks()) > 0)
}

func TestFixityCheckCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	fixityCheck := MakeFixityCheck(aptrustBagIdentifier, "aptrust")
	client := getClient(t)
	resp := client.FixityCheckCreate(fixityCheck)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.NotNil(t, resp.FixityCheck())
}

func TestIngestList(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	params := &url.Values{}
	params.Set("ingested", "true")
	client := getClient(t)
	resp := client.IngestList(params)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.True(t, resp.Count > 0)
	assert.NotEmpty(t, resp.Ingests())
}

func TestIngestCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	ingest := MakeIngest(aptrustBagIdentifier)
	client := getClient(t)
	resp := client.IngestCreate(ingest)
	require.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.NotNil(t, resp.Ingest())
}


// -------------------------------------------------------------------------
// HTTP test handlers from here down...
// -------------------------------------------------------------------------

// Build a simple struct that mimics the structure of a DPN
// JSON list response. That includes keys count, next, previous,
// and results. The caller will add ["results"] with a list of
// objects of the appropriate type.
func listResponseData() (map[string]interface{}) {
	data := make(map[string]interface{})
	data["count"] = 100
	data["next"] = "http://example.com/?page=11"
	data["previous"] = "http://example.com/?page=9"
	return data
}

// Returns some sample URL parameters.
func sampleParams() (url.Values) {
	v := url.Values{}
	v.Add("member", "00000000-0000-0000-0000-000000000000")
	v.Add("ingest_node", "aptrust")
	v.Add("page", "1")
	v.Add("per_page", "20")
	return v
}

// -------------------------------------------------------------------------
// Node handlers - used for unit testing DPNResponse.
// -------------------------------------------------------------------------

func nodeGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := MakeDPNNode()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func nodeListHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*dpn.Node, 4)
	for i := 0; i < 4; i++ {
		list[i] = MakeDPNNode()
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}
