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
	//	"unicode/utf8" // See comment below, in NodeUpdate
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
	result := client.NodeGet("aptrust")
	require.Nil(t, result.Error)
	require.NotNil(t, result.Node)
	assert.NotNil(t, result.Request)
	assert.NotNil(t, result.Response)
	assert.Equal(t, "APTrust", result.Node.Name)
	assert.Equal(t, "aptrust", result.Node.Namespace)
	if !strings.HasPrefix(result.Node.APIRoot, "https://") && !strings.HasPrefix(result.Node.APIRoot, "http://") {
		t.Errorf("APIRoot should begin with http:// or https://")
	}
}

func TestNodeListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	nodeList := client.NodeListGet(nil)
	require.Nil(t, nodeList.Error)
	require.NotEmpty(t, nodeList.Results)
	assert.EqualValues(t, 5, nodeList.Count)
	assert.Equal(t, 5, len(nodeList.Results))
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
// func TestNodeUpdate(t *testing.T) {
// 	if runRestTests(t) == false {
// 		return
// 	}
// 	client := getClient(t)
// 	result := client.NodeGet("sdr")
// 	require.Nil(t, result.Error)

// 	origName := result.Node.Name
// 	if origName == "" {
// 		origName = "No Name"
// 	}
// 	// Reverse the name.
//     newName := make([]rune, utf8.RuneCountInString(origName));
//     i := len(origName);
//     for _, c := range origName {
// 		i--;
// 		newName[i] = c;
//     }
// 	result.Node.Name = string(newName)
// 	savedNodeResult := client.NodeUpdate(result.Node)
// 	require.Nil(t, savedNodeResult.Error)
// 	require.NotNil(t, savedNodeResult.Node)
// 	assert.NotNil(t, savedNodeResult.Request)
// 	assert.NotNil(t, savedNodeResult.Response)

// 	// This is broken on the server, causing our test to fail.
// 	// Uncomment when the server is fixed.
// 	// 	assert.Equal(t, newName, savedNodeResult.Node.Name)
// }

func TestNodeGetLastPullDate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	nodes := []string{"tdr", "sdr", "hathi", "chron"}
	for _, node := range nodes {
		lastPull, err := client.NodeGetLastPullDate(node)
		if err != nil {
			t.Errorf("Error getting last pull date for %s: %v", node, err)
		}
		if lastPull.IsZero() {
			t.Errorf("Error getting last pull date for %s is empty", node)
		}
	}
}

func TestMemberListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	memberList := client.MemberListGet(nil)
	assert.Nil(t, memberList.Error)
	assert.EqualValues(t, 5, memberList.Count)
	assert.EqualValues(t, 5, len(memberList.Results))
	params := url.Values{}
	params.Set("name", "Faber College")
	memberList  = client.MemberListGet(&params)
	assert.Nil(t, memberList.Error)
	assert.NotNil(t, memberList.Request)
	assert.NotNil(t, memberList.Response)
	assert.EqualValues(t, 1, memberList.Count)
	assert.Equal(t, 1, len(memberList.Results))
}

func TestMemberGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	result := client.MemberGet(memberIdentifier)
	require.Nil(t, result.Error)
	require.NotNil(t, result.Member)
	assert.NotNil(t, result.Request)
	assert.NotNil(t, result.Response)
	assert.Equal(t, memberIdentifier, result.Member.MemberId)
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
	result := client.MemberCreate(&member)
	require.Nil(t, result.Error)
	require.NotNil(t, result.Member)
	assert.NotNil(t, result.Request)
	assert.NotNil(t, result.Response)
	assert.Equal(t, member.MemberId, result.Member.MemberId)
	assert.Equal(t, member.Name, result.Member.Name)
	assert.Equal(t, member.Email, result.Member.Email)
}

func TestMemberUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	memberResult := client.MemberGet(memberIdentifier)
	require.NotNil(t, memberResult)
	require.Nil(t, memberResult.Error)
	newName := fmt.Sprintf("GO-UPDATED-%s", uuid.NewV4().String())
	memberResult.Member.Name = newName
	newMemberResult := client.MemberUpdate(memberResult.Member)
	require.NotNil(t, newMemberResult)
	require.Nil(t, newMemberResult.Error)
	assert.Equal(t, newName, newMemberResult.Member.Name)
}

func TestDPNBagGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bagResult := client.DPNBagGet(aptrustBagIdentifier)
	require.NotNil(t, bagResult)
	require.Nil(t, bagResult.Error)
	assert.Equal(t, aptrustBagIdentifier, bagResult.Bag.UUID)
	assert.Equal(t, "APTrust Bag 1", bagResult.Bag.LocalId)
	assert.EqualValues(t, 71680, bagResult.Bag.Size)
	assert.Equal(t, aptrustBagIdentifier, bagResult.Bag.FirstVersionUUID)
	assert.Equal(t, "D", bagResult.Bag.BagType)
	assert.EqualValues(t, 1, bagResult.Bag.Version)
	assert.Equal(t, "aptrust", bagResult.Bag.IngestNode)
	assert.Equal(t, "aptrust", bagResult.Bag.AdminNode)
	assert.Equal(t, "2015-09-15T17:56:03Z", bagResult.Bag.CreatedAt.Format(time.RFC3339))
	assert.Equal(t, "2015-09-15T17:56:03Z", bagResult.Bag.UpdatedAt.Format(time.RFC3339))
	assert.Equal(t, 2, len(bagResult.Bag.ReplicatingNodes))
	require.True(t, len(bagResult.Bag.ReplicatingNodes) > 1)
	assert.Equal(t, "chron", bagResult.Bag.ReplicatingNodes[0])
	assert.Equal(t, "hathi", bagResult.Bag.ReplicatingNodes[1])
}

func TestDPNBagListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bagList := client.DPNBagListGet(nil)
	require.NotNil(t, bagList)
	require.Nil(t, bagList.Error)

	unfilteredCount := bagList.Count
	if unfilteredCount == 0 {
		t.Errorf("DPNBagListGet returned zero results. Are there any bags in the registry?")
		return
	}
	aptrustCount := 0
	for i := range bagList.Results {
		bag := bagList.Results[i]
		if bag.IngestNode == "aptrust" {
			aptrustCount++
		}
	}

	// Test filters
	// Get all bags updated after December 31, 1969
	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params := url.Values{}
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	bagList = client.DPNBagListGet(&params)
	require.NotNil(t, bagList)
	require.Nil(t, bagList.Error)
	assert.Equal(t, unfilteredCount, bagList.Count)

	// Get all bags updated after 1 hour from now
	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	bagList = client.DPNBagListGet(&params)
	require.NotNil(t, bagList)
	require.Nil(t, bagList.Error)
	assert.EqualValues(t, 0, bagList.Count)
}

func TestDPNBagCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bag := MakeDPNBag()
	dpnBagResult := client.DPNBagCreate(bag)
	require.NotNil(t, dpnBagResult)
	require.Nil(t, dpnBagResult.Error)
	assert.Equal(t, bag.UUID, dpnBagResult.Bag.UUID)
	assert.Equal(t, bag.LocalId, dpnBagResult.Bag.LocalId)
	assert.Equal(t, bag.Size, dpnBagResult.Bag.Size)
	assert.Equal(t, bag.FirstVersionUUID, dpnBagResult.Bag.FirstVersionUUID)
	assert.Equal(t, bag.Version, dpnBagResult.Bag.Version)
	assert.Equal(t, bag.BagType, dpnBagResult.Bag.BagType)
	assert.NotEmpty(t, dpnBagResult.Bag.IngestNode)
	assert.Equal(t, dpnBagResult.Bag.IngestNode, dpnBagResult.Bag.AdminNode)
	assert.NotEmpty(t, dpnBagResult.Bag.CreatedAt)
	assert.NotEmpty(t, dpnBagResult.Bag.UpdatedAt)
}

func TestDPNBagUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bag := MakeDPNBag()
	dpnBagResult := client.DPNBagCreate(bag)
	require.NotNil(t, dpnBagResult)
	require.Nil(t, dpnBagResult.Error)

	// We have to set UpdatedAt ahead, or the server won't update
	// record we're sending.
	newTimestamp := time.Now().UTC().Add(1 * time.Second).Truncate(time.Second)
	newLocalId := fmt.Sprintf("GO-TEST-BAG-%s", uuid.NewV4().String())

	dpnBag := dpnBagResult.Bag
	dpnBag.UpdatedAt = newTimestamp
	dpnBag.LocalId = newLocalId

	updatedBagResult := client.DPNBagUpdate(dpnBag)
	require.NotNil(t, updatedBagResult)
	require.Nil(t, updatedBagResult.Error)
	assert.InDelta(t, newTimestamp.Unix(), updatedBagResult.Bag.UpdatedAt.Unix(), float64(2.0))
	assert.Equal(t, newLocalId, updatedBagResult.Bag.LocalId)
}

func TestReplicationTransferGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xferResult := client.ReplicationTransferGet(replicationIdentifier)
	require.NotNil(t, xferResult)
	require.Nil(t, xferResult.Error)
	assert.Equal(t, "aptrust", xferResult.Xfer.FromNode)
	assert.Equal(t, "hathi", xferResult.Xfer.ToNode)
	assert.Equal(t, aptrustBagIdentifier, xferResult.Xfer.Bag)
	assert.Equal(t, replicationIdentifier, xferResult.Xfer.ReplicationId)

	if xferResult.Xfer.FixityNonce != nil && *xferResult.Xfer.FixityNonce != "" {
		t.Errorf("FixityNonce: expected '', got '%s'", *xferResult.Xfer.FixityNonce)
	}
	if xferResult.Xfer.FixityValue == nil || *xferResult.Xfer.FixityValue != fixityForReplication {
		t.Errorf("FixityValue: expected '%s', got '%s'", fixityForReplication, *xferResult.Xfer.FixityValue)
	}

	assert.Equal(t, "sha256", xferResult.Xfer.FixityAlgorithm)
	assert.False(t, xferResult.Xfer.Cancelled)
	assert.True(t, xferResult.Xfer.Stored)
	assert.True(t, xferResult.Xfer.StoreRequested)
	assert.Equal(t, "rsync", xferResult.Xfer.Protocol)

	expectedTarName := fmt.Sprintf("%s.tar", aptrustBagIdentifier)
	assert.True(t, strings.HasSuffix(xferResult.Xfer.Link, expectedTarName))
	assert.Equal(t, "2015-09-15T19:38:31Z", xferResult.Xfer.CreatedAt.Format(time.RFC3339))
	assert.Equal(t, "2015-09-15T19:38:31Z", xferResult.Xfer.UpdatedAt.Format(time.RFC3339))
}

func TestReplicationListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xferList := client.ReplicationListGet(nil)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)
	assert.True(t, xferList.Count > 0)
	assert.True(t, len(xferList.Results) > 0)

	totalRecordCount := xferList.Count

	params := &url.Values{}
	params.Set("bag_valid", "true")
	xferList = client.ReplicationListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)

	params.Set("bag_valid", "false")
	xferList = client.ReplicationListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)

	params.Del("bag_valid")
	params.Set("fixity_accept", "true")
	xferList = client.ReplicationListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)

	params.Set("fixity_accept", "false")
	xferList  = client.ReplicationListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)

	params.Del("fixity_accept")

	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	xferList = client.ReplicationListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)

	assert.Equal(t, totalRecordCount, xferList.Count)

	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	xferList = client.ReplicationListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)
	assert.EqualValues(t, 0, xferList.Count)
}

func TestReplicationTransferCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	dpnBagResult := client.DPNBagCreate(bag)
	require.NotNil(t, dpnBagResult)
	require.Nil(t, dpnBagResult.Error)

	// Make sure we can create a transfer request.
	xfer := MakeXferRequest("aptrust", "chron", dpnBagResult.Bag.UUID)
	xferResult := client.ReplicationTransferCreate(xfer)
	require.NotNil(t, xferResult)
	require.Nil(t, xferResult.Error)

	assert.Equal(t, xfer.FromNode, xferResult.Xfer.FromNode)
	assert.Equal(t, xfer.ToNode, xferResult.Xfer.ToNode)
	assert.Equal(t, xfer.Bag, xferResult.Xfer.Bag)
	assert.NotEmpty(t, xferResult.Xfer.ReplicationId)
	assert.Equal(t, xfer.FixityAlgorithm, xferResult.Xfer.FixityAlgorithm)
	assert.Equal(t, xfer.FixityNonce, xferResult.Xfer.FixityNonce)
	assert.Equal(t, xfer.FixityValue, xferResult.Xfer.FixityValue)
	assert.Equal(t, xfer.Stored, xferResult.Xfer.Stored)
	assert.Equal(t, xfer.StoreRequested, xferResult.Xfer.StoreRequested)
	assert.Equal(t, xfer.Cancelled, xferResult.Xfer.Cancelled)
	assert.Equal(t, xfer.CancelReason, xferResult.Xfer.CancelReason)
	assert.Equal(t, xfer.Protocol, xferResult.Xfer.Protocol)
	assert.Equal(t, xfer.Link, xferResult.Xfer.Link)
	assert.NotEmpty(t, xferResult.Xfer.CreatedAt)
	assert.NotEmpty(t, xferResult.Xfer.UpdatedAt)
}

func TestReplicationTransferUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	dpnBagResult := client.DPNBagCreate(bag)
	require.NotNil(t, dpnBagResult)
	require.Nil(t, dpnBagResult.Error)

	// Make sure we can create a transfer request.
	xfer := MakeXferRequest("chron", "aptrust", bag.UUID)

	// Null out the fixity value, because once it's set, we can't change
	// it. And below, we want to set a bad fixity value to see what happens.
	xfer.FixityValue = nil
	xferResult := client.ReplicationTransferCreate(xfer)
	require.NotNil(t, xferResult)
	require.Nil(t, xferResult.Error)

	// Mark as received, with a bad fixity.
	newXfer := xferResult.Xfer
	newFixityValue :=  "1234567890"
	newXfer.UpdatedAt = newXfer.UpdatedAt.Add(1 * time.Second)
	newXfer.FixityValue = &newFixityValue

	updatedXfer := client.ReplicationTransferUpdate(newXfer)
	require.NotNil(t, updatedXfer.Xfer)
	require.Nil(t, updatedXfer.Error)
	assert.False(t, updatedXfer.Xfer.StoreRequested)
}

func TestRestoreTransferGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xferResult := client.RestoreTransferGet(restoreIdentifier)
	require.NotNil(t, xferResult)
	require.Nil(t, xferResult.Error)
	assert.Equal(t, "hathi", xferResult.Xfer.FromNode)
	assert.Equal(t, "aptrust", xferResult.Xfer.ToNode)
	assert.Equal(t, aptrustBagIdentifier, xferResult.Xfer.Bag)
	assert.Equal(t, restoreIdentifier, xferResult.Xfer.RestoreId)
	assert.Equal(t, "rsync", xferResult.Xfer.Protocol)
	assert.Equal(t, "2015-09-15T19:38:31Z", xferResult.Xfer.CreatedAt.Format(time.RFC3339))
	assert.Equal(t, "2015-09-15T19:38:31Z", xferResult.Xfer.UpdatedAt.Format(time.RFC3339))
	expectedTarName := fmt.Sprintf("%s.tar", aptrustBagIdentifier)
	assert.True(t, strings.HasSuffix(xferResult.Xfer.Link, expectedTarName))
}

func TestRestoreListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xferList := client.RestoreListGet(nil)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)
	assert.NotEmpty(t, xferList.Results)
	assert.False(t, xferList.Count == 0)

	totalRecordCount := xferList.Count

	params := &url.Values{}
	params.Set("bag_valid", "true")
	xferList  = client.RestoreListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)

	params.Set("bag_valid", "false")
	xferList = client.RestoreListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)

	params.Del("bag_valid")
	params.Set("fixity_accept", "true")
	xferList = client.RestoreListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)

	params.Set("fixity_accept", "false")
	xferList = client.RestoreListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)

	params.Del("fixity_accept")

	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	xferList = client.RestoreListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)
	assert.Equal(t, totalRecordCount, xferList.Count)

	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	xferList = client.RestoreListGet(params)
	require.NotNil(t, xferList)
	require.Nil(t, xferList.Error)
	assert.EqualValues(t, 0, xferList.Count)
	assert.Empty(t, xferList.Results)
}

func TestRestoreTransferCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	dpnBagResult := client.DPNBagCreate(bag)
	require.NotNil(t, dpnBagResult)
	require.Nil(t, dpnBagResult.Error)

	// Make sure we can create a transfer request.
	xfer := MakeRestoreRequest("tdr", "aptrust", bag.UUID)
	xferResult := client.RestoreTransferCreate(xfer)
	require.NotNil(t, xferResult)
	require.Nil(t, xferResult.Error)

	assert.Equal(t, xfer.FromNode, xferResult.Xfer.FromNode)
	assert.Equal(t, xfer.ToNode, xferResult.Xfer.ToNode)
	assert.Equal(t, xfer.Bag, xferResult.Xfer.Bag)
	assert.NotEmpty(t, xferResult.Xfer.RestoreId)
	assert.Equal(t, xfer.Protocol, xferResult.Xfer.Protocol)
	assert.Equal(t, xfer.Link, xferResult.Xfer.Link)
	assert.NotEmpty(t, xferResult.Xfer.CreatedAt)
	assert.NotEmpty(t, xferResult.Xfer.UpdatedAt)
}

func TestRestoreTransferUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	dpnBagResult := client.DPNBagCreate(bag)
	require.NotNil(t, dpnBagResult)
	require.Nil(t, dpnBagResult.Error)

	// Make sure we can create a transfer request.
	xfer := MakeRestoreRequest("chron", "aptrust", bag.UUID)
	xferResult := client.RestoreTransferCreate(xfer)
	require.NotNil(t, xferResult)
	require.Nil(t, xferResult.Error)

	// Accept this one...
	xferResult.Xfer.Accepted = true

	updatedXfer := client.RestoreTransferUpdate(xferResult.Xfer)
	require.NotNil(t, updatedXfer)
	require.Nil(t, updatedXfer.Error)
	assert.True(t, updatedXfer.Xfer.Accepted)
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
                     "last_pull_date":    null
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
