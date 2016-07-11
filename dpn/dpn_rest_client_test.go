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
	"strconv"
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
	assert.Equal(t, 5, nodeList.Count)
	assert.Equal(t, 5, len(nodeList.Results))
	assert.NotNil(t, nodeList.Request)
	assert.NotNil(t, nodeList.Response)
}

func TestNodeUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	result := client.NodeGet("sdr")
	require.Nil(t, result.Error)

	origName := result.Node.Name
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
	result.Node.Name = string(newName)
	savedNodeResult := client.NodeUpdate(result.Node)
	require.Nil(t, savedNodeResult.Error)
	require.NotNil(t, savedNodeResult.Node)
	assert.NotNil(t, savedNodeResult.Request)
	assert.NotNil(t, savedNodeResult.Response)

	// This is broken on the server, causing our test to fail.
	// Uncomment when the server is fixed.
	// 	assert.Equal(t, newName, savedNodeResult.Node.Name)
}

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
	assert.Equal(t, 5, memberList.Count)
	assert.Equal(t, 5, len(memberList.Results))
	params := url.Values{}
	params.Set("name", "Faber College")
	memberList  = client.MemberListGet(&params)
	assert.Nil(t, memberList.Error)
	assert.NotNil(t, memberList.Request)
	assert.NotNil(t, memberList.Response)
	assert.Equal(t, 1, memberList.Count)
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
	assert.Equal(t, memberIdentifier, result.Member.UUID)
}

func TestMemberCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	id := uuid.NewV4().String()
	member := dpn.Member{
		UUID: id,
		Name: fmt.Sprintf("GO-TEST-MEMBER-%s", id),
		Email: fmt.Sprintf("%s@example.com", id),
	}
	result := client.MemberCreate(&member)
	require.Nil(t, result.Error)
	require.NotNil(t, result.Member)
	assert.NotNil(t, result.Request)
	assert.NotNil(t, result.Response)
	assert.Equal(t, member.UUID, result.Member.UUID)
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
	memberResult.Member.UpdatedAt = time.Now().UTC().Truncate(time.Second)
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
	assert.Equal(t, 1, bagResult.Bag.Version)
	assert.Equal(t, "aptrust", bagResult.Bag.IngestNode)
	assert.Equal(t, "aptrust", bagResult.Bag.AdminNode)
	assert.Equal(t, "2015-09-15T17:56:03Z", bagResult.Bag.CreatedAt.Format(time.RFC3339))
	assert.Equal(t, "2015-09-15T17:56:03Z", bagResult.Bag.UpdatedAt.Format(time.RFC3339))
	assert.Equal(t, 2, len(bagResult.Bag.ReplicatingNodes))
	require.True(t, len(bagResult.Bag.ReplicatingNodes) > 1)
	assert.Equal(t, "chron", bagResult.Bag.ReplicatingNodes[0])
	assert.Equal(t, "hathi", bagResult.Bag.ReplicatingNodes[1])
	require.NotNil(t, bagResult.Bag.Fixities)
	assert.Equal(t, "7569cf2d4bcd8b000b75bcbca82512be6e34f90f5a5479ccf7322b4d56825fde",
		bagResult.Bag.Fixities.Sha256)
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
	assert.Equal(t, 0, bagList.Count)
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
	require.NotNil(t, dpnBagResult.Bag.Fixities)
	assert.Equal(t, bag.Fixities.Sha256, dpnBagResult.Bag.Fixities.Sha256)
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
	assert.Equal(t, newTimestamp, updatedBagResult.Bag.UpdatedAt)
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
	assert.Equal(t, aptrustBagIdentifier, xferResult.Xfer.BagId)
	assert.Equal(t, replicationIdentifier, xferResult.Xfer.ReplicationId)

	if xferResult.Xfer.FixityNonce != nil && *xferResult.Xfer.FixityNonce != "" {
		t.Errorf("FixityNonce: expected '', got '%s'", *xferResult.Xfer.FixityNonce)
	}
	if xferResult.Xfer.FixityValue != nil && *xferResult.Xfer.FixityValue != "" {
		t.Errorf("FixityValue: expected empty, got '%s'", *xferResult.Xfer.FixityValue)
	}

	assert.Equal(t, "sha256", xferResult.Xfer.FixityAlgorithm)
	assert.True(t, *xferResult.Xfer.FixityAccept)
	assert.True(t, *xferResult.Xfer.BagValid)
	assert.Equal(t, "stored", xferResult.Xfer.Status)
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
	assert.Equal(t, 0, xferList.Count)
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
	assert.Equal(t, xfer.BagId, xferResult.Xfer.BagId)
	assert.NotEmpty(t, xferResult.Xfer.ReplicationId)
	assert.Equal(t, xfer.FixityAlgorithm, xferResult.Xfer.FixityAlgorithm)
	assert.Equal(t, xfer.FixityNonce, xferResult.Xfer.FixityNonce)
	assert.Equal(t, xfer.FixityValue, xferResult.Xfer.FixityValue)
	assert.Equal(t, xfer.FixityAccept, xferResult.Xfer.FixityAccept)
	assert.Equal(t, xfer.BagValid, xferResult.Xfer.BagValid)
	assert.Equal(t, xfer.Status, xferResult.Xfer.Status)
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
	//remoteClient := getRemoteClient(t, "chron")

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// Make sure we can create a transfer request.
	xfer := MakeXferRequest("chron", "aptrust", dpnBag.UUID)

	// Null out the fixity value, because once it's set, we can't change
	// it. And below, we want to set a bad fixity value to see what happens.
	xfer.FixityValue = nil
	newXfer, err := client.ReplicationTransferCreate(xfer)
	if err != nil {
		t.Errorf("ReplicationTransferCreate returned error %v", err)
		return
	}
	if newXfer == nil {
		t.Errorf("ReplicationTransferCreate did not return an object")
		return
	}

	// Mark as received, with a bad fixity.
	bagValid := true
	newFixityValue :=  "1234567890"
	newXfer.Status = "received"
	newXfer.UpdatedAt = newXfer.UpdatedAt.Add(1 * time.Second)
	newXfer.BagValid = &bagValid
	newXfer.FixityValue = &newFixityValue

	updatedXfer, err := client.ReplicationTransferUpdate(newXfer)
	if err != nil {
		t.Errorf("ReplicationTransferUpdate returned error %v", err)
		return
	}
	if updatedXfer == nil {
		t.Errorf("ReplicationTransferUpdate did not return an object")
		return
	}

	// ... make sure status is correct
	if updatedXfer.Status != "received" {
		t.Errorf("Status is %s; expected received", updatedXfer.Status)
	}


	// Mark as confirmed and send a bad fixity value.
	// The server should cancel this transfer.
	// At this point, we're testing the server's behavior,
	// not the behavior of our own code. This kind of test
	// belongs in the Rails spec.
	newXfer.Status = "confirmed"
	newXfer.UpdatedAt = newXfer.UpdatedAt.Add(1 * time.Second)

	updatedXfer, err = client.ReplicationTransferUpdate(newXfer)
	if err != nil {
		t.Errorf("ReplicationTransferUpdate returned error %v", err)
		return
	}
	if updatedXfer == nil {
		t.Errorf("ReplicationTransferUpdate did not return an object")
		return
	}

	// Make sure the fields were set correctly.
	if updatedXfer.FixityValue == nil || *updatedXfer.FixityValue != "1234567890" {
		val := "nil"
		if updatedXfer.FixityValue != nil {
			val = *updatedXfer.FixityValue
		}
		t.Errorf("FixityValue was %s; expected 1234567890", val)
	}
	if updatedXfer.FixityAccept == nil || *updatedXfer.FixityAccept != false {
		value := "nil"
		if updatedXfer.FixityAccept != nil {
			value = strconv.FormatBool(*updatedXfer.FixityAccept)
		}
		t.Errorf("FixityAccept is %s; expected false", value)
	}
	if updatedXfer.FixityAccept == nil || *updatedXfer.BagValid != true {
		value := "nil"
		if updatedXfer.BagValid != nil {
			value = strconv.FormatBool(*updatedXfer.BagValid)
		}
		t.Errorf("BagValid is %s; expected true", value)
	}
	// Note: Status will be cancelled instead of received because
	// we sent a bogus checksum, and that causes the server to cancel
	// the transfer.
	if updatedXfer.Status != "cancelled" {
		t.Errorf("Status is %s; expected cancelled", updatedXfer.Status)
	}
	if updatedXfer.UpdatedAt.After(newXfer.UpdatedAt) == false {
		t.Errorf("UpdatedAt was not updated")
	}
}

func TestRestoreTransferGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xfer, err := client.RestoreTransferGet(restoreIdentifier)
	if err != nil {
		t.Error(err)
		return
	}
	if xfer.FromNode != "hathi" {
		t.Errorf("FromNode: expected 'hathi', got '%s'", xfer.FromNode)
	}
	if xfer.ToNode != "aptrust" {
		t.Errorf("ToNode: expected 'aptrust', got '%s'", xfer.ToNode)
	}
	if xfer.BagId != aptrustBagIdentifier {
		t.Errorf("UUID: expected '%s', got '%s'",
			aptrustBagIdentifier, xfer.BagId)
	}
	if xfer.RestoreId != restoreIdentifier {
		t.Errorf("RestoreId: expected '%s', got '%s'", restoreIdentifier, xfer.RestoreId)
	}
	if xfer.Status != "requested" {
		t.Errorf("Status: expected 'requested', got '%s'", xfer.Status)
	}
	if xfer.Protocol != "rsync" {
		t.Errorf("Protocol: expected 'R', got '%s'", xfer.Protocol)
	}
	if xfer.CreatedAt.Format(time.RFC3339) != "2015-09-15T19:38:31Z" {
		t.Errorf("CreatedAt: expected '2015-09-15T19:38:31Z', got '%s'",
			xfer.CreatedAt.Format(time.RFC3339))
	}
	if xfer.UpdatedAt.Format(time.RFC3339) != "2015-09-15T19:38:31Z" {
		t.Errorf("UpdatedAt: expected '2015-09-15T19:38:31Z', got '%s'",
			xfer.UpdatedAt.Format(time.RFC3339))
	}
	expectedTarName := fmt.Sprintf("%s.tar", aptrustBagIdentifier)
	if !strings.HasSuffix(xfer.Link, expectedTarName) {
		t.Errorf("Expected link to end with '%s', got '%s'", expectedTarName, xfer.Link)
	}
}

func TestRestoreListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xferList, err := client.RestoreListGet(nil)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList == nil {
		t.Errorf("RestoreListGet returned nil result")
		return
	}
	if xferList.Count == 0 || len(xferList.Results) == 0 {
		t.Errorf("RestoreListGet returned zero results")
		return
	}

	totalRecordCount := xferList.Count

	params := &url.Values{}
	params.Set("bag_valid", "true")
	xferList, err = client.RestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Set("bag_valid", "false")
	xferList, err = client.RestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Del("bag_valid")
	params.Set("fixity_accept", "true")
	xferList, err = client.RestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Set("fixity_accept", "false")
	xferList, err = client.RestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Del("fixity_accept")

	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	xferList, err = client.RestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList.Count != totalRecordCount {
		t.Errorf("Expected %d records, got %d", totalRecordCount, xferList.Count)
	}

	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	xferList, err = client.RestoreListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList.Count != 0 {
		t.Errorf("Expected 0 records, got %d", xferList.Count)
	}
}

func TestRestoreTransferCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// Make sure we can create a transfer request.
	xfer := MakeRestoreRequest("tdr", "aptrust", dpnBag.UUID)
	newXfer, err := client.RestoreTransferCreate(xfer)
	if err != nil {
		t.Errorf("RestoreTransferCreate returned error %v", err)
		return
	}
	if newXfer == nil {
		t.Errorf("RestoreTransferCreate did not return an object")
		return
	}

	// Make sure the fields were set correctly.
	if newXfer.FromNode != xfer.FromNode {
		t.Errorf("FromNode is %s; expected %s", newXfer.FromNode, xfer.FromNode)
	}
	if newXfer.ToNode != xfer.ToNode {
		t.Errorf("ToNode is %s; expected %s", newXfer.ToNode, xfer.ToNode)
	}
	if newXfer.BagId != xfer.BagId {
		t.Errorf("UUID is %s; expected %s", newXfer.BagId, xfer.BagId)
	}
	if newXfer.RestoreId == "" {
		t.Errorf("RestoreId is missing")
	}
	if newXfer.Status != "requested" {
		t.Errorf("Status is %s; expected requested", newXfer.Status)
	}
	if newXfer.Protocol != xfer.Protocol {
		t.Errorf("Protocol is %s; expected %s", newXfer.Protocol, xfer.Protocol)
	}
	if newXfer.Link != xfer.Link {
		t.Errorf("Link is %s; expected %s", newXfer.Link, xfer.Link)
	}
	if newXfer.CreatedAt.IsZero() {
		t.Errorf("CreatedAt was not set")
	}
	if newXfer.UpdatedAt.IsZero() {
		t.Errorf("UpdatedAt was not set")
	}
}

func TestRestoreTransferUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeDPNBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// Make sure we can create a transfer request.
	xfer := MakeRestoreRequest("chron", "aptrust", dpnBag.UUID)
	newXfer, err := client.RestoreTransferCreate(xfer)
	if err != nil {
		t.Errorf("RestoreTransferCreate returned error %v", err)
		return
	}
	if newXfer == nil {
		t.Errorf("RestoreTransferCreate did not return an object")
		return
	}

	// Reject this one...
	newXfer.Status = "rejected"

	updatedXfer, err := client.RestoreTransferUpdate(newXfer)
	if err != nil {
		t.Errorf("RestoreTransferUpdate returned error %v", err)
		return
	}
	if updatedXfer == nil {
		t.Errorf("RestoreTransferUpdate did not return an object")
		return
	}

	// ... make sure status is correct
	if updatedXfer.Status != "rejected" {
		t.Errorf("Status is '%s'; expected 'rejected'", updatedXfer.Status)
	}


	// Update the allowed fields. We're going to send a bad
	// fixity value, because we don't know the good one, so
	// the server will cancel this transfer.
	link := "rsync://blah/blah/blah/yadda/yadda/beer"
	newXfer.Status = "prepared"
	newXfer.Link = link

	// Now that there are no milliseconds on the DPN timestamps,
	// we have to sleep for more than 1 second to test whether
	// UpdatedAt timestamps change after update.
	time.Sleep(1500 * time.Millisecond)
	newXfer.UpdatedAt = time.Now()

	updatedXfer, err = client.RestoreTransferUpdate(newXfer)
	if err != nil {
		t.Errorf("RestoreTransferUpdate returned error %v", err)
		return
	}
	if updatedXfer == nil {
		t.Errorf("RestoreTransferUpdate did not return an object")
		return
	}

	// Make sure values were stored...
	if updatedXfer.Status != "prepared" {
		t.Errorf("Status is %s; expected prepared", updatedXfer.Status)
	}
	if updatedXfer.Link != link {
		t.Errorf("Status is %s; expected %s", updatedXfer.Link, link)
	}
	if updatedXfer.UpdatedAt.After(newXfer.UpdatedAt) == false {
		t.Errorf("UpdatedAt was not updated")
	}
}

func TestGetRemoteClient(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	config := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	client := getClient(t)
	nodes := []string { "chron", "hathi", "sdr", "tdr" }
	for _, node := range nodes {
		_, err := client.GetRemoteClient(node, config)
		if err != nil {
			t.Errorf("Error creating remote client: %v", err)
		}
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
	if data["last_pull_date"] != "1980-01-01T00:00:00Z" {
		t.Errorf("Got unexpected last_pull_date %s", data["last_pull_date"])
	}
}
