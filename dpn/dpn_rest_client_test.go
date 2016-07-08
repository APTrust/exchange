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
	remoteClient, err := client.GetRemoteClient(namespace, config)
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
	nodeList, err := client.NodeListGet(nil)
	require.Nil(t, nodeList.Error)
	require.NotEmpty(t, nodeList.Results)
	assert.Equal(t, 5, nodeList.Results.Count)
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
	if err != nil {
		t.Error(err)
		return
	}
	origName := result.Name
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
	result.Name = string(newName)
	savedNode, err := client.NodeUpdate(result)
	if err != nil {
		t.Error(err)
		return
	}
	if savedNode == nil {
		t.Errorf("Call to NodeUpdate returned nil")
		return
	}
	// This is broken on the server, causing our test to fail.
	// Uncomment when the server is fixed.
	// if savedNode.Name != string(newName) {
	// t.Errorf("Expected name %s, got %s", string(newName), savedNode.Name)
	// }
}

func TestNodeGetLastPullDate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	nodes := []string{"tdr", "sdr", "hathi", "chron"}
	for _, node := range nodes {
		lastPull, err := client.DPNGetLastPullDate(node)
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
	memberList, err := client.MemberListGet(nil)
	if err != nil {
		t.Errorf("MemberListGet returned error: %v", err)
	}
	if len(memberList.Results) != 5 {
		t.Errorf("MemberListGet returned %d results; expected %d",
			len(memberList.Results), 5)
	}
	params := url.Values{}
	params.Set("name", "Faber College")
	memberList, err = client.MemberListGet(&params)
	if err != nil {
		t.Errorf("MemberListGet returned error: %v", err)
	}
	if len(memberList.Results) != 1 {
		t.Errorf("MemberListGet returned %d results; expected %d",
			len(memberList.Results), 1)
	}
}

func TestMemberGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	member, err := client.MemberGet(memberIdentifier)
	if err != nil {
		t.Errorf("MemberGet returned error: %v", err)
	}
	if member == nil {
		t.Errorf("MemberGet returned nothing")
		return
	}
	if member.UUID != memberIdentifier {
		t.Errorf("MemberGet returned the wrong member")
	}
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
	newMember, err := client.MemberCreate(&member)
	if err != nil {
		t.Errorf("MemberGet returned error: %v", err)
	}
	if newMember == nil {
		t.Errorf("MemberGet returned nothing")
		return
	}
	if newMember.UUID != member.UUID {
		t.Errorf("New member UUID was not saved correctly")
	}
	if newMember.Name != member.Name {
		t.Errorf("New member Name was not saved correctly")
	}
	if newMember.Email != member.Email {
		t.Errorf("New member Email was not saved correctly")
	}
}

func TestMemberUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	member, err := client.MemberGet(memberIdentifier)
	if err != nil {
		t.Errorf("MemberGet returned error: %v", err)
	}
	if member == nil {
		t.Errorf("MemberGet returned nothing")
		return
	}
	newName := fmt.Sprintf("GO-UPDATED-%s", uuid.NewV4().String())
	member.Name = newName
	member.UpdatedAt = time.Now().UTC().Truncate(time.Second)
	newMember, err := client.MemberUpdate(member)
	if err != nil {
		t.Errorf("MemberGet returned error: %v", err)
	}
	if newMember == nil {
		t.Errorf("MemberGet returned nothing")
		return
	}
	if newMember.Name != newName {
		t.Errorf("New member Name was not updated correctly")
	}
}

func TestDPNBagGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	dpnBag, err := client.DPNBagGet(aptrustBagIdentifier)
	if err != nil {
		t.Error(err)
		return
	}
	if dpnBag.UUID != aptrustBagIdentifier {
		t.Errorf("UUID: expected '%s', got '%s'", aptrustBagIdentifier, dpnBag.UUID)
	}
	if dpnBag.LocalId != "APTrust Bag 1" {
		t.Errorf("LocalId: expected 'APTrust Bag 1', got '%s'", dpnBag.LocalId)
	}
	if dpnBag.Size != 71680 {
		t.Errorf("Size: expected 71680, got %d", dpnBag.Size)
	}
	if dpnBag.FirstVersionUUID != aptrustBagIdentifier {
		t.Errorf("FirstVersionUUID: expected '%s', got '%s'",
			aptrustBagIdentifier, dpnBag.FirstVersionUUID)
	}
	if dpnBag.BagType != "D" {
		t.Errorf("BagType: expected 'D', got '%s'", dpnBag.BagType)
	}
	if dpnBag.Version != 1 {
		t.Errorf("Version: expected 1, got %d", dpnBag.Version)
	}
	if dpnBag.IngestNode != "aptrust" {
		t.Errorf("IngestNode: expected 'aptrust', got '%s'", dpnBag.IngestNode)
	}
	if dpnBag.AdminNode != "aptrust" {
		t.Errorf("AdminNode: expected 'aptrust', got '%s'", dpnBag.AdminNode)
	}
	if dpnBag.CreatedAt.Format(time.RFC3339) != "2015-09-15T17:56:03Z" {
		t.Errorf("CreatedAt: expected '2015-09-15T17:56:03Z', got '%s'",
			dpnBag.CreatedAt.Format(time.RFC3339))
	}
	if dpnBag.UpdatedAt.Format(time.RFC3339) != "2015-09-15T17:56:03Z" {
		t.Errorf("UpdatedAt: expected '2015-09-15T17:56:03Z', got '%s'",
			dpnBag.UpdatedAt.Format(time.RFC3339))
	}
	//
	// TODO - We're not using Rights/Interpretive bags at launch. If that changes,
    //        Add Rights/Interpretive to this test object and then uncomment the
	//        following tests.
	//
	// if len(dpnBag.Rights) != 1 {
	// t.Errorf("Rights: expected 1 item, got %d", len(dpnBag.Rights))
	// }
	// if dpnBag.Rights[0] != "ff297922-a5b2-4b66-9475-3ce98b074d37" {
	// t.Errorf("Rights[0]: expected 'ff297922-a5b2-4b66-9475-3ce98b074d37', got '%s'",
	// dpnBag.Rights[0])
	// }
	// if len(dpnBag.Interpretive) != 1 {
	// t.Errorf("Interpretive: expected 1 item, got %d", len(dpnBag.Interpretive))
	// }
	// if dpnBag.Interpretive[0] != "821decbb-4063-48b1-adef-1d3906bf7b87" {
	// t.Errorf("Interpretive[0]: expected '821decbb-4063-48b1-adef-1d3906bf7b87', got '%s'",
	// dpnBag.Interpretive[0])
	// }
	if len(dpnBag.ReplicatingNodes) != 2 {
		t.Errorf("ReplicatingNodes: expected 2 items, got %d", len(dpnBag.ReplicatingNodes))
	}
	if len(dpnBag.ReplicatingNodes) == 0 {
		t.Errorf("Got zero replicating nodes. Abandoning test.")
		return
	}
	if dpnBag.ReplicatingNodes[0] != "chron" {
		t.Errorf("ReplicatingNodes[0]: expected 'chron', got '%s'",
			dpnBag.ReplicatingNodes[0])
	}
	if dpnBag.ReplicatingNodes[1] != "hathi" {
		t.Errorf("ReplicatingNodes[1]: expected 'hathi', got '%s'",
			dpnBag.ReplicatingNodes[1])
	}
	if dpnBag.Fixities == nil || dpnBag.Fixities.Sha256 == "" {
		t.Errorf("Fixities: should not be empty")
	}
	expectedFixity := "7569cf2d4bcd8b000b75bcbca82512be6e34f90f5a5479ccf7322b4d56825fde"
	if dpnBag.Fixities.Sha256 != expectedFixity {
		t.Errorf("Fixities.Sha256: expected '%s', got '%s'",
			expectedFixity, dpnBag.Fixities.Sha256)
	}
}

func TestDPNBagListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bagList, err := client.DPNBagListGet(nil)
	if err != nil {
		t.Error(err)
		return
	}
	if bagList == nil {
		t.Errorf("DPNBagListGet returned nil result")
		return
	}
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
	bagList, err = client.DPNBagListGet(&params)
	if err != nil {
		t.Error(err)
		return
	}
	if bagList.Count != unfilteredCount {
		t.Errorf("Filter by 'after' returned %d results, expected %d", bagList.Count, unfilteredCount)
	}

	// Get all bags updated after 1 hour from now
	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	bagList, err = client.DPNBagListGet(&params)
	if err != nil {
		t.Error(err)
		return
	}
	if bagList.Count != 0 {
		t.Errorf("Filter by 'after' returned %d results, expected 0", bagList.Count)
	}

}

func TestDPNBagCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bag := MakeBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// We should get back a copy of the same bag we sent,
	// with some additional info filled in.
	if dpnBag.UUID != bag.UUID {
		t.Errorf("UUIDs don't match. Ours = %s, Theirs = %s", bag.UUID, dpnBag.UUID)
	}
	if dpnBag.LocalId != bag.LocalId {
		t.Errorf("LocalIds don't match. Ours = %s, Theirs = %s", bag.LocalId, dpnBag.LocalId)
	}
	if dpnBag.Size != bag.Size {
		t.Errorf("Sizes don't match. Ours = %d, Theirs = %d", bag.Size, dpnBag.Size)
	}
	if dpnBag.FirstVersionUUID != bag.FirstVersionUUID {
		t.Errorf("FirstVersionUUIDs don't match. Ours = %s, Theirs = %s",
			bag.FirstVersionUUID, dpnBag.FirstVersionUUID)
	}
	if dpnBag.Version != bag.Version {
		t.Errorf("Versions don't match. Ours = %d, Theirs = %d", bag.Version, dpnBag.Version)
	}
	if dpnBag.BagType != bag.BagType {
		t.Errorf("BagTypes don't match. Ours = %s, Theirs = %s", bag.BagType, dpnBag.BagType)
	}
	if dpnBag.Fixities == nil || dpnBag.Fixities.Sha256 == "" {
		t.Errorf("Bag fixities are missing")
	}
	if dpnBag.Fixities.Sha256 != bag.Fixities.Sha256 {
		t.Errorf("Fixities don't match. Ours = %s, Theirs = %s",
			bag.Fixities.Sha256, dpnBag.Fixities.Sha256)
	}

	// These tests really check that the server is behaving correctly,
	// which isn't our business, but if it's not, we want to know.
	if dpnBag.IngestNode == "" {
		t.Errorf("IngestNode was not set")
	}
	if dpnBag.IngestNode != dpnBag.AdminNode {
		t.Errorf("Ingest/Admin node mismatch. Ingest = %s, Admin = %s",
			dpnBag.IngestNode, dpnBag.AdminNode)
	}
	if dpnBag.CreatedAt.IsZero() {
		t.Errorf("CreatedAt was not set")
	}
	if dpnBag.UpdatedAt.IsZero() {
		t.Errorf("UpdatedAt was not set")
	}

	// We were using Rights and Interpretive bags, but these are hold
	// as of fall, 2015.
	anotherBag := MakeBag()
	//anotherBag.Rights = append(anotherBag.Rights, bag.UUID)
	//anotherBag.Interpretive = append(anotherBag.Interpretive, bag.UUID)

	dpnBag, err = client.DPNBagCreate(anotherBag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error when creating bag " +
			"with rights and interpretive UUIDs: %v", err)
		return
	}

}

func TestDPNBagUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	bag := MakeBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// We have to set UpdatedAt ahead, or the server won't update
	// record we're sending.
	newTimestamp := time.Now().UTC().Add(1 * time.Second).Truncate(time.Second)
	newLocalId := fmt.Sprintf("GO-TEST-BAG-%s", uuid.NewV4().String())

	dpnBag.UpdatedAt = newTimestamp
	dpnBag.LocalId = newLocalId

	updatedBag, err := client.DPNBagUpdate(dpnBag)
	if err != nil {
		t.Errorf("DPNBagUpdate returned error %v", err)
		return
	}
	if updatedBag.UpdatedAt != newTimestamp {
		t.Errorf("Expected UpdatedAt = '%s', got '%s'",
			newTimestamp, updatedBag.UpdatedAt)
	}
	if updatedBag.LocalId != newLocalId {
		t.Errorf("Expected LocalId '%s', got '%s'",
			newLocalId, updatedBag.LocalId)
	}
}

func TestReplicationTransferGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xfer, err := client.ReplicationTransferGet(replicationIdentifier)
	if err != nil {
		t.Error(err)
		return
	}
	if xfer.FromNode != "aptrust" {
		t.Errorf("FromNode: expected 'aptrust', got '%s'", xfer.FromNode)
	}
	if xfer.ToNode != "hathi" {
		t.Errorf("ToNode: expected 'hathi', got '%s'", xfer.ToNode)
	}
	if xfer.BagId != aptrustBagIdentifier {
		t.Errorf("UUID: expected '%s', got '%s'", aptrustBagIdentifier, xfer.BagId)
	}
	if xfer.ReplicationId != replicationIdentifier {
		t.Errorf("ReplicationId: expected '%s', got '%s'", replicationIdentifier, xfer.ReplicationId)
	}
	if xfer.FixityNonce != nil && *xfer.FixityNonce != "" {
		t.Errorf("FixityNonce: expected '', got '%s'", *xfer.FixityNonce)
	}
	if xfer.FixityValue != nil && *xfer.FixityValue != "" {
		t.Errorf("FixityValue: expected empty, got '%s'", *xfer.FixityValue)
	}
	if xfer.FixityAlgorithm != "sha256" {
		t.Errorf("FixityAlgorithm: expected 'sha256', got '%s'", xfer.FixityAlgorithm)
	}
	if *xfer.FixityAccept != true {
		t.Errorf("FixityAccept: expected true, got %t", *xfer.FixityAccept)
	}
	if *xfer.BagValid != true {
		t.Errorf("BagValid: expected true, got %s", *xfer.BagValid)
	}
	if xfer.Status != "stored" {
		t.Errorf("Status: expected 'requested', got '%s'", xfer.Status)
	}
	if xfer.Protocol != "rsync" {
		t.Errorf("Protocol: expected 'R', got '%s'", xfer.Protocol)
	}
	expectedTarName := fmt.Sprintf("%s.tar", aptrustBagIdentifier)
	if !strings.HasSuffix(xfer.Link, expectedTarName) {
		t.Errorf("Expected link to end with '%s', got '%s'", expectedTarName, xfer.Link)
	}
	if xfer.CreatedAt.Format(time.RFC3339) != "2015-09-15T19:38:31Z" {
		t.Errorf("CreatedAt: expected '2015-09-15T19:38:31Z', got '%s'",
			xfer.CreatedAt.Format(time.RFC3339))
	}
	if xfer.UpdatedAt.Format(time.RFC3339) != "2015-09-15T19:38:31Z" {
		t.Errorf("UpdatedAt: expected '2015-09-15T19:38:31Z', got '%s'",
			xfer.UpdatedAt.Format(time.RFC3339))
	}
}

func TestReplicationListGet(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	xferList, err := client.ReplicationListGet(nil)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList == nil {
		t.Errorf("ReplicationListGet returned nil result")
		return
	}
	if xferList.Count == 0 || len(xferList.Results) == 0 {
		t.Errorf("ReplicationListGet returned zero results")
		return
	}

	totalRecordCount := xferList.Count

	params := &url.Values{}
	params.Set("bag_valid", "true")
	xferList, err = client.ReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Set("bag_valid", "false")
	xferList, err = client.ReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Del("bag_valid")
	params.Set("fixity_accept", "true")
	xferList, err = client.ReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Set("fixity_accept", "false")
	xferList, err = client.ReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	params.Del("fixity_accept")

	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	params.Set("after", aLongTimeAgo.Format(time.RFC3339Nano))
	xferList, err = client.ReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList.Count != totalRecordCount {
		t.Errorf("Expected %d records, got %d", totalRecordCount, xferList.Count)
	}

	params.Set("after", time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano))
	xferList, err = client.ReplicationListGet(params)
	if err != nil {
		t.Error(err)
		return
	}
	if xferList.Count != 0 {
		t.Errorf("Expected 0 records, got %d", xferList.Count)
	}
}

func TestReplicationTransferCreate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeBag()
	dpnBag, err := client.DPNBagCreate(bag)
	if err != nil {
		t.Errorf("DPNBagCreate returned error %v", err)
		return
	}

	// Make sure we can create a transfer request.
	xfer := MakeXferRequest("aptrust", "chron", dpnBag.UUID)
	newXfer, err := client.ReplicationTransferCreate(xfer)
	if err != nil {
		t.Errorf("ReplicationTransferCreate returned error %v", err)
	}
	if newXfer == nil {
		t.Errorf("ReplicationTransferCreate did not return an object")
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
	if newXfer.ReplicationId == "" {
		t.Errorf("ReplicationId is missing")
	}
	if newXfer.FixityAlgorithm != xfer.FixityAlgorithm {
		t.Errorf("FixityAlgorithm is %s; expected %s",
			newXfer.FixityAlgorithm, xfer.FixityAlgorithm)
	}
	if newXfer.FixityNonce != nil {
		t.Errorf("FixityNonce is %s; expected nil",
			*newXfer.FixityNonce)
	}
	if newXfer.FixityValue != nil {
		t.Errorf("FixityValue: expected nil but got %s",
			*newXfer.FixityValue)
	}
	if newXfer.FixityAccept != nil {
		t.Errorf("FixityAccept is %t; expected nil", *newXfer.FixityAccept)
	}
	if newXfer.BagValid != nil {
		t.Errorf("BagValid is %s; expected nil", *newXfer.BagValid)
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

func TestReplicationTransferUpdate(t *testing.T) {
	if runRestTests(t) == false {
		return
	}
	client := getClient(t)
	//remoteClient := getRemoteClient(t, "chron")

	// The transfer request must refer to an actual bag,
	// so let's make a bag...
	bag := MakeBag()
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
	bag := MakeBag()
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
	bag := MakeBag()
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
