package workers_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/APTrust/exchange/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The following package-level vars tell the HTTP test handlers
// how to behave when we make requests. We have to do this because
// we don't have control over the requests themselves, which are
// generated by other libraries.
var NumberOfRequestsToIncludeInState = 0

const (
	NotStarted = 0
	InProgress = 1
	Completed  = 2
)

var DescribeRestoreStateAs = NotStarted

const TEST_ID = 1000

// Regex to extract ID from URL
var URL_ID_REGEX = regexp.MustCompile(`\/(\d+)\/`)

// Test server to handle Pharos requests
var pharosTestServer = httptest.NewServer(http.HandlerFunc(pharosHandler))

// Test server to handle S3 requests
var s3TestServer = httptest.NewServer(http.HandlerFunc(s3Handler))

func getGlacierRestoreWorker(t *testing.T) *workers.APTGlacierRestoreInit {
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err)
	return workers.NewGlacierRestore(_context)
}

func getObjectWorkItem(id int, objectIdentifier string) *models.WorkItem {
	workItemStateId := 1000
	return &models.WorkItem{
		Id:                    id,
		ObjectIdentifier:      objectIdentifier,
		GenericFileIdentifier: "",
		Name:             "glacier_bag.tar",
		Bucket:           "aptrust.receiving.test.edu",
		ETag:             "0000000000000000",
		BagDate:          testutil.RandomDateTime(),
		InstitutionId:    33,
		User:             "frank.zappa@example.com",
		Date:             testutil.RandomDateTime(),
		Note:             "",
		Action:           constants.ActionGlacierRestore,
		Stage:            constants.StageRequested,
		Status:           constants.StatusPending,
		Outcome:          "",
		Retry:            true,
		Node:             "",
		Pid:              0,
		NeedsAdminReview: false,
		WorkItemStateId:  &workItemStateId,
	}
}

func getFileWorkItem(id int, objectIdentifier, fileIdentifier string) *models.WorkItem {
	workItem := getObjectWorkItem(id, objectIdentifier)
	workItem.GenericFileIdentifier = fileIdentifier
	return workItem
}

func getPharosClientForTest(url string) *network.PharosClient {
	client, _ := network.NewPharosClient(url, "v2", "frankzappa", "abcxyz")
	return client
}

func getTestComponents(t *testing.T, fileOrObject string) (*workers.APTGlacierRestoreInit, *models.GlacierRestoreState) {
	worker := getGlacierRestoreWorker(t)
	require.NotNil(t, worker)

	// Tell the worker to talk to our S3 test server and Pharos
	// test server, defined below
	worker.S3Url = s3TestServer.URL
	worker.Context.PharosClient = getPharosClientForTest(pharosTestServer.URL)

	// Set up the GlacierRestoreStateObject
	objIdentifier := "test.edu/glacier_bag"

	// Note that we're getting a WorkItem that has a GenericFileIdentifier
	var workItem *models.WorkItem
	if fileOrObject == "object" {
		workItem = getObjectWorkItem(TEST_ID, objIdentifier)
	} else {
		workItem = getFileWorkItem(TEST_ID, objIdentifier, objIdentifier+"/file1.txt")
	}
	nsqMessage := testutil.MakeNsqMessage(fmt.Sprintf("%d", TEST_ID))

	state, err := worker.GetGlacierRestoreState(nsqMessage, workItem)
	require.Nil(t, err)
	require.NotNil(t, state)
	return worker, state
}

// ------ TESTS --------

func TestNewGlacierRestore(t *testing.T) {
	glacierRestore := getGlacierRestoreWorker(t)
	require.NotNil(t, glacierRestore)
	assert.NotNil(t, glacierRestore.Context)
	assert.NotNil(t, glacierRestore.RequestChannel)
	assert.NotNil(t, glacierRestore.CleanupChannel)
}

func TestGetGlacierRestoreState(t *testing.T) {
	worker, state := getTestComponents(t, "object")

	NumberOfRequestsToIncludeInState = 0
	worker.Context.PharosClient = getPharosClientForTest(pharosTestServer.URL)
	state, err := worker.GetGlacierRestoreState(state.NSQMessage, state.WorkItem)
	require.Nil(t, err)
	require.NotNil(t, state)
	assert.NotNil(t, state.WorkSummary)
	assert.Empty(t, state.Requests)

	NumberOfRequestsToIncludeInState = 10
	state, err = worker.GetGlacierRestoreState(state.NSQMessage, state.WorkItem)
	require.Nil(t, err)
	require.NotNil(t, state)
	assert.NotNil(t, state.WorkSummary)
	require.NotEmpty(t, state.Requests)
	assert.Equal(t, NumberOfRequestsToIncludeInState, len(state.Requests))
}

func TestRequestObject(t *testing.T) {
	worker, state := getTestComponents(t, "object")
	require.Nil(t, state.IntellectualObject)
	worker.RequestObject(state)
	require.NotNil(t, state.IntellectualObject)
	require.NotEmpty(t, state.IntellectualObject.GenericFiles)
}

func TestRestoreRequestNeeded(t *testing.T) {
	glacierRestore := getGlacierRestoreWorker(t)
	require.NotNil(t, glacierRestore)

	// Tell the worker to talk to our S3 test server and Pharos
	// test server, defined below
	glacierRestore.S3Url = s3TestServer.URL
	glacierRestore.Context.PharosClient = getPharosClientForTest(pharosTestServer.URL)

	// Set up the GlacierRestoreStateObject
	objIdentifier := "test.edu/glacier_bag"
	workItem := getObjectWorkItem(TEST_ID, objIdentifier)
	nsqMessage := testutil.MakeNsqMessage(fmt.Sprintf("%d", TEST_ID))

	glacierRestoreState, err := glacierRestore.GetGlacierRestoreState(nsqMessage, workItem)
	require.Nil(t, err)
	require.NotNil(t, glacierRestoreState)
	require.Nil(t, glacierRestoreState.IntellectualObject)

	// Now let's check to see if we need to issue a Glacier restore
	// request for the following file. Tell the s3 test server to
	// reply that this restore has not been requested yet for this item.
	DescribeRestoreStateAs = NotStarted
	gf := testutil.MakeGenericFile(0, 0, objIdentifier)
	fileUUID, _ := gf.PreservationStorageFileName()
	requestNeeded, err := glacierRestore.RestoreRequestNeeded(glacierRestoreState, gf)
	require.Nil(t, err)
	assert.True(t, requestNeeded)

	// Make sure the GlacierRestore worker created a
	// GlacierRestoreRequest record for this file.
	// In the test environment, glacierRestoreRequest.GlacierBucket
	// will be an empty string.
	glacierRestoreRequest := glacierRestoreState.FindRequest(gf.Identifier)
	require.NotNil(t, glacierRestoreRequest)
	assert.Equal(t, fileUUID, glacierRestoreRequest.GlacierKey)
	// Request cannot have been accepted, because it hasn't been issued.
	assert.False(t, glacierRestoreRequest.RequestAccepted)
	assert.False(t, glacierRestoreRequest.IsAvailableInS3)
	assert.False(t, glacierRestoreRequest.SomeoneElseRequested)
	assert.True(t, glacierRestoreRequest.RequestedAt.IsZero())
	assert.WithinDuration(t, time.Now().UTC(), glacierRestoreRequest.LastChecked, 10*time.Second)

	// Check to see if we need to issue a Glacier restore
	// request for a file that we've already requested and whose
	// restoration is currently in progress. Tell the s3 test server to
	// reply that restore is in progress for this item.
	DescribeRestoreStateAs = InProgress
	gf = testutil.MakeGenericFile(0, 0, objIdentifier)
	fileUUID, _ = gf.PreservationStorageFileName()
	requestNeeded, err = glacierRestore.RestoreRequestNeeded(glacierRestoreState, gf)
	require.Nil(t, err)
	assert.False(t, requestNeeded)

	// Make sure the GlacierRestore worker created a
	// GlacierRestoreRequest record for this file.
	glacierRestoreRequest = glacierRestoreState.FindRequest(gf.Identifier)
	require.NotNil(t, glacierRestoreRequest)
	assert.Equal(t, fileUUID, glacierRestoreRequest.GlacierKey)
	// Request must have been accepted, because the restore is in progress.
	assert.True(t, glacierRestoreRequest.RequestAccepted)
	assert.False(t, glacierRestoreRequest.IsAvailableInS3)
	assert.False(t, glacierRestoreRequest.SomeoneElseRequested)
	assert.False(t, glacierRestoreRequest.RequestedAt.IsZero())
	assert.WithinDuration(t, time.Now().UTC(), glacierRestoreRequest.LastChecked, 10*time.Second)

	// Check to see if we need to issue a Glacier restore
	// request for a file that's already been restored to S3.
	// Tell the s3 test server to reply that restore is complete for this item.
	DescribeRestoreStateAs = Completed
	gf = testutil.MakeGenericFile(0, 0, objIdentifier)
	fileUUID, _ = gf.PreservationStorageFileName()
	requestNeeded, err = glacierRestore.RestoreRequestNeeded(glacierRestoreState, gf)
	require.Nil(t, err)
	assert.False(t, requestNeeded)

	// Make sure the GlacierRestore worker created a
	// GlacierRestoreRequest record for this file.
	glacierRestoreRequest = glacierRestoreState.FindRequest(gf.Identifier)
	require.NotNil(t, glacierRestoreRequest)
	assert.Equal(t, fileUUID, glacierRestoreRequest.GlacierKey)
	// Request must have been accepted, because the restore is in progress.
	assert.True(t, glacierRestoreRequest.RequestAccepted)
	assert.True(t, glacierRestoreRequest.IsAvailableInS3)
	assert.False(t, glacierRestoreRequest.SomeoneElseRequested)
	assert.False(t, glacierRestoreRequest.RequestedAt.IsZero())
	assert.False(t, glacierRestoreRequest.EstimatedDeletionFromS3.IsZero())
	assert.WithinDuration(t, time.Now().UTC(), glacierRestoreRequest.LastChecked, 10*time.Second)
}

func TestGetS3HeadClient(t *testing.T) {
	glacierRestore := getGlacierRestoreWorker(t)
	require.NotNil(t, glacierRestore)

	// Standard
	client, err := glacierRestore.GetS3HeadClient(constants.StorageStandard)
	require.Nil(t, err)
	require.NotNil(t, client)
	assert.Equal(t, glacierRestore.Context.Config.APTrustS3Region, client.AWSRegion)
	assert.Equal(t, glacierRestore.Context.Config.PreservationBucket, client.BucketName)

	// Glacier OH
	client, err = glacierRestore.GetS3HeadClient(constants.StorageGlacierOH)
	require.Nil(t, err)
	require.NotNil(t, client)
	assert.Equal(t, glacierRestore.Context.Config.GlacierRegionOH, client.AWSRegion)
	assert.Equal(t, glacierRestore.Context.Config.GlacierBucketOH, client.BucketName)

	// Glacier OR
	client, err = glacierRestore.GetS3HeadClient(constants.StorageGlacierOR)
	require.Nil(t, err)
	require.NotNil(t, client)
	assert.Equal(t, glacierRestore.Context.Config.GlacierRegionOR, client.AWSRegion)
	assert.Equal(t, glacierRestore.Context.Config.GlacierBucketOR, client.BucketName)

	// Glacier VA
	client, err = glacierRestore.GetS3HeadClient(constants.StorageGlacierVA)
	require.Nil(t, err)
	require.NotNil(t, client)
	assert.Equal(t, glacierRestore.Context.Config.GlacierRegionVA, client.AWSRegion)
	assert.Equal(t, glacierRestore.Context.Config.GlacierBucketVA, client.BucketName)
}

func TestGetIntellectualObject(t *testing.T) {
	glacierRestore := getGlacierRestoreWorker(t)
	require.NotNil(t, glacierRestore)

	// Tell the worker to talk to our S3 test server and Pharos
	// test server, defined below
	glacierRestore.S3Url = s3TestServer.URL
	glacierRestore.Context.PharosClient = getPharosClientForTest(pharosTestServer.URL)

	// Set up the GlacierRestoreStateObject
	objIdentifier := "test.edu/glacier_bag"
	workItem := getObjectWorkItem(TEST_ID, objIdentifier)
	nsqMessage := testutil.MakeNsqMessage(fmt.Sprintf("%d", TEST_ID))

	glacierRestoreState, err := glacierRestore.GetGlacierRestoreState(nsqMessage, workItem)
	require.Nil(t, err)
	require.NotNil(t, glacierRestoreState)
	require.Nil(t, glacierRestoreState.IntellectualObject)

	obj, err := glacierRestore.GetIntellectualObject(glacierRestoreState)
	assert.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, 12, len(obj.GenericFiles))
}

func TestGetGenericFile(t *testing.T) {
	glacierRestore := getGlacierRestoreWorker(t)
	require.NotNil(t, glacierRestore)

	// Tell the worker to talk to our S3 test server and Pharos
	// test server, defined below
	glacierRestore.S3Url = s3TestServer.URL
	glacierRestore.Context.PharosClient = getPharosClientForTest(pharosTestServer.URL)

	// Set up the GlacierRestoreStateObject
	objIdentifier := "test.edu/glacier_bag"

	// Note that we're getting a WorkItem that has a GenericFileIdentifier
	workItem := getFileWorkItem(TEST_ID, objIdentifier, objIdentifier+"/file1.txt")
	nsqMessage := testutil.MakeNsqMessage(fmt.Sprintf("%d", TEST_ID))

	glacierRestoreState, err := glacierRestore.GetGlacierRestoreState(nsqMessage, workItem)
	require.Nil(t, err)
	require.NotNil(t, glacierRestoreState)
	require.Nil(t, glacierRestoreState.GenericFile)

	gf, err := glacierRestore.GetGenericFile(glacierRestoreState)
	assert.Nil(t, err)
	require.NotNil(t, gf)
	assert.NotEmpty(t, gf.Identifier)
	assert.NotEmpty(t, gf.StorageOption)
	assert.NotEmpty(t, gf.URI)
}

func TestUpdateWorkItem(t *testing.T) {
	glacierRestore := getGlacierRestoreWorker(t)
	require.NotNil(t, glacierRestore)

	// Tell the worker to talk to our S3 test server and Pharos
	// test server, defined below
	glacierRestore.S3Url = s3TestServer.URL
	glacierRestore.Context.PharosClient = getPharosClientForTest(pharosTestServer.URL)

	// Set up the GlacierRestoreStateObject
	objIdentifier := "test.edu/glacier_bag"

	// Note that we're getting a WorkItem that has a GenericFileIdentifier
	workItem := getFileWorkItem(TEST_ID, objIdentifier, objIdentifier+"/file1.txt")
	nsqMessage := testutil.MakeNsqMessage(fmt.Sprintf("%d", TEST_ID))

	glacierRestoreState, err := glacierRestore.GetGlacierRestoreState(nsqMessage, workItem)
	require.Nil(t, err)
	require.NotNil(t, glacierRestoreState)

	glacierRestoreState.WorkItem.Note = "Updated note"
	glacierRestoreState.WorkItem.Node = "blah-blah-blah"
	glacierRestoreState.WorkItem.Pid = 9800
	glacierRestoreState.WorkItem.Status = constants.StatusSuccess

	updatedWorkItem := glacierRestore.UpdateWorkItem(glacierRestoreState)
	assert.Empty(t, glacierRestoreState.WorkSummary.Errors)
	assert.Equal(t, "Updated note", updatedWorkItem.Note)
	assert.Equal(t, "blah-blah-blah", updatedWorkItem.Node)
	assert.Equal(t, 9800, updatedWorkItem.Pid)
	assert.Equal(t, constants.StatusSuccess, updatedWorkItem.Status)
}

func TestSaveWorkItemState(t *testing.T) {

}

func TestFinishWithError(t *testing.T) {

}

func TestRequeueForAdditionalRequests(t *testing.T) {

}

func TestRequeueToCheckState(t *testing.T) {

}

func TestCreateRestoreWorkItem(t *testing.T) {

}

func TestRequestAllFiles(t *testing.T) {

}

func TestRequestFile(t *testing.T) {

}

func TestGetRequestDetails(t *testing.T) {

}

func TestGetRequestRecord(t *testing.T) {

}

func TestInitializeRetrieval(t *testing.T) {

}

// -------------------------------------------------------------------------
// TODO: End-to-end test with the following:
//
// 1. IntellectualObject where all requests succeed.
// 2. IntellectualObject where some requests do not succeed.
//    This should be requeued for retry.
// 3. GenericFile where request succeeds.
// 4. GenericFile where request fails (and is retried).
//
// TODO: Mocks for the following...
//
// 1. Glacier restore request
// 2. S3 head request
// 3. NSQ requeue
//
// Will need a customized Context object where URLs for NSQ,
// Pharos, S3, and Glacier point to the mock services.
// -------------------------------------------------------------------------

// -------------------------------------------------------------------------
// HTTP test handlers
// -------------------------------------------------------------------------

func getRequestData(r *http.Request) (map[string]interface{}, error) {
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	data := make(map[string]interface{})
	err := decoder.Decode(&data)
	return data, err
}

func getIdFromUrl(url string) int {
	id := 1000
	matches := URL_ID_REGEX.FindAllStringSubmatch(url, 1)
	if len(matches[0]) > 0 {
		id, _ = strconv.Atoi(matches[0][1])
	}
	return id
}

func workItemGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeWorkItem()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

// Simulate updating of WorkItem. Pharos returns the updated WorkItem,
// so this mock can just return the JSON as-is, and then the test
// code can check that to see whether the worker sent the right data
// to Pharos.
func workItemPutHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(body))
}

func workItemStateGetHandler(w http.ResponseWriter, r *http.Request) {
	id := getIdFromUrl(r.URL.String())
	obj := testutil.MakeWorkItemState()
	obj.WorkItemId = id
	obj.Action = constants.ActionGlacierRestore
	obj.State = ""
	state := &models.GlacierRestoreState{}
	state.WorkSummary = testutil.MakeWorkSummary()

	// Add some Glacier request records to this object, if necessary
	for i := 0; i < NumberOfRequestsToIncludeInState; i++ {
		fileIdentifier := fmt.Sprintf("test.edu/glacier_bag/file_%d.pdf", i+1)
		request := testutil.MakeGlacierRestoreRequest(fileIdentifier, true)
		state.Requests = append(state.Requests, request)
	}
	jsonBytes, err := json.Marshal(state)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error encoding JSON data: %v", err)
		fmt.Fprintln(w, err.Error())
		return
	}
	obj.State = string(jsonBytes)

	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func workItemStatePutHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement this.
}

func intellectualObjectGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeIntellectualObject(12, 0, 0, 0)
	obj.StorageOption = constants.StorageGlacierOH
	for i, gf := range obj.GenericFiles {
		gf.Identifier = fmt.Sprintf("%s/file_%d.txt", obj.Identifier, i)
		gf.StorageOption = constants.StorageGlacierOH
	}
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func genericFileGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeGenericFile(0, 2, "test.edu/glacier_bag")
	obj.StorageOption = constants.StorageGlacierOH
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

// pharosHandler handles all requests that the GlacierRestoreInit
// worker would send to Pharos.
func pharosHandler(w http.ResponseWriter, r *http.Request) {
	url := r.URL.String()
	if strings.Contains(url, "/item_state/") {
		if r.Method == http.MethodGet {
			workItemStateGetHandler(w, r)
		} else {
			workItemStatePutHandler(w, r)
		}
	} else if strings.Contains(url, "/items/") {
		if r.Method == http.MethodGet {
			workItemGetHandler(w, r)
		} else {
			workItemPutHandler(w, r)
		}
	} else if strings.Contains(url, "/objects/") {
		intellectualObjectGetHandler(w, r)
	} else if strings.Contains(url, "/files/") {
		genericFileGetHandler(w, r)
	} else {
		panic(fmt.Sprintf("Don't know how to handle request for %s", url))
	}
}

// s3Handler handles all the requests that the GlacierRestoreInit
// worker would send to S3 (including requests to move Glacier objects
// back into S3).
func s3Handler(w http.ResponseWriter, r *http.Request) {
	if DescribeRestoreStateAs == NotStarted {
		network.S3HeadHandler(w, r)
	} else if DescribeRestoreStateAs == InProgress {
		network.S3HeadRestoreInProgressHandler(w, r)
	} else if DescribeRestoreStateAs == Completed {
		network.S3HeadRestoreCompletedHandler(w, r)
	}
}
