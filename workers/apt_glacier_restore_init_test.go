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
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// var instead of const so we can get pointer address
var (
	ID_WITHOUT_REQUESTS = 1000
	ID_WITH_REQUESTS    = 1001
)

// Regex to extract ID from URL
var URL_ID_REGEX = regexp.MustCompile(`\/(\d+)\/`)

var pharosTestServer = httptest.NewServer(http.HandlerFunc(pharosHandler))

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

func TestNewGlacierRestore(t *testing.T) {
	glacierRestore := getGlacierRestoreWorker(t)
	require.NotNil(t, glacierRestore)
	assert.NotNil(t, glacierRestore.Context)
	assert.NotNil(t, glacierRestore.RequestChannel)
	assert.NotNil(t, glacierRestore.CleanupChannel)
}

func TestGetGlacierRestoreState(t *testing.T) {
	glacierRestore := getGlacierRestoreWorker(t)
	require.NotNil(t, glacierRestore)

	objIdentifier := "test.edu/glacier_bag"
	workItem := getObjectWorkItem(ID_WITHOUT_REQUESTS, objIdentifier)
	workItem.WorkItemStateId = &ID_WITHOUT_REQUESTS
	nsqMessage := testutil.MakeNsqMessage(fmt.Sprintf("%d", ID_WITHOUT_REQUESTS))

	// ID_WITHOUT_REQUESTS should return a GlacierRestoreState with no request records
	glacierRestore.Context.PharosClient = getPharosClientForTest(pharosTestServer.URL)
	glacierRestoreState, err := glacierRestore.GetGlacierRestoreState(nsqMessage, workItem)
	require.Nil(t, err)
	require.NotNil(t, glacierRestoreState)
	assert.NotNil(t, glacierRestoreState.WorkSummary)
	assert.Empty(t, glacierRestoreState.Requests)

	// ID_WITH_REQUESTS should return a GlacierRestoreState with 4 request records
	workItem.WorkItemStateId = &ID_WITH_REQUESTS
	nsqMessage = testutil.MakeNsqMessage(fmt.Sprintf("%d", ID_WITH_REQUESTS))
	glacierRestoreState, err = glacierRestore.GetGlacierRestoreState(nsqMessage, workItem)
	require.Nil(t, err)
	require.NotNil(t, glacierRestoreState)
	assert.NotNil(t, glacierRestoreState.WorkSummary)
	require.NotEmpty(t, glacierRestoreState.Requests)
	assert.Equal(t, 4, len(glacierRestoreState.Requests))
}

func TestHandleMessage(t *testing.T) {

}

func TestRequestObject(t *testing.T) {

}

func TestRestoreRequestNeeded(t *testing.T) {

}

func TestGetS3HeadClient(t *testing.T) {

}

func TestGetIntellectualObject(t *testing.T) {

}

func TestGetGenericFile(t *testing.T) {

}

func TestUpdateWorkItem(t *testing.T) {

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

func workItemPutHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement this.
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
	if id == ID_WITH_REQUESTS {
		for i := 0; i < 4; i++ {
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
	}

	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func workItemStatePutHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement this.
}

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
	} else {
		panic(fmt.Sprintf("Don't know how to handle request for %s", url))
	}
}
