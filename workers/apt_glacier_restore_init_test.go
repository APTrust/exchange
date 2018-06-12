package workers_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/APTrust/exchange/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

var workItemTestServer = httptest.NewServer(http.HandlerFunc(workItemGetHandler))
var workItemStateTestServer = httptest.NewServer(http.HandlerFunc(workItemStateGetHandler))

func getGlacierRestoreWorker(t *testing.T) *workers.APTGlacierRestoreInit {
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err)
	return workers.NewGlacierRestore(_context)
}

func getObjectWorkItem(id int, objectIdentifier string) *models.WorkItem {
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
	}
}

func getFileWorkItem(id int, objectIdentifier, fileIdentifier string) *models.WorkItem {
	workItem := getObjectWorkItem(id, objectIdentifier)
	workItem.GenericFileIdentifier = fileIdentifier
	return workItem
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

	id := 1000
	objIdentifier := "test.edu/glacier_bag"
	workItem := getObjectWorkItem(id, objIdentifier)
	nsqMessage := testutil.MakeNsqMessage(fmt.Sprintf("%d", id))

	glacierRestore.Context.Config.PharosURL = workItemTestServer.URL
	glacierRestoreState, err := glacierRestore.GetGlacierRestoreState(nsqMessage, workItem)
	require.NotNil(t, glacierRestoreState)
	require.Nil(t, err)
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

func workItemGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeWorkItem()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func workItemStateGetHandler(w http.ResponseWriter, r *http.Request) {
	data, err := getRequestData(r)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON data: %v", err)
		fmt.Fprintln(w, err.Error())
		return
	}
	obj := testutil.MakeWorkItemState()
	obj.WorkItemId = data["id"].(int)
	obj.Action = constants.ActionGlacierRestore
	obj.State = ""

	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}
