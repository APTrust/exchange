package workers_test

import (
	"github.com/APTrust/exchange/util/testutil"
	"github.com/APTrust/exchange/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	//"net/http"
	//"net/http/httptest"
	"testing"
)

func getGlacierRestoreWorker(t *testing.T) *workers.APTGlacierRestoreInit {
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err)
	return workers.NewGlacierRestore(_context)
}

func TestNewGlacierRestore(t *testing.T) {
	glacierRestore := getGlacierRestoreWorker(t)
	require.NotNil(t, glacierRestore)
	assert.NotNil(t, glacierRestore.Context)
	assert.NotNil(t, glacierRestore.RequestChannel)
	assert.NotNil(t, glacierRestore.CleanupChannel)
}

func TestGetGlacierRestoreState(t *testing.T) {

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
