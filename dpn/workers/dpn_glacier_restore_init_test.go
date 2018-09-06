package workers_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	dpn_models "github.com/APTrust/exchange/dpn/models"
	dpn_network "github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/dpn/workers"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	//	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	//	"os"
	//	"regexp"
	//	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Settings to test different S3 responses.
const (
	NotStartedHead      = 0
	NotStartedAcceptNow = 1
	NotStartedRejectNow = 2
	InProgressHead      = 3
	InProgressGlacier   = 4
	Completed           = 5
)

var DescribeRestoreStateAs = NotStartedHead

// Test server to mock Pharos, S3, and DPN requests
var pharosTestServer = httptest.NewServer(http.HandlerFunc(pharosHandler))
var s3TestServer = httptest.NewServer(http.HandlerFunc(s3Handler))
var dpnTestServer = httptest.NewServer(http.HandlerFunc(dpnHandler))
var nsqServer = httptest.NewServer(http.HandlerFunc(nsqHandler))

func getFixityWorkItem() *apt_models.DPNWorkItem {
	timestamp := testutil.TEST_TIMESTAMP
	emptyTime := time.Time{}
	return &apt_models.DPNWorkItem{
		Id:             999,
		RemoteNode:     "tdr",
		Task:           constants.DPNTaskFixity,
		Identifier:     testutil.EMPTY_UUID,
		QueuedAt:       &timestamp,
		CompletedAt:    &emptyTime,
		ProcessingNode: nil,
		Pid:            0,
		Stage:          constants.StageRequested,
		Status:         constants.StatusPending,
		Retry:          true,
		Note:           nil,
		State:          nil,
		CreatedAt:      timestamp,
		UpdatedAt:      timestamp,
	}
}

func getPharosClientForTest(url string) *network.PharosClient {
	client, _ := network.NewPharosClient(url, "v2", "frankzappa", "abcxyz")
	return client
}

func getDPNClientForTest(url string) *dpn_network.DPNRestClient {
	dpnConfig := apt_models.DPNConfig{
		AcceptInvalidSSLCerts: true,
		RemoteNodeTokens:      make(map[string]string),
		RemoteNodeURLs:        make(map[string]string),
	}
	client, _ := dpn_network.NewDPNRestClient(url, "v1", "api_key", "aptrust", dpnConfig)
	return client
}

func getDGITestWorker(t *testing.T) *workers.DPNGlacierRestoreInit {
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err)

	worker, err := workers.DPNNewGlacierRestoreInit(_context)
	require.Nil(t, err)
	require.NotNil(t, worker)

	// Tell the worker to talk to our S3 test server and Pharos
	// test server, defined below
	worker.S3Url = s3TestServer.URL
	worker.Context.PharosClient = getPharosClientForTest(pharosTestServer.URL)
	worker.LocalDPNRestClient = getDPNClientForTest(dpnTestServer.URL)
	worker.Context.NSQClient.URL = nsqServer.URL

	return worker
}

func TestDGIInit(t *testing.T) {
	worker := getDGITestWorker(t)
	require.NotNil(t, worker)
	assert.NotNil(t, worker.Context)
	assert.NotNil(t, worker.RequestChannel)
	assert.NotNil(t, worker.CleanupChannel)
	assert.NotNil(t, worker.LocalDPNRestClient)
}

func TestDGIHandleMessage(t *testing.T) {
	worker := getDGITestWorker(t)

	// Create an NSQMessage with a delegate that will capture
	// the data our worker sends back to the NSQ server.
	message := testutil.MakeNsqMessage("1234")
	delegate := testutil.NewNSQTestDelegate()
	message.Delegate = delegate

	// Tell our S3 mock server to accept this request.
	DescribeRestoreStateAs = NotStartedAcceptNow

	// Create a PostTestChannel. The worker will send the
	// DPNGlacierRestoreState object into this channel when
	// all other processing is complete.
	worker.PostTestChannel = make(chan *dpn_models.DPNGlacierRestoreState)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for state := range worker.PostTestChannel {
			assert.Empty(t, state.ErrorMessage)
			assert.NotNil(t, state.DPNBag)
			assert.NotEmpty(t, state.GlacierBucket)
			assert.NotEmpty(t, state.GlacierKey)
			assert.False(t, state.RequestedAt.IsZero())
			// assert.True(t, state.RequestAccepted)
			// assert.False(t, state.IsAvailableInS3)
			// assert.Equal(t, "requeue", delegate.Operation)
			// assert.Equal(t, 1*time.Minute, delegate.Delay)
			// assert.Equal(t, "Requeued to check on status of Glacier restore requests.", state.DPNWorkItem.Note)
			// assert.Equal(t, constants.StatusStarted, state.DPNWorkItem.Status)
			assert.True(t, state.DPNWorkItem.Retry)
			wg.Done()
		}
	}()

	worker.HandleMessage(message)
	wg.Wait()
}

func TestDGIRequestRestore(t *testing.T) {

}

func TestDGICleanup(t *testing.T) {

}

func TestDGIFinishWithSuccess(t *testing.T) {

}

func TestDGIFinishWithError(t *testing.T) {

}

func TestDGIInitializeRetrieval(t *testing.T) {

}

func TestDGIGetRestoreState(t *testing.T) {

}

func TestDGISaveDPNWorkItem(t *testing.T) {

}

// ----------------------------------------------------------------------------------
// HTTP handlers for unit tests...
// ----------------------------------------------------------------------------------

// Must handle DPNWorkItem GET and PUT.
func pharosHandler(w http.ResponseWriter, r *http.Request) {
	url := r.URL.String()
	if strings.Contains(url, "/dpn_items/") {
		if r.Method == http.MethodGet {
			dpnItemGetHandler(w, r)
		} else if r.Method == http.MethodPut {
			dpnItemPutHandler(w, r)
		}
	}
}

// Return a DPN work item describing a fixity check that needs to
// be completed.
func dpnItemGetHandler(w http.ResponseWriter, r *http.Request) {
	timestamp := testutil.TEST_TIMESTAMP
	obj := &apt_models.DPNWorkItem{
		Id:          1234,
		RemoteNode:  "tdr",
		Task:        constants.DPNTaskFixity,
		Identifier:  testutil.EMPTY_UUID,
		QueuedAt:    &timestamp,
		CompletedAt: nil,
		Note:        nil,
		Retry:       true,
		Stage:       constants.StageRequested,
		Status:      constants.StatusPending,
		CreatedAt:   timestamp,
		UpdatedAt:   timestamp,
	}
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	w.Write(objJson)
}

// Simulate updating of DPNWorkItem by simply returning the item.
func dpnItemPutHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

// Return a DPN Bag record
func dpnHandler(w http.ResponseWriter, r *http.Request) {
	// Must handle Bag GET request.
	obj := &dpn_models.DPNBag{
		UUID:             testutil.EMPTY_UUID,
		Interpretive:     []string{},
		Rights:           []string{},
		ReplicatingNodes: []string{},
		LocalId:          fmt.Sprintf("GO-TEST-BAG-%s", testutil.EMPTY_UUID),
		Size:             12345678,
		FirstVersionUUID: testutil.EMPTY_UUID,
		Version:          1,
		BagType:          "D",
		IngestNode:       "aptrust",
		AdminNode:        "aptrust",
		Member:           "9a000000-0000-4000-a000-000000000001", // Sunnyvale College
		CreatedAt:        testutil.TEST_TIMESTAMP,
		UpdatedAt:        testutil.TEST_TIMESTAMP,
	}
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	w.Write(objJson)
}

// s3Handler handles all the requests that the GlacierRestoreInit
// worker would send to S3 (including requests to move Glacier objects
// back into S3).
func s3Handler(w http.ResponseWriter, r *http.Request) {
	if DescribeRestoreStateAs == NotStartedHead {
		// S3 HEAD handler will tell us this item is in Glacier, but not yet S3
		network.S3HeadHandler(w, r)
	} else if DescribeRestoreStateAs == NotStartedAcceptNow {
		// Restore handler accepts a Glacier restore requests
		network.S3RestoreHandler(w, r)
	} else if DescribeRestoreStateAs == NotStartedRejectNow {
		// Reject handler reject a Glacier restore requests
		network.S3RestoreRejectHandler(w, r)
	} else if DescribeRestoreStateAs == InProgressHead {
		// This handler is an S3 call that tells us the Glacier restore
		// is in progress, but not yet complete.
		network.S3HeadRestoreInProgressHandler(w, r)
	} else if DescribeRestoreStateAs == InProgressGlacier {
		// This is a Glacier API call that tells us the restore is
		// in progress, but not yet complete.
		network.S3RestoreInProgressHandler(w, r)
	} else if DescribeRestoreStateAs == Completed {
		// This is an S3 API call, where the HEAD response includes
		// info saying the restore is complete and the item will be
		// available in S3 until a specific date/time.
		network.S3HeadRestoreCompletedHandler(w, r)
	}
}

// Just say Okaly-dolaly, Flanders.
func nsqHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "OK")
}
