package workers_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	dpn_models "github.com/APTrust/exchange/dpn/models"
	dpn_network "github.com/APTrust/exchange/dpn/network"
	dpn_testutil "github.com/APTrust/exchange/dpn/util/testutil"
	"github.com/APTrust/exchange/dpn/workers"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
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

// This package-level setting tells our S3 mock server
// what kind of response we want for the current test
// scenario.
var DescribeRestoreStateAs = NotStartedHead

// This package-level setting tells our Pharos mock server
// whether to return a DPNWorkItem with an empty UUID. When
// this is true, the mock server returns data with an empty
// UUID that does not correspond to any DPN bag anywhere,
// which allows us to test some failure scenarios. When this is
// false, our Pharos mock server returns a DPNWorkItem with the
// UUID of a bag that actually exists in our test bucket. This
// allows us to test the happy path of successful outcomes.
var ReturnDPNWorkItemWithEmptyUUID = false

// Test server to mock Pharos, S3, and DPN requests
var pharosTestServer = httptest.NewServer(http.HandlerFunc(pharosHandler))
var s3TestServer = httptest.NewServer(http.HandlerFunc(s3Handler))
var dpnTestServer = httptest.NewServer(http.HandlerFunc(dpnHandler))
var nsqServer = httptest.NewServer(http.HandlerFunc(nsqHandler))

// This resets ReturnDPNWorkItemWithEmptyUUID to false.
// Use in defer statements so the setting reverts to false
// after an individual test completes.
func ResetDPNWorkItemUUID() {
	ReturnDPNWorkItemWithEmptyUUID = false
}

func getFixityWorkItem() *apt_models.DPNWorkItem {
	timestamp := testutil.TEST_TIMESTAMP
	emptyTime := time.Time{}
	return &apt_models.DPNWorkItem{
		Id:             999,
		RemoteNode:     "tdr",
		Task:           constants.DPNTaskFixity,
		Identifier:     dpn_testutil.DPN_TEST_BAG_UUID,
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

func getDGITestItems(t *testing.T) (*workers.DPNGlacierRestoreInit, *nsq.Message, *testutil.NSQTestDelegate, *workers.DPNRestoreHelper) {
	worker := getDGITestWorker(t)
	message := testutil.MakeNsqMessage("1234")
	// Create an NSQMessage with a delegate that will capture
	// the data our worker sends back to the NSQ server.
	delegate := testutil.NewNSQTestDelegate()
	message.Delegate = delegate
	helper, err := workers.NewDPNRestoreHelper(message, worker.Context,
		worker.LocalDPNRestClient, constants.ActionFixityCheck,
		"GlacierRestoreSummary")
	require.Nil(t, err)
	require.NotNil(t, helper)
	return worker, message, delegate, helper
}

func TestDGIInit(t *testing.T) {
	worker := getDGITestWorker(t)
	require.NotNil(t, worker)
	assert.NotNil(t, worker.Context)
	assert.NotNil(t, worker.RequestChannel)
	assert.NotNil(t, worker.CleanupChannel)
	assert.NotNil(t, worker.LocalDPNRestClient)
}

func TestDGIHandleAcceptedButNotComplete(t *testing.T) {
	// If Glacier accepts our restore request, or if
	// it's in progress but not yet complete, the worker
	// should re-check the item every few hours.

	// This is an initial request being accepted by Glacier.
	DescribeRestoreStateAs = NotStartedAcceptNow
	test_DGIHandleAcceptedButNotComplete(t)

	// This is a previously accepted request that is still
	// in process of being restored.
	DescribeRestoreStateAs = InProgressGlacier
	test_DGIHandleAcceptedButNotComplete(t)
}

func test_DGIHandleAcceptedButNotComplete(t *testing.T) {
	worker, message, delegate, _ := getDGITestItems(t)
	expectedNote := "Glacier restore initiated. Will check availability in S3 every 3 hours."

	// Create a PostTestChannel. The worker will send the
	// DPNRetrievalManifest object into this channel when
	// all other processing is complete.
	worker.PostTestChannel = make(chan *workers.DPNRestoreHelper)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for helper := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, helper.Manifest.DPNBag)
			assert.NotEmpty(t, helper.Manifest.GlacierBucket)
			assert.False(t, helper.Manifest.RequestedFromGlacierAt.IsZero())

			// Request was accepted, and manifest should reflect that.
			assert.True(t, helper.Manifest.GlacierRequestAccepted)
			assert.False(t, helper.Manifest.IsAvailableInS3)
			assert.False(t, helper.Manifest.GlacierRestoreSummary.HasErrors())

			// Make sure we requeued to recheck progress later.
			assert.Equal(t, "requeue", delegate.Operation)
			assert.Equal(t, 3*time.Hour, delegate.Delay)

			// Make sure the error message was copied into the DPNWorkItem note.
			require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.Equal(t, expectedNote, *helper.Manifest.DPNWorkItem.Note)

			// Make sure we closed out the WorkSummary correctly.
			assert.True(t, helper.Manifest.GlacierRestoreSummary.Started())
			assert.True(t, helper.Manifest.GlacierRestoreSummary.Finished())
			assert.True(t, helper.Manifest.GlacierRestoreSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StatusStarted, helper.Manifest.DPNWorkItem.Status)
			assert.True(t, helper.Manifest.DPNWorkItem.Retry)

			wg.Done()
		}
	}()

	worker.HandleMessage(message)
	wg.Wait()
}

func TestDGIHandleNotStartedRejectNow(t *testing.T) {
	worker, message, delegate, _ := getDGITestItems(t)

	// Tell our S3 mock server to reject this request.
	DescribeRestoreStateAs = NotStartedRejectNow

	// Because the request will be rejected, we expect this error.
	expectedError := fmt.Sprintf("Request to restore aptrust.dpn.test/%s: Glacier restore service is temporarily unavailable. Try again later.", dpn_testutil.DPN_TEST_BAG_UUID)

	// Create a PostTestChannel. The worker will send the
	// DPNRetrievalManifest object into this channel when
	// all other processing is complete.
	worker.PostTestChannel = make(chan *workers.DPNRestoreHelper)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for helper := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, helper.Manifest.DPNBag)
			assert.NotEmpty(t, helper.Manifest.GlacierBucket)
			assert.False(t, helper.Manifest.RequestedFromGlacierAt.IsZero())

			// Request was rejected, and manifest should reflect that.
			assert.False(t, helper.Manifest.GlacierRequestAccepted)
			assert.False(t, helper.Manifest.IsAvailableInS3)
			assert.Equal(t, expectedError, helper.Manifest.GlacierRestoreSummary.FirstError())

			// Rejection is non-fatal. Make sure we requeued.
			assert.Equal(t, "requeue", delegate.Operation)
			assert.Equal(t, 1*time.Minute, delegate.Delay)

			// Make sure DPNWorkItem note contains the right info.
			require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.Equal(t, expectedError, *helper.Manifest.DPNWorkItem.Note)

			// Make sure we closed out the WorkSummary correctly
			assert.True(t, helper.Manifest.GlacierRestoreSummary.Started())
			assert.True(t, helper.Manifest.GlacierRestoreSummary.Finished())
			assert.False(t, helper.Manifest.GlacierRestoreSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StatusStarted, helper.Manifest.DPNWorkItem.Status)
			assert.True(t, helper.Manifest.DPNWorkItem.Retry)
			wg.Done()
		}
	}()

	worker.HandleMessage(message)
	wg.Wait()
}

func TestDGIHandleCompleted(t *testing.T) {
	worker, message, delegate, _ := getDGITestItems(t)

	// Tell our S3 mock server to say this request
	// has already been completed.
	DescribeRestoreStateAs = Completed

	// Because the request will be rejected, we expect this error.
	expected := "Item is available in S3 for download."

	// Create a PostTestChannel. The worker will send the
	// DPNRetrievalManifest object into this channel when
	// all other processing is complete.
	worker.PostTestChannel = make(chan *workers.DPNRestoreHelper)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for helper := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, helper.Manifest.DPNBag)
			assert.NotEmpty(t, helper.Manifest.GlacierBucket)
			assert.False(t, helper.Manifest.RequestedFromGlacierAt.IsZero())

			// Request was rejected, and manifest should reflect that.
			assert.True(t, helper.Manifest.GlacierRequestAccepted)
			assert.True(t, helper.Manifest.IsAvailableInS3)
			assert.False(t, helper.Manifest.GlacierRestoreSummary.HasErrors())

			// Item was completed, so the message should be marked finished.
			assert.Equal(t, "finish", delegate.Operation)

			// Make sure the DPNWorkItem note includes a meaningful status message.
			require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.Equal(t, expected, *helper.Manifest.DPNWorkItem.Note)

			// Make sure we closed out the WorkSummary correctly
			assert.True(t, helper.Manifest.GlacierRestoreSummary.Started())
			assert.True(t, helper.Manifest.GlacierRestoreSummary.Finished())
			assert.True(t, helper.Manifest.GlacierRestoreSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StageAvailableInS3, helper.Manifest.DPNWorkItem.Stage)
			assert.Equal(t, constants.StatusStarted, helper.Manifest.DPNWorkItem.Status)
			assert.True(t, helper.Manifest.DPNWorkItem.Retry)
			wg.Done()
		}
	}()

	worker.HandleMessage(message)
	wg.Wait()
}

func TestDGIRestoreRequestNeeded(t *testing.T) {
	worker, _, _, helper := getDGITestItems(t)

	// Request is needed because mock S3 service
	// is telling worker this request has not been
	// initiated.
	DescribeRestoreStateAs = NotStartedAcceptNow
	needed, err := worker.RestoreRequestNeeded(helper)
	require.Nil(t, err)
	assert.True(t, needed)

	// Request is needed because mock S3 service
	// is telling worker this request has not been
	// initiated.
	DescribeRestoreStateAs = NotStartedRejectNow
	needed, err = worker.RestoreRequestNeeded(helper)
	require.Nil(t, err)
	assert.True(t, needed)

	// Request is NOT needed because S3 HEAD request
	// tells us restore has been initiated but is not
	// yet complete. In this case, we requeue for a
	// later HEAD request to see if it is complete.
	DescribeRestoreStateAs = InProgressGlacier
	needed, err = worker.RestoreRequestNeeded(helper)
	require.Nil(t, err)
	assert.False(t, needed)

	// Request is NOT needed because S3 HEAD request
	// tells us the item has already been restored
	// from Glacier to S3.
	DescribeRestoreStateAs = Completed
	needed, err = worker.RestoreRequestNeeded(helper)
	require.Nil(t, err)
	assert.False(t, needed)
}

func TestDGIFinishWithSuccess(t *testing.T) {
	node := "server1.aptrust.org"
	pid := 8477

	// Test a fully completed item (available in S3)
	worker, _, delegate, helper := getDGITestItems(t)
	helper.Manifest.IsAvailableInS3 = true
	helper.Manifest.DPNWorkItem.ProcessingNode = &node
	helper.Manifest.DPNWorkItem.Pid = pid
	worker.FinishWithSuccess(helper)
	assert.Equal(t, constants.StageAvailableInS3, helper.Manifest.DPNWorkItem.Stage)
	assert.Equal(t, "Item is available in S3 for download.", *helper.Manifest.DPNWorkItem.Note)
	assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
	assert.Equal(t, "finish", delegate.Operation)

	// Test an in-progress item (not yet in S3)
	worker, _, delegate, helper = getDGITestItems(t)
	helper.Manifest.IsAvailableInS3 = false
	helper.Manifest.DPNWorkItem.ProcessingNode = &node
	helper.Manifest.DPNWorkItem.Pid = pid
	worker.FinishWithSuccess(helper)
	assert.Equal(t, "Glacier restore initiated. Will check availability in S3 every 3 hours.",
		*helper.Manifest.DPNWorkItem.Note)
	assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
	assert.Equal(t, "requeue", delegate.Operation)
	assert.Equal(t, 3*time.Hour, delegate.Delay)
}

func TestDGIFinishWithError(t *testing.T) {
	node := "server1.aptrust.org"
	pid := 8477

	// Test with a non-fatal error
	worker, _, delegate, helper := getDGITestItems(t)
	helper.Manifest.DPNWorkItem.ProcessingNode = &node
	helper.Manifest.DPNWorkItem.Pid = pid
	helper.Manifest.GlacierRestoreSummary.AddError("Oops! Deliberate error for testing.")
	helper.Manifest.GlacierRestoreSummary.ErrorIsFatal = false
	worker.FinishWithError(helper)
	assert.Equal(t, "Oops! Deliberate error for testing.", *helper.Manifest.DPNWorkItem.Note)
	assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
	assert.Equal(t, "requeue", delegate.Operation)
	assert.Equal(t, 1*time.Minute, delegate.Delay)

	// Test with a fatal error
	worker, _, delegate, helper = getDGITestItems(t)
	helper.Manifest.DPNWorkItem.ProcessingNode = &node
	helper.Manifest.DPNWorkItem.Pid = pid
	helper.Manifest.GlacierRestoreSummary.AddError("Oopsie!")
	helper.Manifest.GlacierRestoreSummary.ErrorIsFatal = true
	worker.FinishWithError(helper)
	assert.Equal(t, "Oopsie!", *helper.Manifest.DPNWorkItem.Note)
	assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
	assert.Equal(t, "finish", delegate.Operation)
}

func TestDGIInitializeRetrieval(t *testing.T) {
	worker, _, _, helper := getDGITestItems(t)

	DescribeRestoreStateAs = NotStartedAcceptNow
	helper.Manifest.RequestedFromGlacierAt = time.Time{}
	worker.InitializeRetrieval(helper)
	assert.False(t, helper.Manifest.RequestedFromGlacierAt.IsZero())
	assert.True(t, helper.Manifest.GlacierRequestAccepted)

	DescribeRestoreStateAs = NotStartedRejectNow
	helper.Manifest.RequestedFromGlacierAt = time.Time{}
	worker.InitializeRetrieval(helper)
	assert.False(t, helper.Manifest.RequestedFromGlacierAt.IsZero())
	assert.False(t, helper.Manifest.GlacierRequestAccepted)
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
	bagUUID := dpn_testutil.DPN_TEST_BAG_UUID
	if ReturnDPNWorkItemWithEmptyUUID {
		bagUUID = testutil.EMPTY_UUID
	}
	obj := &apt_models.DPNWorkItem{
		Id:          1234,
		RemoteNode:  "tdr",
		Task:        constants.DPNTaskFixity,
		Identifier:  bagUUID,
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
	bagUUID := dpn_testutil.DPN_TEST_BAG_UUID
	if ReturnDPNWorkItemWithEmptyUUID {
		bagUUID = testutil.EMPTY_UUID
	}
	url := r.URL.String()
	var objJson []byte
	if strings.Contains(url, "/digest") {
		// Digest request
		obj := &dpn_models.MessageDigest{
			Bag:       bagUUID,
			Algorithm: constants.AlgSha256,
			Node:      "aptrust",
			Value:     "1234567890",
			CreatedAt: testutil.TEST_TIMESTAMP,
		}
		objJson, _ = json.Marshal(obj)
	} else if strings.Contains(url, "/fixity_check/") {
		obj := &dpn_models.FixityCheck{
			FixityCheckId: testutil.EMPTY_UUID,
			Bag:           bagUUID,
			Node:          "aptrust",
			Success:       true,
			FixityAt:      testutil.TEST_TIMESTAMP,
			CreatedAt:     testutil.TEST_TIMESTAMP,
		}
		objJson, _ = json.Marshal(obj)
	} else {
		// Bag GET request
		obj := &dpn_models.DPNBag{
			UUID:             bagUUID,
			Interpretive:     []string{},
			Rights:           []string{},
			ReplicatingNodes: []string{},
			LocalId:          "DPN-TEST-BAG",
			Size:             dpn_testutil.DPN_TEST_BAG_SIZE,
			FirstVersionUUID: bagUUID,
			Version:          1,
			BagType:          "D",
			IngestNode:       "aptrust",
			AdminNode:        "aptrust",
			Member:           "9a000000-0000-4000-a000-000000000001", // Sunnyvale College
			CreatedAt:        testutil.TEST_TIMESTAMP,
			UpdatedAt:        testutil.TEST_TIMESTAMP,
		}
		objJson, _ = json.Marshal(obj)
	}
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
		// This is an S3 API call, where the response includes
		// info saying the restore is complete and the item will be
		// available in S3 until a specific date/time.
		network.S3HeadRestoreCompletedHandler(w, r)
	}
}

// Just say Okaly-dolaly, Flanders.
func nsqHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "OK")
}
