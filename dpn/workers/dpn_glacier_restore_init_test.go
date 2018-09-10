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

func getDGITestItems(t *testing.T) (*workers.DPNGlacierRestoreInit, *nsq.Message, *testutil.NSQTestDelegate, *dpn_models.DPNRetrievalManifest) {
	worker := getDGITestWorker(t)
	message := testutil.MakeNsqMessage("1234")
	// Create an NSQMessage with a delegate that will capture
	// the data our worker sends back to the NSQ server.
	delegate := testutil.NewNSQTestDelegate()
	message.Delegate = delegate
	manifest := worker.GetRetrievalManifest(message)
	require.NotNil(t, manifest)
	return worker, message, delegate, manifest
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
	worker.PostTestChannel = make(chan *dpn_models.DPNRetrievalManifest)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for manifest := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, manifest.DPNBag)
			assert.NotEmpty(t, manifest.GlacierBucket)
			assert.False(t, manifest.RequestedFromGlacierAt.IsZero())

			// Request was accepted, and manifest should reflect that.
			assert.True(t, manifest.GlacierRequestAccepted)
			assert.False(t, manifest.IsAvailableInS3)
			assert.False(t, manifest.GlacierRestoreSummary.HasErrors())

			// Make sure we requeued to recheck progress later.
			assert.Equal(t, "requeue", delegate.Operation)
			assert.Equal(t, 3*time.Hour, delegate.Delay)

			// Make sure the error message was copied into the DPNWorkItem note.
			require.NotNil(t, manifest.DPNWorkItem.Note)
			assert.Equal(t, expectedNote, *manifest.DPNWorkItem.Note)

			// Make sure we closed out the WorkSummary correctly.
			assert.True(t, manifest.GlacierRestoreSummary.Started())
			assert.True(t, manifest.GlacierRestoreSummary.Finished())
			assert.True(t, manifest.GlacierRestoreSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StatusStarted, manifest.DPNWorkItem.Status)
			assert.True(t, manifest.DPNWorkItem.Retry)

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
	expectedError := "Request to restore aptrust.dpn.test/00000000-0000-0000-0000-000000000000: Glacier restore service is temporarily unavailable. Try again later."

	// Create a PostTestChannel. The worker will send the
	// DPNRetrievalManifest object into this channel when
	// all other processing is complete.
	worker.PostTestChannel = make(chan *dpn_models.DPNRetrievalManifest)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for manifest := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, manifest.DPNBag)
			assert.NotEmpty(t, manifest.GlacierBucket)
			assert.False(t, manifest.RequestedFromGlacierAt.IsZero())

			// Request was rejected, and manifest should reflect that.
			assert.False(t, manifest.GlacierRequestAccepted)
			assert.False(t, manifest.IsAvailableInS3)
			assert.Equal(t, expectedError, manifest.GlacierRestoreSummary.FirstError())

			// Rejection is non-fatal. Make sure we requeued.
			assert.Equal(t, "requeue", delegate.Operation)
			assert.Equal(t, 1*time.Minute, delegate.Delay)

			// Make sure DPNWorkItem note contains the right info.
			require.NotNil(t, manifest.DPNWorkItem.Note)
			assert.Equal(t, expectedError, *manifest.DPNWorkItem.Note)

			// Make sure we closed out the WorkSummary correctly
			assert.True(t, manifest.GlacierRestoreSummary.Started())
			assert.True(t, manifest.GlacierRestoreSummary.Finished())
			assert.False(t, manifest.GlacierRestoreSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StatusStarted, manifest.DPNWorkItem.Status)
			assert.True(t, manifest.DPNWorkItem.Retry)
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
	worker.PostTestChannel = make(chan *dpn_models.DPNRetrievalManifest)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for manifest := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, manifest.DPNBag)
			assert.NotEmpty(t, manifest.GlacierBucket)
			assert.False(t, manifest.RequestedFromGlacierAt.IsZero())

			// Request was rejected, and manifest should reflect that.
			assert.True(t, manifest.GlacierRequestAccepted)
			assert.True(t, manifest.IsAvailableInS3)
			assert.False(t, manifest.GlacierRestoreSummary.HasErrors())

			// Item was completed, so the message should be marked finished.
			assert.Equal(t, "finish", delegate.Operation)

			// Make sure the DPNWorkItem note includes a meaningful status message.
			require.NotNil(t, manifest.DPNWorkItem.Note)
			assert.Equal(t, expected, *manifest.DPNWorkItem.Note)

			// Make sure we closed out the WorkSummary correctly
			assert.True(t, manifest.GlacierRestoreSummary.Started())
			assert.True(t, manifest.GlacierRestoreSummary.Finished())
			assert.True(t, manifest.GlacierRestoreSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StageAvailableInS3, manifest.DPNWorkItem.Stage)
			assert.Equal(t, constants.StatusStarted, manifest.DPNWorkItem.Status)
			assert.True(t, manifest.DPNWorkItem.Retry)
			wg.Done()
		}
	}()

	worker.HandleMessage(message)
	wg.Wait()
}

func TestGetRetrievalManifest(t *testing.T) {
	_, _, _, manifest := getDGITestItems(t)
	require.NotNil(t, manifest)
	assert.NotNil(t, manifest.DPNBag)
	assert.NotNil(t, manifest.GlacierRestoreSummary)
	assert.False(t, manifest.GlacierRestoreSummary.Started())
	assert.False(t, manifest.GlacierRestoreSummary.Finished())
	assert.Empty(t, manifest.LocalPath)
	assert.Empty(t, manifest.RestorationURL)
	assert.Empty(t, manifest.S3Bucket)
	assert.Empty(t, manifest.ActualFixityValue)
	assert.NotEmpty(t, manifest.GlacierBucket)
	assert.NotEmpty(t, manifest.ExpectedFixityValue)
	assert.Equal(t, constants.ActionFixityCheck, manifest.TaskType)
	assert.True(t, manifest.RequestedFromGlacierAt.IsZero())
	assert.False(t, manifest.GlacierRequestAccepted)
	assert.True(t, manifest.EstimatedDeletionFromS3.IsZero())
	assert.False(t, manifest.IsAvailableInS3)
}

func TestDGIRestoreRequestNeeded(t *testing.T) {
	worker, _, _, manifest := getDGITestItems(t)

	// Request is needed because mock S3 service
	// is telling worker this request has not been
	// initiated.
	DescribeRestoreStateAs = NotStartedAcceptNow
	needed, err := worker.RestoreRequestNeeded(manifest)
	require.Nil(t, err)
	assert.True(t, needed)

	// Request is needed because mock S3 service
	// is telling worker this request has not been
	// initiated.
	DescribeRestoreStateAs = NotStartedRejectNow
	needed, err = worker.RestoreRequestNeeded(manifest)
	require.Nil(t, err)
	assert.True(t, needed)

	// Request is NOT needed because S3 HEAD request
	// tells us restore has been initiated but is not
	// yet complete. In this case, we requeue for a
	// later HEAD request to see if it is complete.
	DescribeRestoreStateAs = InProgressGlacier
	needed, err = worker.RestoreRequestNeeded(manifest)
	require.Nil(t, err)
	assert.False(t, needed)

	// Request is NOT needed because S3 HEAD request
	// tells us the item has already been restored
	// from Glacier to S3.
	DescribeRestoreStateAs = Completed
	needed, err = worker.RestoreRequestNeeded(manifest)
	require.Nil(t, err)
	assert.False(t, needed)
}

func TestDGIFinishWithSuccess(t *testing.T) {
	node := "server1.aptrust.org"
	pid := 8477

	// Test a fully completed item (available in S3)
	worker, _, delegate, manifest := getDGITestItems(t)
	manifest.IsAvailableInS3 = true
	manifest.DPNWorkItem.ProcessingNode = &node
	manifest.DPNWorkItem.Pid = pid
	worker.FinishWithSuccess(manifest)
	assert.Equal(t, constants.StageAvailableInS3, manifest.DPNWorkItem.Stage)
	assert.Equal(t, "Item is available in S3 for download.", *manifest.DPNWorkItem.Note)
	assert.Nil(t, manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, 0, manifest.DPNWorkItem.Pid)
	assert.Equal(t, "finish", delegate.Operation)

	// Test an in-progress item (not yet in S3)
	worker, _, delegate, manifest = getDGITestItems(t)
	manifest.IsAvailableInS3 = false
	manifest.DPNWorkItem.ProcessingNode = &node
	manifest.DPNWorkItem.Pid = pid
	worker.FinishWithSuccess(manifest)
	assert.Equal(t, "Glacier restore initiated. Will check availability in S3 every 3 hours.", *manifest.DPNWorkItem.Note)
	assert.Nil(t, manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, 0, manifest.DPNWorkItem.Pid)
	assert.Equal(t, "requeue", delegate.Operation)
	assert.Equal(t, 3*time.Hour, delegate.Delay)
}

func TestDGIFinishWithError(t *testing.T) {
	node := "server1.aptrust.org"
	pid := 8477

	// Test with a non-fatal error
	worker, _, delegate, manifest := getDGITestItems(t)
	manifest.DPNWorkItem.ProcessingNode = &node
	manifest.DPNWorkItem.Pid = pid
	manifest.GlacierRestoreSummary.AddError("Oops! Deliberate error for testing.")
	manifest.GlacierRestoreSummary.ErrorIsFatal = false
	worker.FinishWithError(manifest)
	assert.Equal(t, "Oops! Deliberate error for testing.", *manifest.DPNWorkItem.Note)
	assert.Nil(t, manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, 0, manifest.DPNWorkItem.Pid)
	assert.Equal(t, "requeue", delegate.Operation)
	assert.Equal(t, 1*time.Minute, delegate.Delay)

	// Test with a fatal error
	worker, _, delegate, manifest = getDGITestItems(t)
	manifest.DPNWorkItem.ProcessingNode = &node
	manifest.DPNWorkItem.Pid = pid
	manifest.GlacierRestoreSummary.AddError("Oopsie!")
	manifest.GlacierRestoreSummary.ErrorIsFatal = true
	worker.FinishWithError(manifest)
	assert.Equal(t, "Oopsie!", *manifest.DPNWorkItem.Note)
	assert.Nil(t, manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, 0, manifest.DPNWorkItem.Pid)
	assert.Equal(t, "finish", delegate.Operation)
}

func TestDGIInitializeRetrieval(t *testing.T) {
	worker, _, _, manifest := getDGITestItems(t)

	DescribeRestoreStateAs = NotStartedAcceptNow
	manifest.RequestedFromGlacierAt = time.Time{}
	worker.InitializeRetrieval(manifest)
	assert.False(t, manifest.RequestedFromGlacierAt.IsZero())
	assert.True(t, manifest.GlacierRequestAccepted)

	DescribeRestoreStateAs = NotStartedRejectNow
	manifest.RequestedFromGlacierAt = time.Time{}
	worker.InitializeRetrieval(manifest)
	assert.False(t, manifest.RequestedFromGlacierAt.IsZero())
	assert.False(t, manifest.GlacierRequestAccepted)
}

func TestDGISaveDPNWorkItem(t *testing.T) {
	worker, _, _, manifest := getDGITestItems(t)
	originalNote := *manifest.DPNWorkItem.Note
	worker.SaveDPNWorkItem(manifest)
	assert.False(t, manifest.GlacierRestoreSummary.HasErrors())
	// SaveDPNWorkItem will set an error message here if it has problems.
	// Unchanged note means there were no problems.
	assert.Equal(t, originalNote, *manifest.DPNWorkItem.Note)
	// Should retry if no fatal error
	assert.True(t, manifest.DPNWorkItem.Retry)

	worker, _, _, manifest = getDGITestItems(t)
	manifest.GlacierRestoreSummary.ErrorIsFatal = true
	originalNote = *manifest.DPNWorkItem.Note
	worker.SaveDPNWorkItem(manifest)
	assert.False(t, manifest.GlacierRestoreSummary.HasErrors())
	assert.Equal(t, originalNote, *manifest.DPNWorkItem.Note)
	// Should NOT retry if there was a fatal error
	assert.False(t, manifest.DPNWorkItem.Retry)

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
	url := r.URL.String()
	var objJson []byte
	if strings.Contains(url, "/digest") {
		// Fixity request
		obj := &dpn_models.MessageDigest{
			Bag:       testutil.EMPTY_UUID,
			Algorithm: constants.AlgSha256,
			Node:      "aptrust",
			Value:     "1234567890",
			CreatedAt: testutil.TEST_TIMESTAMP,
		}
		objJson, _ = json.Marshal(obj)
	} else {
		// Bag GET request
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
