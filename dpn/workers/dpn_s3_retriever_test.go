package workers_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/dpn/workers"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func getDPNS3TestWorker(t *testing.T) *workers.DPNS3Retriever {
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err)

	worker, err := workers.NewDPNS3Retriever(_context)
	require.Nil(t, err)
	require.NotNil(t, worker)

	// Tell the worker to talk to our S3 test server and Pharos
	// test server, defined below
	// worker.S3Url = s3TestServer.URL
	worker.Context.PharosClient = getPharosClientForTest(pharosTestServer.URL)
	worker.LocalDPNRestClient = getDPNClientForTest(dpnTestServer.URL)
	worker.Context.NSQClient.URL = nsqServer.URL

	return worker
}

func getDPNS3TestItems(t *testing.T) (*workers.DPNS3Retriever, *nsq.Message, *testutil.NSQTestDelegate, *workers.DPNRestoreHelper) {
	worker := getDPNS3TestWorker(t)
	message := testutil.MakeNsqMessage("1234")
	// Create an NSQMessage with a delegate that will capture
	// the data our worker sends back to the NSQ server.
	delegate := testutil.NewNSQTestDelegate()
	message.Delegate = delegate
	helper, err := workers.NewDPNRestoreHelper(message, worker.Context,
		worker.LocalDPNRestClient, constants.ActionFixityCheck,
		"LocalCopySummary")
	require.Nil(t, err)
	require.NotNil(t, helper)
	return worker, message, delegate, helper
}

func TestNewDPNS3Retriever(t *testing.T) {
	worker := getDPNS3TestWorker(t)
	assert.NotNil(t, worker.Context)
	assert.NotNil(t, worker.LocalDPNRestClient)
	assert.NotNil(t, worker.FetchChannel)
	assert.NotNil(t, worker.CleanupChannel)
	assert.Nil(t, worker.PostTestChannel)
}

func TestDPNS3Retriever_DownloadFile(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	// Download a file that exists.
	// This hack temporarily changes the restoration bucket
	// and key to a bucket/key we know exists. The bucket
	// aptrust.integration.test always contains the items
	// in testdata/s3_bags/TestData.zip
	worker, _, _, helper := getDPNS3TestItems(t)
	worker.Context.Config.DPN.DPNRestorationBucket = "aptrust.integration.test"
	helper.Manifest.DPNBag.UUID = "example.edu.tagsample_good"
	helper.Manifest.DPNBag.Size = uint64(40960)
	expectedLocalPath := filepath.Join(worker.Context.Config.DPN.DPNRestorationDirectory,
		helper.Manifest.DPNBag.UUID+".tar")
	worker.DownloadFile(helper)
	assert.False(t, helper.WorkSummary.HasErrors())
	assert.Equal(t, expectedLocalPath, helper.Manifest.LocalPath)
	assert.True(t, helper.FileExistsAndIsComplete())

	// Download a file that does not exist
	worker, _, _, helper = getDPNS3TestItems(t)
	worker.Context.Config.DPN.DPNRestorationBucket = "aptrust.integration.test"
	helper.Manifest.DPNBag.UUID = "this_file_does_not_exist"
	expectedLocalPath = filepath.Join(worker.Context.Config.DPN.DPNRestorationDirectory,
		helper.Manifest.DPNBag.UUID+".tar")
	worker.DownloadFile(helper)
	assert.True(t, helper.WorkSummary.HasErrors())
	assert.True(t, helper.WorkSummary.ErrorIsFatal)
	assert.Equal(t, expectedLocalPath, helper.Manifest.LocalPath)
	assert.False(t, helper.FileExistsAndIsComplete())
}

func TestDPNS3Retriever_FinishWithSuccess(t *testing.T) {
	worker, _, delegate, helper := getDPNS3TestItems(t)
	helper.Manifest.LocalPath = "path/to/file.tar"
	worker.FinishWithSuccess(helper)
	require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
	assert.Equal(t, "Bag has been downloaded to path/to/file.tar", *helper.Manifest.DPNWorkItem.Note)
	assert.Equal(t, constants.StageValidate, helper.Manifest.DPNWorkItem.Stage)
	assert.Equal(t, constants.StatusPending, helper.Manifest.DPNWorkItem.Status)
	assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
	assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, "finish", delegate.Operation)
}

func TestDPNS3Retriever_FinishWithError(t *testing.T) {
	// Test with non-fatal error
	worker, _, delegate, helper := getDPNS3TestItems(t)
	helper.WorkSummary.AddError("Oops 1")
	helper.WorkSummary.AddError("Oops 2")
	helper.WorkSummary.ErrorIsFatal = false
	worker.FinishWithError(helper)
	require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
	assert.Equal(t, "Oops 1\nOops 2", *helper.Manifest.DPNWorkItem.Note)
	assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
	assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, "requeue", delegate.Operation)
	assert.Equal(t, 3*time.Minute, delegate.Delay)

	// Test with fatal error
	worker, _, delegate, helper = getDPNS3TestItems(t)
	helper.WorkSummary.AddError("Oops 1")
	helper.WorkSummary.AddError("Oops 2")
	helper.WorkSummary.ErrorIsFatal = true
	worker.FinishWithError(helper)
	require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
	assert.Equal(t, "Oops 1\nOops 2", *helper.Manifest.DPNWorkItem.Note)
	assert.Equal(t, constants.StatusFailed, helper.Manifest.DPNWorkItem.Status)
	assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
	assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
	assert.Equal(t, "finish", delegate.Operation)
}

func TestDPNS3Retriever_DownloadSuccess(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	worker, _, delegate, helper := getDPNS3TestItems(t)
	worker.Context.Config.DPN.DPNRestorationBucket = "aptrust.integration.test"
	helper.Manifest.DPNBag.UUID = "example.edu.tagsample_good"
	helper.Manifest.DPNBag.Size = uint64(40960)

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
			require.NotNil(t, helper.Manifest.DPNWorkItem)

			// Make sure no errors and item is on disk.
			// Also not that these two point the same WorkSummary object.
			assert.False(t, helper.Manifest.LocalCopySummary.HasErrors())
			assert.Equal(t, "", helper.Manifest.LocalCopySummary.AllErrorsAsString())
			assert.False(t, helper.WorkSummary.HasErrors())

			// Make sure the file is on disk
			assert.True(t, helper.FileExistsAndIsComplete())

			// NSQ message should be marked finished.
			assert.Equal(t, "finish", delegate.Operation)

			// Make sure the error message was copied into the DPNWorkItem note.
			expectedNote := "Bag has been downloaded to " + helper.Manifest.LocalPath
			require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.Equal(t, expectedNote, *helper.Manifest.DPNWorkItem.Note)

			// Make sure we closed out the WorkSummary correctly.
			// helper.WorkSummary.Finished() will not be true here
			// because HandleMessage sets the start time, and we're
			// bypassing that here, going directly into the FetchChannel.
			assert.True(t, helper.WorkSummary.Finished())
			assert.True(t, helper.WorkSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StageValidate, helper.Manifest.DPNWorkItem.Stage)
			assert.Equal(t, constants.StatusPending, helper.Manifest.DPNWorkItem.Status)
			assert.True(t, helper.Manifest.DPNWorkItem.Retry)

			wg.Done()
		}
	}()

	worker.FetchChannel <- helper
	wg.Wait()
}

func TestDPNS3Retriever_HandleMessageFail(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	worker, message, delegate, _ := getDPNS3TestItems(t)

	worker.PostTestChannel = make(chan *workers.DPNRestoreHelper)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for helper := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, helper.Manifest.DPNBag)
			require.NotNil(t, helper.Manifest.DPNWorkItem)

			// Make sure no errors and item is on disk.
			// Also not that these two point the same WorkSummary object.
			assert.True(t, helper.Manifest.LocalCopySummary.HasErrors())
			assert.True(t, helper.WorkSummary.HasErrors())

			// Error is fatal because the bucket we requested does not exist.
			assert.True(t, helper.WorkSummary.ErrorIsFatal)
			assert.True(t, strings.HasPrefix(helper.WorkSummary.AllErrorsAsString(), "NoSuchBucket"))

			// NSQ message should be marked finished.
			assert.Equal(t, "finish", delegate.Operation)

			// Make sure the error message was copied into the DPNWorkItem note.
			require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.Equal(t, helper.WorkSummary.AllErrorsAsString(), *helper.Manifest.DPNWorkItem.Note)

			// Make sure we closed out the WorkSummary correctly.
			assert.True(t, helper.WorkSummary.Started())
			assert.True(t, helper.WorkSummary.Finished())
			assert.False(t, helper.WorkSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StageFetch, helper.Manifest.DPNWorkItem.Stage)
			assert.Equal(t, constants.StatusFailed, helper.Manifest.DPNWorkItem.Status)
			assert.False(t, helper.Manifest.DPNWorkItem.Retry)

			wg.Done()
		}
	}()

	worker.HandleMessage(message)
	wg.Wait()
}
