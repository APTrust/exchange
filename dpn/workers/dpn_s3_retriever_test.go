package workers_test

import (
	//	"encoding/json"
	//	"fmt"
	"github.com/APTrust/exchange/constants"
	//	dpn_models "github.com/APTrust/exchange/dpn/models"
	//	dpn_network "github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/dpn/workers"
	//	apt_models "github.com/APTrust/exchange/models"
	//	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	//	"io/ioutil"
	//	"net/http"
	//	"net/http/httptest"
	//	"strings"
	//	"sync"
	"path/filepath"
	"testing"
	//	"time"
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

}

func TestDPNS3Retriever_FinishWithError(t *testing.T) {

}

func TestDPNS3Retriever_SendToFixityQueue(t *testing.T) {

}
