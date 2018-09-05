package workers_test

import (
	//	"encoding/json"
	//	"fmt"
	"github.com/APTrust/exchange/constants"
	dpn_network "github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/dpn/workers"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	//	"github.com/nsqio/go-nsq"
	//	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	//	"io/ioutil"
	"net/http"
	"net/http/httptest"
	//	"os"
	//	"regexp"
	//	"strconv"
	//	"strings"
	//	"sync"
	"testing"
	"time"
)

// Test server to mock Pharos, S3, and DPN requests
var pharosTestServer = httptest.NewServer(http.HandlerFunc(pharosHandler))
var s3TestServer = httptest.NewServer(http.HandlerFunc(s3Handler))
var dpnTestServer = httptest.NewServer(http.HandlerFunc(dpnHandler))

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

	return worker
}

func TestDGIInit(t *testing.T) {

}

func TestDGIHandleMessage(t *testing.T) {

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

func pharosHandler(w http.ResponseWriter, r *http.Request) {
	// Must handle DPNWorkItem GET and PUT.
}

func s3Handler(w http.ResponseWriter, r *http.Request) {
	// Must handle Glacier restore requests, returning a number of
	// different responses.
}

func dpnHandler(w http.ResponseWriter, r *http.Request) {
	// Must handle Bag GET request.
}
