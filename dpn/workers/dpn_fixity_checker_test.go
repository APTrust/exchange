package workers_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/dpn/workers"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	//	"path/filepath"
	//	"strings"
	//	"sync"
	"testing"
	//	"time"
)

func getDPNFixityTestWorker(t *testing.T) *workers.DPNFixityChecker {
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err)

	worker, err := workers.NewDPNFixityChecker(_context)
	require.Nil(t, err)
	require.NotNil(t, worker)

	// Tell the worker to talk to our S3 test server and Pharos
	// test server, defined below
	worker.Context.PharosClient = getPharosClientForTest(pharosTestServer.URL)
	worker.LocalDPNRestClient = getDPNClientForTest(dpnTestServer.URL)
	worker.Context.NSQClient.URL = nsqServer.URL

	return worker
}

func getDPNFixityTestItems(t *testing.T) (*workers.DPNFixityChecker, *nsq.Message, *testutil.NSQTestDelegate, *workers.DPNRestoreHelper) {
	worker := getDPNFixityTestWorker(t)
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

func TestNewDPNFixityChecker(t *testing.T) {
	worker := getDPNFixityTestWorker(t)
	assert.NotNil(t, worker.Context)
	assert.NotNil(t, worker.LocalDPNRestClient)
	assert.NotNil(t, worker.ValidationChannel)
	assert.NotNil(t, worker.CleanupChannel)
	assert.NotNil(t, worker.BagValidationConfig)
}
