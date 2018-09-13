package workers_test

import (
	"github.com/APTrust/exchange/constants"
	dpn_testutil "github.com/APTrust/exchange/dpn/util/testutil"
	"github.com/APTrust/exchange/dpn/workers"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"path/filepath"
	"runtime"
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

func getBagPath(t *testing.T, bagname string) string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	pathToBag, err := filepath.Abs(path.Join(dir, "..", "..",
		"testdata", "unit_test_bags", "dpn", bagname))
	if err != nil {
		assert.Fail(t, "Can't figure out Abs path: %s", err.Error())
	}
	return pathToBag
}

func pathToDPNTestBag(t *testing.T) string {
	bagName := dpn_testutil.DPN_TEST_BAG_UUID + ".tar"
	return getBagPath(t, bagName)
}

func deleteBoltDBFile(t *testing.T) {
	dbFile := dpn_testutil.DPN_TEST_BAG_UUID + ".valdb"
	dbPath := getBagPath(t, dbFile)
	os.Remove(dbPath)
}

func TestNewDPNFixityChecker(t *testing.T) {
	worker := getDPNFixityTestWorker(t)
	assert.NotNil(t, worker.Context)
	assert.NotNil(t, worker.LocalDPNRestClient)
	assert.NotNil(t, worker.ValidationChannel)
	assert.NotNil(t, worker.CleanupChannel)
	assert.NotNil(t, worker.BagValidationConfig)
}

func TestDPNFixityChecker_ValidateBag(t *testing.T) {
	defer deleteBoltDBFile(t)
	worker, _, _, helper := getDPNFixityTestItems(t)
	helper.Manifest.DPNBag.UUID = dpn_testutil.DPN_TEST_BAG_UUID
	helper.Manifest.ExpectedFixityValue = dpn_testutil.DPN_TEST_BAG_FIXITY
	helper.Manifest.LocalPath = pathToDPNTestBag(t)
	worker.ValidateBag(helper)
	assert.Equal(t, dpn_testutil.DPN_TEST_BAG_FIXITY, helper.Manifest.ActualFixityValue)
	assert.False(t, helper.WorkSummary.HasErrors())

	worker, _, _, helper = getDPNFixityTestItems(t)
	helper.Manifest.DPNBag.UUID = dpn_testutil.DPN_TEST_BAG_UUID
	helper.Manifest.ExpectedFixityValue = "This fixity value won't match"
	helper.Manifest.LocalPath = pathToDPNTestBag(t)
	worker.ValidateBag(helper)
	assert.NotEqual(t, helper.Manifest.ExpectedFixityValue, helper.Manifest.ActualFixityValue)
	assert.False(t, helper.WorkSummary.HasErrors())
}

func TestDPNFixityChecker_SaveFixityRecord(t *testing.T) {

}

func TestDPNFixityChecker_FinishWithSuccess(t *testing.T) {

}

func TestDPNFixityChecker_FinishWithError(t *testing.T) {

}

func TestDPNFixityChecker_HandleMessageSuccess(t *testing.T) {

}

func TestDPNFixityChecker_HandleMessageFail(t *testing.T) {

}
