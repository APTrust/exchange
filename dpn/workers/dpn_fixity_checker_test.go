package workers_test

import (
	"github.com/APTrust/exchange/constants"
	dpn_testutil "github.com/APTrust/exchange/dpn/util/testutil"
	"github.com/APTrust/exchange/dpn/workers"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
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
		"ValidationSummary")
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

func pathToInvalidDPNTestBag(t *testing.T) string {
	bagName := dpn_testutil.INVALID_DPN_TEST_BAG_UUID + ".tar"
	return getBagPath(t, bagName)
}

func deleteBoltDBFile(t *testing.T) {
	dbPath1 := getBagPath(t, dpn_testutil.DPN_TEST_BAG_UUID+".valdb")
	dbPath2 := getBagPath(t, dpn_testutil.INVALID_DPN_TEST_BAG_UUID+".valdb")
	os.Remove(dbPath1)
	os.Remove(dbPath2)
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
	helper.Manifest.ActualFixityValue = ""
	helper.Manifest.LocalPath = pathToDPNTestBag(t)
	worker.ValidateBag(helper)
	assert.NotEmpty(t, helper.Manifest.ActualFixityValue)
	assert.Equal(t, dpn_testutil.DPN_TEST_BAG_FIXITY, helper.Manifest.ActualFixityValue)
	assert.False(t, helper.WorkSummary.HasErrors())

	worker, _, _, helper = getDPNFixityTestItems(t)
	helper.Manifest.DPNBag.UUID = dpn_testutil.DPN_TEST_BAG_UUID
	helper.Manifest.ExpectedFixityValue = "This fixity value won't match"
	helper.Manifest.ActualFixityValue = ""
	helper.Manifest.LocalPath = pathToDPNTestBag(t)
	worker.ValidateBag(helper)
	assert.NotEmpty(t, helper.Manifest.ActualFixityValue)
	assert.NotEqual(t, helper.Manifest.ExpectedFixityValue, helper.Manifest.ActualFixityValue)
	assert.False(t, helper.WorkSummary.HasErrors())
}

func TestDPNFixityChecker_SaveFixityRecord(t *testing.T) {
	// Can't save because there's no ExpectedFixityValue
	worker, _, _, helper := getDPNFixityTestItems(t)
	helper.Manifest.ExpectedFixityValue = ""
	helper.Manifest.ActualFixityValue = ""
	worker.SaveFixityRecord(helper)
	require.True(t, helper.WorkSummary.HasErrors())
	assert.Equal(t, "Cannot create DPN FixityCheck record because because ExpectedFixityValue is missing from manifest.", helper.WorkSummary.FirstError())
	assert.Nil(t, helper.Manifest.FixityCheck)
	assert.True(t, helper.Manifest.FixityCheckSavedAt.IsZero())

	// Can't save because there's no ActualFixityValue
	worker, _, _, helper = getDPNFixityTestItems(t)
	helper.Manifest.ExpectedFixityValue = dpn_testutil.DPN_TEST_BAG_FIXITY
	helper.Manifest.ActualFixityValue = ""
	worker.SaveFixityRecord(helper)
	require.True(t, helper.WorkSummary.HasErrors())
	assert.Equal(t, "Cannot create DPN FixityCheck record because because ActualFixityValue is missing from manifest.", helper.WorkSummary.FirstError())
	assert.Nil(t, helper.Manifest.FixityCheck)
	assert.True(t, helper.Manifest.FixityCheckSavedAt.IsZero())

	// Record saved with matching fixity
	worker, _, _, helper = getDPNFixityTestItems(t)
	helper.Manifest.DPNBag.UUID = dpn_testutil.DPN_TEST_BAG_UUID
	helper.Manifest.ExpectedFixityValue = dpn_testutil.DPN_TEST_BAG_FIXITY
	helper.Manifest.ActualFixityValue = dpn_testutil.DPN_TEST_BAG_FIXITY
	worker.SaveFixityRecord(helper)
	require.False(t, helper.WorkSummary.HasErrors())
	assert.False(t, helper.Manifest.FixityCheckSavedAt.IsZero())
	require.NotNil(t, helper.Manifest.FixityCheck)
	assert.True(t, util.LooksLikeUUID(helper.Manifest.FixityCheck.FixityCheckId))
	assert.Equal(t, dpn_testutil.DPN_TEST_BAG_UUID, helper.Manifest.FixityCheck.Bag)
	assert.Equal(t, worker.Context.Config.DPN.LocalNode, helper.Manifest.FixityCheck.Node)
	assert.True(t, helper.Manifest.FixityCheck.Success)
	assert.False(t, helper.Manifest.FixityCheck.FixityAt.IsZero())
	assert.False(t, helper.Manifest.FixityCheck.CreatedAt.IsZero())

	// Record saved with mismatched fixity
	worker, _, _, helper = getDPNFixityTestItems(t)
	helper.Manifest.DPNBag.UUID = dpn_testutil.DPN_TEST_BAG_UUID
	helper.Manifest.ExpectedFixityValue = dpn_testutil.DPN_TEST_BAG_FIXITY
	helper.Manifest.ActualFixityValue = "Blah blah blah"
	worker.SaveFixityRecord(helper)
	require.True(t, helper.WorkSummary.HasErrors())
	assert.True(t, strings.Contains(helper.WorkSummary.FirstError(), "does not match expected fixity"))
	assert.False(t, helper.Manifest.FixityCheckSavedAt.IsZero())
	require.NotNil(t, helper.Manifest.FixityCheck)
	assert.True(t, util.LooksLikeUUID(helper.Manifest.FixityCheck.FixityCheckId))
	assert.Equal(t, dpn_testutil.DPN_TEST_BAG_UUID, helper.Manifest.FixityCheck.Bag)
	assert.Equal(t, worker.Context.Config.DPN.LocalNode, helper.Manifest.FixityCheck.Node)
	assert.False(t, helper.Manifest.FixityCheck.Success)
	assert.False(t, helper.Manifest.FixityCheck.FixityAt.IsZero())
	assert.False(t, helper.Manifest.FixityCheck.CreatedAt.IsZero())
}

// Implicitly tests FinishWithSuccess
func TestDPNFixityChecker_HandleMessageSuccess(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	worker, message, delegate, _ := getDPNFixityTestItems(t)
	worker.PreTestChannel = make(chan *workers.DPNRestoreHelper)
	worker.PostTestChannel = make(chan *workers.DPNRestoreHelper)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for helper := range worker.PreTestChannel {
			// In PreTestChannel, set path to a bag we can actually
			// validate, then push into the normal validation channel.
			helper.Manifest.LocalPath = pathToDPNTestBag(t)
			worker.ValidationChannel <- helper
		}
	}()
	go func() {
		for helper := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, helper.Manifest.DPNBag)
			assert.NotEmpty(t, helper.Manifest.DPNWorkItem)
			assert.NotEmpty(t, helper.Manifest.ExpectedFixityValue)
			assert.NotEmpty(t, helper.Manifest.ActualFixityValue)
			assert.Equal(t, helper.Manifest.ExpectedFixityValue, helper.Manifest.ActualFixityValue)

			assert.NotNil(t, helper.Manifest.FixityCheck)
			assert.False(t, helper.Manifest.FixityCheckSavedAt.IsZero())

			// Make sure we requeued to recheck progress later.
			assert.Equal(t, "finish", delegate.Operation)

			// Make sure the error message was copied into the DPNWorkItem note.
			require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.True(t, strings.HasPrefix(*helper.Manifest.DPNWorkItem.Note,
				"Fixity check complete. Saved to DPN"))

			// Make sure we closed out the WorkSummary correctly.
			assert.True(t, helper.Manifest.ValidationSummary.Started())
			assert.True(t, helper.Manifest.ValidationSummary.Finished())
			assert.True(t, helper.Manifest.ValidationSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StatusSuccess, helper.Manifest.DPNWorkItem.Status)
			require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.True(t, strings.HasPrefix(*helper.Manifest.DPNWorkItem.Note,
				"Fixity check complete. Saved to DPN with FixityCheckId"))
			assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
			assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
			assert.True(t, helper.Manifest.DPNWorkItem.Retry)

			wg.Done()
		}
	}()
	worker.HandleMessage(message)
	wg.Wait()
}

// Implicitly tests FinishWithError
func TestDPNFixityChecker_HandleMessageFailNoBag(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	worker, message, delegate, _ := getDPNFixityTestItems(t)
	worker.PreTestChannel = make(chan *workers.DPNRestoreHelper)
	worker.PostTestChannel = make(chan *workers.DPNRestoreHelper)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for helper := range worker.PreTestChannel {
			// In PreTestChannel, set path to a bag we can actually
			// validate, then push into the normal validation channel.
			helper.Manifest.LocalPath = pathToDPNTestBag(t) + "_does_not_exist"
			worker.ValidationChannel <- helper
		}
	}()
	go func() {
		for helper := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, helper.Manifest.DPNBag)
			assert.NotEmpty(t, helper.Manifest.DPNWorkItem)
			assert.NotEmpty(t, helper.Manifest.ExpectedFixityValue)
			assert.Empty(t, helper.Manifest.ActualFixityValue)

			assert.Nil(t, helper.Manifest.FixityCheck)

			// Make sure we requeued to recheck progress later.
			assert.Equal(t, "finish", delegate.Operation)

			// Make sure the error message was copied into the DPNWorkItem note.
			assert.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.True(t, strings.HasPrefix(*helper.Manifest.DPNWorkItem.Note, "Bag does not exist at"))

			// Make sure we closed out the WorkSummary correctly.
			assert.True(t, helper.Manifest.ValidationSummary.Started())
			assert.True(t, helper.Manifest.ValidationSummary.Finished())
			assert.False(t, helper.Manifest.ValidationSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StatusFailed, helper.Manifest.DPNWorkItem.Status)
			require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
			assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
			assert.False(t, helper.Manifest.DPNWorkItem.Retry)

			wg.Done()
		}
	}()
	worker.HandleMessage(message)
	wg.Wait()
}

// Implicitly tests FinishWithError
func TestDPNFixityChecker_HandleMessageFailInvalidBag(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	worker, message, delegate, _ := getDPNFixityTestItems(t)
	worker.PreTestChannel = make(chan *workers.DPNRestoreHelper)
	worker.PostTestChannel = make(chan *workers.DPNRestoreHelper)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for helper := range worker.PreTestChannel {
			// In PreTestChannel, set path to a bag we can actually
			// validate, then push into the normal validation channel.
			helper.Manifest.LocalPath = pathToInvalidDPNTestBag(t)
			worker.ValidationChannel <- helper
		}
	}()
	go func() {
		for helper := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, helper.Manifest.DPNBag)
			assert.NotEmpty(t, helper.Manifest.DPNWorkItem)
			assert.NotEmpty(t, helper.Manifest.ExpectedFixityValue)
			assert.Empty(t, helper.Manifest.ActualFixityValue)

			assert.Nil(t, helper.Manifest.FixityCheck)

			// Make sure we requeued to recheck progress later.
			assert.Equal(t, "finish", delegate.Operation)

			// Make sure the error message was copied into the DPNWorkItem note.
			assert.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.True(t, strings.Contains(*helper.Manifest.DPNWorkItem.Note,
				"/atom/gen.go' in manifest 'manifest-sha256.txt' is missing"))
			assert.True(t, strings.Contains(*helper.Manifest.DPNWorkItem.Note,
				"Bad sha256 digest for 'dpn-tags/dpn-info.txt'"))

			// Make sure we closed out the WorkSummary correctly.
			assert.True(t, helper.Manifest.ValidationSummary.Started())
			assert.True(t, helper.Manifest.ValidationSummary.Finished())
			assert.False(t, helper.Manifest.ValidationSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StatusFailed, helper.Manifest.DPNWorkItem.Status)
			require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
			assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
			assert.False(t, helper.Manifest.DPNWorkItem.Retry)

			wg.Done()
		}
	}()
	worker.HandleMessage(message)
	wg.Wait()
}

// Implicitly tests FinishWithError
func TestDPNFixityChecker_HandleMessageFailChecksumMismatch(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	worker, message, delegate, _ := getDPNFixityTestItems(t)
	worker.PreTestChannel = make(chan *workers.DPNRestoreHelper)
	worker.PostTestChannel = make(chan *workers.DPNRestoreHelper)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for helper := range worker.PreTestChannel {
			// In PreTestChannel, set path to a bag we can actually
			// validate, then push into the normal validation channel.
			helper.Manifest.LocalPath = pathToDPNTestBag(t)
			// Set a bad fixity value. The bag will pass validation,
			// but the tagmanifest fixity won't match.
			helper.Manifest.ExpectedFixityValue = "1234567890"
			worker.ValidationChannel <- helper
		}
	}()
	go func() {
		for helper := range worker.PostTestChannel {
			// Check the basics
			assert.NotNil(t, helper.Manifest.DPNBag)
			assert.NotEmpty(t, helper.Manifest.DPNWorkItem)
			assert.Equal(t, "1234567890", helper.Manifest.ExpectedFixityValue)
			assert.Equal(t, dpn_testutil.DPN_TEST_BAG_FIXITY, helper.Manifest.ActualFixityValue)
			assert.NotEqual(t, helper.Manifest.ExpectedFixityValue, helper.Manifest.ActualFixityValue)

			assert.NotNil(t, helper.Manifest.FixityCheck)
			assert.False(t, helper.Manifest.FixityCheckSavedAt.IsZero())

			// Make sure we requeued to recheck progress later.
			assert.Equal(t, "finish", delegate.Operation)

			// Make sure the error message was copied into the DPNWorkItem note.
			assert.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.True(t, strings.Contains(*helper.Manifest.DPNWorkItem.Note, "does not match expected fixity"))

			// Make sure we closed out the WorkSummary correctly.
			assert.True(t, helper.Manifest.ValidationSummary.Started())
			assert.True(t, helper.Manifest.ValidationSummary.Finished())
			assert.False(t, helper.Manifest.ValidationSummary.Succeeded())

			// Make sure we updated the DPNWorkItem appropriately
			assert.Equal(t, constants.StatusFailed, helper.Manifest.DPNWorkItem.Status)
			require.NotNil(t, helper.Manifest.DPNWorkItem.Note)
			assert.Nil(t, helper.Manifest.DPNWorkItem.ProcessingNode)
			assert.Equal(t, 0, helper.Manifest.DPNWorkItem.Pid)
			assert.False(t, helper.Manifest.DPNWorkItem.Retry)

			wg.Done()
		}
	}()
	worker.HandleMessage(message)
	wg.Wait()
}
