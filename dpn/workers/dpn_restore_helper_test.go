package workers_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/dpn/workers"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"runtime"
	"testing"
)

func getRestoreHelper(t *testing.T) *workers.DPNRestoreHelper {
	worker := getDGITestWorker(t)
	message := testutil.MakeNsqMessage("1234")
	helper, err := workers.NewDPNRestoreHelper(message, worker.Context,
		worker.LocalDPNRestClient, constants.ActionFixityCheck,
		"GlacierRestoreSummary")
	require.Nil(t, err)
	require.NotNil(t, helper)
	return helper
}

func TestNewDPNRestoreHelper(t *testing.T) {
	helper := getRestoreHelper(t)
	assert.NotNil(t, helper.Manifest.DPNBag)
	assert.NotNil(t, helper.Manifest.GlacierRestoreSummary)
	assert.False(t, helper.Manifest.GlacierRestoreSummary.Started())
	assert.False(t, helper.Manifest.GlacierRestoreSummary.Finished())
	assert.Empty(t, helper.Manifest.LocalPath)
	assert.Empty(t, helper.Manifest.RestorationURL)
	assert.Empty(t, helper.Manifest.ActualFixityValue)
	assert.NotEmpty(t, helper.Manifest.GlacierBucket)
	assert.NotEmpty(t, helper.Manifest.ExpectedFixityValue)
	assert.Equal(t, constants.ActionFixityCheck, helper.Manifest.TaskType)
	assert.True(t, helper.Manifest.RequestedFromGlacierAt.IsZero())
	assert.False(t, helper.Manifest.GlacierRequestAccepted)
	assert.True(t, helper.Manifest.EstimatedDeletionFromS3.IsZero())
	assert.False(t, helper.Manifest.IsAvailableInS3)
}

func TestRestoreHelper_SaveDPNWorkItem(t *testing.T) {
	// getDGITestItems() is defined in dpn_glacier_restore_init_test.go
	// It does a lot of setup, including pointing the worker and context
	// toward URLs for mock Pharos, DPN and NSQ services, which are also
	// set up in dpn_glacier_restore_init_test.go
	_, _, _, helper := getDGITestItems(t)
	originalNote := *helper.Manifest.DPNWorkItem.Note
	helper.SaveDPNWorkItem()
	assert.False(t, helper.Manifest.GlacierRestoreSummary.HasErrors())
	// SaveDPNWorkItem will set an error message here if it has problems.
	// Unchanged note means there were no problems.
	assert.Equal(t, originalNote, *helper.Manifest.DPNWorkItem.Note)
	// Should retry if no fatal error
	assert.True(t, helper.Manifest.DPNWorkItem.Retry)

	_, _, _, helper = getDGITestItems(t)
	helper.Manifest.GlacierRestoreSummary.ErrorIsFatal = true
	originalNote = *helper.Manifest.DPNWorkItem.Note
	helper.SaveDPNWorkItem()
	assert.False(t, helper.Manifest.GlacierRestoreSummary.HasErrors())
	assert.Equal(t, originalNote, *helper.Manifest.DPNWorkItem.Note)
	// Should NOT retry if there was a fatal error
	assert.False(t, helper.Manifest.DPNWorkItem.Retry)
}

func TestRestoreHelper_FileExistsAndIsComplete(t *testing.T) {
	helper := getRestoreHelper(t)

	// False, because there is no file path specified.
	helper.Manifest.LocalPath = ""
	assert.False(t, helper.FileExistsAndIsComplete())

	// Get some info about this file.
	_, filename, _, _ := runtime.Caller(0)
	file, err := os.Open(filename)
	require.Nil(t, err)
	defer file.Close()
	fileInfo, err := file.Stat()
	fileSize := uint64(fileInfo.Size())

	// False, because the file size does not match
	helper.Manifest.LocalPath = filename
	helper.Manifest.DPNBag.Size = fileSize * uint64(2)
	assert.False(t, helper.FileExistsAndIsComplete())

	// True, because the file exists and size matches
	helper.Manifest.DPNBag.Size = fileSize
	assert.True(t, helper.FileExistsAndIsComplete())
}
