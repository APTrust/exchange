package workers_test

import (
	// "encoding/json"
	// "fmt"
	"github.com/APTrust/exchange/constants"
	// dpn_models "github.com/APTrust/exchange/dpn/models"
	// dpn_network "github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/dpn/workers"
	// apt_models "github.com/APTrust/exchange/models"
	// "github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	// "github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	// "io/ioutil"
	// "net/http"
	// "net/http/httptest"
	// "strings"
	// "sync"
	"testing"
	// "time"
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
	assert.Empty(t, helper.Manifest.S3Bucket)
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
