package workers_test

import (
	// "encoding/json"
	// "fmt"
	"github.com/APTrust/exchange/constants"
	// dpn_models "github.com/APTrust/exchange/dpn/models"
	// dpn_network "github.com/APTrust/exchange/dpn/network"
	//"github.com/APTrust/exchange/dpn/workers"
	// apt_models "github.com/APTrust/exchange/models"
	// "github.com/APTrust/exchange/network"
	// "github.com/APTrust/exchange/util/testutil"
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

func TestGetRetrievalManifest(t *testing.T) {
	// getDGITestItems(t) defined in dpn_glacier_restore_init_test.go
	_, _, _, helper := getDGITestItems(t)
	require.NotNil(t, helper)
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
