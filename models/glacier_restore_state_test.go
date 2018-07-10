package models_test

import (
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func getGlacierRestoreRequest(gfIdentifier string, accepted bool) *models.GlacierRestoreRequest {
	if gfIdentifier == "" {
		gfIdentifier = testutil.RandomFileIdentifier("test.edu/testbag")
	}
	now := time.Now()
	randomMinutes := rand.Int31n(20)
	requestedAt := now.Add(time.Duration(randomMinutes*-1) * time.Minute)
	return &models.GlacierRestoreRequest{
		GenericFileIdentifier:   gfIdentifier,
		GlacierBucket:           "bucket",
		GlacierKey:              uuid.NewV4().String(),
		RequestAccepted:         accepted,
		RequestedAt:             requestedAt,
		EstimatedDeletionFromS3: requestedAt.Add(time.Duration(5*24) * time.Hour),
		SomeoneElseRequested:    false,
	}
}

func getGlacierRestoreState() *models.GlacierRestoreState {
	nsqMessage := testutil.MakeNsqMessage("42")
	workItem := testutil.MakeWorkItem()
	return models.NewGlacierRestoreState(nsqMessage, workItem)
}

func TestNewGlacierRestoreState(t *testing.T) {
	state := getGlacierRestoreState()
	require.NotNil(t, state)
	assert.NotNil(t, state.WorkSummary)
	assert.NotNil(t, state.Requests)
}

func TestGlacierRestoreStateFindRequest(t *testing.T) {
	state := getGlacierRestoreState()
	require.NotNil(t, state)
	for i := 0; i < 10; i++ {
		identifier := fmt.Sprintf("test.edu/bag/file_%d", i)
		state.Requests = append(state.Requests, getGlacierRestoreRequest(identifier, true))
	}
	ids := []string{
		"test.edu/bag/file_4",
		"test.edu/bag/file_6",
	}
	for _, id := range ids {
		req := state.FindRequest(id)
		assert.NotNil(t, req)
		assert.Equal(t, id, req.GenericFileIdentifier)
	}
	assert.Nil(t, state.FindRequest("test.edu/bag/file_does_not_exist"))
}

func TestGlacierRestoreStateGetReport(t *testing.T) {
	firstRequestTime, _ := time.Parse(time.RFC3339, "2018-08-01T12:00:00+00:00")
	firstDeletionTime, _ := time.Parse(time.RFC3339, "2016-08-06T15:33:00+00:00")
	state := getGlacierRestoreState()
	require.NotNil(t, state)
	fileIdentifiers := make([]string, 32)
	for i := 0; i < 30; i++ {
		identifier := fmt.Sprintf("test.edu/bag/file_%d", i)
		fileIdentifiers[i] = identifier
		accepted := (i%6 != 0) // every 6th item will be accepted = false
		request := getGlacierRestoreRequest(identifier, accepted)
		request.RequestedAt = firstRequestTime.Add(time.Minute * time.Duration(i))
		request.EstimatedDeletionFromS3 = firstDeletionTime.Add(time.Minute * time.Duration(i))
		state.Requests = append(state.Requests, request)
	}
	fileIdentifiers[30] = "test.edu/bag/file_30"
	fileIdentifiers[31] = "test.edu/bag/file_31"

	report := state.GetReport(fileIdentifiers)
	require.NotNil(t, report)

	assert.Equal(t, len(fileIdentifiers), report.FilesRequired)
	assert.Equal(t, len(fileIdentifiers)-2, report.FilesRequested)
	assert.Equal(t, 2, len(report.FilesNotRequested))
	assert.Equal(t, 5, len(report.RequestsNotAccepted))

	assert.True(t, util.StringListContains(report.FilesNotRequested, "test.edu/bag/file_30"))
	assert.True(t, util.StringListContains(report.FilesNotRequested, "test.edu/bag/file_31"))

	assert.True(t, util.StringListContains(report.RequestsNotAccepted, "test.edu/bag/file_0"))
	assert.True(t, util.StringListContains(report.RequestsNotAccepted, "test.edu/bag/file_6"))
	assert.True(t, util.StringListContains(report.RequestsNotAccepted, "test.edu/bag/file_12"))
	assert.True(t, util.StringListContains(report.RequestsNotAccepted, "test.edu/bag/file_18"))
	assert.True(t, util.StringListContains(report.RequestsNotAccepted, "test.edu/bag/file_24"))

	assert.Equal(t, firstRequestTime, report.EarliestRequest)
	assert.Equal(t, firstRequestTime.Add(time.Minute*time.Duration(29)), report.LatestRequest)
	// First request was marked as not accepted in the loop above,
	// so second request will have the earliest S3 expiry time.
	assert.Equal(t, firstDeletionTime.Add(time.Minute*time.Duration(1)), report.EarliestExpiry)
	assert.Equal(t, firstDeletionTime.Add(time.Minute*time.Duration(29)), report.LatestExpiry)
}

func TestNewGlacierRequestReport(t *testing.T) {
	report := models.NewGlacierRequestReport()
	require.NotNil(t, report)
	assert.NotNil(t, report.FilesNotRequested)
	assert.Empty(t, report.FilesNotRequested)
	assert.NotNil(t, report.RequestsNotAccepted)
	assert.Empty(t, report.RequestsNotAccepted)
}

func TestAllRetrievalsInitiated(t *testing.T) {
	state := getGlacierRestoreState()
	require.NotNil(t, state)
	fileIdentifiers := make([]string, 30)
	for i := 0; i < 30; i++ {
		identifier := fmt.Sprintf("test.edu/bag/file_%d", i)
		fileIdentifiers[i] = identifier
		state.Requests = append(state.Requests, getGlacierRestoreRequest(identifier, true))
	}

	report := state.GetReport(fileIdentifiers)
	require.NotNil(t, report)
	assert.True(t, report.AllRetrievalsInitiated())

	fileIdentifiers = append(fileIdentifiers, "test.edu/bag/file_30")
	fileIdentifiers = append(fileIdentifiers, "test.edu/bag/file_31")

	report = state.GetReport(fileIdentifiers)
	require.NotNil(t, report)
	assert.False(t, report.AllRetrievalsInitiated())
}

func TestAllItemsInS3(t *testing.T) {
	state := getGlacierRestoreState()
	require.NotNil(t, state)
	fileIdentifiers := make([]string, 30)
	for i := 0; i < 30; i++ {
		identifier := fmt.Sprintf("test.edu/bag/file_%d", i)
		fileIdentifiers[i] = identifier
		req := getGlacierRestoreRequest(identifier, true)
		req.IsAvailableInS3 = true
		state.Requests = append(state.Requests, req)
	}

	report := state.GetReport(fileIdentifiers)
	require.NotNil(t, report)
	assert.True(t, report.AllItemsInS3())
	assert.Empty(t, report.FilesNotRequested)
	assert.Empty(t, report.RequestsNotAccepted)
	assert.Empty(t, report.FilesNotYetInS3)

	fileIdentifiers = append(fileIdentifiers, "test.edu/bag/file_30")
	fileIdentifiers = append(fileIdentifiers, "test.edu/bag/file_31")

	report = state.GetReport(fileIdentifiers)
	require.NotNil(t, report)
	assert.False(t, report.AllItemsInS3())

	req := getGlacierRestoreRequest("ned/flanders", true)
	req.IsAvailableInS3 = false
	state.Requests = append(state.Requests, req)

	req = getGlacierRestoreRequest("maude/flanders", true)
	req.IsAvailableInS3 = false
	state.Requests = append(state.Requests, req)

	report = state.GetReport(fileIdentifiers)
	require.NotNil(t, report)
	assert.False(t, report.AllItemsInS3())
	assert.Equal(t, 2, len(report.FilesNotYetInS3))
}

func TestGetFileIdentifiers(t *testing.T) {
	state := getGlacierRestoreState()
	require.NotNil(t, state)

	assert.Empty(t, state.GetFileIdentifiers())

	state.GenericFile = testutil.MakeGenericFile(0, 0, "test.edu/bag")
	gfIdentifiers := state.GetFileIdentifiers()
	assert.Equal(t, 1, len(gfIdentifiers))
	assert.Equal(t, state.GenericFile.Identifier, gfIdentifiers[0])

	state = getGlacierRestoreState()
	require.NotNil(t, state)

	state.IntellectualObject = testutil.MakeIntellectualObject(20, 0, 0, 0)
	gfIdentifiers = state.GetFileIdentifiers()
	assert.Equal(t, 20, len(gfIdentifiers))
}
