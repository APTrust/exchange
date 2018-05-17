package models_test

import (
	"fmt"
	"github.com/APTrust/exchange/models"
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
}

func TestNewGlacierRequestReport(t *testing.T) {

}

func TestAllRetrievalsInitiated(t *testing.T) {

}
