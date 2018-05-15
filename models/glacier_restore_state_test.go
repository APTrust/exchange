package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/satori/go.uuid"
	//"github.com/stretchr/testify/assert"
	//"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func getRestoreRequest(gfIdentifier string, accepted bool) *models.GlacierRestoreRequest {
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

func TestNewGlacierRestoreState(t *testing.T) {

}

func TestGlacierRestoreStateFindRequest(t *testing.T) {

}

func TestGlacierRestoreStateGetReport(t *testing.T) {

}

func TestNewGlacierRequestReport(t *testing.T) {

}

func TestAllRetrievalsInitiated(t *testing.T) {

}
