package workers

import (
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"time"
)

type APTRestoreSpotTest struct {
	Context          *context.Context
	CreatedBefore    time.Time
	NotRestoredSince time.Time
	MaxSize          int64
}

// NewAPTRestoreSpotTest creates a new restore spot test worker.
// This is meant to run as a cron job.
//
// Param maxSize tells the worker to choose bags no larger than
// maxSize for restoration. Param createdBefore means choose bags
// created before this date. Param notRestoredSince means choose
// bags that have not been restored since this date (which helps
// prevent us restoring the same bag again and again).
func NewAPTRestoreSpotTest(_context *context.Context, maxSize int64, createdBefore, notRestoredSince time.Time) *APTRestoreSpotTest {
	return &APTRestoreSpotTest{
		Context:          _context,
		CreatedBefore:    createdBefore,
		NotRestoredSince: notRestoredSince,
		MaxSize:          maxSize,
	}
}

// Run runs the spot test by choosing ONE bag from each institution
// that matches the specified criteria (smaller than maxSize, createdBefore
// the specified date and notRestoredSince the specified date).
// It creates a Restore WorkItem for each bag, and returns the WorkItems
// it created. The caller can get the WorkItem.Id and object identifier from there.
func (restoreTest *APTRestoreSpotTest) Run() []*models.WorkItem {

	return nil
}
