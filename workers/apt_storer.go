package workers

import (
//	"fmt"
//	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
//	"github.com/APTrust/exchange/network"
//	"github.com/APTrust/exchange/util"
//	"github.com/APTrust/exchange/util/fileutil"
//	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
//	"net/http"
//	"os"
//	"path/filepath"
//	"strconv"
//	"strings"
	"sync"
//	"time"
)

// Stores GenericFiles in long-term storage (S3 and Glacier).
type APTStorer struct {
	Context             *context.Context
	StorageChannel      chan *models.IngestState
	CleanupChannel      chan *models.IngestState
	RecordChannel       chan *models.IngestState
	WaitGroup           sync.WaitGroup
}

func NewAPTStorer(_context *context.Context) (*APTStorer) {
	storer := &APTStorer{
		Context: _context,
	}

	// Set up buffered channels
	workerBufferSize := _context.Config.FetchWorker.Workers * 10
	storer.StorageChannel = make(chan *models.IngestState, workerBufferSize)
	storer.CleanupChannel = make(chan *models.IngestState, workerBufferSize)
	storer.RecordChannel = make(chan *models.IngestState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.FetchWorker.Workers; i++ {
		go storer.store()
		go storer.cleanup()
		go storer.record()
	}
	return storer
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (storer *APTStorer) HandleMessage(message *nsq.Message) (error) {

	// ---------------------------------------------------------
	// TODO: Make sure no other worker is working on this item.
	// ---------------------------------------------------------

	ingestState, err := storer.loadIngestState(message)
	if err != nil {
		storer.Context.MessageLog.Error(err.Error())
		return err
	}

	// Tell Pharos that we've started to store this item.
	ingestState.WorkItem, err = storer.markWorkItemStarted(ingestState)
	if err != nil {
		storer.Context.MessageLog.Error(err.Error())
		return err
	}

	// Disable auto response, so we can tell NSQ when we need to
	// that we're still working on this item.
	message.DisableAutoResponse()

	// Clear out any old errors, because we're going to retry
	// whatever may have failed on the last run.
	ingestState.IngestManifest.StoreResult.ClearErrors()

	storer.Context.MessageLog.Info("Putting %s/%s into record queue",
		ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key)

	storer.RecordChannel <- ingestState

	// Return no error, so NSQ knows we're OK.
	return nil
}

func (storer *APTStorer) store () {

}

func (storer *APTStorer) cleanup () {

}

func (storer *APTStorer) record () {

}

func (storer *APTStorer) loadIngestState (message *nsq.Message) (*models.IngestState, error) {
	return nil, nil
}

func (storer *APTStorer) markWorkItemStarted (ingestState *models.IngestState) (*models.WorkItem, error) {
	return nil, nil
}

func (storer *APTStorer) markWorkItemSucceeded (ingestState *models.IngestState) (*models.WorkItem, error) {
	return nil, nil
}

func (storer *APTStorer) markWorkItemFailed (ingestState *models.IngestState) (*models.WorkItem, error) {
	return nil, nil
}

func (storer *APTStorer) markWorkItemRequeued (ingestState *models.IngestState) (*models.WorkItem, error) {
	return nil, nil
}
