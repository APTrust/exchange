package workers

import (
//	"fmt"
	"github.com/APTrust/exchange/constants"
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
	for i := 0; i < _context.Config.StoreWorker.Workers; i++ {
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

// -------------------------------------------------------------------------
// Step 1 of 3: Put the item in long-term storage
//
// -------------------------------------------------------------------------
func (storer *APTStorer) store () {
	for ingestState := range storer.StorageChannel {
		existingObj, err := storer.getExistingObject(ingestState.IngestManifest.Object.Identifier)
		if err != nil {
			ingestState.IngestManifest.StoreResult.AddError(err.Error())
		} else {
			for _, gf := range ingestState.IngestManifest.Object.GenericFiles {
				if existingObj != nil {
					existingFile := existingObj.FindGenericFile(gf.OriginalPath())
					if existingFile != nil {
						existingSha256 := existingFile.GetChecksum(constants.AlgSha256)
						if existingSha256.Digest == gf.IngestSha256 {
							gf.IngestNeedsSave = false
						}
					}
				}
				if gf.IngestNeedsSave {
					if gf.IngestStoredAt.IsZero() {
						storer.copyToPrimaryStorage(ingestState, gf)
					}
					if gf.IngestReplicatedAt.IsZero() {
						storer.copyToSecondaryStorage(ingestState, gf)
					}
				}
			}
		}
		storer.CleanupChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 2 of 3: Delete the bag file(s) if storage succeeded
//
// -------------------------------------------------------------------------
func (storer *APTStorer) cleanup () {
//	for ingestState := range storer.CleanupChannel {

//	}
}

// -------------------------------------------------------------------------
// Step 3 of 3: Record IntellectualObject and GenericFile data in Pharos
//
// -------------------------------------------------------------------------
func (storer *APTStorer) record () {
//	for ingestState := range storer.RecordChannel {

//	}
}

func (storer *APTStorer) loadIngestState (message *nsq.Message) (*models.IngestState, error) {
	// Load WorkItem and WorkItemState
	// Get IngestManifest from WorkItemState
	return nil, nil
}

func (storer *APTStorer) getExistingObject (objectIdentifier string) (*models.IntellectualObject, error) {
	// Get the IntellectualObject from Pharos
	// If it exists, get the GenericFiles and add them in
	return nil, nil
}

// Copy the GenericFile to primary storage (S3)
func (storer *APTStorer) copyToPrimaryStorage (ingestState *models.IngestState, gf *models.GenericFile) {

}

// Copy the GenericFile to secondary storage (Glacier)
func (storer *APTStorer) copyToSecondaryStorage (ingestState *models.IngestState, gf *models.GenericFile) {

}

// Tell Pharos we've started copying this item's files to
// long-term storage.
func (storer *APTStorer) markWorkItemStarted (ingestState *models.IngestState) (*models.WorkItem, error) {
	return nil, nil
}

// Tell Pharos we finished copying all files to long-term
// storage (successfully).
func (storer *APTStorer) markWorkItemSucceeded (ingestState *models.IngestState) (*models.WorkItem, error) {
	return nil, nil
}

// Tell Pharos that the storage attempt failed with a
// fatal error, and should not be retried.
func (storer *APTStorer) markWorkItemFailed (ingestState *models.IngestState) (*models.WorkItem, error) {
	return nil, nil
}

// Tell Pharos that storage failed with a transient
// error, and we should try this item again later.
func (storer *APTStorer) markWorkItemRequeued (ingestState *models.IngestState) (*models.WorkItem, error) {
	return nil, nil
}
