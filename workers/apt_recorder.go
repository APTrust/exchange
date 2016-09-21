package workers

import (
//	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
//	"github.com/APTrust/exchange/network"
//	"github.com/APTrust/exchange/util"
//	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nsqio/go-nsq"
//	"io"
//	"net/url"
//	"os"
//	"strings"
	"sync"
//	"time"
)

// Records ingest data (objects, files and events) in Pharos
type APTRecorder struct {
	Context             *context.Context
	RecordChannel       chan *models.IngestState
	CleanupChannel      chan *models.IngestState
	WaitGroup           sync.WaitGroup
}

func NewAPTRecorder(_context *context.Context) (*APTRecorder) {
	recorder := &APTRecorder{
		Context: _context,
	}
	// Set up buffered channels
	workerBufferSize := _context.Config.RecordWorker.Workers * 10
	recorder.RecordChannel = make(chan *models.IngestState, workerBufferSize)
	recorder.CleanupChannel = make(chan *models.IngestState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.RecordWorker.Workers; i++ {
		go recorder.record()
		go recorder.cleanup()
	}
	return recorder
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (recorder *APTRecorder) HandleMessage(message *nsq.Message) (error) {
	ingestState, err := GetIngestState(message, recorder.Context, false)
	if err != nil {
		recorder.Context.MessageLog.Error(err.Error())
		return err
	}

	// If this item was queued more than once, and this process or any
	// other is currently working on it, just finish the message and
	// assume that the in-progress worker will take care of the original.
	if ingestState.WorkItem.Node != "" && ingestState.WorkItem.Pid != 0 {
		recorder.Context.MessageLog.Info("Marking WorkItem %d (%s/%s) as finished " +
			"without doing any work, because this item is currently in process by " +
			"node %s, pid %s. WorkItem was last updated at %s.",
			ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
			ingestState.WorkItem.Name, ingestState.WorkItem.Node,
			ingestState.WorkItem.Pid, ingestState.WorkItem.UpdatedAt)
		message.Finish()
		return nil
	}

	// Disable auto response, so we can tell NSQ when we need to
	// that we're still working on this item.
	message.DisableAutoResponse()

	// Clear out any old errors, because we're going to retry
	// whatever may have failed on the last run.
	ingestState.IngestManifest.RecordResult.ClearErrors()

	// Tell Pharos that we've started to record this item.
	err = MarkWorkItemStarted(ingestState, recorder.Context,
		constants.StageRecord, "Recording object, file and event metadata in Pharos.")
	if err != nil {
		recorder.Context.MessageLog.Error(err.Error())
		return err
	}

	recorder.Context.MessageLog.Info("Putting %s/%s into record channel",
		ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key)

	recorder.RecordChannel <- ingestState

	// Return no error, so NSQ knows we're OK.
	return nil
}

// Step 1: Record data in Pharos
func (recorder *APTRecorder) record () {
	for ingestState := range recorder.RecordChannel {
		// Create all events locally
		// Save IntellectualObject
		// Save GenericFiles (batch)
		// Save PremisEvents (batch)
		// Change WorkItem state as cleanup/pending
		// Save WorkItemState
		recorder.CleanupChannel <- ingestState
	}
}

// Step 2: Delete tar file from staging area and from receiving bucket.
func (recorder *APTRecorder) cleanup () {
//	for ingestState := range recorder.CleanupChannel {
		// Delete local tar file (and untarred files)
		// Delete tar file from receiving bucket
		// Tell Pharos cleanup is complete (WorkItem complete)
		// Save WorkItemState with full CleanupResult
//	}
}

func (recorder *APTRecorder) saveAllPharosData (ingestState *models.IngestState) {

}

func (recorder *APTRecorder) saveIntellectualObject (ingestState *models.IngestState) {

}

func (recorder *APTRecorder) saveGenericFiles (ingestState *models.IngestState) {

}

func (recorder *APTRecorder) savePremisEvents (ingestState *models.IngestState) {

}

func (recorder *APTRecorder) deleteBagFromStaging (ingestState *models.IngestState) {

}

func (recorder *APTRecorder) deleteBagFromReceivingBucket (ingestState *models.IngestState) {

}
