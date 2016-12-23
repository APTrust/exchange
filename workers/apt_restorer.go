package workers

import (
	"fmt"
	//	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	//	"github.com/APTrust/exchange/network"
	//	"github.com/APTrust/exchange/util"
	"github.com/nsqio/go-nsq"
	//	"sync"
	//	"time"
)

// APTRestorer restores bags by reassmbling their contents and
// pushing them into the depositor's restoration bucket.
type APTRestorer struct {
	// Context contains basic information required to run,
	// connect to Pharos, S3, etc.
	Context *context.Context
	// PackageChannel is for the go routines that reassemble
	// the S3 files into a new bag.
	PackageChannel chan *models.RestoreState
	// CopyChannel is for the goroutines that copy the newly
	// packaged bag to the depositor's restoration bucket in S3.
	CopyChannel chan *models.RestoreState
	// PostProcess channel is for the goroutines that record
	// the outcome of the restoration in Pharos and NSQ, and
	// do any other required cleanup.
	PostProcessChannel chan *models.RestoreState
}

func NewAPTRestorer(_context *context.Context) *APTRestorer {
	restorer := &APTRestorer{
		Context: _context,
	}
	// Set up buffered channels
	workerBufferSize := _context.Config.RestoreWorker.Workers * 10
	restorer.PackageChannel = make(chan *models.RestoreState, workerBufferSize)
	restorer.CopyChannel = make(chan *models.RestoreState, workerBufferSize)
	restorer.PostProcessChannel = make(chan *models.RestoreState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.RestoreWorker.Workers; i++ {
		go restorer.buildBag()
		go restorer.copy()
		go restorer.postProcess()
	}
	return restorer
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (restorer *APTRestorer) HandleMessage(message *nsq.Message) error {
	// Build the RestoreState object by fetching WorkItem and IntellectualObject
	// from Pharos.
	restoreState := &models.RestoreState{}

	// If this item was queued more than once, and this process or any
	// other is currently working on it, just finish the message and
	// assume that the in-progress worker will take care of the original.
	if restoreState.WorkItem.Node != "" && restoreState.WorkItem.Pid != 0 {
		restorer.Context.MessageLog.Info("Marking WorkItem %d (%s/%s) as finished "+
			"without doing any work, because this item is currently in process by "+
			"node %s, pid %s. WorkItem was last updated at %s.",
			restoreState.WorkItem.Id, restoreState.WorkItem.Bucket,
			restoreState.WorkItem.Name, restoreState.WorkItem.Node,
			restoreState.WorkItem.Pid, restoreState.WorkItem.UpdatedAt)
		message.Finish()
		return nil
	}

	// Disable auto response, so we can tell NSQ when we need to
	// that we're still working on this item.
	message.DisableAutoResponse()

	// Clear out any old errors, because we're going to retry
	// whatever may have failed on the last run.
	restoreState.RestoreSummary.ClearErrors()

	// Figure out where we should start.
	// If packaging is incomplete, start there.
	// If packaging is complete, but copy is not, start there.
	// If copy is complete, but recording is not, start there.

	// Tell Pharos that we're building the bag: constants.StagePackage, constants.StatusStarted

	//restorer.Context.MessageLog.Info("Putting %s/%s into record channel",
	//	ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key)

	//restorer.RecordChannel <- ingestState

	// Return no error, so NSQ knows we're OK.
	return nil
}

func (restorer *APTRestorer) buildBag() {
	for restoreState := range restorer.PackageChannel {
		// Assemble all files, tar, and validate.
		// Touch NSQ often.
		fmt.Println(restoreState)
	}
}

func (restorer *APTRestorer) copy() {
	for restoreState := range restorer.CopyChannel {
		// Copy bag to S3
		fmt.Println(restoreState)
	}
}

func (restorer *APTRestorer) postProcess() {
	for restoreState := range restorer.PostProcessChannel {
		// Mark item completed in Pharos and finish NSQ.
		fmt.Println(restoreState)
	}
}
