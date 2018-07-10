package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	//	"github.com/APTrust/exchange/network"
	"github.com/nsqio/go-nsq"
	// "os"
	"time"
)

// APTFileRestorer restores files from S3 and Glacier long-term storage.
type APTFileRestorer struct {
	// Context contains basic information required to run,
	// connect to Pharos, S3, etc.
	Context *context.Context
	// RestoreChannel is for the go routines that restore GenericFiles
	// from primary and secondary storage.
	RestoreChannel chan *models.FileRestoreState
	// PostProcess channel is for the goroutines that record
	// the outcome of the deletion in Pharos and finish or
	// requeue the NSQ message.
	PostProcessChannel chan *models.FileRestoreState
}

func NewAPTFileRestorer(_context *context.Context) *APTFileRestorer {
	restorer := &APTFileRestorer{
		Context: _context,
	}

	// Set up buffered channels
	workerBufferSize := _context.Config.FileRestoreWorker.Workers * 10
	restorer.RestoreChannel = make(chan *models.FileRestoreState, workerBufferSize)
	restorer.PostProcessChannel = make(chan *models.FileRestoreState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.RestoreWorker.Workers; i++ {
		go restorer.restore()
		go restorer.postProcess()
	}
	return restorer
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (restorer *APTFileRestorer) HandleMessage(message *nsq.Message) error {
	// Build the FileRestoreState object by fetching WorkItem and IntellectualObject
	// from Pharos.
	restoreState, err := restorer.buildState(message)
	if err != nil {
		restorer.Context.MessageLog.Error(err.Error())
		return err
	}

	restoreState.RestoreSummary.ClearErrors()
	restoreState.WorkItem.Note = "Starting file restore process"
	restoreState.WorkItem.SetNodeAndPid()
	restoreState.WorkItem.Status = constants.StatusStarted
	restorer.saveWorkItem(restoreState)
	restorer.RestoreChannel <- restoreState
	return nil
}

func (restorer *APTFileRestorer) restore() {
	for restoreState := range restorer.RestoreChannel {
		restoreState.RestoreSummary.Attempted = true
		restoreState.RestoreSummary.AttemptNumber += 1
		restoreState.RestoreSummary.Start()

		//
		// *** TODO ***
		//
		// 1. Make sure the file has not already been restored. Check the
		//    name and etag of the item in the restoration bucket, if it
		//    exists.
		// 2. If file has not already been restored, copy it to the depositor's
		//    restoration bucket now.

		restoreState.RestoreSummary.Finish()
		restorer.PostProcessChannel <- restoreState
	}
}

func (restorer *APTFileRestorer) postProcess() {
	for restoreState := range restorer.PostProcessChannel {
		if restoreState.RestoreSummary.HasErrors() {
			restorer.finishWithError(restoreState)
		} else {
			restorer.finishWithSuccess(restoreState)
		}
	}
}

func (restorer *APTFileRestorer) copyToRestorationBucket(restoreState *models.FileRestoreState, fromWhere string) {
	// Find the key we'll need to restore.
	// key, err := restoreState.GenericFile.PreservationStorageFileName()
	// if err != nil {
	// 	restoreState.RestoreSummary.AddError("For file %s: %v", restoreState.GenericFile.Identifier, err)
	// 	restoreState.RestoreSummary.ErrorIsFatal = true
	// 	return
	// }

	// Set up the proper S3 or Glacier client
	// 1. Determine region and bucket name based on GenericFile StorageOption.
	// 2. Create a client to copy from that region.
	// 3. Run the S3 bucket-to-bucket copy, renaming the file (?) from
	//    the UUID name to the GenericFile.Identifier
	// 4. Set FileRestoreState.RestoredToURL and FileRestoreState.CopiedToRestorationAt
}

func (restorer *APTFileRestorer) buildState(message *nsq.Message) (*models.FileRestoreState, error) {
	restoreState := models.NewFileRestoreState(message)
	workItem, err := GetWorkItem(message, restorer.Context)
	if err != nil {
		return nil, err
	}
	restoreState.WorkItem = workItem
	if workItem.GenericFileIdentifier == "" {
		return nil, fmt.Errorf("WorkItem %d is missing generic file identifier",
			workItem.Id)
	}
	resp := restorer.Context.PharosClient.GenericFileGet(workItem.GenericFileIdentifier, false)
	if resp.Error != nil {
		return nil, fmt.Errorf("Error getting generic file '%s': %v",
			workItem.GenericFileIdentifier, resp.Error)
	}
	gf := resp.GenericFile()
	if gf == nil {
		return nil, fmt.Errorf("Pharos client got nil for generic file '%s'",
			workItem.GenericFileIdentifier)
	}
	restoreState.GenericFile = gf
	return restoreState, nil
}

func (restorer *APTFileRestorer) finishWithError(restoreState *models.FileRestoreState) {
	note := restoreState.RestoreSummary.AllErrorsAsString()
	maxAttempts := restorer.Context.Config.FileRestoreWorker.MaxAttempts
	if restoreState.RestoreSummary.AttemptNumber > maxAttempts {
		note = fmt.Sprintf("Too many failed restore attempts (%d). "+
			"Errors: %s",
			maxAttempts,
			restoreState.RestoreSummary.AllErrorsAsString())
		restoreState.RestoreSummary.ErrorIsFatal = true
	}
	if restoreState.RestoreSummary.ErrorIsFatal {
		restoreState.WorkItem.Status = constants.StatusFailed
		restoreState.WorkItem.Retry = false
		restoreState.WorkItem.NeedsAdminReview = true
	} else {
		// Non-fatal error gets a retry.
		restoreState.WorkItem.Status = constants.StatusPending
		restoreState.WorkItem.Stage = constants.StageRequested
	}
	restoreState.WorkItem.Date = time.Now().UTC()
	restoreState.WorkItem.Note = note
	restoreState.WorkItem.Node = ""
	restoreState.WorkItem.Pid = 0
	restoreState.WorkItem.StageStartedAt = nil

	restorer.saveWorkItem(restoreState)

	restorer.Context.MessageLog.Error(restoreState.RestoreSummary.AllErrorsAsString())

	if restoreState.RestoreSummary.ErrorIsFatal {
		restorer.Context.MessageLog.Error("Deletion of %s failed",
			restoreState.GenericFile.Identifier)
		restoreState.NSQMessage.Finish()
	} else {
		restorer.Context.MessageLog.Warning("Requeuing %s",
			restoreState.GenericFile.Identifier)
		restoreState.NSQMessage.Requeue(1 * time.Minute)
	}
}

func (restorer *APTFileRestorer) finishWithSuccess(restoreState *models.FileRestoreState) {
	restoreState.WorkItem.Date = time.Now().UTC()
	restoreState.WorkItem.Note = fmt.Sprintf(
		"File restored to %s at %s by request of %s",
		restoreState.CopiedToRestorationAt.Format(time.RFC3339),
		restoreState.RestoredToURL,
		restoreState.WorkItem.User)
	restoreState.WorkItem.Node = ""
	restoreState.WorkItem.Pid = 0
	restoreState.WorkItem.Status = constants.StatusSuccess
	restoreState.WorkItem.Stage = constants.StageResolve
	restorer.saveWorkItem(restoreState)
	restoreState.NSQMessage.Finish()
}

func (restorer *APTFileRestorer) saveWorkItem(restoreState *models.FileRestoreState) {
	resp := restorer.Context.PharosClient.WorkItemSave(restoreState.WorkItem)
	// We can proceed if this call fails. Pharos just won't show users
	// the current state of processing for this item.
	if resp.Error != nil {
		restorer.Context.MessageLog.Warning(
			"Error marking WorkItem %d as %s/%s for object %s: %v",
			restoreState.WorkItem.Id,
			restoreState.WorkItem.Stage,
			restoreState.WorkItem.Status,
			restoreState.WorkItem.GenericFileIdentifier,
			resp.Error)
	}
}
