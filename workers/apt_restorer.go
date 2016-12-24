package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	//	"github.com/APTrust/exchange/network"
	//	"github.com/APTrust/exchange/util"
	"github.com/nsqio/go-nsq"
	"os"
	"path/filepath"
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
	// ValidateChannel is for go routines that validate the
	// newly assembled bag before sending it off to the restoration bucket.
	ValidateChannel chan *models.RestoreState
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
	restorer.ValidateChannel = make(chan *models.RestoreState, workerBufferSize)
	restorer.CopyChannel = make(chan *models.RestoreState, workerBufferSize)
	restorer.PostProcessChannel = make(chan *models.RestoreState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.RestoreWorker.Workers; i++ {
		go restorer.buildBag()
		go restorer.validateBag()
		go restorer.copyToRestorationBucket()
		go restorer.postProcess()
	}
	return restorer
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (restorer *APTRestorer) HandleMessage(message *nsq.Message) error {
	// Build the RestoreState object by fetching WorkItem and IntellectualObject
	// from Pharos.
	restoreState, err := restorer.buildState(message)
	if err != nil {
		restorer.Context.MessageLog.Error(err.Error())
		return err
	}

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

	// Tell Pharos that we're building the bag: constants.StagePackage, constants.StatusStarted
	restorer.markWorkItemStarted(restoreState)

	// We may have partially processed this item before and then been
	// forced to quit due to some transient error like not being able
	// to contact Pharos or S3. Figure out how far we got on the last
	// attempt to process, and resume there. Clear errors from prior
	// processing before resuming.
	if restoreState.CopySummary.Finished() {
		restorer.logWhereThisIsGoing(restoreState, "PostProcessChannel")
		restoreState.RecordSummary.ClearErrors()
		restorer.PostProcessChannel <- restoreState
	} else if restoreState.ValidateSummary.Finished() {
		restorer.logWhereThisIsGoing(restoreState, "CopyChannel")
		restoreState.CopySummary.ClearErrors()
		restorer.CopyChannel <- restoreState
	} else if restoreState.PackageSummary.Finished() {
		restorer.logWhereThisIsGoing(restoreState, "ValidateChannel")
		restoreState.ValidateSummary.ClearErrors()
		restorer.ValidateChannel <- restoreState
	} else {
		restorer.logWhereThisIsGoing(restoreState, "PackageChannel")
		restoreState.PackageSummary.ClearErrors()
		restorer.PackageChannel <- restoreState
	}

	// Return no error, so NSQ knows we're OK.
	return nil
}

func (restorer *APTRestorer) buildBag() {
	for restoreState := range restorer.PackageChannel {
		restoreState.TouchNSQ()
		restoreState.PackageSummary.Start()
		// Assemble all files and tar the bag.
		restoreState.PackageSummary.Finish()
	}
}

func (restorer *APTRestorer) validateBag() {
	for restoreState := range restorer.PackageChannel {
		// Assemble all files, tar, and validate.
		// Touch NSQ often.
		restoreState.TouchNSQ()
	}
}

func (restorer *APTRestorer) copyToRestorationBucket() {
	for restoreState := range restorer.CopyChannel {
		// Copy bag to S3
		restoreState.TouchNSQ()
	}
}

func (restorer *APTRestorer) postProcess() {
	for restoreState := range restorer.PostProcessChannel {
		// Mark item completed in Pharos and finish NSQ.
		restoreState.TouchNSQ()
	}
}

func (restorer *APTRestorer) buildState(message *nsq.Message) (*models.RestoreState, error) {
	restoreState := models.NewRestoreState(message)
	workItem, err := GetWorkItem(message, restorer.Context)
	if err != nil {
		return nil, err
	}
	restoreState.WorkItem = workItem

	// Get the saved state of this item, if there is one.
	if workItem.WorkItemStateId != nil {
		resp := restorer.Context.PharosClient.WorkItemStateGet(*workItem.WorkItemStateId)
		if resp.Error != nil {
			restorer.Context.MessageLog.Warning("Could not retrieve WorkItemState with id %d: %v",
				*workItem.WorkItemStateId, resp.Error)
		} else {
			workItemState := resp.WorkItemState()
			savedState := &models.RestoreState{}
			err = json.Unmarshal([]byte(workItemState.State), savedState)
			if err != nil {
				return nil, fmt.Errorf("Could not unmarshal WorkItemState.State: %v", err)
			}
			restoreState.PackageSummary = savedState.PackageSummary
			restoreState.ValidateSummary = savedState.ValidateSummary
			restoreState.CopySummary = savedState.CopySummary
			restoreState.RecordSummary = savedState.RecordSummary
			restoreState.LocalBagDir = savedState.LocalBagDir
			restoreState.LocalTarFile = savedState.LocalTarFile
			restoreState.RestoredToUrl = savedState.RestoredToUrl
			restoreState.CopiedToRestorationAt = savedState.CopiedToRestorationAt
		}
	}

	// Get the intellectual object. This should not have changed
	// during the processing of this request, because Pharos does
	// not permit delete operations while a restore is pending.
	response := restorer.Context.PharosClient.IntellectualObjectGet(
		restoreState.WorkItem.ObjectIdentifier, true, false)
	if response.Error != nil {
		return nil, err
	}
	restoreState.IntellectualObject = response.IntellectualObject()

	// LocalBagDir will not be set if we were unable to retrieve
	// WorkItemState above.
	if restoreState.LocalBagDir == "" {
		restoreState.LocalBagDir = filepath.Join(
			restorer.Context.Config.RestoreDirectory,
			restoreState.IntellectualObject.Identifier)
	}
	return restoreState, nil
}

func (restorer *APTRestorer) markWorkItemStarted(restoreState *models.RestoreState) {
	restoreState.WorkItem.Stage = constants.StagePackage
	restoreState.WorkItem.Status = constants.StatusStarted
	restoreState.WorkItem.Node, _ = os.Hostname()
	restoreState.WorkItem.Pid = os.Getpid()
	resp := restorer.Context.PharosClient.WorkItemSave(restoreState.WorkItem)
	// We can proceed if this call fails. Pharos just won't show users
	// the current state of processing for this item.
	if resp.Error != nil {
		restorer.Context.MessageLog.Warning("Error marking WorkItem %d started for object %s: %v",
			restoreState.WorkItem.Id, restoreState.IntellectualObject.Identifier, resp.Error)
	}
}

func (restorer *APTRestorer) logWhereThisIsGoing(restoreState *models.RestoreState, channelName string) {
	restorer.Context.MessageLog.Info("Putting %s into %s channel",
		restoreState.WorkItem.ObjectIdentifier, channelName)
}
