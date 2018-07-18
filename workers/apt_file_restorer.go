package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
	"github.com/nsqio/go-nsq"
	"os"
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
	// the outcome of the restoration in Pharos and finish or
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
	message.DisableAutoResponse()
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
	restorer.saveWorkItem(restoreState, false)
	restorer.RestoreChannel <- restoreState
	return nil
}

func (restorer *APTFileRestorer) restore() {
	for restoreState := range restorer.RestoreChannel {
		restoreState.RestoreSummary.Attempted = true
		restoreState.RestoreSummary.AttemptNumber += 1
		restoreState.RestoreSummary.Start()

		if restorer.alreadyRestored(restoreState) {
			restorationBucket := util.RestorationBucketFor(restoreState.IntellectualObject.Institution)
			restorer.Context.MessageLog.Info("File %s has already been restored to %s",
				restoreState.GenericFile.Identifier, restorationBucket)
		} else {
			restoreState.NSQMessage.Touch()
			restorer.copyToRestorationBucket(restoreState)
			restoreState.NSQMessage.Touch()
		}

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

func (restorer *APTFileRestorer) copyToRestorationBucket(restoreState *models.FileRestoreState) {
	sourceRegion, sourceBucket, err := restorer.Context.Config.StorageRegionAndBucketFor(restoreState.GenericFile.StorageOption)
	if err != nil {
		restoreState.RestoreSummary.AddError(err.Error())
		return
	}
	restorationBucket := util.RestorationBucketFor(restoreState.IntellectualObject.Institution)
	// PT #159115778: Get a client for the S3 restoration region, since
	// this is the region we're writing to.
	restorationRegion := restorer.Context.Config.APTrustS3Region
	fileUUID, err := restoreState.GenericFile.PreservationStorageFileName()
	if err != nil {
		restoreState.RestoreSummary.AddError("Error getting file UUID: %v", err)
		return
	}
	restorer.Context.MessageLog.Info("Copying %s from %s to %s", restoreState.GenericFile.Identifier,
		sourceRegion, restorationRegion)
	copier := network.NewS3Copy(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		restorationRegion, // PT #159115778: Not source region!
		sourceBucket,
		fileUUID,
		restorationBucket,
		restoreState.GenericFile.Identifier)
	copier.Copy()
	if copier.ErrorMessage != "" {
		restoreState.RestoreSummary.AddError("Error copying to restoration bucket: %s",
			copier.ErrorMessage)
		return
	}
	restoreState.RestoredToURL = fmt.Sprintf("%s%s/%s", constants.S3UriPrefix, restorationBucket, restoreState.GenericFile.Identifier)
	restoreState.CopiedToRestorationAt = time.Now().UTC()
}

func (restorer *APTFileRestorer) alreadyRestored(restoreState *models.FileRestoreState) bool {
	restorationBucket := util.RestorationBucketFor(restoreState.IntellectualObject.Institution)
	client := network.NewS3Head(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		restorer.Context.Config.APTrustS3Region,
		restorationBucket)
	client.Head(restoreState.GenericFile.Identifier)
	if client.Response != nil && client.ErrorMessage == "" {
		sizeInS3 := int64(-1)
		if client.Response.ContentLength != nil {
			sizeInS3 = *client.Response.ContentLength
		}
		return restoreState.GenericFile.Size == sizeInS3
	}
	return false
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

	// Get the GenericFile
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

	// Get the IntellectualObject of which the file is a part.
	// We need this primarily for the Institution identifier.
	resp = restorer.Context.PharosClient.IntellectualObjectGet(workItem.ObjectIdentifier, false, false)
	if resp.Error != nil {
		return nil, fmt.Errorf("Error getting intellectual object '%s': %v",
			workItem.ObjectIdentifier, resp.Error)
	}
	obj := resp.IntellectualObject()
	if obj == nil {
		return nil, fmt.Errorf("Pharos client got nil for intellectual object '%s'",
			workItem.ObjectIdentifier)
	}
	restoreState.IntellectualObject = obj

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

	restorer.saveWorkItem(restoreState, true)

	restorer.Context.MessageLog.Error(restoreState.RestoreSummary.AllErrorsAsString())

	if restoreState.RestoreSummary.ErrorIsFatal {
		restorer.Context.MessageLog.Error("Restoration of %s failed",
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
	restorer.saveWorkItem(restoreState, true)
	restoreState.NSQMessage.Finish()
}

func (restorer *APTFileRestorer) saveWorkItem(restoreState *models.FileRestoreState, logJson bool) {
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
	if logJson {
		stateJson, err := json.Marshal(restoreState)
		if err != nil {
			errMessage := fmt.Sprintf("Cannot marshal restoreState JSON: %v", err)
			restorer.Context.MessageLog.Error(errMessage)
		} else {
			restorer.logJson(restoreState, string(stateJson))
		}
	}
}

// LogJson dumps the WorkItemState.State into the JSON log, surrounded by
// markers that make it easy to find.
func (restorer *APTFileRestorer) logJson(restoreState *models.FileRestoreState, jsonString string) {
	if restoreState == nil || restoreState.GenericFile == nil {
		restorer.Context.MessageLog.Warning("Can't log JSON state because state or generic file is nil for WorkItem %d", restoreState.WorkItem.Id)
		return
	}
	timestamp := time.Now().UTC().Format(time.RFC3339)
	startMessage := fmt.Sprintf("-------- BEGIN %s | WorkItem: %d | Time: %s --------",
		restoreState.GenericFile.Identifier, restoreState.WorkItem.Id, timestamp)
	endMessage := fmt.Sprintf("-------- END %s | WorkItem: %d | Time: %s --------",
		restoreState.GenericFile.Identifier, restoreState.WorkItem.Id, timestamp)
	restorer.Context.JsonLog.Println(startMessage, "\n",
		jsonString, "\n",
		endMessage)
}
