package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/nsqio/go-nsq"
	"os"
	"strings"
	"time"
)

// APTFileDeleter deletes files from S3 and Glacier long-term storage.
type APTFileDeleter struct {
	// Context contains basic information required to run,
	// connect to Pharos, S3, etc.
	Context *context.Context
	// DeleteChannel is for the go routines that delete GenericFiles
	// from primary and secondary storage.
	DeleteChannel chan *models.DeleteState
	// PostProcess channel is for the goroutines that record
	// the outcome of the deletion in Pharos and finish or
	// requeue the NSQ message.
	PostProcessChannel chan *models.DeleteState
	// isIntegrationTest will be true if we're running in the
	// integration test context.
	isIntegrationTest bool
}

func NewAPTFileDeleter(_context *context.Context) *APTFileDeleter {
	deleter := &APTFileDeleter{
		Context: _context,
	}

	// Set up buffered channels
	workerBufferSize := _context.Config.FileDeleteWorker.Workers * 10
	deleter.DeleteChannel = make(chan *models.DeleteState, workerBufferSize)
	deleter.PostProcessChannel = make(chan *models.DeleteState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.RestoreWorker.Workers; i++ {
		go deleter.delete()
		go deleter.postProcess()
	}

	deleter.isIntegrationTest = ((strings.HasSuffix(_context.Config.ActiveConfig, "integration.json") ||
		strings.HasSuffix(_context.Config.ActiveConfig, "integration_update.json")) &&
		strings.Contains(_context.Config.PreservationBucket, ".test."))

	return deleter
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (deleter *APTFileDeleter) HandleMessage(message *nsq.Message) error {
	// Build the RestoreState object by fetching WorkItem and IntellectualObject
	// from Pharos.
	deleteState, err := deleter.buildState(message)
	if err != nil {
		deleter.Context.MessageLog.Error(err.Error())
		return err
	}

	deleteState.DeleteSummary.ClearErrors()
	deleteState.WorkItem.Note = "Starting delete process"
	deleteState.WorkItem.SetNodeAndPid()
	deleteState.WorkItem.Status = constants.StatusStarted
	deleter.saveWorkItem(deleteState)

	// Don't proceed without approval from institutional admin,
	// unless we're running integration tests.
	needsApproval := (deleteState.WorkItem.InstitutionalApprover == nil ||
		*deleteState.WorkItem.InstitutionalApprover == "")
	if needsApproval && !deleter.isIntegrationTest {
		deleteState.DeleteSummary.AddError("Cannot delete %s because institutional approver is missing",
			deleteState.GenericFile.Identifier)
		deleteState.DeleteSummary.ErrorIsFatal = true
		deleter.PostProcessChannel <- deleteState
	} else {
		// OK. We have approval.
		deleter.DeleteChannel <- deleteState
	}
	return nil
}

func (deleter *APTFileDeleter) delete() {
	for deleteState := range deleter.DeleteChannel {
		deleteState.DeleteSummary.Attempted = true
		deleteState.DeleteSummary.AttemptNumber += 1
		deleteState.DeleteSummary.Start()

		fileUUID, err := deleteState.GenericFile.PreservationStorageFileName()
		if err != nil {
			deleteState.DeleteSummary.AddError(err.Error())
		} else {
			// In some cases, we may have deleted the file on a
			// previous run, then failed to record the deletion
			// event.
			if deleteState.DeletedFromPrimaryAt.IsZero() {
				deleter.deleteFromStorage(deleteState, "s3")
			} else {
				deleter.Context.MessageLog.Info("File %s (%s) was previously "+
					"deleted from primary storage",
					deleteState.GenericFile.Identifier, fileUUID)
			}
			if deleteState.DeletedFromSecondaryAt.IsZero() {
				deleter.deleteFromStorage(deleteState, "glacier")
			} else {
				deleter.Context.MessageLog.Info("File %s (%s) was previously "+
					"deleted from secondary storage",
					deleteState.GenericFile.Identifier, fileUUID)
			}
		}
		deleteState.DeleteSummary.Finish()
		deleter.PostProcessChannel <- deleteState
	}
}

func (deleter *APTFileDeleter) postProcess() {
	for deleteState := range deleter.PostProcessChannel {
		if !deleteState.DeleteSummary.HasErrors() {
			deleter.recordFileDeletionEvent(deleteState)
		}
		if !deleteState.DeleteSummary.HasErrors() {
			deleter.markFileDeleted(deleteState)
		}
		if !deleteState.DeleteSummary.HasErrors() {
			deleter.markObjectDeletedIfAppropriate(deleteState)
		}
		if deleteState.DeleteSummary.HasErrors() {
			deleter.finishWithError(deleteState)
		} else {
			deleter.finishWithSuccess(deleteState)
		}
	}
}

func (deleter *APTFileDeleter) deleteFromStorage(deleteState *models.DeleteState, fromWhere string) {
	// Find the key we'll need to delete.
	key, err := deleteState.GenericFile.PreservationStorageFileName()
	if err != nil {
		deleteState.DeleteSummary.AddError("For file %s: %v", deleteState.GenericFile.Identifier, err)
		deleteState.DeleteSummary.ErrorIsFatal = true
		return
	}
	keys := make([]string, 1)
	keys[0] = key
	deleter.Context.MessageLog.Info("Deleting %s (key %s) from %s",
		deleteState.GenericFile.Identifier, key, fromWhere)

	// Set up the proper S3 or Glacier client
	var region string
	var bucket string
	if fromWhere == "s3" {
		region = deleter.Context.Config.APTrustS3Region
		bucket = deleter.Context.Config.PreservationBucket
	} else if fromWhere == "glacier" {
		region = deleter.Context.Config.APTrustGlacierRegion
		bucket = deleter.Context.Config.ReplicationBucket
	} else {
		deleteState.DeleteSummary.AddError("Cannot delete %s from %s because "+
			"deleter doesn't know where %s is",
			deleteState.GenericFile.Identifier, fromWhere, fromWhere)
		deleteState.DeleteSummary.ErrorIsFatal = true
		return
	}
	client := network.NewS3ObjectDelete(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		region, bucket, keys)
	client.DeleteList()
	if client.ErrorMessage != "" {
		msg := fmt.Sprintf("Error deleting %s from %s: %v",
			deleteState.GenericFile.Identifier,
			fromWhere, client.ErrorMessage)
		deleteState.DeleteSummary.AddError(msg)
	} else {
		if fromWhere == "s3" {
			deleteState.DeletedFromPrimaryAt = time.Now().UTC()
		} else if fromWhere == "glacier" {
			deleteState.DeletedFromSecondaryAt = time.Now().UTC()
		}
		deleter.Context.MessageLog.Info("Deleted %s (key %s) from %s",
			deleteState.GenericFile.Identifier, key, fromWhere)
	}
}

func (deleter *APTFileDeleter) buildState(message *nsq.Message) (*models.DeleteState, error) {
	deleteState := models.NewDeleteState(message)
	workItem, err := GetWorkItem(message, deleter.Context)
	if err != nil {
		return nil, err
	}
	deleteState.WorkItem = workItem
	if workItem.GenericFileIdentifier == "" {
		return nil, fmt.Errorf("WorkItem %d is missing generic file identifier",
			workItem.Id)
	}
	resp := deleter.Context.PharosClient.GenericFileGet(workItem.GenericFileIdentifier, false)
	if resp.Error != nil {
		return nil, fmt.Errorf("Error getting generic file '%s': %v",
			workItem.GenericFileIdentifier, resp.Error)
	}
	gf := resp.GenericFile()
	if gf == nil {
		return nil, fmt.Errorf("Pharos client got nil for generic file '%s'",
			workItem.GenericFileIdentifier)
	}
	deleteState.GenericFile = gf
	return deleteState, nil
}

func (deleter *APTFileDeleter) finishWithError(deleteState *models.DeleteState) {
	note := deleteState.DeleteSummary.AllErrorsAsString()
	maxAttempts := deleter.Context.Config.FileDeleteWorker.MaxAttempts
	if deleteState.DeleteSummary.AttemptNumber > maxAttempts {
		note = fmt.Sprintf("Too many failed delete attempts (%d). "+
			"Errors: %s",
			maxAttempts,
			deleteState.DeleteSummary.AllErrorsAsString())
		deleteState.DeleteSummary.ErrorIsFatal = true
	}
	if deleteState.DeleteSummary.ErrorIsFatal {
		deleteState.WorkItem.Status = constants.StatusFailed
		deleteState.WorkItem.Retry = false
		deleteState.WorkItem.NeedsAdminReview = true
	}
	deleteState.WorkItem.Date = time.Now().UTC()
	deleteState.WorkItem.Note = note
	deleteState.WorkItem.Node = ""
	deleteState.WorkItem.Pid = 0
	deleteState.WorkItem.StageStartedAt = nil
	deleteState.WorkItem.Status = constants.StatusPending
	deleteState.WorkItem.Stage = constants.StageRequested

	deleter.saveWorkItem(deleteState)

	deleter.Context.MessageLog.Error(deleteState.DeleteSummary.AllErrorsAsString())

	if deleteState.DeleteSummary.ErrorIsFatal {
		deleter.Context.MessageLog.Error("Deletion of %s failed",
			deleteState.GenericFile.Identifier)
		deleteState.NSQMessage.Finish()
	} else {
		deleter.Context.MessageLog.Warning("Requeuing %s",
			deleteState.GenericFile.Identifier)
		deleteState.NSQMessage.Requeue(1 * time.Minute)
	}
}

func (deleter *APTFileDeleter) finishWithSuccess(deleteState *models.DeleteState) {
	fileUUID, err := deleteState.GenericFile.PreservationStorageFileName()
	if err != nil {
		deleteState.DeleteSummary.AddError(err.Error())
		return
	}
	deleteState.WorkItem.Date = time.Now().UTC()
	deleteState.WorkItem.Note = fmt.Sprintf(
		"File %s (%s) deleted at %s by request of %s",
		deleteState.GenericFile.Identifier,
		fileUUID,
		deleteState.DeletedFromSecondaryAt.Format(time.RFC3339),
		deleteState.WorkItem.User)
	deleteState.WorkItem.Node = ""
	deleteState.WorkItem.Pid = 0
	deleteState.WorkItem.Status = constants.StatusSuccess
	deleteState.WorkItem.Stage = constants.StageResolve
	deleter.saveWorkItem(deleteState)
	deleteState.NSQMessage.Finish()
}

func (deleter *APTFileDeleter) recordFileDeletionEvent(deleteState *models.DeleteState) {
	fileUUID, err := deleteState.GenericFile.PreservationStorageFileName()
	if err != nil {
		deleteState.DeleteSummary.AddError(err.Error())
		return
	}
	requestedBy := deleteState.WorkItem.User
	instApprover := ""
	if deleteState.WorkItem.InstitutionalApprover != nil {
		instApprover = *deleteState.WorkItem.InstitutionalApprover
	}
	aptrustApprover := ""
	if deleteState.WorkItem.APTrustApprover != nil {
		instApprover = *deleteState.WorkItem.APTrustApprover
	}
	timestamp := deleteState.DeletedFromSecondaryAt
	event := models.NewEventFileDeletion(fileUUID, requestedBy, instApprover, aptrustApprover, timestamp)
	event.IntellectualObjectId = deleteState.GenericFile.IntellectualObjectId
	event.IntellectualObjectIdentifier = deleteState.GenericFile.IntellectualObjectIdentifier
	event.GenericFileId = deleteState.GenericFile.Id
	event.GenericFileIdentifier = deleteState.GenericFile.Identifier
	resp := deleter.Context.PharosClient.PremisEventSave(event)
	if resp.Error != nil {
		msg := fmt.Sprintf("Error saving deletion event for file '%s' (%s): %v",
			deleteState.GenericFile.Identifier, fileUUID, resp.Error)
		bytes, _ := resp.RawResponseData()
		if bytes != nil {
			msg += fmt.Sprintf(" - Pharos response: %s", string(bytes))
		}
		deleteState.DeleteSummary.AddError(msg)
		return
	} else {
		deleter.Context.MessageLog.Info("Saved deletion event %s for file %s",
			event.Identifier, deleteState.GenericFile.Identifier)
	}
}

func (deleter *APTFileDeleter) markFileDeleted(deleteState *models.DeleteState) {
	resp := deleter.Context.PharosClient.GenericFileFinishDelete(deleteState.GenericFile.Identifier)
	if resp.Error != nil {
		deleteState.DeleteSummary.AddError("Error marking %s as deleted: %v",
			deleteState.GenericFile.Identifier, resp.Error)
	}
}

func (deleter *APTFileDeleter) markObjectDeletedIfAppropriate(deleteState *models.DeleteState) {
	// Get the object with its events, but don't get GenericFiles, because
	// there may be thousands of them.
	objIdentifier := deleteState.GenericFile.IntellectualObjectIdentifier
	resp := deleter.Context.PharosClient.IntellectualObjectGet(objIdentifier, false, true)
	if resp.Error != nil {
		deleteState.DeleteSummary.AddError(
			"Error checking for state and events on IntellectualObject %s: %v",
			objIdentifier, resp.Error)
		return
	}
	obj := resp.IntellectualObject()
	if obj == nil {
		deleteState.DeleteSummary.AddError(
			"When checking for state and events, Pharos returned nil for IntellectualObject %s: %v",
			objIdentifier, resp.Error)
		return
	}
	if obj.State == "D" {
		deleter.Context.MessageLog.Info("Object %s is already marked deleted", objIdentifier)
		return
	}
	lastIngest := time.Time{}
	lastDelete := time.Time{}
	// Typical object has ~5 events
	for _, event := range obj.PremisEvents {
		if event.EventType == constants.EventIngestion && event.DateTime.After(lastIngest) {
			lastIngest = event.DateTime
		} else if event.EventType == constants.EventDeletion && event.DateTime.After(lastDelete) {
			lastDelete = event.DateTime
		}
	}
	if lastDelete.IsZero() {
		deleter.Context.MessageLog.Info("No delete event for object %s", objIdentifier)
		return
	}
	// If we know we have a delete event on this object, check to see if the
	// object still has any active files. This call is can be expensive, so
	// we avoid it until this point.
	if lastDelete.After(lastIngest) {
		resp = deleter.Context.PharosClient.IntellectualObjectGet(objIdentifier, true, false)
		if resp.Error != nil {
			deleteState.DeleteSummary.AddError(
				"Error checking for active files on IntellectualObject %s: %v",
				objIdentifier, resp.Error)
		}
		obj = resp.IntellectualObject()
		if obj == nil {
			deleteState.DeleteSummary.AddError(
				"When checking for active files, Pharos returned nil for IntellectualObject %s: %v",
				objIdentifier, resp.Error)
			return
		}
	} else {
		deleter.Context.MessageLog.Info("No recent delete event for object %s", objIdentifier)
		return
	}
	if len(obj.GenericFiles) == 0 {
		resp := deleter.Context.PharosClient.IntellectualObjectFinishDelete(objIdentifier)
		if resp.Error != nil {
			deleteState.DeleteSummary.AddError("Error marking %s as deleted: %v",
				deleteState.GenericFile.Identifier, resp.Error)
		} else {
			deleter.Context.MessageLog.Info(
				"Marked IntellectualObject %s as deleted (has delete event and no more active files)",
				objIdentifier)
		}
	}
}

func (deleter *APTFileDeleter) saveWorkItem(deleteState *models.DeleteState) {
	resp := deleter.Context.PharosClient.WorkItemSave(deleteState.WorkItem)
	// We can proceed if this call fails. Pharos just won't show users
	// the current state of processing for this item.
	if resp.Error != nil {
		deleter.Context.MessageLog.Warning(
			"Error marking WorkItem %d as %s/%s for object %s: %v",
			deleteState.WorkItem.Id,
			deleteState.WorkItem.Stage,
			deleteState.WorkItem.Status,
			deleteState.WorkItem.GenericFileIdentifier,
			resp.Error)
	}
}
