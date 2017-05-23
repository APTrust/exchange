package workers

import (
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

const (
	// GENERIC_FILE_BATCH_SIZE describes how many generic files
	// we should batch into a single HTTP POST when recording a
	// new IntellectualObject.
	GENERIC_FILE_BATCH_SIZE = 100
)

// Records ingest data (objects, files and events) in Pharos
type APTRecorder struct {
	Context        *context.Context
	RecordChannel  chan *models.IngestState
	CleanupChannel chan *models.IngestState
}

func NewAPTRecorder(_context *context.Context) *APTRecorder {
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
func (recorder *APTRecorder) HandleMessage(message *nsq.Message) error {
	ingestState, err := GetIngestState(message, recorder.Context, false)
	if err != nil {
		recorder.Context.MessageLog.Error(err.Error())
		return err
	}

	// If this item was queued more than once, and this process or any
	// other is currently working on it, just finish the message and
	// assume that the in-progress worker will take care of the original.
	if ingestState.WorkItem.Node != "" && ingestState.WorkItem.Pid != 0 {
		recorder.Context.MessageLog.Info("Marking WorkItem %d (%s/%s) as finished "+
			"without doing any work, because this item is currently in process by "+
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
func (recorder *APTRecorder) record() {
	for ingestState := range recorder.RecordChannel {
		ingestState.IngestManifest.RecordResult.Start()
		ingestState.IngestManifest.RecordResult.Attempted = true
		ingestState.IngestManifest.RecordResult.AttemptNumber += 1
		recorder.buildEventsAndChecksums(ingestState)
		if !ingestState.IngestManifest.RecordResult.HasErrors() {
			recorder.saveAllPharosData(ingestState)
		}
		recorder.CleanupChannel <- ingestState
	}
}

// Step 2: Delete tar file from staging area and from receiving bucket.
func (recorder *APTRecorder) cleanup() {
	for ingestState := range recorder.CleanupChannel {
		// See if we have fatal errors, or too many recurring transient errors
		attemptNumber := ingestState.IngestManifest.RecordResult.AttemptNumber
		maxAttempts := recorder.Context.Config.RecordWorker.MaxAttempts
		itsTimeToGiveUp := (ingestState.IngestManifest.HasFatalErrors() ||
			(ingestState.IngestManifest.HasErrors() && attemptNumber >= maxAttempts))

		if itsTimeToGiveUp {
			recorder.Context.MessageLog.Error("Failed to record %s/%s. Errors: %s.",
				ingestState.WorkItem.Bucket, ingestState.WorkItem.Name,
				ingestState.IngestManifest.AllErrorsAsString())
			ingestState.FinishNSQ()
			MarkWorkItemFailed(ingestState, recorder.Context)
		} else if ingestState.IngestManifest.RecordResult.HasErrors() {
			recorder.Context.MessageLog.Info("Requeueing WorkItem %d (%s/%s) due to transient errors. %s",
				ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
				ingestState.WorkItem.Name,
				ingestState.IngestManifest.AllErrorsAsString())
			ingestState.RequeueNSQ(1000)
			MarkWorkItemRequeued(ingestState, recorder.Context)
		} else {
			MarkWorkItemStarted(ingestState, recorder.Context, constants.StageCleanup,
				"Bag has been stored and recorded. Deleting files from receiving bucket "+
					"and staging area.")

			// Remove both the bag and the validation DB
			DeleteFileFromStaging(ingestState.IngestManifest.BagPath, recorder.Context)
			DeleteFileFromStaging(ingestState.IngestManifest.DBPath, recorder.Context)

			recorder.deleteBagFromReceivingBucket(ingestState)
			MarkWorkItemSucceeded(ingestState, recorder.Context, constants.StageCleanup)
			ingestState.FinishNSQ()
		}

		// Save our WorkItemState
		ingestState.IngestManifest.RecordResult.Finish()
		RecordWorkItemState(ingestState, recorder.Context, ingestState.IngestManifest.RecordResult)
	}
}

// Make sure the IntellectualObject and its component files have
// all of the checksums and PREMIS events we'll need to save.
// We build these now so that the PREMIS events will have UUIDs,
// and if we ever have to re-record this IntellectualObject after
// a partial save, we'll know which events are already recorded
// in Pharos and which were not. This was a problem in the old
// system, where record failured were common, and PREMIS events
// often wound up being recorded twice.
func (recorder *APTRecorder) buildEventsAndChecksums(ingestState *models.IngestState) {
	obj := ingestState.IngestManifest.Object
	err := obj.BuildIngestEvents()
	if err != nil {
		ingestState.IngestManifest.RecordResult.AddError(err.Error())
		ingestState.IngestManifest.RecordResult.ErrorIsFatal = true
	}
	err = obj.BuildIngestChecksums()
	if err != nil {
		ingestState.IngestManifest.RecordResult.AddError(err.Error())
		ingestState.IngestManifest.RecordResult.ErrorIsFatal = true
	}
}

func (recorder *APTRecorder) saveAllPharosData(ingestState *models.IngestState) {
	if ingestState.IngestManifest.Object.Id == 0 {
		recorder.saveIntellectualObject(ingestState)
		if ingestState.IngestManifest.RecordResult.HasErrors() {
			recorder.Context.MessageLog.Error("Error saving IntellectualObject %s/%s: %v",
				ingestState.WorkItem.Bucket, ingestState.WorkItem.Name,
				ingestState.IngestManifest.RecordResult.AllErrorsAsString())
			return
		} else {
			recorder.Context.MessageLog.Info("Saved %s/%s with id %d",
				ingestState.WorkItem.Bucket, ingestState.WorkItem.Name,
				ingestState.IngestManifest.Object.Id)
		}
	} else {
		recorder.Context.MessageLog.Info(
			"No need to save %s/%s already has id %d",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name,
			ingestState.IngestManifest.Object.Id)
	}
	recorder.saveGenericFiles(ingestState)
	if ingestState.IngestManifest.RecordResult.HasErrors() {
		recorder.Context.MessageLog.Error("Error saving one or more GenericFiles for "+
			"IntellectualObject %s/%s: %v",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name,
			ingestState.IngestManifest.RecordResult.AllErrorsAsString())
		return
	}
}

func (recorder *APTRecorder) saveIntellectualObject(ingestState *models.IngestState) {
	obj := ingestState.IngestManifest.Object

	// If we're ingesting a new version of a previously ingested bag,
	// we'll want to update the old record. Otherwise, we'll create a
	// new one. 99.99% of the time, Pharos will return a 404 here, because
	// it's a new ingest.
	resp := recorder.Context.PharosClient.IntellectualObjectGet(obj.Identifier, false, false)
	existingObject := resp.IntellectualObject()
	if existingObject != nil {
		// PharosClient will know to update, rather than create,
		// when it sees the Object's non-zero id.
		obj.Id = existingObject.Id
	}

	resp = recorder.Context.PharosClient.IntellectualObjectSave(obj)
	if resp.Error != nil {
		ingestState.IngestManifest.RecordResult.AddError(resp.Error.Error())
		return
	}
	savedObject := resp.IntellectualObject()
	if savedObject == nil {
		ingestState.IngestManifest.RecordResult.AddError(
			"Pharos returned nil IntellectualObject after save.")
		return
	}
	obj.Id = savedObject.Id
	obj.CreatedAt = savedObject.CreatedAt
	obj.UpdatedAt = savedObject.UpdatedAt
	obj.PropagateIdsToChildren()
	recorder.savePremisEventsForObject(ingestState)
}

func (recorder *APTRecorder) saveGenericFiles(ingestState *models.IngestState) {
	filesToCreate := make([]*models.GenericFile, 0)
	filesToUpdate := make([]*models.GenericFile, 0)
	for i, gf := range ingestState.IngestManifest.Object.GenericFiles {
		// We run this check here, rather than in the validator,
		// because this is an APTrust-specific policy.
		if !util.HasSavableName(gf.OriginalPath()) {
			recorder.Context.MessageLog.Info("Will not save %s: does not match savable name pattern.",
				gf.Identifier)
			gf.IngestNeedsSave = false
		}
		if i%GENERIC_FILE_BATCH_SIZE == 0 {
			recorder.createGenericFiles(ingestState, filesToCreate)
			if ingestState.IngestManifest.RecordResult.HasErrors() {
				break
			}
			recorder.updateGenericFiles(ingestState, filesToUpdate)
			if ingestState.IngestManifest.RecordResult.HasErrors() {
				break
			}
			filesToCreate = make([]*models.GenericFile, 0)
			filesToUpdate = make([]*models.GenericFile, 0)
		}
		if gf.IngestNeedsSave {
			if gf.IngestPreviousVersionExists {
				if gf.Id > 0 {
					filesToUpdate = append(filesToUpdate, gf)
				} else {
					msg := fmt.Sprintf("GenericFile %s has a previous version, but its Id is missing.",
						gf.Identifier)
					recorder.Context.MessageLog.Error(msg)
					ingestState.IngestManifest.RecordResult.AddError(msg)
				}
			} else if gf.IngestNeedsSave && gf.Id == 0 {
				filesToCreate = append(filesToCreate, gf)
			}
		}
	}
	if !ingestState.IngestManifest.RecordResult.HasErrors() {
		recorder.createGenericFiles(ingestState, filesToCreate)
		recorder.updateGenericFiles(ingestState, filesToUpdate)
	}
}

func (recorder *APTRecorder) createGenericFiles(ingestState *models.IngestState, files []*models.GenericFile) {
	if len(files) == 0 {
		return
	}
	resp := recorder.Context.PharosClient.GenericFileSaveBatch(files)
	if resp.Error != nil {
		body, _ := resp.RawResponseData()
		recorder.Context.MessageLog.Error(
			"Pharos returned this after attempt to save batch of GenericFiles:\n%s",
			string(body))
		ingestState.IngestManifest.RecordResult.AddError(resp.Error.Error())
	}
	// We may have managed to save some files despite the error.
	// If so, record what was saved.
	for _, savedFile := range resp.GenericFiles() {
		gf := ingestState.IngestManifest.Object.FindGenericFile(savedFile.OriginalPath())
		if gf == nil {
			ingestState.IngestManifest.RecordResult.AddError("After save, could not find file '%s' "+
				"in IntellectualObject.", savedFile.OriginalPath())
			continue
		}
		// Merge attributes set by Pharos into our GenericFile record.
		// Attributes include Id, CreatedAt, UpdatedAt on GenericFile
		// and all of its Checksums and PremisEvents. This also
		// propagates the new GenericFile.Id down to the PremisEvents
		// and Checksums.
		errors := gf.MergeAttributes(savedFile)
		for _, err := range errors {
			ingestState.IngestManifest.RecordResult.AddError(err.Error())
		}
	}
}

func (recorder *APTRecorder) updateGenericFiles(ingestState *models.IngestState, files []*models.GenericFile) {
	if len(files) == 0 {
		return
	}
	for _, gf := range files {
		resp := recorder.Context.PharosClient.GenericFileSave(gf)
		if resp.Error != nil {
			ingestState.IngestManifest.RecordResult.AddError(
				"Error updating '%s': %v", gf.Identifier, resp.Error)
			continue
		}
		// Shouldn't need to call this. Should already have Id?
		gf.PropagateIdsToChildren()
	}
}

func (recorder *APTRecorder) savePremisEventsForObject(ingestState *models.IngestState) {
	obj := ingestState.IngestManifest.Object
	for i, event := range obj.PremisEvents {
		resp := recorder.Context.PharosClient.PremisEventSave(event)
		if resp.Error != nil {
			ingestState.IngestManifest.RecordResult.AddError(
				"While saving events for '%s', error adding PremisEvent '%s': %v",
				obj.Identifier, event.EventType, resp.Error)
		} else {
			obj.PremisEvents[i].MergeAttributes(resp.PremisEvent())
		}
	}
}

func (recorder *APTRecorder) deleteBagFromReceivingBucket(ingestState *models.IngestState) {
	ingestState.IngestManifest.CleanupResult.Start()
	ingestState.IngestManifest.CleanupResult.Attempted = true
	ingestState.IngestManifest.CleanupResult.AttemptNumber += 1
	// Remove the bag from the receiving bucket, if ingest succeeded
	if recorder.Context.Config.DeleteOnSuccess == false {
		// We don't actually delete files if config is dev, test, or integration.
		recorder.Context.MessageLog.Info("Skipping deletion step because config.DeleteOnSuccess == false")
		// Set deletion timestamp, so we know this method was called.
		ingestState.IngestManifest.Object.IngestDeletedFromReceivingAt = time.Now().UTC()
		ingestState.IngestManifest.CleanupResult.Finish()
		return
	}
	deleter := network.NewS3ObjectDelete(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		ingestState.IngestManifest.S3Bucket,
		[]string{ingestState.IngestManifest.S3Key})
	deleter.DeleteList()
	if deleter.ErrorMessage != "" {
		message := fmt.Sprintf("In cleanup, error deleting S3 item %s/%s: %s",
			ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key,
			deleter.ErrorMessage)
		recorder.Context.MessageLog.Warning(message)
		ingestState.IngestManifest.CleanupResult.AddError(message)
	} else {
		message := fmt.Sprintf("Deleted S3 item %s/%s",
			ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key)
		recorder.Context.MessageLog.Info(message)
		ingestState.IngestManifest.Object.IngestDeletedFromReceivingAt = time.Now().UTC()
	}
	ingestState.IngestManifest.CleanupResult.Finish()
}
