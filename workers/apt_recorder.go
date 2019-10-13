package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/storage"
	"github.com/nsqio/go-nsq"
	"os"
	"strings"
	"time"
)

const (
	// GENERIC_FILE_BATCH_SIZE describes how many generic files
	// we should batch into a single HTTP POST when recording a
	// new IntellectualObject.
	GENERIC_FILE_BATCH_SIZE = 200
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
	log := recorder.Context.MessageLog
	ingestState, err := GetIngestState(message, recorder.Context, false)
	if err != nil {
		recorder.Context.MessageLog.Error(err.Error())
		return err
	}

	// Skip this if it's already being worked on.
	if ingestState.WorkItem.IsInProgress() {
		log.Info(ingestState.WorkItem.MsgSkippingInProgress())
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
		recorder.saveAllPharosData(ingestState)
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
			recorder.logFailure(ingestState)
			ingestState.FinishNSQ()
			MarkWorkItemFailed(ingestState, recorder.Context)
		} else if ingestState.IngestManifest.RecordResult.HasErrors() {
			recorder.logRequeue(ingestState)
			ingestState.RequeueNSQ(1000)
			MarkWorkItemRequeued(ingestState, recorder.Context)
		} else {
			MarkWorkItemStarted(ingestState, recorder.Context, constants.StageCleanup,
				"Bag has been stored and recorded. Deleting files from receiving bucket "+
					"and staging area.")

			// Call this before calling DeleteFileFromStaging on the valdb file,
			// because this writes to valdb.
			recorder.deleteBagFromReceivingBucket(ingestState)

			// Remove both the bag and the validation DB (unless we're running integration tests)
			DeleteFileFromStaging(ingestState.IngestManifest.BagPath, recorder.Context)
			if recorder.Context.Config.DeleteOnSuccess == true {
				DeleteFileFromStaging(ingestState.IngestManifest.DBPath, recorder.Context)
			}

			MarkWorkItemSucceeded(ingestState, recorder.Context, constants.StageCleanup)
			ingestState.FinishNSQ()
		}

		// Save our WorkItemState
		ingestState.IngestManifest.RecordResult.Finish()
		LogJson(ingestState, recorder.Context.JsonLog)
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
func (recorder *APTRecorder) saveAllPharosData(ingestState *models.IngestState) {
	db, err := storage.NewBoltDB(ingestState.IngestManifest.DBPath)
	if db == nil {
		// Happens when a prior worker process is killed,
		// e.g. through supervisord, system restart, etc.
		ingestState.IngestManifest.RecordResult.AddError("Bolt DB at %s is nil", ingestState.IngestManifest.DBPath)
		return
	} else {
		defer db.Close()
	}
	if err != nil {
		ingestState.IngestManifest.RecordResult.AddError(err.Error())
		return
	}
	obj, err := db.GetIntellectualObject(db.ObjectIdentifier())
	if err != nil {
		ingestState.IngestManifest.RecordResult.AddError(err.Error())
		return
	}
	if obj == nil {
		// May happen when a prior worker process is killed,
		// e.g. through supervisord, system restart, etc.
		ingestState.IngestManifest.RecordResult.AddError("IntellectualObject not found in Bolt DB")
		return
	}
	err = obj.BuildIngestEvents(db.FileCount())
	if err != nil {
		ingestState.IngestManifest.RecordResult.AddError(err.Error())
		ingestState.IngestManifest.RecordResult.ErrorIsFatal = true
		return
	}

	// Save the IntellectualObject
	if ingestState.IngestManifest.Object.Id == 0 {
		recorder.saveIntellectualObject(ingestState, obj)
		if ingestState.IngestManifest.RecordResult.HasErrors() {
			recorder.logSaveError(ingestState)
			return
		} else {
			recorder.logSaveSuccess(ingestState)
		}
	} else {
		recorder.logNoNeedToSave(ingestState)
	}

	// Save the object in our local db
	err = db.Save(obj.Identifier, obj)
	if err != nil {
		ingestState.IngestManifest.RecordResult.AddError(err.Error())
		return
	}

	recorder.saveFiles(ingestState, obj, db)
}

func (recorder *APTRecorder) saveFiles(ingestState *models.IngestState, obj *models.IntellectualObject, db *storage.BoltDB) {
	offset := 0
	for {
		batch := db.FileIdentifierBatch(offset, GENERIC_FILE_BATCH_SIZE)
		newFiles := make([]*models.GenericFile, 0)
		existingFiles := make([]*models.GenericFile, 0)
		for _, gfIdentifier := range batch {
			gf, err := db.GetGenericFile(gfIdentifier)
			if err != nil {
				ingestState.IngestManifest.RecordResult.AddError(err.Error())
				ingestState.IngestManifest.RecordResult.ErrorIsFatal = true
			}
			gf.IntellectualObjectId = obj.Id
			if gf.IngestNeedsSave == false {
				continue
			}
			recorder.buildGenericFileChecksums(gf, ingestState)
			recorder.buildGenericFileEvents(gf, ingestState)

			if gf.IngestPreviousVersionExists {
				if gf.Id > 0 {
					existingFiles = append(existingFiles, gf)
				} else {
					recorder.logMissingId(ingestState, gf)
				}
			} else if gf.Id == 0 {
				newFiles = append(newFiles, gf)
			}
		}

		// Save this batch of files in Pharos
		recorder.createGenericFiles(ingestState, newFiles)
		recorder.updateGenericFiles(ingestState, existingFiles)

		// Update the GenericFile records in BoltDB
		recorder.saveGenericFilesInBoltDB(ingestState, db, newFiles)
		recorder.saveGenericFilesInBoltDB(ingestState, db, existingFiles)

		offset += len(batch)
		if len(batch) < GENERIC_FILE_BATCH_SIZE {
			break
		}
	}

}

func (recorder *APTRecorder) saveIntellectualObject(ingestState *models.IngestState, obj *models.IntellectualObject) {
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

	// PT #154003667: Make sure item is marked as active, since this is an ingest.
	// This fixes a rare case where an object has been deleted and comes back from
	// Pharos with State = "D", and now we're re-ingesting a new version of it.
	obj.State = "A"

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
	recorder.savePremisEventsForObject(ingestState, obj)
}

// createGenericFiles creates new GenericFile records in Pharos
func (recorder *APTRecorder) createGenericFiles(ingestState *models.IngestState, files []*models.GenericFile) {
	if len(files) == 0 {
		return
	}
	fileMap := make(map[string]*models.GenericFile, len(files))
	identifiers := make([]string, len(files))
	for i, gf := range files {
		fileMap[gf.Identifier] = gf
		identifiers[i] = gf.Identifier
	}
	resp := recorder.Context.PharosClient.GenericFileSaveBatch(files)
	if resp.Error != nil {
		body, _ := resp.RawResponseData()
		recorder.Context.MessageLog.Error(
			"Pharos returned this after attempt to save batch of GenericFiles:\n%s",
			string(body))
		recorder.Context.MessageLog.Error(
			"File identifiers in failed batch:\b%s", strings.Join(identifiers, ", "))
		ingestState.IngestManifest.RecordResult.AddError(resp.Error.Error())
	}
	// We may have managed to save some files despite the error.
	// If so, record what was saved.
	for _, savedFile := range resp.GenericFiles() {
		if savedFile == nil {
			// PT #157398417
			// This happens after GenericFileSaveBatch returns an error.
			recorder.Context.MessageLog.Warning("Nil GenericFile from resp.GenericFiles()")
			continue
		}
		gf := fileMap[savedFile.Identifier]
		if gf == nil {
			ingestState.IngestManifest.RecordResult.AddError("After save, could not find file '%s' "+
				"in batch.", savedFile.Identifier)
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

// updateGenericFiles updates existing GenericFile records in Pharos
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

// savePremisEventsForObject saves the object-level Premis events.
func (recorder *APTRecorder) savePremisEventsForObject(ingestState *models.IngestState, obj *models.IntellectualObject) {
	for i, event := range obj.PremisEvents {
		if event.Id > 0 {
			recorder.Context.MessageLog.Info("PremisEvent %d has already been saved", event.Id)
			continue
		}
		event.IntellectualObjectId = obj.Id
		resp := recorder.Context.PharosClient.PremisEventSave(event)
		if resp.Error != nil {
			method := "??"
			url := "??"
			if resp.Request != nil {
				method = resp.Request.Method
				if resp.Request.URL != nil {
					url = resp.Request.URL.String()
				}
			}
			ingestState.IngestManifest.RecordResult.AddError(
				"While saving events for '%s', error adding PremisEvent '%s'."+
					"Method: %s, URL: %s, Error: %v",
				obj.Identifier, event.EventType, method, url, resp.Error)
		} else {
			obj.PremisEvents[i].MergeAttributes(resp.PremisEvent())
		}
	}
}

// deleteBagFromReceivingBucket deletes the original tar file from the
// depositor's receiving bucket.
func (recorder *APTRecorder) deleteBagFromReceivingBucket(ingestState *models.IngestState) {
	var obj *models.IntellectualObject
	db, err := storage.NewBoltDB(ingestState.IngestManifest.DBPath)
	if err != nil {
		recorder.Context.MessageLog.Warning("Can't open valdb: %v", err)
	}
	if db != nil {
		obj, err = db.GetIntellectualObject(db.ObjectIdentifier())
		if err != nil {
			recorder.Context.MessageLog.Warning("Can't get %s from valdb: %v", db.ObjectIdentifier(), err)
		}
		if obj == nil {
			recorder.Context.MessageLog.Warning("Get %s from valdb returned nil", db.ObjectIdentifier())
		}
		defer db.Close()
	}

	ingestState.IngestManifest.CleanupResult.Start()
	ingestState.IngestManifest.CleanupResult.Attempted = true
	ingestState.IngestManifest.CleanupResult.AttemptNumber += 1

	// Remove the bag from the receiving bucket, if ingest succeeded
	if !recorder.bucketVersionMatchesCurrentVersion(ingestState) {
		recorder.Context.MessageLog.Info(
			"Skipping deletion of %s in WorkItem %d "+
				"because the etag of the tar file in "+
				"the receiving bucket does not match the etag of the bag "+
				"just ingested. (Hint: depositor uploaded a new version "+
				"during ingest and we'll need to ingest that next.)",
			ingestState.IngestManifest.S3Key, ingestState.WorkItem.Id)
		// Set deletion timestamp, so we know this method was called.
		if obj != nil {
			db.Save(obj.Identifier, obj)
		}
		ingestState.IngestManifest.CleanupResult.Finish()
		return
	}
	if recorder.Context.Config.DeleteOnSuccess == false {
		// We don't actually delete files if config is dev, test, or integration.
		recorder.Context.MessageLog.Info("Skipping deletion step because config.DeleteOnSuccess == false")
		// Set deletion timestamp, so we know this method was called.
		if obj != nil {
			obj.IngestDeletedFromReceivingAt = time.Now().UTC()
			db.Save(obj.Identifier, obj)
		}
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
		if obj != nil {
			obj.IngestDeletedFromReceivingAt = time.Now().UTC()
			db.Save(obj.Identifier, obj)
		}
	}
	ingestState.IngestManifest.CleanupResult.Finish()
}

func (recorder *APTRecorder) buildGenericFileChecksums(gf *models.GenericFile, ingestState *models.IngestState) {
	err := gf.BuildIngestChecksums()
	if err != nil {
		ingestState.IngestManifest.RecordResult.AddError(err.Error())
		ingestState.IngestManifest.RecordResult.ErrorIsFatal = true
	}
}

func (recorder *APTRecorder) buildGenericFileEvents(gf *models.GenericFile, ingestState *models.IngestState) {
	err := gf.BuildIngestEvents()
	if err != nil {
		ingestState.IngestManifest.RecordResult.AddError(err.Error())
		ingestState.IngestManifest.RecordResult.ErrorIsFatal = true
	}
}

func (recorder *APTRecorder) saveGenericFilesInBoltDB(ingestState *models.IngestState, db *storage.BoltDB, genericFiles []*models.GenericFile) {
	for _, gf := range genericFiles {
		err := db.Save(gf.Identifier, gf)
		if err != nil {
			ingestState.IngestManifest.RecordResult.AddError(
				"After post to Pharos, error saving %s to valdb: %v",
				gf.Identifier, err.Error())
		}
	}
}

// bucketVersionMatchesCurrentVersion returns true if the e-tag of a
// bag (tar file) in the S3 receiving bucket matches the e-tag of the
// bag we're currently working on. When the tags match, we can safely
// delete the tar file from the receiving bucket.
//
// The problematic case we're trying to avoid is when a depositor has
// uploaded a new version of an existing bag WHILE we're still ingesting
// an older version. In that case, the proper thing to do is to finish
// ingesting the old version and then ingest the new one. We cannot ingest
// the new one if this code deletes it from the receiving bucket.
//
// If the version in the receiving bucket does not match the version we
// just ingested, let's not delete it from the receiving bucket, because
// we will probably start ingesting it soon.
//
// Part of https://trello.com/c/GLURkoKW
func (recorder *APTRecorder) bucketVersionMatchesCurrentVersion(ingestState *models.IngestState) bool {
	eTagMatches := false
	s3ObjectList := network.NewS3ObjectList(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		ingestState.IngestManifest.S3Bucket,
		int64(100),
	)
	s3ObjectList.GetList(ingestState.IngestManifest.S3Key)

	if s3ObjectList.ErrorMessage != "" {
		recorder.Context.MessageLog.Warning(
			"Error checking receiving bucket %s for key %s: %s",
			ingestState.IngestManifest.S3Bucket,
			ingestState.IngestManifest.S3Key,
			s3ObjectList.ErrorMessage)
	}
	// There can really only be one object with this key,
	// but we loop in case someone uploaded an object whose
	// name starts with this key.
	for _, s3Object := range s3ObjectList.Response.Contents {
		if *s3Object.Key == ingestState.IngestManifest.S3Key &&
			strings.Replace(*s3Object.ETag, "\"", "", -1) == ingestState.WorkItem.ETag {
			eTagMatches = true
			break
		}
	}
	return eTagMatches
}

// --------- Messages --------------

func (recorder *APTRecorder) logFailure(ingestState *models.IngestState) {
	recorder.Context.MessageLog.Error("Failed to record %s/%s. Errors: %s.",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name,
		ingestState.IngestManifest.AllErrorsAsString())
}

func (recorder *APTRecorder) logRequeue(ingestState *models.IngestState) {
	recorder.Context.MessageLog.Info("Requeueing WorkItem %d (%s/%s) due to transient errors. %s",
		ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
		ingestState.WorkItem.Name,
		ingestState.IngestManifest.AllErrorsAsString())
}

func (recorder *APTRecorder) logSaveError(ingestState *models.IngestState) {
	recorder.Context.MessageLog.Error("Error saving IntellectualObject %s/%s: %v",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name,
		ingestState.IngestManifest.RecordResult.AllErrorsAsString())
}

func (recorder *APTRecorder) logSaveSuccess(ingestState *models.IngestState) {
	recorder.Context.MessageLog.Info("Saved %s/%s with id %d",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name,
		ingestState.IngestManifest.Object.Id)
}

func (recorder *APTRecorder) logNoNeedToSave(ingestState *models.IngestState) {
	recorder.Context.MessageLog.Info(
		"No need to save %s/%s already has id %d",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name,
		ingestState.IngestManifest.Object.Id)
}

func (recorder *APTRecorder) logMissingId(ingestState *models.IngestState, gf *models.GenericFile) {
	msg := fmt.Sprintf("GenericFile %s has a previous version, but its Id is missing.",
		gf.Identifier)
	recorder.Context.MessageLog.Error(msg)
	ingestState.IngestManifest.RecordResult.AddError(msg)
}
