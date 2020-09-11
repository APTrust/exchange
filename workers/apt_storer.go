package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/storage"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/nsqio/go-nsq"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 15 seemed to be the magic number in the first generation of the software.
// On large uploads, network errors are common.
const MAX_UPLOAD_ATTEMPTS = 15
const FIFTY_MEGABYTES = int64(52428800)
const FIFTY_GIGABYTES = int64(50000000000)
const TWO_HUNDRED_GIGABYTES = int64(200000000000)

const HIGH_FILE_COUNT = 5000
const RESOURCE_REQUEUE_TIMEOUT = 1000 * 60 * 60 // one hour

const LARGE_BAG_IN_PROGRESS = "LargeBagInProgress"
const HIGH_FILE_BAG_IN_PROGRESS = "HighFileBagInProgress"

// Special to deal with huge Fedora and DSpace dumps.
const SMALL_FILE_SIZE = int64(300000)

// Stores GenericFiles in long-term storage (S3 and Glacier).
type APTStorer struct {
	Context        *context.Context
	StorageChannel chan *models.IngestState
	CleanupChannel chan *models.IngestState
	RecordChannel  chan *models.IngestState
	SyncMap        *models.SynchronizedMap
}

func NewAPTStorer(_context *context.Context) *APTStorer {
	storer := &APTStorer{
		Context: _context,
		SyncMap: models.NewSynchronizedMap(),
	}

	// Patch for https://trello.com/c/Ep4pKzZB
	err := CacheBucketNames(_context)
	if err != nil {
		panic(fmt.Sprintf("Cannot cache bucket names from Pharos: %v", err))
	}

	// Set up buffered channels
	workerBufferSize := _context.Config.StoreWorker.Workers * 10
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
func (storer *APTStorer) HandleMessage(message *nsq.Message) error {
	log := storer.Context.MessageLog
	ingestState, err := GetIngestState(message, storer.Context, false)
	if err != nil {
		storer.Context.MessageLog.Error(err.Error())
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
	ingestState.IngestManifest.StoreResult.ClearErrors()

	// Tell Pharos that we've started to store this item.
	err = MarkWorkItemStarted(ingestState, storer.Context,
		constants.StageStore, "Files are being copied to long-term storage.")
	if err != nil {
		storer.Context.MessageLog.Error(err.Error())
		return err
	}

	storer.Context.MessageLog.Info("Putting %s/%s into storage channel",
		ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key)

	storer.StorageChannel <- ingestState

	// Return no error, so NSQ knows we're OK.
	return nil
}

// -------------------------------------------------------------------------
// Step 1 of 3: Put the item in long-term storage
//
// -------------------------------------------------------------------------
func (storer *APTStorer) store() {
	for ingestState := range storer.StorageChannel {

		ingestState.IngestManifest.StoreResult.Start()
		ingestState.IngestManifest.StoreResult.Attempted = true
		ingestState.IngestManifest.StoreResult.AttemptNumber += 1

		start := 0
		limit := storer.Context.Config.StoreWorker.NetworkConnections

		// Do this before trying to open the DB, because opening creates
		// a valdb file if it doesn't already exist. This leaves empty
		// valdb files in the data directory.
		if !fileutil.FileExists(ingestState.IngestManifest.DBPath) {
			msg := fmt.Sprintf("BoltDB file %s is missing.", ingestState.IngestManifest.DBPath)
			ingestState.IngestManifest.StoreResult.AddError(msg)
			ingestState.IngestManifest.StoreResult.ErrorIsFatal = true
			ingestState.IngestManifest.StoreResult.Finish()
			storer.CleanupChannel <- ingestState
			continue

		}

		db, err := storage.NewBoltDB(ingestState.IngestManifest.DBPath)
		if err != nil {
			ingestState.IngestManifest.StoreResult.AddError(
				"In store(), error opening db %s: %v",
				ingestState.IngestManifest.DBPath, err.Error())
			ingestState.IngestManifest.StoreResult.Finish()
			storer.CleanupChannel <- ingestState
			continue
		}
		if db == nil {
			// Happens when a prior worker process is killed,
			// e.g. through supervisord, system restart, etc.
			ingestState.IngestManifest.StoreResult.AddError(
				"In store(), db %s is missing or empty. This object may have "+
					"already been ingested and recorded.",
				ingestState.IngestManifest.DBPath)
			ingestState.IngestManifest.StoreResult.Finish()
			storer.CleanupChannel <- ingestState
			continue
		}
		objIdentifier, err := ingestState.IngestManifest.ObjectIdentifier()
		if err != nil {
			ingestState.IngestManifest.StoreResult.AddError(err.Error())
			ingestState.IngestManifest.StoreResult.Finish()
			storer.CleanupChannel <- ingestState
			continue
		}

		// If this object was previously ingested, we need to store it
		// in the same place as the original ingest. Otherwise, we'd have
		// multiple versions in multiple places. So here, we force the
		// StorageOption to whatever the original StorageOption was.
		//
		// The following test bags should verify this through integration
		// tests:
		//
		// testdata/unit_test_bags/updated/example.edu.sample_glacier_oh.tar
		// testdata/unit_test_bags/updated/example.edu.tagsample_good.tar
		existingStorageOption, err := storer.setStorageOption(db, objIdentifier)
		if err != nil {
			msg := fmt.Sprintf("While trying to get original storage option, "+
				"error looking up IntellectualObject in Pharos or BoltDB: %v", err)
			ingestState.IngestManifest.StoreResult.AddError(msg)
			ingestState.IngestManifest.StoreResult.Finish()
			storer.CleanupChannel <- ingestState
			continue
		}

		// Don't try to process too many high resource items at once.
		// Bags > 200 GB can take a lot of memory store because S3 client
		// has to read chunks into memory before sending them. Chunks can
		// be up to 500 MB each for very large (5 TB) files.
		//
		// Bags with > 5000 files can take a long time to store because
		// we have to keep scanning the tar file and pulling them out,
		// one by one. There's also a lot of HTTPS overhead when writing
		// lots of small files to S3/Glacier.
		continueProcessing, hasManySmallFiles, requeueMessage := storer.checkForHighResourceBag(db, objIdentifier)
		if !continueProcessing {
			storer.Context.MessageLog.Info("[High Resource Bag] Requeueing %s: %s", objIdentifier, requeueMessage)
			ingestState.IngestManifest.StoreResult.AddError(requeueMessage)
			ingestState.IngestManifest.StoreResult.Finish()
			storer.CleanupChannel <- ingestState
			continue
		}

		// For large Fedora/DSpace dumps were we get 200k tiny files
		// in a bag.
		if hasManySmallFiles {
			limit = limit * 20
			storer.Context.MessageLog.Info("Bag %s has many small files. Increasing batch size to %d", objIdentifier, limit)
		}

		for {
			// Get a batch of files to save...
			storageSummaries, hasMoreFiles, err := storer.getStorageSummaryBatch(db, objIdentifier, start, limit)
			if err != nil {
				ingestState.IngestManifest.StoreResult.AddError(err.Error())
				ingestState.IngestManifest.StoreResult.ErrorIsFatal = true
				break
			}
			fileCount := len(storageSummaries)

			// Save them concurrently...
			storer.Context.MessageLog.Info("Saving batch of %d files for %s", fileCount, objIdentifier)
			wg := sync.WaitGroup{}
			wg.Add(fileCount)
			for i := 0; i < fileCount; i++ {
				// Hacked in for Glacier-Only option.
				// Force StorageOption to match original ingest, if there
				// was a prior ingest.
				//
				// The following test bags should verify this through integration
				// tests:
				//
				// testdata/unit_test_bags/updated/example.edu.sample_glacier_oh.tar
				// testdata/unit_test_bags/updated/example.edu.tagsample_good.tar
				if existingStorageOption != "" {
					storageSummaries[i].GenericFile.StorageOption = existingStorageOption
				}

				go func(storageSummary *models.StorageSummary) {
					defer wg.Done()
					storer.saveFile(db, storageSummary)
				}(storageSummaries[i])
			}
			wg.Wait()
			storer.Context.MessageLog.Info("Finished batch of %d files for %s", fileCount, objIdentifier)

			// Tell NSQ we're still on this. Very large files take a long time
			// to copy, and if NSQ doesn't hear from us, it'll assume we timed out.
			ingestState.TouchNSQ()

			// SaveFile and the functions it calls have a pointer to our
			// GenericFile, so it updates that record directly. However,
			// we have to manually copy over any errors that may have
			// occurred.
			for _, storageSummary := range storageSummaries {
				for _, errMsg := range storageSummary.StoreResult.Errors {
					ingestState.IngestManifest.StoreResult.AddError(errMsg)
				}
				if storageSummary.StoreResult.ErrorIsFatal {
					ingestState.IngestManifest.StoreResult.ErrorIsFatal = true
					break
				}
			}

			// Update for the next batch, or stop if there are no more files.
			start += len(storageSummaries)
			if hasMoreFiles == false {
				storer.Context.MessageLog.Info("No more files for %s", objIdentifier)
				break
			}
		}

		db.Close()
		storer.CleanupChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 2 of 3: Delete the bag file(s) if storage succeeded
//
// -------------------------------------------------------------------------
func (storer *APTStorer) cleanup() {
	for ingestState := range storer.CleanupChannel {
		if ingestState.IngestManifest.StoreResult.HasErrors() == false &&
			ingestState.IngestManifest.Object.AllFilesSaved() {
			storer.logDeletingTarFile(ingestState)
			// Delete the bag (the .tar file) but not the .valdb, because
			// .valdb contains information about the object, generic files,
			// and premis events that will be recorded by apt_recorder.
			DeleteFileFromStaging(ingestState.IngestManifest.BagPath, storer.Context)
		}
		storer.RecordChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 3 of 3: Record WorkItem and WorkItemState in Pharos, and push
//              to the apt_record_topic queue if all went well.
//
// -------------------------------------------------------------------------
func (storer *APTStorer) record() {
	for ingestState := range storer.RecordChannel {

		// Copy JSON representation of the IngestManifest to Pharos
		// and to the JSON log.
		ingestState.IngestManifest.StoreResult.Finish()

		objIdentifier, _ := ingestState.IngestManifest.ObjectIdentifier()
		if objIdentifier != "" {
			storer.clearHighResourceBag(objIdentifier)
		}

		// See if we have fatal errors, or too many recurring transient errors
		attemptNumber := ingestState.IngestManifest.StoreResult.AttemptNumber
		maxAttempts := storer.Context.Config.StoreWorker.MaxAttempts
		itsTimeToGiveUp := (ingestState.IngestManifest.HasFatalErrors() ||
			(ingestState.IngestManifest.HasErrors() && attemptNumber >= maxAttempts))

		if itsTimeToGiveUp {
			storer.logFailedToStore(ingestState)
			ingestState.FinishNSQ()
			MarkWorkItemFailed(ingestState, storer.Context)
		} else if ingestState.IngestManifest.StoreResult.HasErrors() {
			timeout := 30000 // thirty seconds
			if strings.Contains(ingestState.IngestManifest.StoreResult.Errors[0], "[High Resource Bag]") {
				storer.Context.MessageLog.Info("Setting long timeout for high resource bag %s", objIdentifier)
				timeout = RESOURCE_REQUEUE_TIMEOUT
			}
			storer.logRequeued(ingestState)
			ingestState.RequeueNSQ(timeout)
			MarkWorkItemRequeued(ingestState, storer.Context)
		} else {
			storer.logFinishedStoring(ingestState)
			ingestState.FinishNSQ()
			MarkWorkItemSucceeded(ingestState, storer.Context, constants.StageRecord)
			PushToQueue(ingestState, storer.Context, storer.Context.Config.RecordWorker.NsqTopic)
		}

		LogJson(ingestState, storer.Context.JsonLog)
		RecordWorkItemState(ingestState, storer.Context, ingestState.IngestManifest.FetchResult)
	}
}

// getStorageSummaryBatch returns a batch of storage summary objects
// and boolean indicating whether the object has more files to get.
func (storer *APTStorer) getStorageSummaryBatch(db *storage.BoltDB, objIdentifier string, start, limit int) (storageSummaries []*models.StorageSummary, hasMoreFiles bool, err error) {
	obj, err := db.GetIntellectualObject(objIdentifier)
	if err != nil {
		return nil, false, err
	}
	storer.Context.MessageLog.Info("Getting batch of %d files for %s, starting at %d",
		limit, objIdentifier, start)
	identifiers := db.FileIdentifierBatch(start, limit)
	hasMoreFiles = len(identifiers) == limit
	storageSummaries = make([]*models.StorageSummary, len(identifiers))
	for i, gfIdentifier := range identifiers {
		gf, err := db.GetGenericFile(gfIdentifier)
		if err != nil {
			return nil, false, err
		}
		summary, err := models.NewStorageSummary(gf, obj.IngestTarFilePath, obj.IngestUntarredPath)
		if err != nil {
			return nil, false, err
		}
		storer.Context.MessageLog.Info("Adding %s to batch", gf.Identifier)
		storageSummaries[i] = summary
	}
	return storageSummaries, hasMoreFiles, nil
}

func (storer *APTStorer) saveFile(db *storage.BoltDB, storageSummary *models.StorageSummary) {
	gf := storageSummary.GenericFile
	if util.LooksLikeJunkFile(gf.OriginalPath()) && (gf.IngestManifestMd5 != "" || gf.IngestManifestSha256 != "") {
		// A.D. 2017-09-21. Normally, we ignore Mac junk files that
		// begin dot-underscore, like ._DS_Store files, because these
		// creep into the tar files without the bagger knowing, and
		// the do not show up in the manifests. For Pivotal Tracker issue
		// https://www.pivotaltracker.com/story/show/151265762, we DO
		// want to keep these junk files if they are listed in the
		// manifest, because that means the bagger knew they were there
		// and intended to keep them.
		gf.IngestNeedsSave = true
		storer.Context.MessageLog.Info("Junk file %s will be saved because it appears in manifest", gf.Identifier)
	} else if !util.HasSavableName(gf.OriginalPath()) {
		// We don't need to save bagit.txt, or certain manifests.
		gf.IngestNeedsSave = false
	} else {
		existingSha256, err := storer.getExistingSha256(gf.Identifier)
		if err != nil {
			storer.Context.MessageLog.Error(err.Error())
			storageSummary.StoreResult.AddError(err.Error())
			return
		}
		// Set this, for the record.
		if existingSha256 != nil {
			gf.IngestPreviousVersionExists = true
			gf.Id = existingSha256.GenericFileId
			// We don't need to save files that were ingested
			// previously and have not changed.
			storer.changedSincePreviousVersion(storageSummary, existingSha256)
		}
	}

	// Now copy to storage only if the file has changed.
	if gf.IngestNeedsSave {
		storer.Context.MessageLog.Info("File %s needs save", gf.Identifier)
		if gf.StorageOption == constants.StorageStandard {
			if gf.IngestStoredAt.IsZero() || gf.IngestStorageURL == "" {
				storer.copyToLongTermStorage(storageSummary, "s3")
			}
			if gf.IngestReplicatedAt.IsZero() || gf.IngestReplicationURL == "" {
				storer.copyToLongTermStorage(storageSummary, "glacier")
			}
		} else {
			// A.D. 2020-06-10: Don't re-upload unnecessarily.
			if gf.IngestStoredAt.IsZero() || gf.IngestStorageURL == "" {
				storer.Context.MessageLog.Info("Skipping S3 because file %s is %s", gf.Identifier, gf.StorageOption)
				// Send directly to Glacier VA, OH or OR.
				storer.copyToLongTermStorage(storageSummary, gf.StorageOption)
			} else {
				storer.Context.MessageLog.Info("Skipping upload of %s because it was stored at %s at %s", gf.Identifier, gf.IngestStorageURL, gf.IngestStoredAt.Format(time.RFC3339))
			}
		}
		// Don't do cleanup until both copies are saved.
		defer storer.cleanupTempFile(gf)
	} else {
		if !util.HasSavableName(gf.OriginalPath()) {
			storer.Context.MessageLog.Info("Skipping %s: doesn't have savable name", gf.Identifier)
		} else {
			storer.Context.MessageLog.Info("Skipping %s: unchanged since previous save", gf.Identifier)
		}
	}
	err := db.Save(gf.Identifier, gf)
	if err != nil {
		msg := fmt.Sprintf("Error saving %s to db %s: %v", gf.Identifier, db.FilePath(), err)
		storageSummary.StoreResult.AddError(msg)
		storer.Context.MessageLog.Error(msg)
	}
}

// changedSincePreviousVersion asks Pharos if a version of this file already
// exists from a prior ingest. If it does, and the checksum of the new
// version matches the checksum of the prior version, we don't need to
// re-save this file.
func (storer *APTStorer) changedSincePreviousVersion(storageSummary *models.StorageSummary, existingSha256 *models.Checksum) {
	gf := storageSummary.GenericFile
	uuid, err := storer.getUuidOfExistingFile(gf.Identifier)
	if err != nil {
		message := fmt.Sprintf("Cannot find existing UUID for %s: %v", gf.Identifier, err.Error())
		storageSummary.StoreResult.AddError(message)
		storer.Context.MessageLog.Error(message)
		// Probably not fatal, but treat it as such for now,
		// because we don't want leave orphan objects in S3,
		// or have the GenericFile.URL not match the actual
		// storage URL. This should only happen if a depositor
		// deletes the existing version of a GenericFile while
		// we are processing this ingest. The window for that
		// to happen is usually between a few seconds and a few
		// hours.
		storageSummary.StoreResult.ErrorIsFatal = true
		return
	}
	if uuid == "" {
		message := fmt.Sprintf("Cannot find existing UUID for %s.", gf.Identifier)
		storageSummary.StoreResult.AddError(message)
		storer.Context.MessageLog.Error(message)
		// Probably not fatal, but treat it as such for now.
		// Same note as in previous if statement above.
		storageSummary.StoreResult.ErrorIsFatal = true
		return
	} else {
		// OK. Set the GenericFile's UUID to match the existing file's
		// UUID, so that we overwrite the existing file, and so the
		// GenericFile record in Pharos still has the correct URL.
		message := fmt.Sprintf("Resetting UUID for '%s' to '%s' so we can overwrite "+
			"the currently stored version of the file.",
			gf.Identifier, uuid)
		storer.Context.MessageLog.Info(message)
		gf.IngestUUID = uuid
	}

	if existingSha256.Digest == gf.IngestSha256 {
		storer.Context.MessageLog.Info(
			"GenericFile %s has same sha256. Does not need save.", gf.Identifier)
		gf.IngestNeedsSave = false
	}
}

// Get the existing sha256 checksum for the generic file, if there is one.
// In some cases, depositors upload a new version of a bag that includes
// unchanged versions of some files. So we check the sha256 of the
// existing version against the sha256 of the one just uploaded. If they're
// the same, we don't bother overwriting the existing file.
func (storer *APTStorer) getExistingSha256(gfIdentifier string) (*models.Checksum, error) {
	storer.Context.MessageLog.Info("Checking Pharos for existing sha256 digest for %s",
		gfIdentifier)
	params := url.Values{}
	params.Add("generic_file_identifier", gfIdentifier)
	params.Add("algorithm", constants.AlgSha256)
	// PT #145151935: Sort by datetime, not created_at
	params.Add("sort", "datetime DESC")
	resp := storer.Context.PharosClient.ChecksumList(params)
	if resp.Error != nil {
		return nil, resp.Error
	}
	existingChecksum := resp.Checksum()
	if existingChecksum == nil {
		return nil, nil
	}
	return existingChecksum, nil
}

// Returns the UUID of an existing GenericFile. The UUID is the last component
// of the S3 storage URL. When we are updating an existing GenericFile, we want
// to overwrite the object in S3/Glacier rather than writing a new one and
// leaving the old one hanging around. To overwrite it, we must know its UUID.
func (storer *APTStorer) getUuidOfExistingFile(gfIdentifier string) (string, error) {
	storer.Context.MessageLog.Info("Checking Pharos for existing UUID for GenericFile %s",
		gfIdentifier)
	resp := storer.Context.PharosClient.GenericFileGet(gfIdentifier, false)
	if resp.Error != nil {
		storer.Context.MessageLog.Warning("Error getting URL %s", resp.Request.URL.String())
		return "", resp.Error
	}
	uuid := ""
	existingGenericFile := resp.GenericFile()
	if resp.Error != nil {
		return "", fmt.Errorf("Pharos cannot find supposedly existing GenericFile '%s'", gfIdentifier)
	}
	parts := strings.Split(existingGenericFile.URI, "/")
	uuid = parts[len(parts)-1]
	if !util.LooksLikeUUID(uuid) {
		return "", fmt.Errorf("Could not extract UUID from URI %s", existingGenericFile.URI)
	}
	return uuid, nil
}

// getPharosObjectStorageOption returns the StorageOption of the
// IntellectualObject from Pharos. If this object was previously ingested,
// we need to store it in the same place as the original ingest. Otherwise,
// we'd have multiple versions in multiple places.
func (storer *APTStorer) setStorageOption(db *storage.BoltDB, objIdentifier string) (string, error) {
	storer.Context.MessageLog.Info("Checking Pharos for original storage type of object %s",
		objIdentifier)
	resp := storer.Context.PharosClient.IntellectualObjectGet(objIdentifier, false, false)

	// Not found should be common, as most ingests are first-time ingests.
	if resp.Response.StatusCode == http.StatusNotFound {
		return "", nil
	}

	// If we have some other error, that's a problem.
	if resp.Error != nil {
		storer.Context.MessageLog.Error("Error getting URL %s", resp.Request.URL.String())
		return "", resp.Error
	}

	existingObject := resp.IntellectualObject()
	if existingObject == nil {
		storer.Context.MessageLog.Info("No existing Pharos object %s, so no need to reset StorageOption",
			objIdentifier)
		return "", nil
	}
	if existingObject.State == "D" {
		storer.Context.MessageLog.Info("Ignoring existing Pharos object %s because state = 'D'",
			objIdentifier)
		return "", nil
	}

	obj, err := db.GetIntellectualObject(objIdentifier)
	if err != nil {
		return "", fmt.Errorf("Can't get IntellectualObject from BoltDB: %v", err)
	}
	if obj == nil {
		return "", fmt.Errorf("BoltDB returned nothing for object identifier: %s", objIdentifier)
	}

	// Force the StorageOption of the item we're ingesting to match the
	// existing (non-deleted) object in Pharos.
	if obj.StorageOption != existingObject.StorageOption {
		storer.Context.MessageLog.Info("Changing StorageOption %s on object %s to %s "+
			"to match StorageOption of existing object in Pharos.",
			obj.StorageOption, objIdentifier, existingObject.StorageOption)
		obj.StorageOption = existingObject.StorageOption
		db.Save(objIdentifier, obj)
	}
	return existingObject.StorageOption, nil
}

// Copy the GenericFile to long-term storage in S3 or Glacier
func (storer *APTStorer) copyToLongTermStorage(storageSummary *models.StorageSummary, sendWhere string) {
	gf := storageSummary.GenericFile
	if !storer.uuidPresent(storageSummary) {
		msg := fmt.Sprintf("Cannot copy GenericFile %s to long-term storage because UUID is missing",
			gf.Identifier)
		storageSummary.StoreResult.AddError(msg)
		storer.Context.MessageLog.Error(msg)
		return
	}
	storer.Context.MessageLog.Info("Sending %s to %s", gf.Identifier, sendWhere)
	for attemptNumber := 1; attemptNumber <= MAX_UPLOAD_ATTEMPTS; attemptNumber++ {
		storer.doUpload(storageSummary, sendWhere, attemptNumber)
		// Stop trying if storage succeeded
		if sendWhere == "glacier" && gf.IngestReplicatedAt.IsZero() == false {
			break
		} else if sendWhere != "glacier" && gf.IngestStoredAt.IsZero() == false {
			// Covers "s3", "Glacier-VA", "Glacier-OH" and "Glacier-OR"
			break
		}
	}
}

func (storer *APTStorer) doUpload(storageSummary *models.StorageSummary, sendWhere string, attemptNumber int) {
	gf := storageSummary.GenericFile
	uploader := storer.initUploader(storageSummary, sendWhere)
	if uploader == nil {
		msg := "S3 uploader is nil. Cannot proceed."
		storageSummary.StoreResult.AddError(msg)
		storer.Context.MessageLog.Error(msg)
		return // We have some config problem here. Stop trying.
	}
	if !storer.assertRequiredMetadata(storageSummary, uploader) {
		return
	}
	tarFileIterator, readCloser := storer.getReadCloser(storageSummary)
	if readCloser != nil && tarFileIterator != nil {
		defer readCloser.Close()
		defer tarFileIterator.Close()

		// Handle large files. Amazon's moronic uploader will read the
		// entire file into memory, unless we give it a reader that
		// supports both Seek() and ReadAt(). We cannot convert a tarReader
		// to do that, because the underlying reader doesn't support
		// ReadAt(). So we have to copy the entire file to disk and then
		// pass the uploader a File object, which does support those
		// methods. Fun.
		reader := readCloser
		if gf.Size > constants.S3LargeFileSize {
			reader, err := storer.getFileReader(readCloser, gf, attemptNumber)
			if err != nil {
				errMsg := fmt.Sprintf("Error copying '%s' from tarfile to "+
					"filesystem at '%s' for large file upload: %v", gf.Identifier,
					storer.getTempFilePath(gf), err)
				storer.Context.MessageLog.Error(errMsg)
				storageSummary.StoreResult.AddError(errMsg)
				return
			}
			defer reader.Close()
		} else {
			storer.Context.MessageLog.Info("Upload file %s (size: %d) directly "+
				"to %s from the tar file", gf.Identifier, gf.Size, sendWhere)
		}

		storer.Context.MessageLog.Info("Starting to upload file %s (size: %d) to %s",
			gf.Identifier, gf.Size, sendWhere)

		// Now do the upload using the tar file reader for smaller files
		// and the File reader for very large files.
		uploader.SendWithSize(reader, gf.Size)

		// For large files, give S3 some time to catch up.
		// On a 50GB+ upload with thousands of parts, S3 seems to always
		// give the wrong size if we ask within milliseconds of the
		// upload completing. Part of PT #148913619.
		if gf.Size > FIFTY_GIGABYTES {
			time.Sleep(10 * time.Second)
		}

		// PT #143660373: S3 zero-size file bug.
		// S3 returns some very weird stuff here,
		// sometimes zero, sometimes 10x the actual file size.
		s3Obj := storer.getS3FileDetail(uploader.AWSRegion, *uploader.UploadInput.Bucket, gf.IngestUUID)
		if s3Obj == nil {
			errMsg := fmt.Sprintf("%s returned nothing for %s (%s).", sendWhere, gf.IngestUUID, gf.Identifier)
			if attemptNumber == MAX_UPLOAD_ATTEMPTS {
				storageSummary.StoreResult.AddError(errMsg)
			} else {
				storer.Context.MessageLog.Warning(errMsg + ". Will retry.")
			}
		} else if *s3Obj.Size != gf.Size {
			errMsg := fmt.Sprintf("%s returned size %d for %s (%s), should be %d.",
				sendWhere, s3Obj.Size, gf.IngestUUID, gf.Identifier, gf.Size)
			if attemptNumber == MAX_UPLOAD_ATTEMPTS {
				storageSummary.StoreResult.AddError(errMsg)
			} else {
				storer.Context.MessageLog.Warning(errMsg + " Will retry.")
			}
		}
		uploadSucceeded := (s3Obj != nil && *s3Obj.Size == gf.Size && uploader.ErrorMessage == "")

		if uploadSucceeded {
			storer.Context.MessageLog.Info("Stored %s in %s after %d attempts",
				gf.Identifier, sendWhere, attemptNumber)
			storer.markFileAsStored(gf, sendWhere, uploader.Response.Location)
			return // Upload succeeded
		} else if uploader.ErrorMessage != "" {
			storer.Context.MessageLog.Error("Upload error for %s: %s",
				gf.Identifier, uploader.ErrorMessage)
			if attemptNumber == MAX_UPLOAD_ATTEMPTS {
				storageSummary.StoreResult.AddError(uploader.ErrorMessage)
			}
		}
	} else {
		storer.Context.MessageLog.Error("Could not get reader from tar file %s.", storageSummary.TarFilePath)
	}
}

// See the comment above, that begins "Handle large files."
// We put temp files on the /mnt, not in /tmp, because they
// may be too large for the root partition.
func (storer *APTStorer) getFileReader(reader io.Reader, gf *models.GenericFile, attemptNumber int) (*os.File, error) {
	var err error
	var tempFile *os.File
	filePath := storer.getTempFilePath(gf)
	// PT #143660373: S3 zero-size file bug.
	// We have to copy larger files from the tar archive to disk,
	// so the AWS S3 uploader doesn't read them into memory.
	// When creating large files on EFS, the first attempt to
	// read them results in a zero-length read, and a zero-length
	// file being written to S3. So here, we try copying the file
	// to disk, closing the file handle, and re-opening it to see
	// if we can get a reliable file reader from EFS.
	if !fileutil.FileExists(filePath) {
		err = storer.createTempFile(reader, gf, attemptNumber)
		if err != nil {
			return nil, err
		}
	}

	// Have to stat twice now, first to check for zero-length files
	// that sometimes show up during heavy ingest. Possibly due to
	// writes being started as services are shutting down.
	stat, err := os.Stat(filePath)
	if err != nil {
		storer.Context.MessageLog.Error("Can't stat %s (%s): %v", filePath, gf.Identifier, err)
	}
	// If zero-size file exists, fix it by recopying.
	if stat != nil && stat.Size() == int64(0) {
		storer.Context.MessageLog.Error("Attempting to fix zero-length temp file %s (%s)", filePath, gf.Identifier)
		err = storer.createTempFile(reader, gf, attemptNumber)
		if err != nil {
			return nil, err
		}
		storer.Context.MessageLog.Error("Fixed zero-length temp file %s (%s)", filePath, gf.Identifier)
	}

	// Now proceed to upload the temp file if all looks well.
	stat, err = os.Stat(filePath)
	if err != nil {
		storer.Context.MessageLog.Error("Can't stat %s (%s): %v", filePath, gf.Identifier, err)
	}

	if stat != nil && stat.Size() == gf.Size {
		tempFile, err = os.Open(filePath)
		if err == nil {
			storer.Context.MessageLog.Info("Using existing temp file at %s "+
				"for %s (Attempt %d)", filePath, gf.Identifier, attemptNumber)
		} else {
			err = fmt.Errorf("Error opening %s (%s): %v", filePath, gf.Identifier, err)
			storer.Context.MessageLog.Error(err.Error())
			return nil, err
		}
		// PT #143660373: S3 zero-size file bug.
		measuredSize := storer.getActualFileSize(tempFile, filePath)
		if measuredSize != gf.Size {
			err = fmt.Errorf("Wrong actual size for %s (%s). Should be %d, got %d",
				filePath, gf.Identifier, gf.Size, measuredSize)
			storer.Context.MessageLog.Error(err.Error())
			return nil, err
		} else {
			storer.Context.MessageLog.Info("Actual measured size of %s is %d", filePath, measuredSize)
		}
	} else {
		err = fmt.Errorf("Temp file for %s at %s is missing or wrong size", gf.Identifier, filePath)
	}
	return tempFile, err
}

// TODO: Move this to where it can be unit tested.
func (storer *APTStorer) createTempFile(reader io.Reader, gf *models.GenericFile, attemptNumber int) error {
	filePath := storer.getTempFilePath(gf)
	storer.Context.MessageLog.Info("Copying file %s (size: %d) to %s "+
		"before uploading. (Attempt %d)", gf.Identifier, gf.Size, filePath,
		attemptNumber)
	err := os.MkdirAll(filepath.Dir(filePath), 0755)
	if err != nil {
		return fmt.Errorf("MkdirAll failed: %v", err)
	}
	// PT #143660373: S3 zero-size file bug. Lots of checks here...
	tempFile, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("Cannot create file: %v", err)
	}
	defer tempFile.Close()

	bytesCopied, err := io.Copy(tempFile, reader)
	if err != nil {
		return fmt.Errorf("Error copying data from tar file: %v", err)
	}
	if bytesCopied != gf.Size {
		return fmt.Errorf("Copied only %d of %d bytes for file %s", bytesCopied, gf.Size, gf.Identifier)
	} else {
		storer.Context.MessageLog.Info("Copied %d bytes for %s to %s", bytesCopied, gf.Identifier, filePath)
	}
	finfo, err := tempFile.Stat()
	if err != nil {
		return fmt.Errorf("Can't stat tempFile %s at %s", gf.Identifier, filePath)
	}
	if finfo.Size() != gf.Size {
		return fmt.Errorf("Temp file has only %d of %d bytes for file %s",
			finfo.Size(), gf.Size, gf.Identifier)
	}
	return nil
}

// Read the actual number of bytes in the EFS file.
// The AWS uploader keeps coming up with zero on the first try. Why?
// Note that this rewinds the file to the beginning after the size check.
func (storer *APTStorer) getActualFileSize(r io.ReadSeeker, filePath string) int64 {
	defer r.Seek(0, io.SeekStart)
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		storer.Context.MessageLog.Error("Error seeking through %s: %v", filePath, err)
		return -1
	}
	return size
}

func (storer *APTStorer) getTempFilePath(gf *models.GenericFile) string {
	return filepath.Join(storer.Context.Config.TarDirectory, "tmp", gf.IngestUUID)
}

func (storer *APTStorer) cleanupTempFile(gf *models.GenericFile) {
	tempFilePath := storer.getTempFilePath(gf)
	// >95% of of files are smaller than constants.S3LargeFileSize
	// so we never even extracted them to disk
	if !fileutil.FileExists(tempFilePath) {
		return
	}
	// Delete the file only if it's been copied to both S3 and Glacier
	// A.D. 2020-09-11:
	// If it's a Glacier-only file, it doesn't need to be replicated.
	// See issue https://trello.com/c/w2Yx9J96
	fileIsStored := !gf.IngestStoredAt.IsZero()
	fileIsReplicated := !gf.IngestReplicatedAt.IsZero() || strings.HasPrefix(gf.StorageOption, "Glacier")
	looksSafeToDelete := fileutil.LooksSafeToDelete(tempFilePath, 12, 3)

	if fileIsStored && fileIsReplicated && looksSafeToDelete {
		storer.Context.MessageLog.Info("Deleting temp file %s: "+
			"file %s has been stored and replicated",
			tempFilePath, gf.Identifier)
		err := os.Remove(tempFilePath)
		if err != nil {
			storer.Context.MessageLog.Error(fmt.Sprintf("Error deleting temp file %s: %v", tempFilePath, err))
		}
	}
}

// Returns true if the GenericFile IngestUUID is present and looks good.
func (storer *APTStorer) uuidPresent(storageSummary *models.StorageSummary) bool {
	gf := storageSummary.GenericFile
	if !util.LooksLikeUUID(gf.IngestUUID) {
		storageSummary.StoreResult.AddError("Cannot save %s to S3/Glacier because "+
			"GenericFile.IngestUUID (%s) is missing or invalid",
			gf.Identifier, gf.IngestUUID)
		storageSummary.StoreResult.ErrorIsFatal = true
		return false
	}
	return true
}

// Initializes the uploader object with connection data and metadata
// for this specific GenericFile.
func (storer *APTStorer) initUploader(storageSummary *models.StorageSummary, sendWhere string) *network.S3Upload {
	gf := storageSummary.GenericFile
	var region string
	var bucket string
	var err error
	if sendWhere == "s3" {
		region = storer.Context.Config.APTrustS3Region
		bucket = storer.Context.Config.PreservationBucket
	} else if sendWhere == "glacier" {
		region = storer.Context.Config.APTrustGlacierRegion
		bucket = storer.Context.Config.ReplicationBucket
	} else {
		region, bucket, err = storer.Context.Config.StorageRegionAndBucketFor(sendWhere)
	}
	if err != nil {
		storageSummary.StoreResult.AddError(err.Error())
		storageSummary.StoreResult.AddError("Cannot save %s to %s because "+
			"storer doesn't know where %s is", gf.Identifier, sendWhere, sendWhere)
		storageSummary.StoreResult.ErrorIsFatal = true
		return nil
	}
	uploader := network.NewS3Upload(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		region,
		bucket,
		gf.IngestUUID,
		gf.FileFormat,
	)
	instIdentifier, err := gf.InstitutionIdentifier()
	if err != nil {
		storageSummary.StoreResult.AddError("Error setting institution in S3 metadata: %v. "+
			"Storing without institution tag.", err)
	}
	uploader.AddMetadata("institution", instIdentifier)
	uploader.AddMetadata("bag", gf.IntellectualObjectIdentifier)
	uploader.AddMetadata("bagpath", gf.OriginalPath())
	uploader.AddMetadata("md5", gf.IngestMd5)
	uploader.AddMetadata("sha256", gf.IngestSha256)
	return uploader
}

// Returns a reader that can read the file from within the tar archive.
// The S3 uploader uses this reader to stream data to S3 and Glacier.
func (storer *APTStorer) getReadCloser(storageSummary *models.StorageSummary) (*fileutil.TarFileIterator, io.ReadCloser) {
	gf := storageSummary.GenericFile
	tarFilePath := storageSummary.TarFilePath
	tfi, err := fileutil.NewTarFileIterator(storageSummary.TarFilePath)
	if err != nil {
		msg := fmt.Sprintf("Can't get TarFileIterator for %s: %v", tarFilePath, err)
		storer.Context.MessageLog.Error(msg)
		storageSummary.StoreResult.AddError(msg)
		return nil, nil
	}
	origPathWithBagName, err := gf.OriginalPathWithBagName()
	if err != nil {
		msg := fmt.Sprintf("Can't get original path for %s: %s", gf.Identifier, err.Error())
		storer.Context.MessageLog.Error(msg)
		storageSummary.StoreResult.AddError(msg)
		return nil, nil
	}
	readCloser, err := tfi.Find(origPathWithBagName)
	if err != nil {
		msg := fmt.Sprintf("Can't get reader for %s: %v", gf.Identifier, err)
		storer.Context.MessageLog.Error(msg)
		storageSummary.StoreResult.AddError(msg)
		if readCloser != nil {
			readCloser.Close()
		}
		return nil, nil
	}
	return tfi, readCloser
}

// Make sure we send data to S3/Glacier with all of the required metadata.
func (storer *APTStorer) assertRequiredMetadata(storageSummary *models.StorageSummary, s3Upload *network.S3Upload) bool {
	allKeysPresent := true
	keys := []string{"institution", "bag", "bagpath", "md5", "sha256"}
	for _, key := range keys {
		value := s3Upload.UploadInput.Metadata[key]
		if value == nil || *value == "" {
			storageSummary.StoreResult.AddError("S3Upload is missing required "+
				"metadata key %s", key)
			storageSummary.StoreResult.ErrorIsFatal = true
			allKeysPresent = false
		}
	}
	return allKeysPresent
}

func (storer *APTStorer) markFileAsStored(gf *models.GenericFile, sendWhere, storageUrl string) {
	// For new Glacier-only storage, condition if sendWhere != "glacier"
	// covers S3, Glacier-OH, Glacier-OR, and Glacier-VA
	if sendWhere != "glacier" {
		gf.IngestStoredAt = time.Now().UTC()
		gf.IngestStorageURL = storageUrl
		gf.URI = storageUrl
		events := gf.FindEventsByType(constants.EventIdentifierAssignment)
		var event *models.PremisEvent
		for i := range events {
			existingEvent := events[i]
			if strings.HasPrefix(existingEvent.OutcomeDetail, "http://") ||
				strings.HasPrefix(existingEvent.OutcomeDetail, "https://") {
				event = existingEvent
				break
			}
		}
		if event != nil {
			event.DateTime = time.Now().UTC()
		}
	} else if sendWhere == "glacier" {
		gf.IngestReplicatedAt = time.Now().UTC()
		gf.IngestReplicationURL = storageUrl
		events := gf.FindEventsByType(constants.EventReplication)
		if events != nil && len(events) > 0 {
			events[0].DateTime = time.Now().UTC()
		}
	}
}

// PT #143660373: S3 zero-size file bug.
func (storer *APTStorer) getS3FileDetail(region, bucket, fileUUID string) *s3.Object {
	s3Client := network.NewS3ObjectList(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		region, bucket, 1)
	s3Client.GetList(fileUUID)
	if len(s3Client.Response.Contents) > 0 {
		return s3Client.Response.Contents[0]
	}
	return nil
}

func (storer *APTStorer) checkForHighResourceBag(db *storage.BoltDB, objIdentifier string) (bool, bool, string) {
	obj, err := db.GetIntellectualObject(objIdentifier)
	if err != nil {
		return false, false, "Cannot get object from BoltDB."
	}
	fileCount := db.FileCount()
	continueProcessing := true
	hasManySmallFiles := false
	message := ""
	if obj.IngestSize > TWO_HUNDRED_GIGABYTES {
		largeBagInProgress := storer.SyncMap.Get(LARGE_BAG_IN_PROGRESS)
		if largeBagInProgress != "" {
			continueProcessing = false
			message = fmt.Sprintf("This bag is large (%d bytes) and large bag %s is currently in progress", obj.IngestSize, largeBagInProgress)
		} else {
			storer.Context.MessageLog.Info("Setting bag %s as large bag (%d bytes).", objIdentifier, obj.IngestSize)
			storer.SyncMap.Add(LARGE_BAG_IN_PROGRESS, objIdentifier)
		}
	} else if fileCount > HIGH_FILE_COUNT {
		averageFileSize := obj.IngestSize / int64(fileCount)

		// Set this flag so we can pick an appropriate batch size
		// when storing.
		hasManySmallFiles = averageFileSize < SMALL_FILE_SIZE

		highFileBagInProgress := storer.SyncMap.Get(HIGH_FILE_BAG_IN_PROGRESS)
		if highFileBagInProgress != "" {
			continueProcessing = false
			message = fmt.Sprintf("This bag has %d files and another high file-count bag is currently in progress", fileCount)
			storer.Context.MessageLog.Info("Defering %s with %d files because high file-count bag %s is currently in progress", objIdentifier, fileCount, highFileBagInProgress)
		} else {
			storer.Context.MessageLog.Info("Setting bag %s as high file-count bag (%d files).", objIdentifier, fileCount)
			storer.SyncMap.Add(HIGH_FILE_BAG_IN_PROGRESS, objIdentifier)
		}
	}

	return continueProcessing, hasManySmallFiles, message
}

func (storer *APTStorer) clearHighResourceBag(objIdentifier string) {
	highFileBagInProgress := storer.SyncMap.Get(HIGH_FILE_BAG_IN_PROGRESS)
	if objIdentifier == highFileBagInProgress {
		storer.SyncMap.Add(HIGH_FILE_BAG_IN_PROGRESS, "")
		storer.Context.MessageLog.Info("Cleared HIGH_FILE_BAG_IN_PROGRESS %s", objIdentifier)
	}
	largeBagInProgress := storer.SyncMap.Get(LARGE_BAG_IN_PROGRESS)
	if objIdentifier == largeBagInProgress {
		storer.SyncMap.Add(LARGE_BAG_IN_PROGRESS, "")
		storer.Context.MessageLog.Info("Cleared LARGE_BAG_IN_PROGRESS %s", objIdentifier)
	}
}

// ----------- Messages ----------------

func (storer *APTStorer) logDeletingTarFile(ingestState *models.IngestState) {
	storer.Context.MessageLog.Info("Deleting tar file %s (%s/%s) "+
		"because all files were stored successfully",
		ingestState.IngestManifest.BagPath,
		ingestState.IngestManifest.S3Bucket,
		ingestState.IngestManifest.S3Key)
}

func (storer *APTStorer) logFailedToStore(ingestState *models.IngestState) {
	storer.Context.MessageLog.Error("Failed to store WorkItem %d (%s/%s).",
		ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
		ingestState.WorkItem.Name)
}

func (storer *APTStorer) logRequeued(ingestState *models.IngestState) {
	storer.Context.MessageLog.Info("Requeueing WorkItem %d (%s/%s) due to transient errors. %s",
		ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
		ingestState.WorkItem.Name,
		ingestState.IngestManifest.AllErrorsAsString())
}

func (storer *APTStorer) logFinishedStoring(ingestState *models.IngestState) {
	storer.Context.MessageLog.Info("Finished storing WorkItem %d (%s/%s).",
		ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
		ingestState.WorkItem.Name)
}
