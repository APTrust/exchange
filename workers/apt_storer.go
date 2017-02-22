package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nsqio/go-nsq"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"
)

// 15 seemed to be the magic number in the first generation of the software.
// On large uploads, network errors are common.
const MAX_UPLOAD_ATTEMPTS = 15
const FIFTY_MEGABYTES = int64(52428800)

// Stores GenericFiles in long-term storage (S3 and Glacier).
type APTStorer struct {
	Context        *context.Context
	StorageChannel chan *models.IngestState
	CleanupChannel chan *models.IngestState
	RecordChannel  chan *models.IngestState
}

func NewAPTStorer(_context *context.Context) *APTStorer {
	storer := &APTStorer{
		Context: _context,
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
	ingestState, err := GetIngestState(message, storer.Context, false)
	if err != nil {
		storer.Context.MessageLog.Error(err.Error())
		return err
	}

	// If this item was queued more than once, and this process or any
	// other is currently working on it, just finish the message and
	// assume that the in-progress worker will take care of the original.
	if ingestState.WorkItem.Node != "" && ingestState.WorkItem.Pid != 0 {
		storer.Context.MessageLog.Info("Marking WorkItem %d (%s/%s) as finished "+
			"without doing any work, because this item is currently in process by "+
			"node %s, pid %d. WorkItem was last updated at %s.",
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
		obj := ingestState.IngestManifest.Object

		for {
			// Get a batch of files to save...
			storageSummaries, hasMoreFiles, err := storer.getStorageSummaryBatch(obj, start, limit)
			if err != nil {
				ingestState.IngestManifest.StoreResult.AddError(err.Error())
				ingestState.IngestManifest.StoreResult.ErrorIsFatal = true
				break
			}
			fileCount := len(storageSummaries)

			// Save them concurrently...
			storer.Context.MessageLog.Info("Saving batch of %d files for %s", fileCount, obj.Identifier)
			wg := sync.WaitGroup{}
			wg.Add(fileCount)
			for i := 0; i < fileCount; i++ {
				go func(storageSummary *models.StorageSummary) {
					defer wg.Done()
					storer.saveFile(storageSummary)
				}(storageSummaries[i])
			}
			wg.Wait()
			storer.Context.MessageLog.Info("Finished batch of %d files for %s", fileCount, obj.Identifier)

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
				break
			}
		}

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
			storer.Context.MessageLog.Info("Deleting tar file %s (%s/%s) "+
				"because all files were stored successfully",
				ingestState.IngestManifest.Object.IngestTarFilePath,
				ingestState.IngestManifest.Object.IngestS3Bucket,
				ingestState.IngestManifest.Object.IngestS3Key)
			DeleteBagFromStaging(ingestState, storer.Context,
				ingestState.IngestManifest.StoreResult)
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
		RecordWorkItemState(ingestState, storer.Context, ingestState.IngestManifest.FetchResult)

		// See if we have fatal errors, or too many recurring transient errors
		attemptNumber := ingestState.IngestManifest.StoreResult.AttemptNumber
		maxAttempts := storer.Context.Config.StoreWorker.MaxAttempts
		itsTimeToGiveUp := (ingestState.IngestManifest.HasFatalErrors() ||
			(ingestState.IngestManifest.HasErrors() && attemptNumber >= maxAttempts))

		if itsTimeToGiveUp {
			storer.Context.MessageLog.Error("Failed to store WorkItem %d (%s/%s).",
				ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
				ingestState.WorkItem.Name)
			ingestState.FinishNSQ()
			MarkWorkItemFailed(ingestState, storer.Context)
		} else if ingestState.IngestManifest.HasErrors() {
			storer.Context.MessageLog.Info("Requeueing WorkItem %d (%s/%s) due to transient errors. %s",
				ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
				ingestState.WorkItem.Name,
				ingestState.IngestManifest.AllErrorsAsString())
			ingestState.RequeueNSQ(1000)
			MarkWorkItemRequeued(ingestState, storer.Context)
		} else {
			storer.Context.MessageLog.Info("Finished storing WorkItem %d (%s/%s).",
				ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
				ingestState.WorkItem.Name)
			ingestState.FinishNSQ()
			MarkWorkItemSucceeded(ingestState, storer.Context, constants.StageRecord)
			PushToQueue(ingestState, storer.Context, storer.Context.Config.RecordWorker.NsqTopic)
		}
	}
}

// getStorageSummaryBatch returns a batch of storage summary objects
// and boolean indicating whether the object has more files to get.
func (storer *APTStorer) getStorageSummaryBatch(obj *models.IntellectualObject, start, limit int) (storageSummaries []*models.StorageSummary, hasMoreFiles bool, err error) {
	end := start + limit
	if end > len(obj.GenericFiles) {
		end = len(obj.GenericFiles)
	}
	fileCount := end - start
	storageSummaries = make([]*models.StorageSummary, fileCount)
	for i := 0; i < fileCount; i++ {
		summary, err := obj.GetStorageSummary(start + i)
		if err != nil {
			return nil, false, err
		}
		storageSummaries[i] = summary
	}
	hasMoreFiles = end < len(obj.GenericFiles)
	return storageSummaries, hasMoreFiles, nil
}

func (storer *APTStorer) saveFile(storageSummary *models.StorageSummary) {
	gf := storageSummary.GenericFile
	if !util.HasSavableName(gf.OriginalPath()) {
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
		if gf.IngestStoredAt.IsZero() || gf.IngestStorageURL == "" {
			storer.copyToLongTermStorage(storageSummary, "s3")
		}
		if gf.IngestReplicatedAt.IsZero() || gf.IngestReplicationURL == "" {
			storer.copyToLongTermStorage(storageSummary, "glacier")
		}
	} else {
		if !util.HasSavableName(gf.OriginalPath()) {
			storer.Context.MessageLog.Info("Skipping %s: doesn't have savable name", gf.Identifier)
		} else {
			storer.Context.MessageLog.Info("Skipping %s: unchanged since previous save", gf.Identifier)
		}
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
	params.Add("sort", "created_at DESC")
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
		uploader := storer.initUploader(storageSummary, sendWhere)
		if uploader == nil {
			msg := "S3 uploader is nil. Cannot proceed."
			storageSummary.StoreResult.AddError(msg)
			storer.Context.MessageLog.Error(msg)
			return // We have some config problem here. Stop trying.
		}
		if storer.assertRequiredMetadata(storageSummary, uploader) {
			tarFileIterator, readCloser := storer.getReadCloser(storageSummary)
			if readCloser != nil && tarFileIterator != nil {
				defer readCloser.Close()
				defer tarFileIterator.Close()
				// HACK to give Amazon's S3 uploader a Seeker, so it doesn't
				// try to read the entire file into memory at once.
				uploader.Send(fileutil.NewTarReadSeekCloser(readCloser.(fileutil.TarReadCloser)), gf.Size)
				storer.Context.MessageLog.Info("Uploaded chunk of %s with part size %d, concurrency %d",
					gf.Identifier, uploader.PartSize(), uploader.Concurrency())
				if uploader.ErrorMessage == "" {
					storer.Context.MessageLog.Info("Stored %s in %s after %d attempts",
						gf.Identifier, sendWhere, attemptNumber)
					storer.markFileAsStored(gf, sendWhere, uploader.Response.Location)
					return // Upload succeeded
				} else {
					storer.Context.MessageLog.Error("Upload error for %s: %s",
						gf.Identifier, uploader.ErrorMessage)
					if attemptNumber == MAX_UPLOAD_ATTEMPTS {
						storageSummary.StoreResult.AddError(uploader.ErrorMessage)
					}
				}
			} else {
				storer.Context.MessageLog.Error("Could not get reader from tar file.")
			}
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
	if sendWhere == "s3" {
		region = storer.Context.Config.APTrustS3Region
		bucket = storer.Context.Config.PreservationBucket
	} else if sendWhere == "glacier" {
		region = storer.Context.Config.APTrustGlacierRegion
		bucket = storer.Context.Config.ReplicationBucket
	} else {
		storageSummary.StoreResult.AddError("Cannot save %s to %s because "+
			"storer doesn't know where %s is", gf.Identifier, sendWhere)
		storageSummary.StoreResult.ErrorIsFatal = true
		return nil
	}
	uploader := network.NewS3Upload(
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
		storageSummary.StoreResult.AddError(msg)
		return nil, nil
	}
	origPathWithBagName, err := gf.OriginalPathWithBagName()
	if err != nil {
		storageSummary.StoreResult.AddError(err.Error())
		return nil, nil
	}
	readCloser, err := tfi.Find(origPathWithBagName)
	if err != nil {
		msg := fmt.Sprintf("Can't get reader for %s: %v", gf.Identifier, err)
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
	if sendWhere == "s3" {
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
