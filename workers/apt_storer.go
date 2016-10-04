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
	"os"
	"strings"
	"sync"
	"time"
)

// 15 seemed to be the magic number in the first generation of the software.
// On large uploads, network errors are common.
const MAX_UPLOAD_ATTEMPTS = 15

// Stores GenericFiles in long-term storage (S3 and Glacier).
type APTStorer struct {
	Context             *context.Context
	StorageChannel      chan *models.IngestState
	CleanupChannel      chan *models.IngestState
	RecordChannel       chan *models.IngestState
	WaitGroup           sync.WaitGroup
}

func NewAPTStorer(_context *context.Context) (*APTStorer) {
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
func (storer *APTStorer) HandleMessage(message *nsq.Message) (error) {
	ingestState, err := GetIngestState(message, storer.Context, false)
	if err != nil {
		storer.Context.MessageLog.Error(err.Error())
		return err
	}

	// If this item was queued more than once, and this process or any
	// other is currently working on it, just finish the message and
	// assume that the in-progress worker will take care of the original.
	if ingestState.WorkItem.Node != "" && ingestState.WorkItem.Pid != 0 {
		storer.Context.MessageLog.Info("Marking WorkItem %d (%s/%s) as finished " +
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
func (storer *APTStorer) store () {
	for ingestState := range storer.StorageChannel {
		for _, gf := range ingestState.IngestManifest.Object.GenericFiles {
			storer.saveFile(ingestState, gf)
		}
		storer.CleanupChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 2 of 3: Delete the bag file(s) if storage succeeded
//
// -------------------------------------------------------------------------
func (storer *APTStorer) cleanup () {
	for ingestState := range storer.CleanupChannel {
		if (ingestState.IngestManifest.StoreResult.HasErrors() == false &&
			ingestState.IngestManifest.Object.AllFilesSaved()) {
			storer.Context.MessageLog.Info("Deleting tar file %s (%s/%s) " +
				"because all files were stored successfully",
				ingestState.IngestManifest.Object.IngestTarFilePath,
				ingestState.IngestManifest.Object.IngestS3Bucket,
				ingestState.IngestManifest.Object.IngestS3Key)
			os.Remove(ingestState.IngestManifest.Object.IngestTarFilePath)
		}
		// If item was untarred, delete the untarred dir,
		// but be sure what you pass to os.RemoveAll() is safe.
		untarredPath := ingestState.IngestManifest.Object.IngestUntarredPath
		if (untarredPath != "" &&
			strings.HasPrefix(untarredPath, storer.Context.Config.TarDirectory) &&
			fileutil.LooksSafeToDelete(untarredPath, 12, 3)) {
			storer.Context.MessageLog.Info("Deleting untarred dir %s (%s/%s) " +
				"because all files were stored successfully",
				untarredPath,
				ingestState.IngestManifest.Object.IngestS3Bucket,
				ingestState.IngestManifest.Object.IngestS3Key)
			os.RemoveAll(untarredPath)
		}
		storer.RecordChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 3 of 3: Record IntellectualObject and GenericFile data in Pharos
//
// -------------------------------------------------------------------------
func (storer *APTStorer) record () {
	for ingestState := range storer.RecordChannel {
		// Copy JSON representation of the IngestManifest to Pharos
		// and to the JSON log.
		RecordWorkItemState(ingestState, storer.Context, ingestState.IngestManifest.FetchResult)

		if ingestState.IngestManifest.HasFatalErrors() {
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

func (storer *APTStorer) saveFile (ingestState *models.IngestState, gf *models.GenericFile) {
	existingSha256, err := storer.getExistingSha256(gf.Identifier)
	if err != nil {
		storer.Context.MessageLog.Error(err.Error())
		ingestState.IngestManifest.StoreResult.AddError(err.Error())
		return
	}
	// Set this, for the record.
	if existingSha256 != nil {
		gf.IngestPreviousVersionExists = true
		gf.Id = existingSha256.GenericFileId

		uuid, err := storer.getUuidOfExistingFile(gf.Identifier)
		if err != nil {
			message := fmt.Sprintf("Cannot find existing UUID for %s: %v", gf.Identifier, err.Error())
			ingestState.IngestManifest.StoreResult.AddError(message)
			storer.Context.MessageLog.Error(message)
			// Probably not fatal, but treat it as such for now,
			// because we don't want leave orphan objects in S3,
			// or have the GenericFile.URL not match the actual
			// storage URL. This should only happen if a depositor
			// deletes the existing version of a GenericFile while
			// we are processing this ingest. The window for that
			// to happen is usually between a few seconds and a few
			// hours.
			ingestState.IngestManifest.StoreResult.ErrorIsFatal = true
			return
		}
		if uuid == "" {
			message := fmt.Sprintf("Cannot find existing UUID for %s.", gf.Identifier)
			ingestState.IngestManifest.StoreResult.AddError(message)
			storer.Context.MessageLog.Error(message)
			// Probably not fatal, but treat it as such for now.
			// Same note as in previous if statement above.
			ingestState.IngestManifest.StoreResult.ErrorIsFatal = true
			return
		} else {
			// OK. Set the GenericFile's UUID to match the existing file's
			// UUID, so that we overwrite the existing file, and so the
			// GenericFile record in Pharos still has the correct URL.
			message := fmt.Sprintf("Resetting UUID for '%s' to '%s' so we can overwrite " +
				"the currently stored version of the file.",
				gf.Identifier, uuid)
			storer.Context.MessageLog.Info(message)
			gf.IngestUUID = uuid
		}

		if existingSha256.Digest != gf.IngestSha256 {
			storer.Context.MessageLog.Info(
				"GenericFile %s has same sha256. Does not need save.", gf.Identifier)
			gf.IngestNeedsSave = false
		}
	}
	// Now copy to storage only if the file has changed.
	if gf.IngestNeedsSave {
		storer.Context.MessageLog.Info("File %s needs save", gf.Identifier)
		if gf.IngestStoredAt.IsZero() || gf.IngestStorageURL == "" {
			storer.copyToLongTermStorage(ingestState, gf, "s3")
		}
		if gf.IngestReplicatedAt.IsZero() || gf.IngestReplicationURL == "" {
			storer.copyToLongTermStorage(ingestState, gf, "glacier")
		}
	}
}

// Get the existing sha256 checksum for the generic file, if there is one.
// In some cases, depositors upload a new version of a bag that includes
// unchanged versions of some files. So we check the sha256 of the
// existing version against the sha256 of the one just uploaded. If they're
// the same, we don't bother overwriting the existing file.
func (storer *APTStorer) getExistingSha256 (gfIdentifier string) (*models.Checksum, error) {
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
func (storer *APTStorer) getUuidOfExistingFile (gfIdentifier string) (string, error) {
	storer.Context.MessageLog.Info("Checking Pharos for existing UUID for GenericFile %s",
		gfIdentifier)
	resp := storer.Context.PharosClient.GenericFileGet(gfIdentifier)
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
func (storer *APTStorer) copyToLongTermStorage (ingestState *models.IngestState, gf *models.GenericFile, sendWhere string) {
	if !storer.uuidPresent(ingestState, gf) {
		msg := fmt.Sprintf("Cannot copy GenericFile %s to long-term storage because UUID is missing",
			gf.Identifier)
		ingestState.IngestManifest.StoreResult.AddError(msg)
		storer.Context.MessageLog.Error(msg)
		return
	}
	storer.Context.MessageLog.Info("Sending %s to %s", gf.Identifier, sendWhere)
	for attemptNumber := 1; attemptNumber <= MAX_UPLOAD_ATTEMPTS; attemptNumber++ {
		uploader := storer.initUploader(ingestState, gf, sendWhere)
		if uploader == nil {
			msg := "S3 uploader is nil. Cannot proceed."
			ingestState.IngestManifest.StoreResult.AddError(msg)
			storer.Context.MessageLog.Error(msg)
			return  // We have some config problem here. Stop trying.
		}
		if storer.assertRequiredMetadata(ingestState, uploader) {
			tarFileIterator, readCloser := storer.getReadCloser(ingestState, gf)
			if readCloser != nil && tarFileIterator != nil {
				defer readCloser.Close()
				defer tarFileIterator.Close()
				uploader.Send(readCloser)
				if uploader.ErrorMessage == "" {
					storer.Context.MessageLog.Info("Stored %s in %s after %d attempts",
						gf.Identifier, sendWhere, attemptNumber)
					storer.markFileAsStored(gf, sendWhere, uploader.Response.Location)
					return // Upload succeeded
				} else {
					storer.Context.MessageLog.Error("Upload error for %s: %s",
						gf.Identifier, uploader.ErrorMessage)
					if attemptNumber == MAX_UPLOAD_ATTEMPTS {
						ingestState.IngestManifest.StoreResult.AddError(uploader.ErrorMessage)
					}
				}
			} else {
				storer.Context.MessageLog.Error("Could not get reader from tar file.")
			}
		}
	}
}

// Returns true if the GenericFile IngestUUID is present and looks good.
func (storer *APTStorer) uuidPresent (ingestState *models.IngestState, gf *models.GenericFile) (bool) {
	if !util.LooksLikeUUID(gf.IngestUUID) {
		ingestState.IngestManifest.StoreResult.AddError("Cannot save %s to S3/Glacier because " +
			"GenericFile.IngestUUID (%s) is missing or invalid",
			gf.Identifier, gf.IngestUUID)
		ingestState.IngestManifest.StoreResult.ErrorIsFatal = true
		return false
	}
	return true
}

// Initializes the uploader object with connection data and metadata
// for this specific GenericFile.
func (storer *APTStorer) initUploader(ingestState *models.IngestState, gf *models.GenericFile, sendWhere string) (*network.S3Upload) {
	var region string
	var bucket string
	if sendWhere == "s3" {
		region = storer.Context.Config.APTrustS3Region
		bucket = storer.Context.Config.PreservationBucket
	} else if sendWhere == "glacier" {
		region = storer.Context.Config.APTrustGlacierRegion
		bucket = storer.Context.Config.ReplicationBucket
	} else {
		ingestState.IngestManifest.StoreResult.AddError("Cannot save %s to %s because " +
			"storer doesn't know where %s is", gf.Identifier, sendWhere)
		ingestState.IngestManifest.StoreResult.ErrorIsFatal = true
		return nil
	}
	uploader := network.NewS3Upload(
		region,
		bucket,
		gf.IngestUUID,
		gf.FileFormat,
	)
	uploader.AddMetadata("institution", ingestState.IngestManifest.Object.Institution)
	uploader.AddMetadata("bag", ingestState.IngestManifest.Object.Identifier)
	uploader.AddMetadata("bagpath", gf.OriginalPath())
	uploader.AddMetadata("md5", gf.IngestMd5)
	uploader.AddMetadata("sha256", gf.IngestSha256)
	return uploader
}

// Returns a reader that can read the file from within the tar archive.
// The S3 uploader uses this reader to stream data to S3 and Glacier.
func (storer *APTStorer) getReadCloser(ingestState *models.IngestState, gf *models.GenericFile) (*fileutil.TarFileIterator, io.ReadCloser) {
	tarFilePath := ingestState.IngestManifest.Object.IngestTarFilePath
	tfi, err := fileutil.NewTarFileIterator(tarFilePath)
	if err != nil {
		ingestState.IngestManifest.StoreResult.AddError("Can't get TarFileIterator " +
			"for %s: %v", tarFilePath, err)
		return nil, nil
	}
	readCloser, err := tfi.Find(gf.Identifier)
	if err != nil {
		ingestState.IngestManifest.StoreResult.AddError("Can't get reader for " +
			"%s: %v", gf.Identifier, err)
		if readCloser != nil {
			readCloser.Close()
		}
		return nil, nil
	}
	return tfi, readCloser
}

// Make sure we send data to S3/Glacier with all of the required metadata.
func (storer *APTStorer) assertRequiredMetadata (ingestState *models.IngestState, s3Upload *network.S3Upload) (bool) {
	allKeysPresent := true
	keys := []string { "institution", "bag", "bagpath", "md5", "sha256" }
	for _, key := range keys {
		value := s3Upload.UploadInput.Metadata[key]
		if value == nil || *value == "" {
			ingestState.IngestManifest.StoreResult.AddError("S3Upload is missing required " +
				"metadata key %s", key)
			ingestState.IngestManifest.StoreResult.ErrorIsFatal = true
			allKeysPresent = false
		}
	}
	return allKeysPresent
}

func (storer *APTStorer) markFileAsStored (gf *models.GenericFile, sendWhere, storageUrl string) {
	if sendWhere == "s3" {
		gf.IngestStoredAt = time.Now().UTC()
		gf.IngestStorageURL = storageUrl
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
