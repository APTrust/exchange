package workers

import (
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
	workerBufferSize := _context.Config.FetchWorker.Workers * 10
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

	// ---------------------------------------------------------
	// TODO: Make sure no other worker is working on this item.
	// ---------------------------------------------------------

	ingestState, err := storer.loadIngestState(message)
	if err != nil {
		storer.Context.MessageLog.Error(err.Error())
		return err
	}

	// Disable auto response, so we can tell NSQ when we need to
	// that we're still working on this item.
	message.DisableAutoResponse()

	// Clear out any old errors, because we're going to retry
	// whatever may have failed on the last run.
	ingestState.IngestManifest.StoreResult.ClearErrors()

	// Tell Pharos that we've started to store this item.
	ingestState.WorkItem, err = storer.markWorkItemStarted(ingestState)
	if err != nil {
		storer.Context.MessageLog.Error(err.Error())
		return err
	}

	storer.Context.MessageLog.Info("Putting %s/%s into record queue",
		ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key)

	storer.RecordChannel <- ingestState

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
	}
}

// -------------------------------------------------------------------------
// Step 3 of 3: Record IntellectualObject and GenericFile data in Pharos
//
// -------------------------------------------------------------------------
func (storer *APTStorer) record () {
	for ingestState := range storer.RecordChannel {
		if ingestState.IngestManifest.HasFatalErrors() {
			// Set WorkItem node=nil, pid=0, retry=false, needs_admin_review=true
			// Finish the NSQ message
			ingestState.FinishNSQ()
			storer.markWorkItemFailed(ingestState)
		} else if ingestState.IngestManifest.HasErrors() {
			// Set WorkItem node=nil, pid=0
			// Requeue the NSQ message
			ingestState.RequeueNSQ(1000)
			storer.markWorkItemRequeued(ingestState)
		} else {
			// Set WorkItem stage to StageStore, status to StatusPending, node=nil, pid=0
			// Finish the NSQ message
			ingestState.FinishNSQ()
			storer.markWorkItemSucceeded(ingestState)
		}
	}
}

func (storer *APTStorer) loadIngestState (message *nsq.Message) (*models.IngestState, error) {
	// Load WorkItem and WorkItemState
	// Get IngestManifest from WorkItemState
	return nil, nil
}


func (storer *APTStorer) saveFile (ingestState *models.IngestState, gf *models.GenericFile) {
	existingSha256, err := storer.getExistingSha256(gf.Identifier)
	if err != nil {
		ingestState.IngestManifest.StoreResult.AddError(err.Error())
		return
	}
	// Set this, for the record.
	if existingSha256 == "" {
		gf.IngestPreviousVersionExists = true
		if existingSha256 != gf.IngestSha256 {
			gf.IngestNeedsSave = false
		}
	}
	// Now copy to storage only if the file has changed.
	if gf.IngestNeedsSave {
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
func (storer *APTStorer) getExistingSha256 (gfIdentifier string) (string, error) {
	storer.Context.MessageLog.Info("Checking Pharos for existing sha256 digest for %s",
		gfIdentifier)
	params := url.Values{}
	params.Add("generic_file_identifier", gfIdentifier)
	params.Add("algorithm", constants.AlgSha256)
	params.Add("sort", "created_at DESC")
	resp := storer.Context.PharosClient.ChecksumList(params)
	if resp.Error != nil {
		return "", resp.Error
	}
	existingChecksum := resp.Checksum()
	if existingChecksum == nil {
		return "", nil
	}
	return existingChecksum.Digest, nil
}

// Copy the GenericFile to long-term storage in S3 or Glacier
func (storer *APTStorer) copyToLongTermStorage (ingestState *models.IngestState, gf *models.GenericFile, sendWhere string) {
	if !storer.uuidPresent(ingestState, gf) {
		return
	}
	for attemptNumber := 1; attemptNumber <= MAX_UPLOAD_ATTEMPTS; attemptNumber++ {
		uploader := storer.initUploader(ingestState, gf, sendWhere)
		if uploader == nil {
			return  // We have some config problem here. Stop trying.
		}
		if storer.assertRequiredMetadata(ingestState, uploader) {
			readCloser := storer.getReadCloser(ingestState, gf)
			if readCloser != nil {
				defer readCloser.Close()
				uploader.Send(readCloser)
				if uploader.ErrorMessage == "" {
					storer.markFileAsStored(gf, sendWhere, uploader.Response.Location)
					return // Upload succeeded
				} else {
					if attemptNumber == MAX_UPLOAD_ATTEMPTS {
						ingestState.IngestManifest.StoreResult.AddError(uploader.ErrorMessage)
					}
				}
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
func (storer *APTStorer) getReadCloser(ingestState *models.IngestState, gf *models.GenericFile) (io.ReadCloser) {
	tarFilePath := ingestState.IngestManifest.Object.IngestTarFilePath
	tfi, err := fileutil.NewTarFileIterator(tarFilePath)
	if tfi != nil {
		defer tfi.Close()
	}
	if err != nil {
		ingestState.IngestManifest.StoreResult.AddError("Can't get TarFileIterator " +
			"for %s: %v", tarFilePath, err)
		return nil
	}
	readCloser, err := tfi.Find(gf.Identifier)
	if err != nil {
		ingestState.IngestManifest.StoreResult.AddError("Can't get reader for " +
			"%s: %v", gf.Identifier, err)
		if readCloser != nil {
			readCloser.Close()
		}
		return nil
	}
	return readCloser
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
	} else if sendWhere == "glacier" {
		gf.IngestReplicatedAt = time.Now().UTC()
		gf.IngestReplicationURL = storageUrl
	}
}
// Tell Pharos we've started copying this item's files to
// long-term storage.
func (storer *APTStorer) markWorkItemStarted (ingestState *models.IngestState) (*models.WorkItem, error) {
	storer.Context.MessageLog.Info("Telling Pharos record started for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	utcNow := time.Now().UTC()
	ingestState.WorkItem.SetNodeAndPid()
	ingestState.WorkItem.Stage = constants.StageFetch
	ingestState.WorkItem.StageStartedAt = &utcNow
	ingestState.WorkItem.Status = constants.StatusStarted
	ingestState.WorkItem.Note = "Copying files to long-term storage"
	resp := storer.Context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.WorkItem(), nil
}

// Tell Pharos we finished copying all files to long-term
// storage (successfully).
func (storer *APTStorer) markWorkItemSucceeded (ingestState *models.IngestState) (error) {
	storer.Context.MessageLog.Info("Telling Pharos storage succeeded for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Node = ""
	ingestState.WorkItem.Pid = 0
	ingestState.WorkItem.Retry = true
	ingestState.WorkItem.StageStartedAt = nil
	ingestState.WorkItem.NeedsAdminReview = false
	ingestState.WorkItem.Stage = constants.StageRecord
	ingestState.WorkItem.Status = constants.StatusPending
	ingestState.WorkItem.Note = "All files copied to S3 and Glacier. Awaiting metadata recording in Pharos."
	resp := storer.Context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		storer.Context.MessageLog.Error("Could not mark WorkItem ready for record for %s/%s: %v",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}

// Tell Pharos that the storage attempt failed with a
// fatal error, and should not be retried.
func (storer *APTStorer) markWorkItemFailed (ingestState *models.IngestState) (error) {
	storer.Context.MessageLog.Info("Telling Pharos storage failed for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Node = ""
	ingestState.WorkItem.Pid = 0
	ingestState.WorkItem.StageStartedAt = nil
	ingestState.WorkItem.Retry = false
	ingestState.WorkItem.NeedsAdminReview = true
	ingestState.WorkItem.Status = constants.StatusFailed
	ingestState.WorkItem.Note = ingestState.IngestManifest.FetchResult.AllErrorsAsString() + ingestState.IngestManifest.ValidateResult.AllErrorsAsString()
	resp := storer.Context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		storer.Context.MessageLog.Error("Could not mark WorkItem failed for %s/%s: %v",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}

// Tell Pharos that storage failed with a transient
// error, and we should try this item again later.
func (storer *APTStorer) markWorkItemRequeued (ingestState *models.IngestState) (error) {
	storer.Context.MessageLog.Info("Telling Pharos storage is being requeued for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Node = ""
	ingestState.WorkItem.Pid = 0
	ingestState.WorkItem.StageStartedAt = nil
	ingestState.WorkItem.Retry = true
	ingestState.WorkItem.NeedsAdminReview = false
	ingestState.WorkItem.Status = constants.StatusStarted
	ingestState.WorkItem.Note = "Item has been requeued due to transient errors. " + ingestState.IngestManifest.FetchResult.AllErrorsAsString() + ingestState.IngestManifest.ValidateResult.AllErrorsAsString()
	resp := storer.Context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		storer.Context.MessageLog.Error("Could not mark WorkItem requeued for %s/%s: %v",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}