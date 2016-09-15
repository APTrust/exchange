package workers

import (
//	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
//	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
//	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"net/url"
	"os"
//	"path/filepath"
//	"strconv"
	"strings"
	"sync"
	"time"
)

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
		if gf.IngestStoredAt.IsZero() {
			storer.copyToPrimaryStorage(ingestState, gf)
		}
		if gf.IngestReplicatedAt.IsZero() {
			storer.copyToSecondaryStorage(ingestState, gf)
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

// Copy the GenericFile to primary storage (S3)
func (storer *APTStorer) copyToPrimaryStorage (ingestState *models.IngestState, gf *models.GenericFile) {
	// 15 seemed to be the magic number in the first generation of the software.
	for attemptNumber := 1; attemptNumber <= 15; attemptNumber++ {
		upload := network.NewS3Upload(
			storer.Context.Config.APTrustS3Region,
			storer.Context.Config.PreservationBucket,
			gf.IngestUUID, // TODO: Make sure this is not empty!
			"",            // TODO: Pass in Reader instead of file!
			gf.FileFormat,
		)
		upload.AddMetadata("institution", ingestState.IngestManifest.Object.Institution)
		upload.AddMetadata("bag", ingestState.IngestManifest.Object.Identifier)
		upload.AddMetadata("bagpath", gf.OriginalPath())
		upload.AddMetadata("md5", gf.IngestMd5)        // TODO: Make sure this isn't empty
		upload.AddMetadata("sha256", gf.IngestSha256)  // TODO: Make sure this isn't empty
		upload.Send()
		if upload.ErrorMessage != "" {
			if attemptNumber == 15 {
				ingestState.IngestManifest.StoreResult.AddError(upload.ErrorMessage)
			}
		}
	}
}

// Copy the GenericFile to secondary storage (Glacier)
func (storer *APTStorer) copyToSecondaryStorage (ingestState *models.IngestState, gf *models.GenericFile) {

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
