package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Fetches bags (tar files) from S3 receiving buckets and validates them.
type APTFetcher struct {
	Context             *context.Context
	BagValidationConfig *validation.BagValidationConfig
	FetchChannel        chan *models.IngestState
	ValidationChannel   chan *models.IngestState
	CleanupChannel      chan *models.IngestState
	RecordChannel       chan *models.IngestState
	WaitGroup           sync.WaitGroup
}

func NewAPTFetcher(_context *context.Context) (*APTFetcher) {
	fetcher := &APTFetcher{
		Context: _context,
	}

	// Load the config settings that describe how to validate
	// APTrust bags. We'll exit here if the config can't be
	// loaded or is invalid.
	fetcher.loadBagValidationConfig()

	// Set up buffered channels
	fetcherBufferSize := _context.Config.FetchWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.FetchWorker.Workers * 10
	fetcher.FetchChannel = make(chan *models.IngestState, fetcherBufferSize)
	fetcher.ValidationChannel = make(chan *models.IngestState, workerBufferSize)
	fetcher.RecordChannel = make(chan *models.IngestState, workerBufferSize)
	fetcher.CleanupChannel = make(chan *models.IngestState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.FetchWorker.NetworkConnections; i++ {
		go fetcher.fetch()
	}
	for i := 0; i < _context.Config.FetchWorker.Workers; i++ {
		go fetcher.validate()
		go fetcher.record()
		go fetcher.cleanup()
	}
	return fetcher
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (fetcher *APTFetcher) HandleMessage(message *nsq.Message) (error) {

	// ---------------------------------------------------------
	// TODO: Make sure no other worker is working on this item.
	// ---------------------------------------------------------

	// Set up our IngestState. Most of this comes from Pharos;
	// some of it we have to build fresh.
	ingestState, err := fetcher.initIngestState(message)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return err
	}
	// Save the state of this item in Pharos.
	RecordWorkItemState(ingestState, fetcher.Context, ingestState.IngestManifest.FetchResult)

	// Tell Pharos that we've started to fetch this item.
	ingestState.WorkItem, err = fetcher.recordFetchStarted(ingestState.WorkItem)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return err
	}

	// NSQ message autoresponse periodically tells the queue
	// that the message is still being processed. This doesn't
	// work for us in cases where we're fetching a file that's
	// 100GB+ in size. We need to manually Touch() NSQ periodically
	// to let the queue know that we're still actively working on
	// the message. Otherwise, NSQ thinks it timed out and sends
	// the message to a new worker.
	message.DisableAutoResponse()

	if fetcher.canSkipFetchAndValidate(ingestState) {
		fetcher.Context.MessageLog.Info("Sending %s/%s straight to record queue",
			ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key)
		fetcher.RecordChannel <- ingestState
	} else {

		// Reserve disk space to download this item, or requeue it
		// if we can't get the disk space.
		if !fetcher.reserveSpaceForDownload(ingestState) {
			message.Requeue(1 * time.Minute)
			return nil
		}

		// Start at fetch, which is the very beginning.
		// This may be the second or third attempt to ingest this bag.
		// If so, clear out old error message from previous attempts.
		ingestState.IngestManifest.FetchResult.ClearErrors()
		ingestState.IngestManifest.ValidateResult.ClearErrors()

		fetcher.Context.MessageLog.Info("Putting %s/%s straight to fetch queue",
			ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key)

		fetcher.FetchChannel <- ingestState
	}

	// Return no error, so NSQ knows we're OK.
	return nil
}

// -------------------------------------------------------------------------
// Step 1 of 4: Fetch
//
// fetch copies the file from S3 to our local staging area.
// If all goes well, the file will wind up in
// ingestState.IngestManifest.Object.IngestTarFilePath
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) fetch() {
	for ingestState := range fetcher.FetchChannel {
		// Tell NSQ we're working on this
		ingestState.TouchNSQ()

		ingestState.IngestManifest.FetchResult.Start()
		ingestState.IngestManifest.FetchResult.Attempted = true
		ingestState.IngestManifest.FetchResult.AttemptNumber += 1

		err := fetcher.downloadFile(ingestState)

		// Download may have taken 1 second or 3 hours.
		// Remind NSQ that we're still on this.
		ingestState.TouchNSQ()

		if err != nil {
			ingestState.IngestManifest.FetchResult.AddError(err.Error())
		}
		ingestState.IngestManifest.FetchResult.Finish()
		fetcher.ValidationChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 2 of 4: Validate
//
// Make sure the tar file is a valid bag.
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) validate() {
	for ingestState := range fetcher.ValidationChannel {
		// Don't time us out, NSQ!
		ingestState.TouchNSQ()

		// Tell Pharos that we've started to validate item.
		// Let's NOT quit if there's an error here. In that case, Pharos
		// might not know that we're validating, but we can still proceed.
		// Restarting the whole fetch process would be expensive.
		ingestState.WorkItem, _ = fetcher.recordValidationStarted(ingestState.WorkItem)

		// Set up a new validator to check this bag.
		var validationResult *validation.ValidationResult
		validator, err := validation.NewBagValidator(
			ingestState.IngestManifest.Object.IngestTarFilePath,
			fetcher.BagValidationConfig)
		if err != nil {
			// Could not create a BagValidator. Should this be fatal?
			ingestState.IngestManifest.ValidateResult.AddError(err.Error())
		} else {

			// Here's where bag validation actually happens. There's a lot
			// going on in this call, which can take anywhere from 2 seconds
			// to 3 hours to complete, depending on the size of the bag.
			// The most time-consuming part of the validation process is
			// calculating md5 and sha256 checksums on every file in the bag.
			// If the bag is 100GB+ in size, that takes a long time.
			validationResult = validator.Validate()

			// The validator creates its own WorkSummary, complete with
			// Start/Finish timestamps, error messages and everything.
			// Just copy that into our IngestManifest.
			ingestState.IngestManifest.ValidateResult = validationResult.ValidationSummary

			// NOTE that we are OVERWRITING the IntellectualObject here
			// with the much more complete version returned by the validator,
			// but we have to reset some basic data that's only available
			// in the current context.
			ingestState.IngestManifest.Object = validationResult.IntellectualObject
			fetcher.setBasicObjectInfo(ingestState)

			// If the bag is invalid, that's a fatal error. We should not do
			// any further processing on it.
			if validationResult.HasErrors() {
				ingestState.IngestManifest.ValidateResult.ErrorIsFatal = true
			}
		}
		ingestState.TouchNSQ()
		fetcher.CleanupChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 3 of 4: Cleanup (conditional)
//
// cleanup deletes the tar file we just downloaded, if we determine that
// something is wrong with it and there should be no further processing.
// If the bag is valid, we leave it in the staging area. The next process
// (store) will pick it up and copy files to S3 and Glacier.
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) cleanup() {
	for ingestState := range fetcher.CleanupChannel {
		tarFile := ingestState.IngestManifest.Object.IngestTarFilePath
		hasErrors := (ingestState.IngestManifest.FetchResult.HasErrors() ||
			ingestState.IngestManifest.ValidateResult.HasErrors())
		if hasErrors && fileutil.FileExists(tarFile) {
			// Most likely bad md5 digest, but perhaps also a partial download.
			fetcher.Context.MessageLog.Info("Deleting due to download error: %s",
				tarFile)
			err := os.Remove(tarFile)
			if err != nil {
				fetcher.Context.MessageLog.Warning(err.Error())
			}
			err = fetcher.Context.VolumeClient.Release(ingestState.IngestManifest.Object.IngestTarFilePath)
			if err != nil {
				fetcher.Context.MessageLog.Warning(err.Error())
			}
		}
		fetcher.RecordChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 4 of 4: Record updates the WorkItem and WorkItemState in Pharos.
//
// record tells Pharos what's happened with this WorkItem,
// and it pushes the item into the next queue (validation)
// if necessary.
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) record() {
	for ingestState := range fetcher.RecordChannel {
		// Record the WorkItemState in Pharos. The next process (record)
		// will need this info to do its work. If there's an error in this
		// step, RecordWorkItemState will log it and add it to the FetchResult.
		// It will also dump a JSON representation of the IngestManifest to
		// the JSON log.
		RecordWorkItemState(ingestState, fetcher.Context, ingestState.IngestManifest.FetchResult)
		if ingestState.IngestManifest.HasFatalErrors() {
			// Set WorkItem node=nil, pid=0, retry=false, needs_admin_review=true
			// Finish the NSQ message
			ingestState.FinishNSQ()
			fetcher.markWorkItemFailed(ingestState)
		} else if ingestState.IngestManifest.HasErrors() {
			// Set WorkItem node=nil, pid=0
			// Requeue the NSQ message
			ingestState.RequeueNSQ(1000)
			fetcher.markWorkItemRequeued(ingestState)
		} else {
			// Set WorkItem stage to StageStore, status to StatusPending, node=nil, pid=0
			// Finish the NSQ message
			ingestState.FinishNSQ()
			fetcher.markWorkItemSucceeded(ingestState)
		}
	}
}

// Loads the bag validation config file specified in the general config
// options. This will die if the bag validation config cannot be loaded
// or is invalid.
func (fetcher *APTFetcher) loadBagValidationConfig() {
	bagValidationConfig, errors := validation.LoadBagValidationConfig(
		fetcher.Context.Config.BagValidationConfigFile)
	if errors != nil && len(errors) > 0 {
		msg := fmt.Sprintf("Could not load bag validation config from %s",
			fetcher.Context.Config.BagValidationConfigFile)
		for _, err := range errors {
			msg += fmt.Sprintf("%s ... ", err.Error())
		}
		fmt.Fprintln(os.Stderr, msg)
		fetcher.Context.MessageLog.Fatal(msg)
	}
	fetcher.BagValidationConfig = bagValidationConfig
}

// Make sure we have space to download this item.
func (fetcher *APTFetcher) reserveSpaceForDownload (ingestState *models.IngestState) (bool) {
	okToDownload := false
	err := fetcher.Context.VolumeClient.Ping(500)
	if err == nil {
		path := ingestState.IngestManifest.Object.IngestTarFilePath
		ok, err := fetcher.Context.VolumeClient.Reserve(path, uint64(ingestState.WorkItem.Size))
		if err != nil {
			fetcher.Context.MessageLog.Warning("Volume service returned an error. " +
				"Will requeue bag %s/%s because we may not have enough space to download %d bytes.",
				ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, ingestState.WorkItem.Size)
		} else if ok {
			// VolumeService says we have enough space for this.
			okToDownload = ok
		}
	} else {
		fetcher.Context.MessageLog.Warning("Volume service is not running or returned an error. " +
			"Continuing as if we have enough space to download %d bytes.",
			ingestState.WorkItem.Size)
		okToDownload = true
	}
	return okToDownload
}

// Returns true if we can skip fetch and validate. We can skip those
// steps if on a previous run we validated the bag, and it's still
// there in our working directory. This anticipates the case where
// we did those steps but were not able to update the WorkItem record
// in Pharos at the end of the fetch/validate process.
func (fetcher *APTFetcher) canSkipFetchAndValidate (ingestState *models.IngestState) (bool) {
	return (ingestState.WorkItem.Stage == constants.StageValidate &&
		ingestState.IngestManifest.ValidateResult.Finished() &&
		!ingestState.IngestManifest.HasFatalErrors() &&
		fileutil.FileExists(ingestState.IngestManifest.Object.IngestTarFilePath))
}

// Set up the basic pieces of data we'll need to process a fetch request.
func (fetcher *APTFetcher) initIngestState (message *nsq.Message) (*models.IngestState, error) {
	workItem, err := fetcher.getWorkItem(message)
	if err != nil {
		return nil, err
	}

	// TODO: Check WorkItem here. If another node or process owns it,
	// let it go.

	workItemState, err := fetcher.getWorkItemState(workItem)
	if err != nil {
		return nil, err
	}
	ingestManifest, err := workItemState.IngestManifest()
	if err != nil {
		return nil, err
	}
	ingestState := &models.IngestState{
		WorkItem: workItem,
		WorkItemState: workItemState,
		IngestManifest: ingestManifest,
	}

	// If this is a new WorkItemState, we didn't load it from Pharos,
	// and we have no IntelObj data. So set the basic IntelObj data now.
	if workItemState.Id == 0 {
		fetcher.setBasicObjectInfo(ingestState)
	}

	return ingestState, err
}

// Set some basic info on our intellectual object. Note that this may be called
// twice. First, when we're processing a brand new WorkItem and have to set the
// basic IntellectualObject data for the first time. Second, after the BagValidator
// returns its version of the IntellectualObject, which may not include this info.
func (fetcher *APTFetcher) setBasicObjectInfo(ingestState *models.IngestState) {
	// instIdentifier is, e.g., virginia.edu, ncsu.edu, etc.
	// We'll download the tar file from the receiving bucket to
	// something like /mnt/apt/data/virginia.edu/name_of_bag.tar
	// See IngestTarFilePath below.
	instIdentifier := util.OwnerOf(ingestState.IngestManifest.S3Bucket)
	ingestState.IngestManifest.Object.BagName = util.CleanBagName(ingestState.IngestManifest.S3Key)
	ingestState.IngestManifest.Object.Institution = instIdentifier
	// -----------------------------------------------------------
	// TODO: Get institution id! (Cache them?)
	// -----------------------------------------------------------
	//ingestState.IngestManifest.Object.InstitutionId =
	ingestState.IngestManifest.Object.IngestS3Bucket = ingestState.IngestManifest.S3Bucket
	ingestState.IngestManifest.Object.IngestS3Key = ingestState.IngestManifest.S3Key
	ingestState.IngestManifest.Object.IngestTarFilePath = filepath.Join(
		fetcher.Context.Config.TarDirectory,
		instIdentifier, ingestState.IngestManifest.S3Key)

}

// Returns the WorkItem record from Pharos that has the WorkItemId
// specified in the NSQ message.
func (fetcher *APTFetcher) getWorkItem(message *nsq.Message) (*models.WorkItem, error) {
	workItemId, err := strconv.Atoi(string(message.Body))
	if err != nil {
		return nil, fmt.Errorf("Could not get WorkItemId from NSQ message body: %v", err)
	}
	resp := fetcher.Context.PharosClient.WorkItemGet(workItemId)
	if resp.Error != nil {
		return nil, fmt.Errorf("Error getting WorkItem %d from Pharos: %v", err)
	}
	workItem := resp.WorkItem()
	if workItem == nil {
		return nil, fmt.Errorf("Pharos returned nil for WorkItem %d", workItemId)
	}
	return workItem, nil
}

// Returns the WorkItemState record from Pharos with the specified workItem.Id,
// or creates a new WorkItemState (if necessary) and returns that. If this is
// the first time we've attempted to ingest this item, we'll have to crate a
// new WorkItemState.
func (fetcher *APTFetcher) getWorkItemState(workItem *models.WorkItem) (*models.WorkItemState, error) {
	var workItemState *models.WorkItemState
	var err error
	resp := fetcher.Context.PharosClient.WorkItemStateGet(workItem.Id)
	if resp.Response.StatusCode == http.StatusNotFound {
		// Record has not been created yet, so build a new one now.
		workItemState, err = fetcher.initWorkItemState(workItem)
		if err != nil {
			return nil, err
		}
	} else if resp.Error != nil {
		// We got some other 4xx/5xx error from the Pharos REST service.
		return nil, fmt.Errorf("Error getting WorkItemState for WorkItem %d from Pharos: %v", resp.Error)
	} else {
		// We didn't get a 404 or any other error. The WorkItemState should be in
		// the response.
		workItemState = resp.WorkItemState()
		if workItemState == nil {
			return nil, fmt.Errorf("Pharos returned nil for WorkItemState with WorkItem id %d", workItem.Id)
		}
	}
	return workItemState, nil
}

// Create a new WorkItemState object for this WorkItem.
// We do this only when Pharos doesn't already have a WorkItemState
// object, which is often the case when ingesting new bags.
func (fetcher *APTFetcher) initWorkItemState (workItem *models.WorkItem) (*models.WorkItemState, error) {
	ingestManifest := models.NewIngestManifest()
	ingestManifest.WorkItemId = workItem.Id
	ingestManifest.S3Bucket = workItem.Bucket
	ingestManifest.S3Key = workItem.Name
	ingestManifest.ETag = workItem.ETag
	workItemState := models.NewWorkItemState(workItem.Id, constants.ActionIngest, "")
	err := workItemState.SetStateFromIngestManifest(ingestManifest)
	if err != nil {
		return nil, err
	}
	return workItemState, nil
}

// Tell Pharos we've started to fetch this item from S3.
func (fetcher *APTFetcher) recordFetchStarted (workItem *models.WorkItem) (*models.WorkItem, error) {
	fetcher.Context.MessageLog.Info("Telling Pharos fetch started for %s/%s", workItem.Bucket, workItem.Name)
	utcNow := time.Now().UTC()
	workItem.SetNodeAndPid()
	workItem.Stage = constants.StageFetch
	workItem.StageStartedAt = &utcNow
	workItem.Status = constants.StatusStarted
	workItem.Note = "Fetching bag from receiving bucket."
	resp := fetcher.Context.PharosClient.WorkItemSave(workItem)
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.WorkItem(), nil
}

// Tell Pharos we've started validation on this item.
func (fetcher *APTFetcher) recordValidationStarted (workItem *models.WorkItem) (*models.WorkItem, error) {
	fetcher.Context.MessageLog.Info("Telling Pharos validation started for %s/%s", workItem.Bucket, workItem.Name)
	workItem.Stage = constants.StageValidate
	workItem.Note = "Validating bag."
	resp := fetcher.Context.PharosClient.WorkItemSave(workItem)
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.WorkItem(), nil
}

// Tell Pharos that this item cannot be ingested, due to a fatal error.
func (fetcher *APTFetcher) markWorkItemFailed (ingestState *models.IngestState) (error) {
	fetcher.Context.MessageLog.Info("Telling Pharos ingest failed for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Node = ""
	ingestState.WorkItem.Pid = 0
	ingestState.WorkItem.StageStartedAt = nil
	ingestState.WorkItem.Retry = false
	ingestState.WorkItem.NeedsAdminReview = true
	ingestState.WorkItem.Status = constants.StatusFailed
	ingestState.WorkItem.Note = ingestState.IngestManifest.FetchResult.AllErrorsAsString() + ingestState.IngestManifest.ValidateResult.AllErrorsAsString()
	resp := fetcher.Context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		fetcher.Context.MessageLog.Error("Could not mark WorkItem failed for %s/%s: %v",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}

// Tell Pharos that this item has been requeued due to transient errors.
func (fetcher *APTFetcher) markWorkItemRequeued (ingestState *models.IngestState) (error) {
	fetcher.Context.MessageLog.Info("Telling Pharos ingest is being requeued for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Node = ""
	ingestState.WorkItem.Pid = 0
	ingestState.WorkItem.StageStartedAt = nil
	ingestState.WorkItem.Retry = true
	ingestState.WorkItem.NeedsAdminReview = false
	ingestState.WorkItem.Status = constants.StatusStarted
	ingestState.WorkItem.Note = "Item has been requeued due to transient errors. " + ingestState.IngestManifest.FetchResult.AllErrorsAsString() + ingestState.IngestManifest.ValidateResult.AllErrorsAsString()
	resp := fetcher.Context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		fetcher.Context.MessageLog.Error("Could not mark WorkItem requeued for %s/%s: %v",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}

// Tell Pharos that this item was successfully downloaded and validated.
func (fetcher *APTFetcher) markWorkItemSucceeded (ingestState *models.IngestState) (error) {
	fetcher.Context.MessageLog.Info("Telling Pharos ingest can proceed for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Node = ""
	ingestState.WorkItem.Pid = 0
	ingestState.WorkItem.Retry = true
	ingestState.WorkItem.StageStartedAt = nil
	ingestState.WorkItem.NeedsAdminReview = false
	ingestState.WorkItem.Stage = constants.StageStore
	ingestState.WorkItem.Status = constants.StatusPending
	ingestState.WorkItem.Note = "Item passed validation and is ready for storage."
	resp := fetcher.Context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		fetcher.Context.MessageLog.Error("Could not mark WorkItem ready for storage for %s/%s: %v",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}

// Download the file, and update the IngestManifest while we're at it.
func (fetcher *APTFetcher) downloadFile (ingestState *models.IngestState) (error) {
	downloader := network.NewS3Download(
		constants.AWSVirginia,
		ingestState.IngestManifest.S3Bucket,
		ingestState.IngestManifest.S3Key,
		ingestState.IngestManifest.Object.IngestTarFilePath,
		true,    // calculate md5 checksum on the entire tar file
		false,   // calculate sha256 checksum on the entire tar file
	)

	// It's fairly common for very large bags to fail more than
	// once on transient network errors (e.g. "Connection reset by peer")
	// So we give this several tries.
	for i := 0; i < 10; i++ {
		downloader.Fetch()
		if downloader.ErrorMessage == "" {
			fetcher.Context.MessageLog.Info("Fetched %s/%s after %d attempts",
				ingestState.IngestManifest.S3Bucket,
				ingestState.IngestManifest.S3Key,
				i + 1)
			break
		}
	}

	// Return now if we failed.
	if downloader.ErrorMessage != "" {
		return fmt.Errorf("Error fetching %s/%s: %v",
			ingestState.IngestManifest.S3Bucket,
			ingestState.IngestManifest.S3Key,
			downloader.ErrorMessage)
	}

	obj := ingestState.IngestManifest.Object
	obj.IngestSize = downloader.BytesCopied
	obj.IngestRemoteMd5 = *downloader.Response.ETag
	obj.IngestLocalMd5 = downloader.Md5Digest

	// The ETag for S3 object uploaded via single-part upload is
	// the file's md5 digest. For objects uploaded via multi-part
	// upload, the ETag is calculated differently and includes a
	// dash near the end, followed by the number of parts in the
	// multipart upload. We can't use that kind of ETag to verify
	// the md5 checksum that we calculated.
	obj.IngestMd5Verifiable = strings.Contains(downloader.Md5Digest, "-")
	if obj.IngestMd5Verifiable {
		obj.IngestMd5Verified = obj.IngestRemoteMd5 == obj.IngestLocalMd5
	}

	// If we got a bad checksum, note the error in the WorkSummary.
	if obj.IngestMd5Verifiable && !obj.IngestMd5Verified {
		ingestState.IngestManifest.FetchResult.AddError("Our md5 '%s' does not match S3 md5 '%s'",
			obj.IngestLocalMd5, obj.IngestRemoteMd5)
		ingestState.IngestManifest.FetchResult.ErrorIsFatal = true
	}

	return nil
}

// This is for direct testing without NSQ.
func (fetcher *APTFetcher) RunWithoutNsq(ingestState *models.IngestState) {
	fetcher.WaitGroup.Add(1)
	fetcher.FetchChannel <- ingestState
	fetcher.Context.MessageLog.Debug("Put %s into Fluctus channel", ingestState.IngestManifest.S3Key)
	fetcher.WaitGroup.Wait()
}
