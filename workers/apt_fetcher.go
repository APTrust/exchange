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

type APTFetcher struct {
	Context             *context.Context
	BagValidationConfig *validation.BagValidationConfig
	FetchChannel        chan *FetchData
	ValidationChannel   chan *FetchData
	CleanupChannel      chan *FetchData
	RecordChannel       chan *FetchData
	WaitGroup           sync.WaitGroup
}

type FetchData struct {
	NSQMessage      *nsq.Message
	WorkItem        *models.WorkItem
	WorkItemState   *models.WorkItemState
	IngestManifest  *models.IngestManifest
}

// Tell NSQ we're still working on this item. NSQMessage will be nil if
// we're doing one-off testing (see RunWithoutNSQ).
func (fetchData *FetchData) TouchNSQ() {
	if fetchData.NSQMessage != nil {
		fetchData.NSQMessage.Touch()
	}
}

func (fetchData *FetchData) FinishNSQ() {
	if fetchData.NSQMessage != nil {
		fetchData.NSQMessage.Finish()
	}
}

func (fetchData *FetchData) RequeueNSQ(milliseconds int) {
	if fetchData.NSQMessage != nil {
		fetchData.NSQMessage.Requeue(time.Duration(milliseconds) * time.Millisecond)
	}
}



func NewATPFetcher(_context *context.Context) (*APTFetcher) {
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
	fetcher.FetchChannel = make(chan *FetchData, fetcherBufferSize)
	fetcher.ValidationChannel = make(chan *FetchData, workerBufferSize)
	fetcher.RecordChannel = make(chan *FetchData, workerBufferSize)
	fetcher.CleanupChannel = make(chan *FetchData, workerBufferSize)
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

func (fetcher *APTFetcher) HandleMessage(message *nsq.Message) (error) {

	// Set up our fetch data. Most of this comes from Pharos;
	// some of it we have to build fresh.
	fetchData, err := fetcher.initFetchData(message)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return err
	}
	// Save the state of this item in Pharos.
	resp := fetcher.Context.PharosClient.WorkItemStateSave(fetchData.WorkItemState)
	if resp.Error != nil {
		return resp.Error
	}
	// Tell Pharos that we've started to fetch this item.
	fetchData.WorkItem, err = fetcher.recordFetchStarted(fetchData.WorkItem)
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

	if fetcher.canSkipFetchAndValidate(fetchData) {
		fetcher.Context.MessageLog.Info("Sending %s/%s straight to record queue",
			fetchData.IngestManifest.S3Bucket, fetchData.IngestManifest.S3Key)
		fetcher.RecordChannel <- fetchData
	} else {

		// Reserve disk space to download this item, or requeue it
		// if we can't get the disk space.
		if !fetcher.reserveSpaceForDownload(fetchData) {
			message.Requeue(1 * time.Minute)
			return nil
		}

		// Start at fetch, which is the very beginning.
		// This may be the second or third attempt to ingest this bag.
		// If so, clear out old error message from previous attempts.
		fetchData.IngestManifest.FetchResult.ClearErrors()
		fetchData.IngestManifest.ValidateResult.ClearErrors()

		fetcher.Context.MessageLog.Info("Putting %s/%s straight to fetch queue",
			fetchData.IngestManifest.S3Bucket, fetchData.IngestManifest.S3Key)

		fetcher.FetchChannel <- fetchData
	}

	// Return no error, so NSQ knows we're OK.
	return nil
}

// -------------------------------------------------------------------------
// Step 1 of 4: Fetch
//
// fetch copies the file from S3 to our local staging area.
// If all goes well, the file will wind up in
// fetchData.IngestManifest.Object.IngestTarFilePath
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) fetch() {
	for fetchData := range fetcher.FetchChannel {
		// Tell NSQ we're working on this
		fetchData.TouchNSQ()

		fetchData.IngestManifest.FetchResult.Start()
		fetchData.IngestManifest.FetchResult.Attempted = true
		fetchData.IngestManifest.FetchResult.AttemptNumber += 1

		err := fetcher.downloadFile(fetchData)

		// Download may have taken 1 second or 3 hours.
		// Remind NSQ that we're still on this.
		fetchData.TouchNSQ()

		if err != nil {
			fetchData.IngestManifest.FetchResult.AddError(err.Error())
		}
		fetchData.IngestManifest.FetchResult.Finish()
		fetcher.ValidationChannel <- fetchData
	}
}

// -------------------------------------------------------------------------
// Step 2 of 4: Validate
//
// Make sure the tar file is a valid bag.
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) validate() {
	for fetchData := range fetcher.ValidationChannel {
		// Don't time us out, NSQ!
		fetchData.TouchNSQ()

		// Tell Pharos that we've started to validate item.
		// Let's NOT quit if there's an error here. In that case, Pharos
		// might not know that we're validating, but we can still proceed.
		// Restarting the whole fetch process would be expensive.
		fetchData.WorkItem, _ = fetcher.recordValidationStarted(fetchData.WorkItem)

		// Set up a new validator to check this bag.
		var validationResult *validation.ValidationResult
		validator, err := validation.NewBagValidator(
			fetchData.IngestManifest.Object.IngestTarFilePath,
			fetcher.BagValidationConfig)
		if err != nil {
			// Could not create a BagValidator. Should this be fatal?
			fetchData.IngestManifest.ValidateResult.AddError(err.Error())
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
			fetchData.IngestManifest.ValidateResult = validationResult.ValidationSummary

			// NOTE that we are OVERWRITING the IntellectualObject here
			// with the much more complete version returned by the validator.
			fetchData.IngestManifest.Object = validationResult.IntellectualObject

			// If the bag is invalid, that's a fatal error. We should not do
			// any further processing on it.
			if validationResult.HasErrors() {
				fetchData.IngestManifest.ValidateResult.ErrorIsFatal = true
			}
		}
		fetchData.TouchNSQ()
		fetcher.CleanupChannel <- fetchData
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
	for fetchData := range fetcher.CleanupChannel {
		tarFile := fetchData.IngestManifest.Object.IngestTarFilePath
		hasErrors := (fetchData.IngestManifest.FetchResult.HasErrors() ||
			fetchData.IngestManifest.ValidateResult.HasErrors())
		if hasErrors && fileutil.FileExists(tarFile) {
			// Most likely bad md5 digest, but perhaps also a partial download.
			fetcher.Context.MessageLog.Info("Deleting due to download error: %s",
				tarFile)
			err := os.Remove(tarFile)
			if err != nil {
				fetcher.Context.MessageLog.Warning(err.Error())
			}
			err = fetcher.Context.VolumeClient.Release(fetchData.IngestManifest.Object.IngestTarFilePath)
			if err != nil {
				fetcher.Context.MessageLog.Warning(err.Error())
			}
		}
		fetcher.RecordChannel <- fetchData
	}
}

// -------------------------------------------------------------------------
// Step 4 of 4: Record updates the WorItem and WorkItemState in Pharos.
//
// record tells Pharos what's happened with this WorkItem,
// and it pushes the item into the next queue (validation)
// if necessary.
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) record() {
	for fetchData := range fetcher.RecordChannel {
	    // Record the WorkItemState in Pharos. The next process (record)
		// will need this info to do its work. If there's an error in this
		// step, recordWorkItemState will log it and add it to the FetchResult.
		// It will also dump a JSON representation of the IngestManifest to
		// the JSON log.
		fetcher.recordWorkItemState(fetchData)
		if fetchData.IngestManifest.HasFatalErrors() {
			// Set WorkItem node=nil, pid=0, retry=false, needs_admin_review=true
			// Finish the NSQ message
			fetchData.FinishNSQ()
			fetcher.markWorkItemFailed(fetchData)
		} else if fetchData.IngestManifest.HasErrors() {
			// Set WorkItem node=nil, pid=0
			// Requeue the NSQ message
			fetchData.RequeueNSQ(1000)
			fetcher.markWorkItemRequeued(fetchData)
		} else {
			// Set WorkItem stage to StageStore, status to StatusPending, node=nil, pid=0
			// Finish the NSQ message
			fetchData.FinishNSQ()
			fetcher.markWorkItemSucceeded(fetchData)
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
			msg += fmt.Sprintf("%s ... ", err.Error)
		}
		fmt.Fprintln(os.Stderr, msg)
		fetcher.Context.MessageLog.Fatal(msg)
	}
	fetcher.BagValidationConfig = bagValidationConfig
}

// Make sure we have space to download this item.
func (fetcher *APTFetcher) reserveSpaceForDownload (fetchData *FetchData) (bool) {
	okToDownload := false
	err := fetcher.Context.VolumeClient.Ping(500)
	if err == nil {
		path := fetchData.IngestManifest.Object.IngestTarFilePath
		ok, err := fetcher.Context.VolumeClient.Reserve(path, uint64(fetchData.WorkItem.Size))
		if err != nil {
			fetcher.Context.MessageLog.Warning("Volume service returned an error. " +
				"Will requeue bag %s/%s because we may not have enough space to download %d bytes.",
				fetchData.WorkItem.Bucket, fetchData.WorkItem.Name, fetchData.WorkItem.Size)
		} else if ok {
			// VolumeService says we have enough space for this.
			okToDownload = ok
		}
	} else {
		fetcher.Context.MessageLog.Warning("Volume service is not running or returned an error. " +
			"Continuing as if we have enough space to download %d bytes.",
			fetchData.WorkItem.Size)
		okToDownload = true
	}
	return okToDownload
}

// Returns true if we can skip fetch and validate. We can skip those
// steps if on a previous run we validated the bag, and it's still
// there in our working directory. This anticipates the case where
// we did those steps but were not able to update the WorkItem record
// in Pharos at the end of the fetch/validate process.
func (fetcher *APTFetcher) canSkipFetchAndValidate (fetchData *FetchData) (bool) {
		return (fetchData.WorkItem.Stage == constants.StageValidate &&
			fetchData.IngestManifest.ValidateResult.Finished() &&
			!fetchData.IngestManifest.HasFatalErrors() &&
			fileutil.FileExists(fetchData.IngestManifest.Object.IngestTarFilePath))
}

// Set up the basic pieces of data we'll need to process a fetch request.
func (fetcher *APTFetcher) initFetchData (message *nsq.Message) (*FetchData, error) {
	workItem, err := fetcher.getWorkItem(message)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return nil, err
	}
	workItemState, err := fetcher.getWorkItemState(workItem)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return nil, err
	}
	ingestManifest, err := workItemState.IngestManifest()
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return nil, err
	}
	fetchData := &FetchData{
		WorkItem: workItem,
		WorkItemState: workItemState,
		IngestManifest: ingestManifest,
	}

	// instIdentifier is, e.g., virginia.edu, ncsu.edu, etc.
	// We'll download the tar file from the receiving bucket to
	// something like /mnt/apt/data/virginia.edu/name_of_bag.tar
	// See IngestTarFilePath below.
	instIdentifier := util.OwnerOf(fetchData.IngestManifest.S3Bucket)

	// Set some basic info on our IntellectualObject.
	// Note that we only do this for a brand new bag that we have
	// never before attempted to ingest. Also note that
	// fetchData.IngestManifest.Object will be overwritten in the
	// validate() function above. The BagValidator will construct a
	// much more comprehensive IntellectualObject. For now, we're just
	// adding in the data we need to get started.
	fetchData.IngestManifest.Object.BagName = util.CleanBagName(fetchData.IngestManifest.S3Key)
	fetchData.IngestManifest.Object.Institution = instIdentifier
	// -----------------------------------------------------------
	// TODO: Get institution id! (Cache them?)
	// -----------------------------------------------------------
	//fetchData.IngestManifest.Object.InstitutionId =
	fetchData.IngestManifest.Object.IngestS3Bucket = fetchData.IngestManifest.S3Bucket
	fetchData.IngestManifest.Object.IngestS3Key = fetchData.IngestManifest.S3Key
	fetchData.IngestManifest.Object.IngestTarFilePath = filepath.Join(
		fetcher.Context.Config.TarDirectory,
		instIdentifier, fetchData.IngestManifest.S3Key)

	return fetchData, err
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
	hostname, _ := os.Hostname()
	if hostname == "" { hostname = "apt_fetcher_host" }
	workItem.Node = hostname
	workItem.Stage = constants.StageFetch
	workItem.Status = constants.StatusStarted
	workItem.Pid = os.Getpid()
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
func (fetcher *APTFetcher) markWorkItemFailed (fetchData *FetchData) (error) {
    fetcher.Context.MessageLog.Info("Telling Pharos ingest failed for %s/%s",
		fetchData.WorkItem.Bucket, fetchData.WorkItem.Name)
	fetchData.WorkItem.Pid = 0
	fetchData.WorkItem.Node = ""
	fetchData.WorkItem.Retry = false
	fetchData.WorkItem.NeedsAdminReview = true
	fetchData.WorkItem.Status = constants.StatusFailed
	fetchData.WorkItem.Note = fetchData.IngestManifest.FetchResult.AllErrorsAsString() + fetchData.IngestManifest.ValidateResult.AllErrorsAsString()
	resp := fetcher.Context.PharosClient.WorkItemSave(fetchData.WorkItem)
	if resp.Error != nil {
		fetcher.Context.MessageLog.Error("Could not mark WorkItem failed for %s/%s: %v",
			fetchData.WorkItem.Bucket, fetchData.WorkItem.Name, resp.Error)
		return resp.Error
	}
	fetchData.WorkItem = resp.WorkItem()
	return nil
}

// Tell Pharos that this item has been requeued due to transient errors.
func (fetcher *APTFetcher) markWorkItemRequeued (fetchData *FetchData) (error) {
    fetcher.Context.MessageLog.Info("Telling Pharos ingest requeued for %s/%s",
		fetchData.WorkItem.Bucket, fetchData.WorkItem.Name)
	fetchData.WorkItem.Pid = 0
	fetchData.WorkItem.Node = ""
	fetchData.WorkItem.Retry = true
	fetchData.WorkItem.NeedsAdminReview = false
	fetchData.WorkItem.Status = constants.StatusStarted
	fetchData.WorkItem.Note = "Item has been requeued due to transient errors. " + fetchData.IngestManifest.FetchResult.AllErrorsAsString() + fetchData.IngestManifest.ValidateResult.AllErrorsAsString()
	resp := fetcher.Context.PharosClient.WorkItemSave(fetchData.WorkItem)
	if resp.Error != nil {
		fetcher.Context.MessageLog.Error("Could not mark WorkItem requeued for %s/%s: %v",
			fetchData.WorkItem.Bucket, fetchData.WorkItem.Name, resp.Error)
		return resp.Error
	}
	fetchData.WorkItem = resp.WorkItem()
	return nil
}

// Tell Pharos that this item was successfully downloaded and validated.
func (fetcher *APTFetcher) markWorkItemSucceeded (fetchData *FetchData) (error) {
    fetcher.Context.MessageLog.Info("Telling Pharos ingest can proceed for %s/%s",
		fetchData.WorkItem.Bucket, fetchData.WorkItem.Name)
	fetchData.WorkItem.Pid = 0
	fetchData.WorkItem.Node = ""
	fetchData.WorkItem.Retry = true
	fetchData.WorkItem.NeedsAdminReview = false
	fetchData.WorkItem.Stage = constants.StageRecord
	fetchData.WorkItem.Status = constants.StatusPending
	fetchData.WorkItem.Note = "Item passed validation and is ready for storage."
	resp := fetcher.Context.PharosClient.WorkItemSave(fetchData.WorkItem)
	if resp.Error != nil {
		fetcher.Context.MessageLog.Error("Could not mark WorkItem ready for storage for %s/%s: %v",
			fetchData.WorkItem.Bucket, fetchData.WorkItem.Name, resp.Error)
		return resp.Error
	}
	fetchData.WorkItem = resp.WorkItem()
	return nil
}

// Download the file, and update the IngestManifest while we're at it.
func (fetcher *APTFetcher) downloadFile (fetchData *FetchData) (error) {
	downloader := network.NewS3Download(
		constants.AWSVirginia,
		fetchData.IngestManifest.S3Bucket,
		fetchData.IngestManifest.S3Key,
		fetchData.IngestManifest.Object.IngestTarFilePath,
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
				fetchData.IngestManifest.S3Bucket,
				fetchData.IngestManifest.S3Key,
				i + 1)
			break
		}
	}

	// Return now if we failed.
	if downloader.ErrorMessage != "" {
		return fmt.Errorf("Error fetching %s/%s: %v",
			fetchData.IngestManifest.S3Bucket,
			fetchData.IngestManifest.S3Key,
			downloader.ErrorMessage)
	}

	obj := fetchData.IngestManifest.Object
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
		fetchData.IngestManifest.FetchResult.AddError("Our md5 '%s' does not match S3 md5 '%s'",
			obj.IngestLocalMd5, obj.IngestRemoteMd5)
		fetchData.IngestManifest.FetchResult.ErrorIsFatal = true
	}

	return nil
}

// Record the WorkItemState for this task. We drop a copy into our
// JSON log as a backup, and updated the WorkItemState in Pharos,
// so the next worker knows what to do with this item.
func (fetcher *APTFetcher) recordWorkItemState(fetchData *FetchData) {
	// Serialize the IngestManifest to JSON, and stuff it into the
	// WorkItemState.State. Subsequent workers need this info to
	// store the object's files in S3 and Glacier, and to record
	// results in Pharos.
	err := fetchData.WorkItemState.SetStateFromIngestManifest(fetchData.IngestManifest)
	if err != nil {
		// If we couldn't serialize the IngestManifest, subsequent workers
		// won't have the info they need to process this bag. We'll have to
		// requeue this item and start all over.
		fetcher.Context.MessageLog.Error(err.Error())
		fetchData.IngestManifest.FetchResult.AddError("Could not convert Ingest Manifest " +
			"to JSON. This item will have to be re-processed. Error was: %v", err)
	} else {
		// OK. We serialized the IngestManifest. Dump a copy into the
		// file system for backup and troubleshooting, and send a copy
		// over to Pharos, so the next worker in the chain (the save worker)
		// can access it.
		fetcher.logJson(fetchData)
		resp := fetcher.Context.PharosClient.WorkItemStateSave(fetchData.WorkItemState)
		if resp.Error != nil {
			// Could not send a copy of the WorkItemState to Pharos.
			// That means subsequent workers won't have the info they
			// need to work on this bag. We'll have to start processing
			// all over again.
			fetcher.Context.MessageLog.Error(err.Error())
			fetchData.IngestManifest.FetchResult.AddError("Could not save WorkItemState " +
				"to Pharos. This item will have to be re-processed. Error was: %v", err)
		} else {
			// Saved to Pharos!
			fetchData.WorkItemState = resp.WorkItemState()
		}
	}
}

// Dump the WorkItemState.State into the JSON log, surrounded my markers that
// make it easy to find. This log gets big.
func (fetcher *APTFetcher) logJson (fetchData *FetchData) {
	timestamp := time.Now().UTC().Format(time.RFC3339)
	startMessage := fmt.Sprintf("-------- BEGIN %s/%s | Etag: %s | Time: %s --------",
		fetchData.WorkItem.Bucket, fetchData.WorkItem.Name, fetchData.WorkItem.ETag,
		timestamp)
	endMessage := fmt.Sprintf("-------- END %s/%s | Etag: %s | Time: %s --------",
		fetchData.WorkItem.Bucket, fetchData.WorkItem.Name, fetchData.WorkItem.ETag,
		timestamp)
	fetcher.Context.JsonLog.Println(startMessage, "\n",
		fetchData.WorkItemState.State, "\n",
		endMessage, "\n")
}

// This is for direct testing without NSQ.
func (fetcher *APTFetcher) RunWithoutNsq(fetchData *FetchData) {
	fetcher.WaitGroup.Add(1)
	fetcher.FetchChannel <- fetchData
	fetcher.Context.MessageLog.Debug("Put %s into Fluctus channel", fetchData.IngestManifest.S3Key)
	fetcher.WaitGroup.Wait()
}
