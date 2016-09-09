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
//	"time"
)

type APTFetcher struct {
	Context             *context.Context
	BagValidationConfig *validation.BagValidationConfig
	FetchChannel        chan *FetchData
	RecordChannel       chan *FetchData
	ValidationChannel   chan *FetchData
	CleanupChannel      chan *FetchData
	WaitGroup           sync.WaitGroup
}

type FetchData struct {
	WorkItem        *models.WorkItem
	WorkItemState   *models.WorkItemState
	IngestManifest  *models.IngestManifest
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
	// Tell Pharos that we've started work on the item.
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

	// Now get to work.
	fetcher.FetchChannel <- fetchData
	return nil
}

// fetch copies the file from S3 to our local staging area.
// If all goes well, the file will wind up in
// fetchData.IngestManifest.Object.IngestTarFilePath
func (fetcher *APTFetcher) fetch() {
	for fetchData := range fetcher.FetchChannel {
		// Tell NSQ we're working on this
		fetchData.IngestManifest.NSQMessage.Touch()

		fetchData.IngestManifest.Fetch.Start()
		fetchData.IngestManifest.Fetch.Attempted = true

		err := fetcher.downloadFile(fetchData)

		// Download may have taken 1 second or 3 hours.
		// Remind NSQ that we're still on this.
		fetchData.IngestManifest.NSQMessage.Touch()

		if err != nil {
			fetchData.IngestManifest.Fetch.AddError(err.Error())
		}
		fetcher.CleanupChannel <- fetchData
	}
}

func (fetcher *APTFetcher) validate() {
//	for fetchData := range fetcher.FetchChannel {
//
//	}
}

// cleanup deletes the tar file we just downloaded, if we determine that
// something is wrong with it and there should be no further processing.
func (fetcher *APTFetcher) cleanup() {
	for fetchData := range fetcher.CleanupChannel {
		tarFile := fetchData.IngestManifest.Object.IngestTarFilePath
		if fetchData.IngestManifest.Fetch.HasErrors() && fileutil.FileExists(tarFile) {
			// Most likely bad md5 digest, but perhaps also a partial download.
			fetcher.Context.MessageLog.Info("Deleting due to download error: %s",
				tarFile)
			os.Remove(tarFile)
		}
		fetcher.RecordChannel <- fetchData
	}
}

// record tells Pharos what's happened with this WorkItem,
// and it pushes the item into the next queue (validation)
// if necessary.
func (fetcher *APTFetcher) record() {
//	for fetchData := range fetcher.RecordChannel {
		// Call fetchData.IngestManifest.Fetch.Finish()

		// Log WorkItemState
		// Save WorkItemState to Pharos

		// If no errors:
		// Set WorkItem stage to StageValidate, status to StatusPending, node=nil, pid=0
		// Finish the NSQ message

		// If transient errors:
		// Set WorkItem node=nil, pid=0
		// Requeue the NSQ message

		// If fatal errors:
		// Set WorkItem node=nil, pid=0, retry=false, needs_admin_review=true
		// Finish the NSQ message
//	}
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

	// Set some basic info on our IntellectualObject
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

// Tell Pharos we've started work on this item.
func (fetcher *APTFetcher) recordFetchStarted (workItem *models.WorkItem) (*models.WorkItem, error) {
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
		fetchData.IngestManifest.Fetch.AddError("Our md5 '%s' does not match S3 md5 '%s'",
			obj.IngestLocalMd5, obj.IngestRemoteMd5)
		fetchData.IngestManifest.Fetch.ErrorIsFatal = true
	}

	return nil
}

// This is for direct testing without NSQ.
func (fetcher *APTFetcher) RunWithoutNsq(fetchData *FetchData) {
	fetcher.WaitGroup.Add(1)
	fetcher.FetchChannel <- fetchData
	fetcher.Context.MessageLog.Debug("Put %s into Fluctus channel", fetchData.IngestManifest.S3Key)
	fetcher.WaitGroup.Wait()
}
