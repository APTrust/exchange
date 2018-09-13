package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	dpn_network "github.com/APTrust/exchange/dpn/network"
	apt_network "github.com/APTrust/exchange/network"
	"github.com/nsqio/go-nsq"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// If S3 download fails with a non-fatal error, how many
// minutes should we wait before trying again?
const MINUTES_BETWEEN_RETRIES = 3

// Fetches from S3 to local storage.
type DPNS3Retriever struct {
	// Context includes logging, config, network connections, and
	// other general resources for the worker.
	Context *context.Context
	// LocalDPNRestClient lets us talk to our local DPN server.
	LocalDPNRestClient *dpn_network.DPNRestClient
	// FetchChannel is for fetching files from S3.
	FetchChannel chan *DPNRestoreHelper
	// CleanupChannel is for post-fetch processing.
	CleanupChannel chan *DPNRestoreHelper
	// PostTestChannel is for testing only. In production, nothing listens
	// on this channel.
	PostTestChannel chan *DPNRestoreHelper
}

func NewDPNS3Retriever(_context *context.Context) (*DPNS3Retriever, error) {
	retriever := &DPNS3Retriever{
		Context: _context,
	}

	// Set up buffered channels
	fetcherBufferSize := _context.Config.FetchWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.FetchWorker.Workers * 10
	retriever.FetchChannel = make(chan *DPNRestoreHelper, fetcherBufferSize)
	retriever.CleanupChannel = make(chan *DPNRestoreHelper, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.FetchWorker.NetworkConnections; i++ {
		go retriever.fetch()
	}
	for i := 0; i < _context.Config.FetchWorker.Workers; i++ {
		go retriever.cleanup()
	}

	// Set up a client to talk to our local DPN server.
	var err error
	retriever.LocalDPNRestClient, err = dpn_network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	return retriever, err
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (retriever *DPNS3Retriever) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	helper, err := NewDPNRestoreHelper(message, retriever.Context,
		retriever.LocalDPNRestClient, constants.ActionFixityCheck,
		"LocalCopySummary")
	if err != nil {
		retriever.Context.MessageLog.Error(err.Error())
		return err
	}
	helper.WorkSummary.ClearErrors()
	helper.WorkSummary.Start()
	helper.Manifest.DPNWorkItem.Status = constants.StatusStarted
	helper.Manifest.DPNWorkItem.Stage = constants.StageFetch
	helper.SaveDPNWorkItem()
	if helper.WorkSummary.HasErrors() {
		retriever.Context.MessageLog.Error("Error setting up manifest for WorkItem %s: %s",
			string(message.Body), helper.WorkSummary.AllErrorsAsString())
		// No use proceeding...
		retriever.CleanupChannel <- helper
		return fmt.Errorf(helper.WorkSummary.AllErrorsAsString())
	}
	if helper.Manifest.DPNWorkItem.IsCompletedOrCancelled() {
		retriever.Context.MessageLog.Info("Skipping WorkItem %d because status is %s",
			helper.Manifest.DPNWorkItem.Id, helper.Manifest.DPNWorkItem.Status)
		retriever.CleanupChannel <- helper
		return nil
	}

	// OK, we're good. Retrieve the file from S3.
	retriever.FetchChannel <- helper
	return nil
}

// Fetch the item from S3, if necessary.
func (retriever *DPNS3Retriever) fetch() {
	for helper := range retriever.FetchChannel {
		// Retrieve the tar file if it's not already on disk.
		helper.Manifest.NsqMessage.Touch()
		if !helper.FileExistsAndIsComplete() {
			retriever.DownloadFile(helper)
		}
		retriever.CleanupChannel <- helper
	}
}

// Cleanup sends data back to Pharos and NSQ about the status of this task.
func (retriever *DPNS3Retriever) cleanup() {
	for helper := range retriever.CleanupChannel {
		helper.Manifest.NsqMessage.Touch()
		helper.WorkSummary.Finish()
		if helper.WorkSummary.HasErrors() {
			retriever.FinishWithError(helper)
		} else {
			retriever.FinishWithSuccess(helper)
		}
		// For testing only. The test code creates the PostTestChannel.
		// When running in demo & production, this channel is nil.
		if retriever.PostTestChannel != nil {
			retriever.PostTestChannel <- helper
		}
	}
}

func (fetcher *DPNS3Retriever) DownloadFile(helper *DPNRestoreHelper) {
	downloader := fetcher.getDownloader(helper)
	// Large downloads often fail more than once on transient
	// network errors (e.g. "Connection reset by peer")
	// So we give this several tries.
	for i := 0; i < 10; i++ {
		helper.Manifest.NsqMessage.Touch()
		succeeded := fetcher.tryDownload(helper, downloader, i)
		if succeeded || helper.WorkSummary.ErrorIsFatal {
			break
		}
	}
}

func (fetcher *DPNS3Retriever) getDownloader(helper *DPNRestoreHelper) *apt_network.S3Download {
	tarFileName := fmt.Sprintf("%s.tar", helper.Manifest.DPNBag.UUID)
	helper.Manifest.LocalPath = filepath.Join(
		fetcher.Context.Config.DPN.DPNRestorationDirectory,
		tarFileName)
	return apt_network.NewS3Download(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,                           // region
		fetcher.Context.Config.DPN.DPNRestorationBucket, // bucket
		tarFileName,               // Key/file to download
		helper.Manifest.LocalPath, // where to put the downloaded file
		false, // calculate md5 checksum on the entire tar file
		false, // calculate sha256 checksum on the entire tar file
	)
}

func (fetcher *DPNS3Retriever) tryDownload(helper *DPNRestoreHelper, downloader *apt_network.S3Download, attemptNumber int) bool {
	succeeded := false
	downloader.ErrorMessage = "" // clear before each attempt
	downloader.Fetch()
	if downloader.ErrorMessage == "" {
		fetcher.Context.MessageLog.Info("Fetched %s/%s after %d attempts",
			downloader.BucketName,
			downloader.KeyName,
			attemptNumber+1)
		succeeded = true
	} else {
		retryMessage := "will retry"
		if attemptNumber >= 9 {
			retryMessage = "will not retry - too many failed attempts"
		}
		fullMessage := fmt.Sprintf("Error fetching %s/%s: %s - %s",
			downloader.BucketName,
			downloader.KeyName,
			downloader.ErrorMessage,
			retryMessage)
		fetcher.Context.MessageLog.Warning(fullMessage)
		if strings.Contains(downloader.ErrorMessage, "NoSuch") {
			helper.WorkSummary.AddError(downloader.ErrorMessage)
			helper.WorkSummary.ErrorIsFatal = true
		} else {
			// Note that we tried 10 times.
			helper.WorkSummary.AddError(fullMessage)
		}
	}
	return succeeded
}

func (fetcher *DPNS3Retriever) FinishWithSuccess(helper *DPNRestoreHelper) {
	// Mark DPNWorkItem as succeeded and push to next queue
	helper.Manifest.DPNWorkItem.ClearNodeAndPid()
	note := fmt.Sprintf("Bag has been downloaded to %s", helper.Manifest.LocalPath)
	helper.Manifest.DPNWorkItem.Note = &note
	helper.Manifest.DPNWorkItem.Stage = constants.StageValidate
	helper.Manifest.DPNWorkItem.Status = constants.StatusPending
	helper.SaveDPNWorkItem()
	helper.Manifest.NsqMessage.Finish()
	fetcher.SendToFixityQueue(helper)
}

func (fetcher *DPNS3Retriever) FinishWithError(helper *DPNRestoreHelper) {
	helper.Manifest.DPNWorkItem.ClearNodeAndPid()
	// Copy errors into the DPNWorkItem note, so we can see them in
	// the Pharos UI.
	errors := helper.WorkSummary.AllErrorsAsString()
	helper.Manifest.DPNWorkItem.Note = &errors
	fetcher.Context.MessageLog.Error(errors)
	if helper.WorkSummary.ErrorIsFatal {
		// Mark the DPNWorkItem as failed
		fetcher.Context.MessageLog.Error("Error for %s is fatal. Not requeueing.",
			helper.Manifest.DPNWorkItem.Identifier)
		helper.Manifest.DPNWorkItem.Status = constants.StatusFailed
		helper.Manifest.DPNWorkItem.Retry = false
		helper.SaveDPNWorkItem()
		helper.Manifest.NsqMessage.Finish()
	} else {
		// Transient errors. Retry DPNWorkItem.
		helper.Manifest.DPNWorkItem.Retry = true
		helper.SaveDPNWorkItem()
		helper.Manifest.NsqMessage.Requeue(MINUTES_BETWEEN_RETRIES * time.Minute)
	}
}

func (fetcher *DPNS3Retriever) SendToFixityQueue(helper *DPNRestoreHelper) {
	topic := fetcher.Context.Config.DPN.DPNFixityWorker.NsqTopic
	err := fetcher.Context.NSQClient.Enqueue(topic, helper.Manifest.DPNWorkItem.Id)
	if err != nil {
		helper.WorkSummary.AddError(
			"S3 download succeeded, but error pushing "+
				"DPNWorkItem %d (%s) into NSQ topic %s: %v",
			helper.Manifest.DPNWorkItem.Id, helper.Manifest.DPNWorkItem.Identifier, topic, err)
		fetcher.Context.MessageLog.Error(helper.Manifest.GlacierRestoreSummary.AllErrorsAsString())
		helper.SaveDPNWorkItem()
	}
}
