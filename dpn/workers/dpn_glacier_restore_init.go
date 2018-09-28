package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	dpn_network "github.com/APTrust/exchange/dpn/network"
	apt_network "github.com/APTrust/exchange/network"
	"github.com/nsqio/go-nsq"
	"strings"
	"time"
)

// Standard retrieval is 3-5 hours.
// Bulk is 5-12 hours, and is cheaper.
// There's no rush on DPN fixity checking, so use the cheaper option.
// https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-two-steps.html#api-downloading-an-archive-two-steps-retrieval-options
// For retrieval pricing, see https://aws.amazon.com/glacier/pricing/
const RETRIEVAL_OPTION = "Bulk"

// After a Glacier restore request has been accepted, we will check
// S3 periodically to see if the item has been restored. This is the
// interval between checks.
const HOURS_BETWEEN_CHECKS = 3

// Keep the files in S3 up to 60 days, in case we're
// having system problems and we need to attempt the
// restore multiple times. We'll have other processes
// clean out the S3 bucket when necessary.
const DAYS_TO_KEEP_IN_S3 = 60

// Requests that an object be restored from Glacier to S3. This is
// the first step toward performing fixity checks on DPN bags, and
// restoring DPN bags, all of which are stored in Glacier.
type DPNGlacierRestoreInit struct {
	// Context includes logging, config, network connections, and
	// other general resources for the worker.
	Context *context.Context
	// LocalDPNRestClient lets us talk to our local DPN server.
	LocalDPNRestClient *dpn_network.DPNRestClient
	// RequestChannel is for requesting an item be moved from Glacier
	// into S3.
	RequestChannel chan *DPNRestoreHelper
	// CleanupChannel is for housekeeping, like updating NSQ.
	CleanupChannel chan *DPNRestoreHelper
	// PostTestChannel is for testing only. In production, nothing listens
	// on this channel.
	PostTestChannel chan *DPNRestoreHelper
	// S3Url is a custom URL that the S3 client should connect to.
	// We use this only in testing, when we want the client to talk
	// to a local test server. This should not be set in demo or
	// production.
	S3Url string
}

func DPNNewGlacierRestoreInit(_context *context.Context) (*DPNGlacierRestoreInit, error) {
	restorer := &DPNGlacierRestoreInit{
		Context: _context,
	}
	// Set up buffered channels
	restorerBufferSize := _context.Config.DPN.DPNGlacierRestoreWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.DPN.DPNGlacierRestoreWorker.Workers * 10
	restorer.RequestChannel = make(chan *DPNRestoreHelper, restorerBufferSize)
	restorer.CleanupChannel = make(chan *DPNRestoreHelper, workerBufferSize)
	// Set up a limited number of go routines to handle the work.
	for i := 0; i < _context.Config.DPN.DPNGlacierRestoreWorker.NetworkConnections; i++ {
		go restorer.RequestRestore()
	}
	for i := 0; i < _context.Config.DPN.DPNGlacierRestoreWorker.Workers; i++ {
		go restorer.Cleanup()
	}
	// Set up a client to talk to our local DPN server.
	var err error
	restorer.LocalDPNRestClient, err = dpn_network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	return restorer, err
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (restorer *DPNGlacierRestoreInit) HandleMessage(message *nsq.Message) error {
	helper, err := NewDPNRestoreHelper(message, restorer.Context,
		restorer.LocalDPNRestClient, constants.ActionFixityCheck,
		"GlacierRestoreSummary")
	if err != nil {
		restorer.Context.MessageLog.Error(err.Error())
		return err
	}
	helper.WorkSummary.ClearErrors()
	helper.WorkSummary.Start()
	helper.Manifest.DPNWorkItem.Status = constants.StatusStarted
	helper.SaveDPNWorkItem()
	if helper.WorkSummary.HasErrors() {
		restorer.Context.MessageLog.Error("Error setting up manifest for WorkItem %s: %s",
			string(message.Body), helper.WorkSummary.AllErrorsAsString())
		// No use proceeding...
		restorer.CleanupChannel <- helper
		return fmt.Errorf(helper.WorkSummary.AllErrorsAsString())
	}
	if helper.Manifest.DPNWorkItem.IsCompletedOrCancelled() {
		restorer.Context.MessageLog.Info("Skipping WorkItem %d because status is %s",
			helper.Manifest.DPNWorkItem.Id, helper.Manifest.DPNWorkItem.Status)
		restorer.CleanupChannel <- helper
		return nil
	}

	// OK, we're good. Ask Glacier to move the file into S3.
	restorer.RequestChannel <- helper
	return nil
}

func (restorer *DPNGlacierRestoreInit) RequestRestore() {
	for helper := range restorer.RequestChannel {
		requestNeeded, err := restorer.RestoreRequestNeeded(helper)
		if err != nil {
			helper.WorkSummary.AddError(
				"Error processing S3 HEAD request for %s: %v",
				helper.Manifest.GlacierKey, err)
		} else if requestNeeded {
			restorer.InitializeRetrieval(helper)
		}
		restorer.CleanupChannel <- helper
	}
}

func (restorer *DPNGlacierRestoreInit) Cleanup() {
	for helper := range restorer.CleanupChannel {
		helper.WorkSummary.Finish()
		if helper.WorkSummary.HasErrors() {
			restorer.FinishWithError(helper)
		} else {
			restorer.FinishWithSuccess(helper)
		}
		// For testing only. The test code creates the PostTestChannel.
		// When running in demo & production, this channel is nil.
		if restorer.PostTestChannel != nil {
			restorer.PostTestChannel <- helper
		}
	}
}

func (restorer *DPNGlacierRestoreInit) FinishWithSuccess(helper *DPNRestoreHelper) {
	helper.Manifest.DPNWorkItem.ClearNodeAndPid()
	note := fmt.Sprintf("Glacier restore initiated. Will check availability "+
		"in S3 every %d hours.", HOURS_BETWEEN_CHECKS)
	if helper.Manifest.IsAvailableInS3 {
		note = "Item is available in S3 for download."
		helper.Manifest.DPNWorkItem.Note = &note
		helper.Manifest.DPNWorkItem.Stage = constants.StageAvailableInS3
		helper.SaveDPNWorkItem()
		restorer.SendToDownloadQueue(helper)
	} else {
		helper.Manifest.DPNWorkItem.Note = &note
		restorer.Context.MessageLog.Info("Requested %s from Glacier. %s", helper.Manifest.GlacierKey, note)
		helper.Manifest.DPNWorkItem.Retry = true
		helper.SaveDPNWorkItem()
		helper.Manifest.NsqMessage.Requeue(HOURS_BETWEEN_CHECKS * time.Hour)
	}
	helper.Manifest.NsqMessage.Finish()
}

func (restorer *DPNGlacierRestoreInit) SendToDownloadQueue(helper *DPNRestoreHelper) {
	topic := restorer.Context.Config.DPN.DPNS3DownloadWorker.NsqTopic
	err := restorer.Context.NSQClient.Enqueue(topic, helper.Manifest.DPNWorkItem.Id)
	if err != nil {
		helper.WorkSummary.AddError(
			"Glacier request succeeded, but error pushing "+
				"DPNWorkItem %d (%s) into NSQ topic %s: %v",
			helper.Manifest.DPNWorkItem.Id, helper.Manifest.DPNWorkItem.Identifier, topic, err)
		restorer.Context.MessageLog.Error(helper.WorkSummary.AllErrorsAsString())
		helper.SaveDPNWorkItem()
	}
}

func (restorer *DPNGlacierRestoreInit) FinishWithError(helper *DPNRestoreHelper) {
	helper.Manifest.DPNWorkItem.ClearNodeAndPid()
	errors := helper.WorkSummary.AllErrorsAsString()
	helper.Manifest.DPNWorkItem.Note = &errors
	restorer.Context.MessageLog.Error(errors)

	attempts := int(helper.Manifest.NsqMessage.Attempts)
	maxAttempts := int(restorer.Context.Config.DPN.DPNGlacierRestoreWorker.MaxAttempts)

	if helper.WorkSummary.ErrorIsFatal {
		restorer.Context.MessageLog.Error("Error for %s is fatal. Not requeueing.",
			helper.Manifest.DPNWorkItem.Identifier)
		helper.Manifest.DPNWorkItem.Status = constants.StatusFailed
		helper.Manifest.DPNWorkItem.Retry = false
		helper.Manifest.NsqMessage.Finish()
	} else if attempts > maxAttempts {
		restorer.Context.MessageLog.Error("Attempt to restore %s failed %d times. Not requeuing.",
			attempts, helper.Manifest.DPNWorkItem.Identifier)
		helper.Manifest.DPNWorkItem.Status = constants.StatusFailed
		helper.Manifest.DPNWorkItem.Retry = false
		helper.Manifest.NsqMessage.Finish()
	} else {
		restorer.Context.MessageLog.Info("Error for %s is transient. Requeueing.",
			helper.Manifest.DPNWorkItem.Identifier)
		helper.Manifest.DPNWorkItem.Retry = true
		helper.Manifest.NsqMessage.Requeue(1 * time.Minute)
	}

	helper.SaveDPNWorkItem()
}

func (restorer *DPNGlacierRestoreInit) RestoreRequestNeeded(helper *DPNRestoreHelper) (bool, error) {
	needsRestoreRequest := false
	s3Client := apt_network.NewS3Head(
		restorer.Context.Config.GetAWSAccessKeyId(),
		restorer.Context.Config.GetAWSSecretAccessKey(),
		restorer.Context.Config.DPN.DPNGlacierRegion,
		helper.Manifest.GlacierBucket)
	// Hack for testing: Tell the client to talk to our own
	// local S3 test server, and clear the bucket name,
	// because that gets prepended to the URL.
	if restorer.S3Url != "" {
		restorer.Context.MessageLog.Warning("Setting S3 URL to %s. This should happen only in testing!",
			restorer.S3Url)
		s3Client.SetSessionEndpoint(restorer.S3Url)
		s3Client.BucketName = ""
	}

	// Ask S3 about the status of this object
	s3Client.Head(helper.Manifest.GlacierKey)

	// Status 409: Conflict is an expected response.
	// It means a restore request has already been initiated.
	if strings.Contains(s3Client.ErrorMessage, "Conflict") {
		restorer.Context.MessageLog.Info("Already in progress: %s ", helper.Manifest.DPNWorkItem.Identifier)
		helper.Manifest.GlacierRequestAccepted = true
		helper.Manifest.RequestedFromGlacierAt = time.Now().UTC()
		return false, nil
	}

	restoreRequestInfo, err := s3Client.GetRestoreRequestInfo()
	if restoreRequestInfo.RequestInProgress {
		// Log and go on
		restorer.Context.MessageLog.Info("Already in progress: %s ", helper.Manifest.DPNWorkItem.Identifier)
		helper.Manifest.GlacierRequestAccepted = true
		helper.Manifest.RequestedFromGlacierAt = time.Now().UTC()
	} else if restoreRequestInfo.RequestIsComplete {
		// Log and update expiry date
		helper.Manifest.GlacierRequestAccepted = true
		helper.Manifest.RequestedFromGlacierAt = time.Now().UTC()
		helper.Manifest.IsAvailableInS3 = true
		helper.Manifest.EstimatedDeletionFromS3 = restoreRequestInfo.S3ExpiryDate
		restorer.Context.MessageLog.Info("Already restored to S3: %s", helper.Manifest.DPNWorkItem.Identifier)
	} else {
		// Not restored yet and not even requested.
		// We need to make a request for this now.
		restorer.Context.MessageLog.Info("Needs Glacier retrieval request: %s", helper.Manifest.DPNWorkItem.Identifier)
		needsRestoreRequest = true
	}
	return needsRestoreRequest, err
}

func (restorer *DPNGlacierRestoreInit) InitializeRetrieval(helper *DPNRestoreHelper) {
	// Request restore from Glacier
	restorer.Context.MessageLog.Info("Requesting Glacier retrieval of %s from %s",
		helper.Manifest.GlacierKey, helper.Manifest.GlacierBucket)

	restoreClient := apt_network.NewS3Restore(
		restorer.Context.Config.GetAWSAccessKeyId(),
		restorer.Context.Config.GetAWSSecretAccessKey(),
		restorer.Context.Config.DPN.DPNGlacierRegion,
		helper.Manifest.GlacierBucket,
		helper.Manifest.GlacierKey,
		RETRIEVAL_OPTION,
		DAYS_TO_KEEP_IN_S3)

	// Custom S3Url is for testing only.
	if restorer.S3Url != "" {
		restorer.Context.MessageLog.Warning("Setting S3 URL to %s. This should happen only in testing!",
			restorer.S3Url)
		restoreClient.TestURL = restorer.S3Url
		restoreClient.BucketName = ""
	}

	// Figure out approximately how long this item will
	// be available in S3, once we restore it.
	now := time.Now().UTC()
	estimatedDeletionFromS3 := now.AddDate(0, 0, DAYS_TO_KEEP_IN_S3)

	// This is where me make the actual request to Glacier.
	restoreClient.Restore()
	if restoreClient.ErrorMessage != "" {
		helper.WorkSummary.AddError(
			"Glacier retrieval request returned an error for %s at %s: %v",
			helper.Manifest.GlacierBucket, helper.Manifest.GlacierKey,
			restoreClient.ErrorMessage)
		restorer.Context.MessageLog.Error("Bad response from Glacier. Requested %s/%s. Got:\n %v",
			helper.Manifest.GlacierBucket, helper.Manifest.GlacierKey, restoreClient.Response)
	}

	// Update this info.
	helper.Manifest.GlacierRequestAccepted = restoreClient.RequestAccepted()
	helper.Manifest.RequestedFromGlacierAt = now
	helper.Manifest.EstimatedDeletionFromS3 = estimatedDeletionFromS3
	helper.Manifest.IsAvailableInS3 = restoreClient.AlreadyInActiveTier

	if restoreClient.RequestRejectedServiceUnavailable {
		helper.WorkSummary.AddError(
			"Request to restore %s/%s: "+
				"Glacier restore service is temporarily unavailable. Try again later.",
			helper.Manifest.GlacierBucket, helper.Manifest.GlacierKey)
		helper.WorkSummary.ErrorIsFatal = false
	}
}
