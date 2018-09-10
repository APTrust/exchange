package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	dpn_network "github.com/APTrust/exchange/dpn/network"
	apt_network "github.com/APTrust/exchange/network"
	"github.com/nsqio/go-nsq"
	"strconv"
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
	RequestChannel chan *models.DPNRetrievalManifest
	// CleanupChannel is for housekeeping, like updating NSQ.
	CleanupChannel chan *models.DPNRetrievalManifest
	// PostTestChannel is for testing only. In production, nothing listens
	// on this channel.
	PostTestChannel chan *models.DPNRetrievalManifest
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
	restorer.RequestChannel = make(chan *models.DPNRetrievalManifest, restorerBufferSize)
	restorer.CleanupChannel = make(chan *models.DPNRetrievalManifest, workerBufferSize)
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
	message.DisableAutoResponse()

	manifest := restorer.GetRetrievalManifest(message)
	manifest.GlacierRestoreSummary.ClearErrors()
	manifest.GlacierRestoreSummary.Start()
	manifest.DPNWorkItem.Status = constants.StatusStarted
	restorer.SaveDPNWorkItem(manifest)
	if manifest.GlacierRestoreSummary.HasErrors() {
		restorer.Context.MessageLog.Error("Error setting up manifest for WorkItem %s: %s",
			string(message.Body), manifest.GlacierRestoreSummary.AllErrorsAsString())
		// No use proceeding...
		restorer.CleanupChannel <- manifest
		return fmt.Errorf(manifest.GlacierRestoreSummary.AllErrorsAsString())
	}
	if manifest.DPNWorkItem.IsCompletedOrCancelled() {
		restorer.Context.MessageLog.Info("Skipping WorkItem %d because status is %s",
			manifest.DPNWorkItem.Id, manifest.DPNWorkItem.Status)
		restorer.CleanupChannel <- manifest
		return nil
	}

	// OK, we're good. Ask Glacier to move the file into S3.
	restorer.RequestChannel <- manifest
	return nil
}

func (restorer *DPNGlacierRestoreInit) RequestRestore() {
	for manifest := range restorer.RequestChannel {
		requestNeeded, err := restorer.RestoreRequestNeeded(manifest)
		if err != nil {
			manifest.GlacierRestoreSummary.AddError(
				"Error processing S3 HEAD request for %s: %v",
				manifest.DPNWorkItem.Identifier, err)
		} else if requestNeeded {
			restorer.InitializeRetrieval(manifest)
		}
		restorer.CleanupChannel <- manifest
	}
}

func (restorer *DPNGlacierRestoreInit) Cleanup() {
	for manifest := range restorer.CleanupChannel {
		manifest.GlacierRestoreSummary.Finish()
		if manifest.GlacierRestoreSummary.HasErrors() {
			restorer.FinishWithError(manifest)
		} else {
			restorer.FinishWithSuccess(manifest)
		}
		// For testing only. The test code creates the PostTestChannel.
		// When running in demo & production, this channel is nil.
		if restorer.PostTestChannel != nil {
			restorer.PostTestChannel <- manifest
		}
	}
}

func (restorer *DPNGlacierRestoreInit) FinishWithSuccess(manifest *models.DPNRetrievalManifest) {
	manifest.DPNWorkItem.ClearNodeAndPid()
	note := fmt.Sprintf("Glacier restore initiated. Will check availability "+
		"in S3 every %d hours.", HOURS_BETWEEN_CHECKS)
	if manifest.IsAvailableInS3 {
		note = "Item is available in S3 for download."
		manifest.DPNWorkItem.Note = &note
		manifest.DPNWorkItem.Stage = constants.StageAvailableInS3
		restorer.SaveDPNWorkItem(manifest)
		restorer.SendToDownloadQueue(manifest)
	} else {
		manifest.DPNWorkItem.Note = &note
		restorer.Context.MessageLog.Info("Requested %s from Glacier. %s", manifest.DPNWorkItem.Identifier, note)
		manifest.DPNWorkItem.Retry = true
		restorer.SaveDPNWorkItem(manifest)
		manifest.NsqMessage.Requeue(HOURS_BETWEEN_CHECKS * time.Hour)
	}
}

func (restorer *DPNGlacierRestoreInit) SendToDownloadQueue(manifest *models.DPNRetrievalManifest) {
	manifest.NsqMessage.Finish()
	topic := restorer.Context.Config.DPN.DPNS3DownloadWorker.NsqTopic
	err := restorer.Context.NSQClient.Enqueue(topic, manifest.DPNWorkItem.Id)
	if err != nil {
		manifest.GlacierRestoreSummary.AddError(
			"Glacier requested succeeded, but error pushing "+
				"DPNWorkItem %d (%s) into NSQ topic %s: %v",
			manifest.DPNWorkItem.Id, manifest.DPNWorkItem.Identifier, topic, err)
		restorer.Context.MessageLog.Error(manifest.GlacierRestoreSummary.AllErrorsAsString())
		restorer.SaveDPNWorkItem(manifest)
	}
}

func (restorer *DPNGlacierRestoreInit) FinishWithError(manifest *models.DPNRetrievalManifest) {
	manifest.DPNWorkItem.ClearNodeAndPid()
	errors := manifest.GlacierRestoreSummary.AllErrorsAsString()
	manifest.DPNWorkItem.Note = &errors
	restorer.Context.MessageLog.Error(errors)

	attempts := int(manifest.NsqMessage.Attempts)
	maxAttempts := int(restorer.Context.Config.DPN.DPNGlacierRestoreWorker.MaxAttempts)

	if manifest.GlacierRestoreSummary.ErrorIsFatal {
		restorer.Context.MessageLog.Error("Error for %s is fatal. Not requeueing.",
			manifest.DPNWorkItem.Identifier)
		manifest.DPNWorkItem.Status = constants.StatusFailed
		manifest.DPNWorkItem.Retry = false
		manifest.NsqMessage.Finish()
	} else if attempts > maxAttempts {
		restorer.Context.MessageLog.Error("Attempt to restore %s failed %d times. Not requeuing.",
			attempts, manifest.DPNWorkItem.Identifier)
		manifest.DPNWorkItem.Status = constants.StatusFailed
		manifest.DPNWorkItem.Retry = false
		manifest.NsqMessage.Finish()
	} else {
		restorer.Context.MessageLog.Info("Error for %s is transient. Requeueing.",
			manifest.DPNWorkItem.Identifier)
		manifest.DPNWorkItem.Retry = true
		manifest.NsqMessage.Requeue(1 * time.Minute)
	}

	restorer.SaveDPNWorkItem(manifest)
}

func (restorer *DPNGlacierRestoreInit) RestoreRequestNeeded(manifest *models.DPNRetrievalManifest) (bool, error) {
	needsRestoreRequest := false
	s3Client := apt_network.NewS3Head(
		restorer.Context.Config.GetAWSAccessKeyId(),
		restorer.Context.Config.GetAWSSecretAccessKey(),
		restorer.Context.Config.DPN.DPNGlacierRegion,
		manifest.GlacierBucket)
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
	s3Client.Head(manifest.DPNWorkItem.Identifier)

	// Status 409: Conflict is an expected response.
	// It means a restore request has already been initiated.
	if strings.Contains(s3Client.ErrorMessage, "Conflict") {
		restorer.Context.MessageLog.Info("Already in progress: %s ", manifest.DPNWorkItem.Identifier)
		manifest.GlacierRequestAccepted = true
		manifest.RequestedFromGlacierAt = time.Now().UTC()
		return false, nil
	}

	restoreRequestInfo, err := s3Client.GetRestoreRequestInfo()
	if restoreRequestInfo.RequestInProgress {
		// Log and go on
		restorer.Context.MessageLog.Info("Already in progress: %s ", manifest.DPNWorkItem.Identifier)
		manifest.GlacierRequestAccepted = true
		manifest.RequestedFromGlacierAt = time.Now().UTC()
	} else if restoreRequestInfo.RequestIsComplete {
		// Log and update expiry date
		manifest.GlacierRequestAccepted = true
		manifest.RequestedFromGlacierAt = time.Now().UTC()
		manifest.IsAvailableInS3 = true
		manifest.EstimatedDeletionFromS3 = restoreRequestInfo.S3ExpiryDate
		restorer.Context.MessageLog.Info("Already restored to S3: %s", manifest.DPNWorkItem.Identifier)
	} else {
		// Not restored yet and not even requested.
		// We need to make a request for this now.
		restorer.Context.MessageLog.Info("Needs Glacier retrieval request: %s", manifest.DPNWorkItem.Identifier)
		needsRestoreRequest = true
	}
	return needsRestoreRequest, err
}

func (restorer *DPNGlacierRestoreInit) InitializeRetrieval(manifest *models.DPNRetrievalManifest) {
	// Request restore from Glacier
	restorer.Context.MessageLog.Info("Requesting Glacier retrieval of %s from %s",
		manifest.DPNWorkItem.Identifier, manifest.GlacierBucket)

	restoreClient := apt_network.NewS3Restore(
		restorer.Context.Config.GetAWSAccessKeyId(),
		restorer.Context.Config.GetAWSSecretAccessKey(),
		restorer.Context.Config.DPN.DPNGlacierRegion,
		manifest.GlacierBucket,
		manifest.DPNWorkItem.Identifier,
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
		manifest.GlacierRestoreSummary.AddError(
			"Glacier retrieval request returned an error for %s at %s: %v",
			manifest.GlacierBucket, manifest.DPNWorkItem.Identifier,
			restoreClient.ErrorMessage)
		restorer.Context.MessageLog.Error("Bad response from Glacier. Requested %s/%s. Got:\n %v",
			manifest.GlacierBucket, manifest.DPNWorkItem.Identifier, restoreClient.Response)
	}

	// Update this info.
	manifest.GlacierRequestAccepted = restoreClient.RequestAccepted()
	manifest.RequestedFromGlacierAt = now
	manifest.EstimatedDeletionFromS3 = estimatedDeletionFromS3
	manifest.IsAvailableInS3 = restoreClient.AlreadyInActiveTier

	if restoreClient.RequestRejectedServiceUnavailable {
		manifest.GlacierRestoreSummary.AddError(
			"Request to restore %s/%s: "+
				"Glacier restore service is temporarily unavailable. Try again later.",
			manifest.GlacierBucket, manifest.DPNWorkItem.Identifier)
		manifest.GlacierRestoreSummary.ErrorIsFatal = false
	}
}

// GetWorkItem returns the WorkItem with the specified Id from Pharos,
// or nil.
func (restorer *DPNGlacierRestoreInit) GetRetrievalManifest(message *nsq.Message) *models.DPNRetrievalManifest {
	msgBody := strings.TrimSpace(string(message.Body))
	restorer.Context.MessageLog.Info("NSQ Message body: '%s'", msgBody)
	manifest := models.NewDPNRetrievalManifest(message)
	manifest.TaskType = constants.ActionFixityCheck
	manifest.GlacierBucket = restorer.Context.Config.DPN.DPNPreservationBucket

	dpnWorkItemId, err := strconv.Atoi(string(msgBody))
	if err != nil || dpnWorkItemId == 0 {
		manifest.GlacierRestoreSummary.AddError(
			"Could not get DPNWorkItem Id from NSQ message body: %v", err)
		return manifest
	}

	restorer.GetDPNWorkItem(manifest, dpnWorkItemId)

	if manifest.DPNWorkItem.State != nil && *manifest.DPNWorkItem.State != "" {
		restoredManifest, err := models.DPNRetrievalManifestFromJson(*manifest.DPNWorkItem.State)
		if err != nil {
			restorer.Context.MessageLog.Warning("Error restoring manifest state "+
				"for DPNWorkItem %d: %v JSON:\n%s", manifest.DPNWorkItem.Id, err,
				manifest.DPNWorkItem.Identifier, *manifest.DPNWorkItem.State)
			restorer.Context.MessageLog.Warning("Starting with new manifest for "+
				"for DPNWorkItem %d (%s)", manifest.DPNWorkItem.Id,
				manifest.DPNWorkItem.Identifier)
		} else {
			restoredManifest.NsqMessage = manifest.NsqMessage
			restoredManifest.DPNWorkItem = manifest.DPNWorkItem
			manifest = restoredManifest
		}
	}

	// This will be nil for new jobs, and should be non-nil if we're reattempting
	// an existing job. DPN bag records are immutable, so we don't need to reload
	// them each time we reattempt a task. (Unlike DPNWorkItem, which can be
	// cancelled between attempts.)
	if manifest.DPNBag == nil {
		restorer.GetDPNBag(manifest)
	}
	if manifest.ExpectedFixityValue == "" {
		restorer.GetBagDigest(manifest)
	}

	return manifest
}

func (restorer *DPNGlacierRestoreInit) GetDPNWorkItem(manifest *models.DPNRetrievalManifest, dpnWorkItemId int) {
	// Get the DPN work item
	resp := restorer.Context.PharosClient.DPNWorkItemGet(dpnWorkItemId)
	if resp.Error != nil {
		manifest.GlacierRestoreSummary.AddError(
			"Error getting DPNWorkItem %d from Pharos: %v",
			dpnWorkItemId, resp.Error)
		return
	}
	dpnWorkItem := resp.DPNWorkItem()
	if dpnWorkItem == nil {
		manifest.GlacierRestoreSummary.AddError(
			"Pharos returned nil for WorkItem %d", dpnWorkItemId)
		return
	}

	manifest.DPNWorkItem = dpnWorkItem
	manifest.DPNWorkItem.SetNodeAndPid()
	note := "Requesting Glacier restoration for fixity"
	manifest.DPNWorkItem.Note = &note
}

func (restorer *DPNGlacierRestoreInit) GetDPNBag(manifest *models.DPNRetrievalManifest) {
	// Get the DPN Bag from the DPN REST server.
	resp := restorer.LocalDPNRestClient.DPNBagGet(manifest.DPNWorkItem.Identifier)
	if resp.Error != nil {
		manifest.GlacierRestoreSummary.AddError(
			"Error getting DPN bag %s from %s: %v",
			manifest.DPNWorkItem.Identifier,
			restorer.Context.Config.DPN.RestClient.LocalServiceURL, resp.Error)
		return
	}
	dpnBag := resp.Bag()
	if dpnBag == nil {
		manifest.GlacierRestoreSummary.AddError(
			"DPN REST server returned nil for bag %s",
			manifest.DPNWorkItem.Identifier)
		return
	}
	manifest.DPNBag = dpnBag
}

func (restorer *DPNGlacierRestoreInit) GetBagDigest(manifest *models.DPNRetrievalManifest) {
	resp := restorer.LocalDPNRestClient.DigestGet(manifest.DPNWorkItem.Identifier, constants.AlgSha256)
	if resp.Error != nil {
		manifest.GlacierRestoreSummary.AddError(
			"Error getting sha256 digest for DPN bag %s from %s: %v",
			manifest.DPNWorkItem.Identifier,
			restorer.Context.Config.DPN.RestClient.LocalServiceURL, resp.Error)
		return
	}
	digest := resp.Digest()
	if digest == nil {
		manifest.GlacierRestoreSummary.AddError(
			"DPN REST server returned nil digest for bag %s",
			manifest.DPNWorkItem.Identifier)
		return
	}
	manifest.ExpectedFixityValue = digest.Value
}

func (restorer *DPNGlacierRestoreInit) SaveDPNWorkItem(manifest *models.DPNRetrievalManifest) {
	jsonData, err := manifest.ToJson()
	if err != nil {
		msg := fmt.Sprintf("Could not marshal DPNRetrievalManifest "+
			"for DPNWorkItem %d: %v", manifest.DPNWorkItem.Id, err)
		restorer.Context.MessageLog.Error(msg)
		note := "JSON serialization error while trying to save DPNWorkItemState."
		manifest.DPNWorkItem.Note = &note
	}

	// Update the DPNWorkItem
	manifest.DPNWorkItem.State = &jsonData
	manifest.DPNWorkItem.Retry = !manifest.GlacierRestoreSummary.ErrorIsFatal

	resp := restorer.Context.PharosClient.DPNWorkItemSave(manifest.DPNWorkItem)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not save DPNWorkItem %d "+
			"for fixity on bag %s to Pharos: %v",
			manifest.DPNWorkItem.Id, manifest.DPNWorkItem.Identifier, err)
		restorer.Context.MessageLog.Error(msg)
		manifest.GlacierRestoreSummary.AddError(msg)
	}
}
