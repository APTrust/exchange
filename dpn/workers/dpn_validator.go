package workers

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"path/filepath"
	"strconv"
)

// dpn_validator validates DPN bags (tar files).

type DPNValidator struct {
	ValidationChannel   chan *models.ReplicationManifest
	PostProcessChannel  chan *models.ReplicationManifest
	BagValidationConfig *validation.BagValidationConfig
	Context             *context.Context
	LocalClient         *network.DPNRestClient
	RemoteClients       map[string]*network.DPNRestClient
}

func NewDPNValidator(_context *context.Context) (*DPNValidator, error) {
	localClient, err := network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	if err != nil {
		return nil, fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	remoteClients, err := localClient.GetRemoteClients()
	if err != nil {
		return nil, err
	}
	validator := &DPNValidator {
		Context: _context,
		LocalClient: localClient,
		RemoteClients: remoteClients,
	}
	workerBufferSize := _context.Config.DPN.DPNValidationWorker.Workers * 4
	validator.ValidationChannel = make(chan *models.ReplicationManifest, workerBufferSize)
	validator.PostProcessChannel = make(chan *models.ReplicationManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNCopyWorker.Workers; i++ {
		go validator.validate()
		go validator.postProcess()
	}
	return validator, nil
}

func (validator *DPNValidator) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	validator.Context.MessageLog.Info("Checking NSQ message %s", string(message.Body))

	// Get the DPNWorkItem, the ReplicationTransfer, and the DPNBag
	manifest := validator.setupReplicationManifest(message)
	if manifest.ValidateSummary.HasErrors() {
		validator.PostProcessChannel <- manifest
		return nil
	}

	// Start processing.
	validator.ValidationChannel <- manifest
	validator.Context.MessageLog.Info("Put xfer request %s (bag %s) from %s " +
		" into the validation channel", manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag, manifest.ReplicationTransfer.FromNode)
	return nil
}

func (validator *DPNValidator) validate() {
	for manifest := range validator.ValidationChannel {
		// Don't time us out, NSQ!
		manifest.NsqMessage.Touch()

		// Tell Pharos that we've started to validate item.
		// Set DPNWorkItem to whatever
		// UpdateDPNWorkItem

		// Set up a new validator to check this bag.
		bagValidator, err := validation.NewBagValidator(manifest.LocalPath,
			validator.BagValidationConfig)
		if err != nil {
			// Could not create a BagValidator. Should this be fatal?
			manifest.ValidateSummary.AddError(err.Error())
		} else {
			// Validation can take hours for very large bags.
			validationResult := bagValidator.Validate()

			// The validator creates its own WorkSummary, complete with
			// Start/Finish timestamps, error messages and everything.
			// Just copy that into our IngestManifest.
			manifest.ValidateSummary = validationResult.ValidationSummary
		}
		manifest.NsqMessage.Touch()
		validator.PostProcessChannel <- manifest
	}
}

func (validator *DPNValidator) postProcess() {
	for manifest := range validator.ValidationChannel {
		// If the bag is invalid, that's a fatal error. We should not do
		// any further processing on it.
		if manifest.ValidateSummary.HasErrors() {
			validator.finishWithError(manifest)
		} else {
			validator.finishWithSuccess(manifest)
		}
	}
}

func (validator *DPNValidator) finishWithError(manifest *models.ReplicationManifest) {
	// Validate errors are fatal. We won't store an invalid bag.
	manifest.ValidateSummary.ErrorIsFatal = true
	manifest.ValidateSummary.Retry = false
	// 1. Cancel transfer on remote node.
	// 2. Delete bag (tar file).
	// 3. Update DPNWorkItem.
	// 4. Finish NSQ message.
	// 5. Dump JSON
}

func (validator *DPNValidator) finishWithSuccess(manifest *models.ReplicationManifest) {
	// 1. Update DPNWorkItem
	// 2. Push item into dpn_record_queue.
	// 3. Dump JSON
}

// setupReplicationManifest creates a ReplicationManifest for this job.
func (validator *DPNValidator) setupReplicationManifest(message *nsq.Message) (*models.ReplicationManifest) {
	manifest := models.NewReplicationManifest(message)
	manifest.ValidateSummary.Start()

	// This is where we have stored our local copy of this bag.
	manifest.LocalPath = filepath.Join(
		validator.Context.Config.DPN.StagingDirectory,
		manifest.ReplicationTransfer.Bag + ".tar")

	// Get the DPNWorkItem that describes this replication.
	workItemId, err := strconv.Atoi(string(manifest.NsqMessage.Body))
	if err != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItemId from" +
			"NSQ message body '%s': %v", manifest.NsqMessage.Body, err)
		manifest.ValidateSummary.AddError(msg)
		manifest.ValidateSummary.ErrorIsFatal = true
		manifest.ValidateSummary.Finish()
		return manifest
	}
	resp := validator.Context.PharosClient.DPNWorkItemGet(workItemId)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItem (id %d) " +
			"from Pharos: %v", workItemId, resp.Error)
		manifest.ValidateSummary.AddError(msg)
		manifest.ValidateSummary.ErrorIsFatal = true
		manifest.ValidateSummary.Finish()
		return manifest
	}
	manifest.DPNWorkItem = resp.DPNWorkItem()

	// Get the latest copy of the ReplicationTransfer from
	// the remote node. There's a chance this replication may
	// have been cancelled since we copied the bag from the
	// remote node.
	GetXferRequest(validator.LocalClient, manifest, manifest.ValidateSummary)
	if manifest.CopySummary.HasErrors() {
		return manifest
	}

	// Get the DPN bag from the remote node. It should not have
	// changed, but at least we know we have the current version
	// of it when we fetch it.
	GetDPNBag(validator.LocalClient, manifest, manifest.CopySummary)

	return manifest
}
