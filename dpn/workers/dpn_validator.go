package workers

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"os"
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
	validator.loadBagValidationConfig()
	workerBufferSize := _context.Config.DPN.DPNValidationWorker.Workers * 4
	validator.ValidationChannel = make(chan *models.ReplicationManifest, workerBufferSize)
	validator.PostProcessChannel = make(chan *models.ReplicationManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNCopyWorker.Workers; i++ {
		go validator.validate()
		go validator.postProcess()
	}
	return validator, nil
}

// Loads the bag validation config file specified in the general config
// options. This will die if the bag validation config cannot be loaded
// or is invalid.
func (validator *DPNValidator) loadBagValidationConfig() {
	bagValidationConfig, errors := validation.LoadBagValidationConfig(
		validator.Context.Config.DPN.BagValidationConfigFile)
	if errors != nil && len(errors) > 0 {
		msg := fmt.Sprintf("Could not load bag validation config from %s",
			validator.Context.Config.BagValidationConfigFile)
		for _, err := range errors {
			msg += fmt.Sprintf("%s ... ", err.Error())
		}
		fmt.Fprintln(os.Stderr, msg)
		validator.Context.MessageLog.Fatal(msg)
	} else {
		fetcher.Context.MessageLog.Info("Loaded bag validation config file %s",
			fetcher.Context.Config.DPN.BagValidationConfigFile)
	}

	validator.BagValidationConfig = bagValidationConfig
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
		manifest.DPNWorkItem.Node, _ = os.Hostname()
		note := "Validating bag"
		manifest.DPNWorkItem.Note = &note
		SaveDPNWorkItemState(validator.Context, manifest, manifest.ValidateSummary)

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

	// Get the remote client that talks to this transfer's FromNode
	remoteClient := validator.RemoteClients[manifest.ReplicationTransfer.FromNode]

	// Tell the FromNode that we're cancelling replication of an invalid bag.
	manifest.ReplicationTransfer.Cancelled = true
	reason := fmt.Sprintf("Bag failed validation. %s", manifest.ValidateSummary.Errors[0])
	manifest.ReplicationTransfer.CancelReason = &reason
	UpdateReplicationTransfer(validator.Context, remoteClient, manifest)

	// Tell Pharos that this DPNWorkItem failed.
	note := "Bag failed validation"
	manifest.DPNWorkItem.Node = ""
	manifest.DPNWorkItem.Note = &note
	SaveDPNWorkItemState(validator.Context, manifest, manifest.ValidateSummary)
	validator.Context.MessageLog.Error(manifest.ValidateSummary.AllErrorsAsString())

	// Delete the tar file from our staging area.
	validator.Context.MessageLog.Info("Deleting %s", manifest.LocalPath)
	os.Remove(manifest.LocalPath)

	// Dump the JSON info about this validation attempt,
	// and tell NSQ we're done.
	LogReplicationJson(manifest, validator.Context.JsonLog)
	manifest.NsqMessage.Finish()
}

func (validator *DPNValidator) finishWithSuccess(manifest *models.ReplicationManifest) {
	// Tell Pharos we're done working on this.
	note := "Bag passed validation"
	manifest.DPNWorkItem.Node = ""
	manifest.DPNWorkItem.Note = &note
	SaveDPNWorkItemState(validator.Context, manifest, manifest.ValidateSummary)

	// Push this DPNWorkItem Id into the next queue, so it can be stored.
	topic := validator.Context.Config.DPN.DPNStoreWorker.NsqTopic
	err := validator.Context.NSQClient.Enqueue(topic, manifest.DPNWorkItem.Id)
	if err != nil {
		msg := fmt.Sprintf("Error pushing into NSQ %s: %v", err)
		manifest.ValidateSummary.AddError(msg)
		validator.Context.MessageLog.Error(msg)
	}

	// Dump the JSON info about this validation attempt,
	// and tell NSQ we're done.
	LogReplicationJson(manifest, validator.Context.JsonLog)
	manifest.NsqMessage.Finish()
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
