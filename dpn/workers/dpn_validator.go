package workers

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"os"
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
	for i := 0; i < _context.Config.DPN.DPNValidationWorker.Workers; i++ {
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
		validator.Context.MessageLog.Info("Loaded bag validation config file %s",
			validator.Context.Config.DPN.BagValidationConfigFile)
	}

	validator.BagValidationConfig = bagValidationConfig
}


func (validator *DPNValidator) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	validator.Context.MessageLog.Info("Validator is checking NSQ message %s", string(message.Body))

	// Get the DPNWorkItem, the ReplicationTransfer, and the DPNBag
	//manifest := validator.setupReplicationManifest(message)
	manifest := SetupReplicationManifest(message, "validate", validator.Context,
		validator.LocalClient, validator.RemoteClients)

	manifest.ValidateSummary.Start()
	if manifest.ValidateSummary.HasErrors() {
		validator.Context.MessageLog.Info("Aargh! Into the bitbucket with NSQ message %s", string(message.Body))
		validator.PostProcessChannel <- manifest
		return nil
	}

	// Start processing.
	validator.Context.MessageLog.Info("Putting xfer request %s (bag %s) from %s " +
		" into the validation channel", manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag, manifest.ReplicationTransfer.FromNode)
	validator.ValidationChannel <- manifest
	return nil
}

/***************************************************************************
  Step 1 of 2: Validate the bag.
***************************************************************************/
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

/***************************************************************************
  Step 2 of 2: Record results and push to the next queue, if bag is valid.

  Recording includes:
    a) Updating the DPNWorkItem in Pharos to say this bag
       passed validation.
    b) Pushing the DPNWorkItem id into the dpn_store topic
       in NSQ, so the store worker knows to copy it to Glacier.
  If bag is not valid:
    a) Cancel the transfer on the FromNode.
    b) Update the DPNWorkItem and record the failure in Pharos.

***************************************************************************/
func (validator *DPNValidator) postProcess() {
	for manifest := range validator.PostProcessChannel {
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

	// Tell the FromNode that we're cancelling replication of an invalid bag,
	// unless the bag was already marked as cancelled, in which case the
	// remote server will just give us an error.
	reason := fmt.Sprintf("Bag failed validation. %s", manifest.ValidateSummary.Errors[0])
	if manifest.Cancelled {
		reason = manifest.ValidateSummary.Errors[0]
	}
	if manifest.ReplicationTransfer.Cancelled == false {
		manifest.ReplicationTransfer.Cancelled = true
		manifest.ReplicationTransfer.CancelReason = &reason
		validator.Context.MessageLog.Warning("Cancelling Replication %s at %s: %s",
			manifest.ReplicationTransfer.ReplicationId,
			manifest.ReplicationTransfer.FromNode,
			reason)
		UpdateReplicationTransfer(validator.Context, remoteClient, manifest)
	}

	manifest.ValidateSummary.Finish()

	// Tell Pharos that this DPNWorkItem failed.
	note := "Bag failed validation"
	if manifest.Cancelled {
		note = manifest.ValidateSummary.Errors[0]
	}
	manifest.DPNWorkItem.Node = ""
	manifest.DPNWorkItem.Note = &note
	SaveDPNWorkItemState(validator.Context, manifest, manifest.ValidateSummary)
	validator.Context.MessageLog.Error(manifest.ValidateSummary.AllErrorsAsString())

	// Delete the tar file from our staging area.
	validator.Context.MessageLog.Info(*manifest.DPNWorkItem.Note)
	validator.Context.MessageLog.Info("Deleting %s", manifest.LocalPath)
	os.Remove(manifest.LocalPath)

	// Dump the JSON info about this validation attempt,
	// and tell NSQ we're done.
	LogReplicationJson(manifest, validator.Context.JsonLog)
	manifest.NsqMessage.Finish()
}

func (validator *DPNValidator) finishWithSuccess(manifest *models.ReplicationManifest) {
	validator.Context.MessageLog.Info("Replication %s (bag %s) passed validation",
		manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag)
	manifest.ValidateSummary.Finish()
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
	} else {
		validator.Context.MessageLog.Info("Replication %s (bag %s) pushed to NSQ topic %s",
		manifest.ReplicationTransfer.ReplicationId,
			manifest.ReplicationTransfer.Bag, topic)
	}

	// Dump the JSON info about this validation attempt,
	// and tell NSQ we're done.
	LogReplicationJson(manifest, validator.Context.JsonLog)
	manifest.NsqMessage.Finish()
}
