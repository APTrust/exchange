package workers

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	dpn_models "github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"os"
	"time"
)

// DPNValidator validates DPN bags (tar files) before we send them off
// to long-term storage.
type DPNValidator struct {
	ValidationChannel   chan *dpn_models.ReplicationManifest
	PostProcessChannel  chan *dpn_models.ReplicationManifest
	BagValidationConfig *validation.BagValidationConfig
	Context             *context.Context
	LocalClient         *network.DPNRestClient
	RemoteClients       map[string]*network.DPNRestClient
}

// NewDPNValidator returns a new DPNValidator object.
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
	validator := &DPNValidator{
		Context:       _context,
		LocalClient:   localClient,
		RemoteClients: remoteClients,
	}
	validator.BagValidationConfig = LoadDPNBagValidationConfig(validator.Context)
	workerBufferSize := _context.Config.DPN.DPNValidationWorker.Workers * 4
	validator.ValidationChannel = make(chan *dpn_models.ReplicationManifest, workerBufferSize)
	validator.PostProcessChannel = make(chan *dpn_models.ReplicationManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNValidationWorker.Workers; i++ {
		go validator.validate()
		go validator.postProcess()
	}
	return validator, nil
}

// HandleMessage is the NSQ message handler. The NSQ consumer will pass each
// message in the subscribed channel to this function.
func (validator *DPNValidator) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	validator.Context.MessageLog.Info("Validator is checking NSQ message %s", string(message.Body))

	// Get the DPNWorkItem, the ReplicationTransfer, and the DPNBag
	//manifest := validator.setupReplicationManifest(message)
	manifest := SetupReplicationManifest(message, "validate", validator.Context,
		validator.LocalClient, validator.RemoteClients)

	// If there were any errors setting up the replication manifest,
	// quit now. Most likely issue is that we couldn't get the
	// DPNWorkItem from Pharos.
	if manifest.ValidateSummary.HasErrors() {
		validator.Context.MessageLog.Info("Aargh! Into the bitbucket with NSQ message %s", string(message.Body))
		validator.PostProcessChannel <- manifest
		return nil
	}

	// If there's any other reason we should not proceed, stop here.
	if !validator.validationShouldProceed(manifest, message) {
		message.Finish()
		return nil
	}

	manifest.ValidateSummary.Start()
	manifest.ValidateSummary.Attempted = true
	manifest.ValidateSummary.AttemptNumber += 1

	// Start processing.
	validator.logStartingValidation(manifest)
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
		hostname, _ := os.Hostname()
		note := "Validating bag"
		manifest.DPNWorkItem.Note = &note
		manifest.DPNWorkItem.ProcessingNode = &hostname
		manifest.DPNWorkItem.Pid = os.Getpid()
		SaveDPNWorkItemState(validator.Context, manifest, manifest.ValidateSummary)

		// Set up a new validator to check this bag.
		bagValidator, err := validation.NewValidator(manifest.LocalPath,
			validator.BagValidationConfig, true)
		if err != nil {
			// Could not create a BagValidator. Should this be fatal?
			validator.Context.MessageLog.Error(err.Error())
			manifest.ValidateSummary.AddError(err.Error())
		} else {
			// Validation can take hours for very large bags.
			summary, err := bagValidator.Validate()
			if err != nil {
				now := time.Now().UTC()
				manifest.ValidateSummary = apt_models.NewWorkSummary()
				manifest.ValidateSummary.Attempted = true
				manifest.ValidateSummary.StartedAt = now
				manifest.ValidateSummary.FinishedAt = now
				manifest.ValidateSummary.AddError(err.Error())
			} else {
				manifest.ValidateSummary = summary
			}
		}
		manifest.NsqMessage.Touch()
		if bagValidator != nil && fileutil.LooksSafeToDelete(bagValidator.DBName(), 12, 3) {
			os.Remove(bagValidator.DBName())
		}
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

func (validator *DPNValidator) finishWithError(manifest *dpn_models.ReplicationManifest) {
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
	now := time.Now().UTC()
	note := "Bag failed validation"
	if manifest.Cancelled {
		note = manifest.ValidateSummary.Errors[0]
	}
	manifest.DPNWorkItem.Note = &note
	manifest.DPNWorkItem.CompletedAt = &now
	manifest.DPNWorkItem.ProcessingNode = nil
	manifest.DPNWorkItem.Pid = 0
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

func (validator *DPNValidator) finishWithSuccess(manifest *dpn_models.ReplicationManifest) {
	validator.Context.MessageLog.Info("Replication %s (bag %s) passed validation",
		manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag)
	manifest.ValidateSummary.Finish()
	// Tell Pharos we're done working on this.
	note := "Bag passed validation"
	manifest.DPNWorkItem.Note = &note
	manifest.DPNWorkItem.ProcessingNode = nil
	manifest.DPNWorkItem.Pid = 0
	SaveDPNWorkItemState(validator.Context, manifest, manifest.ValidateSummary)

	// Push this DPNWorkItem Id into the next queue, so it can be stored.
	topic := validator.Context.Config.DPN.DPNReplicationStoreWorker.NsqTopic
	err := validator.Context.NSQClient.Enqueue(topic, manifest.DPNWorkItem.Id)
	if err != nil {
		msg := fmt.Sprintf("Error pushing into NSQ %s: %v", manifest.DPNWorkItem.Identifier, err)
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

func (validator *DPNValidator) validationShouldProceed(manifest *dpn_models.ReplicationManifest, message *nsq.Message) bool {
	shouldProceed := true
	if manifest.DPNWorkItem.IsBeingProcessed() {
		validator.logItemAlreadyInProcess(manifest)
		shouldProceed = false
	} else if manifest.ReplicationTransfer.Stored {
		EnsureItemIsMarkedComplete(validator.Context, manifest)
		validator.logReplicationStored(manifest)
		shouldProceed = false
	} else if manifest.ReplicationTransfer.Cancelled {
		EnsureItemIsMarkedCancelled(validator.Context, manifest)
		validator.logReplicationCancelled(manifest)
		shouldProceed = false
	} else if manifest.ValidateSummary.Finished() {
		validator.logValidationAlreadyComplete(manifest)
		shouldProceed = false
	} else if !fileutil.FileExists(manifest.LocalPath) {
		validator.logThatFileIsMissing(manifest, message)
		manifest.ValidateSummary.AddError("Tar file %s is not on disk", manifest.LocalPath)
		manifest.ValidateSummary.ErrorIsFatal = true
		shouldProceed = false
	}
	return shouldProceed
}

func (validator *DPNValidator) logThatFileIsMissing(manifest *dpn_models.ReplicationManifest, message *nsq.Message) {
	validator.Context.MessageLog.Info("Message %s: Bag %s for replication %s is missing from disk",
		string(message.Body), manifest.DPNBag.UUID, manifest.ReplicationTransfer.ReplicationId)
}

func (validator *DPNValidator) logReplicationStored(manifest *dpn_models.ReplicationManifest) {
	validator.Context.MessageLog.Info("Replication %s for bag %s has already been stored",
		manifest.ReplicationTransfer.ReplicationId, manifest.DPNBag.UUID)
}

func (validator *DPNValidator) logReplicationCancelled(manifest *dpn_models.ReplicationManifest) {
	validator.Context.MessageLog.Info("Replication %s for bag %s was cancelled",
		manifest.ReplicationTransfer.ReplicationId, manifest.DPNBag.UUID)
}

func (validator *DPNValidator) logValidationAlreadyComplete(manifest *dpn_models.ReplicationManifest) {
	validator.Context.MessageLog.Info("Validation of %s (bag %s) has already been completed",
		manifest.ReplicationTransfer.ReplicationId, manifest.DPNBag.UUID)
}

func (validator *DPNValidator) logStartingValidation(manifest *dpn_models.ReplicationManifest) {
	validator.Context.MessageLog.Info("Putting xfer request %s (bag %s) from %s "+
		" into the validation channel", manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag, manifest.ReplicationTransfer.FromNode)
}

func (validator *DPNValidator) logItemAlreadyInProcess(manifest *dpn_models.ReplicationManifest) {
	node := "unknown"
	if manifest.DPNWorkItem.ProcessingNode != nil {
		node = *manifest.DPNWorkItem.ProcessingNode
	}
	validator.Context.MessageLog.Info("Skipping xfer request %s (bag %s): item is already "+
		" being processed by node %s, pid %d.", manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag, node, manifest.DPNWorkItem.Pid)
}
