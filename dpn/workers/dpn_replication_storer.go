package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	apt_network "github.com/APTrust/exchange/network"
	"github.com/nsqio/go-nsq"
	"os"
	"time"
)

// DPNReplicationStorer copies replicated bags from our staging
// area to Glacier long-term storage. We only copy bags that have
// been validated.
type DPNReplicationStorer struct {
	StoreChannel       chan *models.ReplicationManifest
	PostProcessChannel chan *models.ReplicationManifest
	Context            *context.Context
	LocalClient        *network.DPNRestClient
	RemoteClients      map[string]*network.DPNRestClient
}

// NewDPNReplicationStorer creates a new DPNReplicationStorer object.
func NewDPNReplicationStorer(_context *context.Context) (*DPNReplicationStorer, error) {
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
	storer := &DPNReplicationStorer{
		Context:       _context,
		LocalClient:   localClient,
		RemoteClients: remoteClients,
	}
	workerBufferSize := _context.Config.DPN.DPNReplicationStoreWorker.Workers * 4
	storer.StoreChannel = make(chan *models.ReplicationManifest, workerBufferSize)
	storer.PostProcessChannel = make(chan *models.ReplicationManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNReplicationStoreWorker.Workers; i++ {
		go storer.store()
		go storer.postProcess()
	}
	return storer, nil
}

// HandleMessage is the NSQ message handler. The NSQ consumer will pass each
// message in the subscribed channel to this function.
func (storer *DPNReplicationStorer) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	storer.Context.MessageLog.Info("Storer is checking NSQ message %s", string(message.Body))

	// Get the DPNWorkItem, the ReplicationTransfer, and the DPNBag
	manifest := SetupReplicationManifest(message, "store", storer.Context,
		storer.LocalClient, storer.RemoteClients)

	// If there were any problems setting getting the work item,
	// the replication transfer request, or other essential info,
	// bail now.
	if manifest.StoreSummary.HasErrors() {
		storer.Context.MessageLog.Info("Aargh! Into the bitbucket with NSQ message %s", string(message.Body))
		storer.PostProcessChannel <- manifest
		return nil
	}

	// Stop processing if item is already stored, or cancelled, or if
	// it's already in process by one of our service workers.
	if !storer.storeShouldProceed(manifest, message) {
		message.Finish()
		return nil
	}

	manifest.StoreSummary.Start()
	manifest.StoreSummary.Attempted = true
	manifest.StoreSummary.AttemptNumber += 1

	// Start processing.
	storer.logStartingStorage(manifest)
	storer.StoreChannel <- manifest
	return nil
}

func (storer *DPNReplicationStorer) store() {
	for manifest := range storer.StoreChannel {
		// Don't time us out, NSQ!
		manifest.NsqMessage.Touch()

		// Tell Pharos that we've started to validate item.
		hostname, _ := os.Hostname()
		note := "Storing bag"
		manifest.DPNWorkItem.Note = &note
		manifest.DPNWorkItem.ProcessingNode = &hostname
		manifest.DPNWorkItem.Pid = os.Getpid()
		SaveDPNWorkItemState(storer.Context, manifest, manifest.StoreSummary)

		// Upload to Glacier.
		// Give it a few tries, since larger bags occasionally
		// encounter network errors.
		for i := 0; i < 10; i++ {
			storer.copyToLongTermStorage(manifest)
			if manifest.CopySummary.HasErrors() == false {
				break
			}
		}

		manifest.NsqMessage.Touch()
		storer.PostProcessChannel <- manifest
	}
}

func (storer *DPNReplicationStorer) postProcess() {
	for manifest := range storer.PostProcessChannel {
		if manifest.StoreSummary.HasErrors() {
			storer.finishWithError(manifest)
		} else {
			storer.finishWithSuccess(manifest)
		}
	}
}

func (storer *DPNReplicationStorer) copyToLongTermStorage(manifest *models.ReplicationManifest) {
	manifest.StoreSummary.ClearErrors()
	upload := apt_network.NewS3Upload(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		storer.Context.Config.DPN.DPNPreservationBucket,
		fmt.Sprintf("%s.tar", manifest.ReplicationTransfer.Bag),
		"application/x-tar")
	upload.AddMetadata("from_node", manifest.ReplicationTransfer.FromNode)
	upload.AddMetadata("transfer_id", manifest.ReplicationTransfer.ReplicationId)
	upload.AddMetadata("member", manifest.DPNBag.Member)
	upload.AddMetadata("local_id", manifest.DPNBag.LocalId)
	upload.AddMetadata("version", fmt.Sprintf("%d", manifest.DPNBag.Version))
	reader, err := os.Open(manifest.LocalPath)
	if reader != nil {
		defer reader.Close()
	}
	if err != nil {
		manifest.StoreSummary.AddError("Error opening reader for tar file: %v", err)
		return
	}
	upload.Send(reader)
	if upload.ErrorMessage != "" {
		manifest.StoreSummary.AddError("Error uploading tar file: %s", upload.ErrorMessage)
		return
	}
	manifest.StorageURL = upload.Response.Location
}

func (storer *DPNReplicationStorer) finishWithError(manifest *models.ReplicationManifest) {

	// Give up only if we've failed too many times.
	note := "Bag could not be copied to long-term storage"
	maxAttempts := storer.Context.Config.DPN.DPNReplicationStoreWorker.MaxAttempts
	if manifest.StoreSummary.AttemptNumber > maxAttempts {
		note := fmt.Sprintf("Failed to copy to Glacier too many times (%d). %s",
			maxAttempts,
			manifest.StoreSummary.Errors[0])
		manifest.StoreSummary.ErrorIsFatal = true
		manifest.StoreSummary.Retry = false
		storer.Context.MessageLog.Error("Cancelling Replication %s at %s: "+
			"Copy to Glacier has failed %d times.",
			manifest.ReplicationTransfer.ReplicationId,
			manifest.ReplicationTransfer.FromNode,
			maxAttempts)

		// Get the remote client that talks to this transfer's FromNode
		remoteClient := storer.RemoteClients[manifest.ReplicationTransfer.FromNode]

		// Tell the FromNode that we're cancelling replication of an invalid bag,
		// unless the bag was already marked as cancelled, in which case the
		// remote server will just give us an error.
		reason := fmt.Sprintf("Attempt to copy bag to remote storage failed %d times", maxAttempts)
		if manifest.Cancelled {
			reason = manifest.StoreSummary.Errors[0]
		}
		if manifest.ReplicationTransfer.Cancelled == false {
			manifest.ReplicationTransfer.Cancelled = true
			manifest.ReplicationTransfer.CancelReason = &reason
			storer.Context.MessageLog.Warning("Cancelling Replication %s at %s: %s",
				manifest.ReplicationTransfer.ReplicationId,
				manifest.ReplicationTransfer.FromNode,
				reason)
			UpdateReplicationTransfer(storer.Context, remoteClient, manifest)
		}

		// Delete the tar file from our staging area.
		storer.Context.MessageLog.Info(note)
		storer.Context.MessageLog.Info("Deleting %s", manifest.LocalPath)
		os.Remove(manifest.LocalPath)
	}

	manifest.StoreSummary.Finish()
	manifest.DPNWorkItem.Note = &note
	manifest.DPNWorkItem.ProcessingNode = nil
	manifest.DPNWorkItem.Pid = 0
	SaveDPNWorkItemState(storer.Context, manifest, manifest.StoreSummary)
	storer.Context.MessageLog.Error(manifest.StoreSummary.AllErrorsAsString())

	// Dump the JSON info about this validation attempt,
	// and tell NSQ we're done.
	LogReplicationJson(manifest, storer.Context.JsonLog)

	if manifest.StoreSummary.ErrorIsFatal {
		manifest.NsqMessage.Finish()
	} else {
		manifest.NsqMessage.Requeue(1 * time.Minute)
	}
}

func (storer *DPNReplicationStorer) finishWithSuccess(manifest *models.ReplicationManifest) {
	storer.Context.MessageLog.Info("Replication %s (bag %s) stored at %s",
		manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag,
		manifest.StorageURL)

	// Tell the remote node that we stored this item.
	manifest.ReplicationTransfer.Stored = true
	remoteClient := storer.RemoteClients[manifest.ReplicationTransfer.FromNode]
	if remoteClient == nil {
		manifest.StoreSummary.AddError("Cannot get remote client for %s",
			manifest.ReplicationTransfer.FromNode)
	} else {
		UpdateReplicationTransfer(storer.Context, remoteClient, manifest)
	}

	note := "Bag copied to long-term storage"
	if manifest.StoreSummary.HasErrors() {
		note += " but could not set Stored=true on FromNode."
	} else {
		// Tell Pharos we're done working on this.
		manifest.StoreSummary.Finish()
		manifest.DPNWorkItem.CompletedAt = &manifest.StoreSummary.FinishedAt
	}
	manifest.DPNWorkItem.Note = &note
	manifest.DPNWorkItem.ProcessingNode = nil
	manifest.DPNWorkItem.Pid = 0
	SaveDPNWorkItemState(storer.Context, manifest, manifest.StoreSummary)

	// Dump the JSON info about this validation attempt,
	// and tell NSQ we're done.
	LogReplicationJson(manifest, storer.Context.JsonLog)
	if manifest.StoreSummary.HasErrors() == false {
		manifest.NsqMessage.Finish()
	} else {
		manifest.NsqMessage.Requeue(1 * time.Minute)
	}

	// Delete the tar file from our staging area.
	// Once we've copied it into storage there's nothing
	// left to do, and we don't need a local copy.
	storer.Context.MessageLog.Info(note)
	storer.Context.MessageLog.Info("Deleting %s", manifest.LocalPath)
	os.Remove(manifest.LocalPath)
}

func (storer *DPNReplicationStorer) storeShouldProceed(manifest *models.ReplicationManifest, message *nsq.Message) bool {
	shouldProceed := true
	if manifest.DPNWorkItem.IsBeingProcessed() {
		storer.logItemAlreadyInProcess(manifest)
		shouldProceed = false
	} else if manifest.ReplicationTransfer.Stored {
		EnsureItemIsMarkedComplete(storer.Context, manifest)
		storer.logReplicationStored(manifest)
		shouldProceed = false
	} else if manifest.ReplicationTransfer.Cancelled {
		EnsureItemIsMarkedCancelled(storer.Context, manifest)
		storer.logReplicationCancelled(manifest)
		shouldProceed = false
	}
	return shouldProceed
}

func (storer *DPNReplicationStorer) logReplicationStored(manifest *models.ReplicationManifest) {
	storer.Context.MessageLog.Info("Replication %s for bag %s has already been stored",
		manifest.ReplicationTransfer.ReplicationId, manifest.DPNBag.UUID)
}

func (storer *DPNReplicationStorer) logReplicationCancelled(manifest *models.ReplicationManifest) {
	storer.Context.MessageLog.Info("Replication %s for bag %s was cancelled",
		manifest.ReplicationTransfer.ReplicationId, manifest.DPNBag.UUID)
}

func (storer *DPNReplicationStorer) logStartingStorage(manifest *models.ReplicationManifest) {
	storer.Context.MessageLog.Info("Putting xfer request %s (bag %s) from %s "+
		" into the storage channel", manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag, manifest.ReplicationTransfer.FromNode)
}

func (storer *DPNReplicationStorer) logItemAlreadyInProcess(manifest *models.ReplicationManifest) {
	node := "unknown"
	if manifest.DPNWorkItem.ProcessingNode != nil {
		node = *manifest.DPNWorkItem.ProcessingNode
	}
	storer.Context.MessageLog.Info("Skipping xfer request %s (bag %s): item is already "+
		" being processed by node %s, pid %d.", manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag, node, manifest.DPNWorkItem.Pid)
}
