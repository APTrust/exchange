package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	apt_network "github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nsqio/go-nsq"
	"os"
	"time"
)

// DPNIngestStorer copies bags ingested from APTrust into Glacier
// long-term storage.
type DPNIngestStorer struct {
	StoreChannel       chan *models.DPNIngestManifest
	PostProcessChannel chan *models.DPNIngestManifest
	Context            *context.Context
	LocalClient        *network.DPNRestClient
	RemoteClients      map[string]*network.DPNRestClient
}

// NewDPNIngestStorer returns a new DPNIngestStorer object.
func NewDPNIngestStorer(_context *context.Context) (*DPNIngestStorer, error) {
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
	storer := &DPNIngestStorer{
		Context:       _context,
		LocalClient:   localClient,
		RemoteClients: remoteClients,
	}
	workerBufferSize := _context.Config.DPN.DPNIngestStoreWorker.Workers * 4
	storer.StoreChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	storer.PostProcessChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNIngestStoreWorker.Workers; i++ {
		go storer.store()
		go storer.postProcess()
	}
	return storer, nil
}

// HandleMessage is the NSQ message handler. The NSQ consumer will pass each
// message in the subscribed channel to this function.
func (storer *DPNIngestStorer) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	storer.Context.MessageLog.Info("Storer is checking NSQ message %s", string(message.Body))

	// Set up the manifest WITHOUT the IntellectualObject
	manifest := SetupIngestManifest(message, "store", storer.Context, false)
	manifest.StoreSummary.Start()
	manifest.StoreSummary.Attempted = true
	manifest.StoreSummary.AttemptNumber += 1

	// Handle the case where we cannot get the WorkItem whose id
	// is specified in the NSQ message.
	if manifest.StoreSummary.HasErrors() {
		storer.Context.MessageLog.Info("Cannot process NSQ message %s", string(message.Body))
		storer.PostProcessChannel <- manifest
		return nil
	}

	now := time.Now().UTC()
	hostname, _ := os.Hostname()
	manifest.WorkItem.Stage = constants.StageStore
	manifest.WorkItem.StageStartedAt = &now
	manifest.WorkItem.Status = constants.StatusStarted
	manifest.WorkItem.Node = hostname
	manifest.WorkItem.Pid = os.Getpid()
	manifest.WorkItem.Note = "Starting copy to Glacier"
	SaveWorkItem(storer.Context, manifest, manifest.StoreSummary)

	// Start processing.
	storer.Context.MessageLog.Info("Putting bag %s into the storage channel",
		manifest.WorkItem.ObjectIdentifier)
	storer.StoreChannel <- manifest
	return nil
}

func (storer *DPNIngestStorer) store() {
	for manifest := range storer.StoreChannel {
		// Don't time us out, NSQ!
		manifest.NsqMessage.Touch()

		// Upload to Glacier.
		// Give it a few tries, since larger bags occasionally
		// encounter network errors.
		for i := 0; i < 10; i++ {
			storer.copyToLongTermStorage(manifest)
			if manifest.StoreSummary.HasErrors() == false {
				break
			}
		}

		manifest.NsqMessage.Touch()
		storer.PostProcessChannel <- manifest
	}
}

func (storer *DPNIngestStorer) postProcess() {
	for manifest := range storer.PostProcessChannel {
		if manifest.StoreSummary.HasErrors() {
			storer.finishWithError(manifest)
		} else {
			storer.finishWithSuccess(manifest)
		}
	}
}

func (storer *DPNIngestStorer) copyToLongTermStorage(manifest *models.DPNIngestManifest) {
	manifest.StoreSummary.ClearErrors()
	upload := apt_network.NewS3Upload(
		constants.AWSVirginia,
		storer.Context.Config.DPN.DPNPreservationBucket,
		fmt.Sprintf("%s.tar", manifest.DPNBag.UUID),
		"application/x-tar")
	upload.AddMetadata("from_node", manifest.DPNBag.IngestNode)
	upload.AddMetadata("transfer_id", fmt.Sprintf("None - Ingested Directly at %s", manifest.DPNBag.IngestNode))
	upload.AddMetadata("member", manifest.DPNBag.Member)
	upload.AddMetadata("local_id", manifest.DPNBag.LocalId)
	upload.AddMetadata("version", fmt.Sprintf("%d", manifest.DPNBag.Version))
	reader, err := os.Open(manifest.LocalTarFile)
	if reader != nil {
		defer reader.Close()
	}
	if err != nil {
		manifest.StoreSummary.AddError("Error opening reader for tar file %s: %v",
			manifest.LocalTarFile, err)
		return
	}
	upload.Send(reader)
	if upload.ErrorMessage != "" {
		manifest.StoreSummary.AddError("Error uploading tar file %s: %s",
			manifest.LocalTarFile, upload.ErrorMessage)
		return
	}
	manifest.StorageURL = upload.Response.Location
}

func (storer *DPNIngestStorer) finishWithError(manifest *models.DPNIngestManifest) {

	// Give up only if we've failed too many times.
	note := "Bag could not be copied to long-term storage"
	maxAttempts := storer.Context.Config.DPN.DPNIngestStoreWorker.MaxAttempts
	if manifest.StoreSummary.AttemptNumber > maxAttempts {
		note = fmt.Sprintf("Failed to copy to Glacier too many times (%d). "+
			"TAR FILE IS STILL IN STAGING. Last error: %s",
			maxAttempts,
			manifest.StoreSummary.Errors[0])
		manifest.StoreSummary.ErrorIsFatal = true
		manifest.StoreSummary.Retry = false
		manifest.WorkItem.Status = constants.StatusFailed
	}

	manifest.StoreSummary.Finish()
	manifest.WorkItem.Note = note
	manifest.WorkItem.Node = ""
	manifest.WorkItem.Pid = 0
	SaveWorkItem(storer.Context, manifest, manifest.StoreSummary)
	SaveWorkItemState(storer.Context, manifest, manifest.StoreSummary)
	storer.Context.MessageLog.Error("Failed to copy %s (%s) to long-term storage: %s",
		manifest.DPNBag.UUID,
		manifest.DPNBag.LocalId,
		manifest.StoreSummary.AllErrorsAsString())
	storer.Context.MessageLog.Warning("Tar file %s is still in staging!",
		manifest.LocalTarFile)
	if manifest.StoreSummary.ErrorIsFatal {
		manifest.NsqMessage.Finish()
	} else {
		manifest.NsqMessage.Requeue(1 * time.Minute)
	}
}

func (storer *DPNIngestStorer) finishWithSuccess(manifest *models.DPNIngestManifest) {
	storer.Context.MessageLog.Info("Bag %s (%s) stored at %s",
		manifest.DPNBag.UUID,
		manifest.DPNBag.LocalId,
		manifest.StorageURL)

	manifest.WorkItem.Note = "Bag copied to long-term storage"
	manifest.WorkItem.Stage = constants.StageRecord
	manifest.WorkItem.StageStartedAt = nil
	manifest.WorkItem.Status = constants.StatusPending
	manifest.WorkItem.Node = ""
	manifest.WorkItem.Pid = 0
	SaveWorkItem(storer.Context, manifest, manifest.StoreSummary)
	manifest.StoreSummary.Finish()
	SaveWorkItemState(storer.Context, manifest, manifest.StoreSummary)

	// Push this WorkItem to the next NSQ topic.
	storer.Context.MessageLog.Info("Pushing %s (DPN bag %s) to NSQ topic %s",
		manifest.DPNBag.LocalId, manifest.DPNBag.UUID,
		storer.Context.Config.DPN.DPNIngestRecordWorker.NsqTopic)
	PushToQueue(storer.Context, manifest, manifest.StoreSummary,
		storer.Context.Config.DPN.DPNIngestRecordWorker.NsqTopic)
	if manifest.PackageSummary.HasErrors() {
		storer.Context.MessageLog.Error(manifest.PackageSummary.Errors[0])
	}

	if fileutil.LooksSafeToDelete(manifest.LocalTarFile, 12, 3) {
		err := os.Remove(manifest.LocalTarFile)
		if err != nil {
			storer.Context.MessageLog.Error("Failed to delete %s after upload",
				manifest.LocalTarFile)
		}
	}

	// Tell NSQ we're done storing this.
	manifest.NsqMessage.Finish()
}
