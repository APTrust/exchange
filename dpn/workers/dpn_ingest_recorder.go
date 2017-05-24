package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
	"os"
	"strings"
	"time"
)

// DPNIngestRecorder records information about locally-ingested
// DPN bags in both APTrust and DPN.
type DPNIngestRecorder struct {
	RecordChannel      chan *models.DPNIngestManifest
	PostProcessChannel chan *models.DPNIngestManifest
	Context            *context.Context
	LocalClient        *network.DPNRestClient
	RemoteClients      map[string]*network.DPNRestClient
}

// NewDPNIngestRecord returns a new DPNIngestRecorder.
func NewDPNIngestRecorder(_context *context.Context) (*DPNIngestRecorder, error) {
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
	recorder := &DPNIngestRecorder{
		Context:       _context,
		LocalClient:   localClient,
		RemoteClients: remoteClients,
	}
	workerBufferSize := _context.Config.DPN.DPNIngestRecordWorker.Workers * 4
	recorder.RecordChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	recorder.PostProcessChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNIngestRecordWorker.Workers; i++ {
		go recorder.record()
		go recorder.postProcess()
	}
	return recorder, nil
}

// HandleMessage is the NSQ message handler. The NSQ consumer will pass each
// message in the subscribed channel to this function.
func (recorder *DPNIngestRecorder) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	recorder.Context.MessageLog.Info("Recorder is checking NSQ message %s", string(message.Body))

	// Set up the manifest WITHOUT the IntellectualObject
	manifest := SetupIngestManifest(message, "record", recorder.Context, false)
	manifest.RecordSummary.Start()
	manifest.RecordSummary.Attempted = true
	manifest.RecordSummary.AttemptNumber += 1

	// Handle the case where we cannot get the WorkItem whose id
	// is specified in the NSQ message.
	if manifest.RecordSummary.HasErrors() {
		recorder.Context.MessageLog.Info("Cannot process NSQ message %s", string(message.Body))
		recorder.PostProcessChannel <- manifest
		return nil
	}

	now := time.Now().UTC()
	hostname, _ := os.Hostname()
	manifest.WorkItem.Stage = constants.StageRecord
	manifest.WorkItem.StageStartedAt = &now
	manifest.WorkItem.Status = constants.StatusStarted
	manifest.WorkItem.Node = hostname
	manifest.WorkItem.Pid = os.Getpid()
	manifest.WorkItem.Note = "Recording ingest data in APTrust and DPN registries"
	SaveWorkItem(recorder.Context, manifest, manifest.RecordSummary)

	// Start processing.
	recorder.Context.MessageLog.Info("Putting bag %s into the recording channel",
		manifest.WorkItem.ObjectIdentifier)
	recorder.RecordChannel <- manifest
	return nil
}

// record saves all necessary info about this ingest to Pharos and to
// our local DPN REST server.
func (recorder *DPNIngestRecorder) record() {
	for manifest := range recorder.RecordChannel {
		manifest.NsqMessage.Touch()
		recorder.saveDPNBagRecord(manifest)
		if !manifest.RecordSummary.HasErrors() {
			recorder.saveDPNReplicationRequests(manifest)
		}
		if !manifest.RecordSummary.HasErrors() {
			recorder.saveDPNPremisEvents(manifest)
			recorder.saveIntellectualObject(manifest)
		}
		manifest.NsqMessage.Touch()
		recorder.PostProcessChannel <- manifest
	}
}

// postProcess records the outcome of this attempt to record info,
// and finishes or requeues the NSQ message.
func (recorder *DPNIngestRecorder) postProcess() {
	for manifest := range recorder.PostProcessChannel {
		if manifest.RecordSummary.HasErrors() {
			recorder.finishWithError(manifest)
		} else {
			recorder.finishWithSuccess(manifest)
		}
	}
}

// saveDPNBagRecord saves the new DPN bag to our local DPN REST server.
func (recorder *DPNIngestRecorder) saveDPNBagRecord(manifest *models.DPNIngestManifest) {
	recorder.Context.MessageLog.Info("Saving DPN bag %s (%s)",
		manifest.DPNBag.UUID, manifest.WorkItem.ObjectIdentifier)
	resp := recorder.LocalClient.DPNBagGet(manifest.DPNBag.UUID)
	if resp.Error != nil {
		manifest.RecordSummary.AddError("Error checking local DPN node for bag %s: %v",
			manifest.DPNBag.UUID, resp.Error)
		return
	}
	if resp.Bag() == nil {
		// PT #146014293: Include ourselves as replicating node when creating a bag
		if len(manifest.DPNBag.ReplicatingNodes) == 0 {
			manifest.DPNBag.ReplicatingNodes = append(
				manifest.DPNBag.ReplicatingNodes,
				recorder.Context.Config.DPN.LocalNode)
		}
		resp = recorder.LocalClient.DPNBagCreate(manifest.DPNBag)
		if resp.Error != nil {
			data, _ := resp.RawResponseData()
			manifest.RecordSummary.AddError("Error creating bag %s in local DPN Node: %v "+
				"Server response: %s", manifest.DPNBag.UUID, resp.Error, string(data))
		}
		if resp.Bag() == nil {
			// This should be impossible.
			data, _ := resp.RawResponseData()
			manifest.RecordSummary.AddError("After creating bag %s in local DPN Node, "+
				"server returned no ReplicationTransfer object. Server response: %s",
				manifest.DPNBag.UUID, string(data))
		}
		// For other objects, we may set <object> = resp.<object>()
		// However, the DPN server does not return the bag's message digests,
		// which are currently attached to manifest.DPNBag, so we'll keep the
		// bag we have in manifest.DPNBag. DPN server doesn't change anything
		// else on the DPNBag object when we save it, so our copy will be
		// in line with what's on the server.
	} else {
		recorder.Context.MessageLog.Info("DPN Bag %s is already in the registry",
			manifest.DPNBag.UUID)
	}
}

// getLocalNode returns the full Node record for our local node. We need
// this in order to get a list of which remote nodes we can replicate to.
// Fetching the local node record for each new ingest may seem redundant,
// since we could just fetch the node record once and then cache it.
// However, we occasionally manually add or remove nodes from the ReplicateTo
// list, and fetching the node record for each ingest guarantees that our
// service will pick up those changes immediately.
func (recorder *DPNIngestRecorder) getLocalNode(manifest *models.DPNIngestManifest) *models.Node {
	resp := recorder.LocalClient.NodeGet(recorder.Context.Config.DPN.LocalNode)
	if resp.Error != nil {
		data, _ := resp.RawResponseData()
		manifest.RecordSummary.AddError("Error fetching local node record %s: %v "+
			"Server response: %s", recorder.Context.Config.DPN.LocalNode,
			resp.Error, string(data))
		return nil
	}
	return resp.Node()
}

// chooseReplicationNodes randomly chooses which remote DPN nodes we will
// ask to replicate our new DPN bag.
func (recorder *DPNIngestRecorder) chooseReplicationNodes(manifest *models.DPNIngestManifest, localNode *models.Node) []string {
	howMany := recorder.Context.Config.DPN.ReplicateToNumNodes
	replicateToNodes, err := localNode.ChooseNodesForReplication(howMany)
	if err != nil {
		manifest.RecordSummary.AddError("Cannot choose nodes for replication: %v", err)
		return nil
	}
	return replicateToNodes
}

// buildTransferRequests builds the DPN ReplicationTransfer requests
// that we need to create with the new DPN bag. The ReplicationTransfers
// will be attached to the DPNIngestManifest and saved in our local DPN
// REST server, after we save the DPN bag.
func (recorder *DPNIngestRecorder) buildTransferRequests(manifest *models.DPNIngestManifest) {
	localNode := recorder.getLocalNode(manifest)
	if manifest.RecordSummary.HasErrors() {
		return
	}
	remoteNodes := recorder.chooseReplicationNodes(manifest, localNode)
	if manifest.RecordSummary.HasErrors() {
		return
	}
	for _, toNode := range remoteNodes {
		domain, err := localNode.FQDN()
		if err != nil {
			manifest.RecordSummary.AddError("Can't get FQDN from node %s. APIRoot is %s. "+
				"Error is %v", localNode.Namespace, localNode.APIRoot, err)
			return
		}
		// Link should look like dpn.chron@dpn-prod2.aptrust.org:outbound/umich.edu/UUID.tar
		// That's a symlink to /mnt/efs/dpn/staging/umich.edu/UUID.tar
		// because all of /home/dpn.chron/outbound is a symlink to /mnt/efs/dpn/staging
		relPath := strings.Replace(manifest.LocalTarFile,
			recorder.Context.Config.DPN.StagingDirectory, "", 1)
		if strings.HasPrefix(relPath, "/") {
			relPath = relPath[1:len(relPath)]
		}
		link := fmt.Sprintf("dpn.%s@%s:outbound/%s", toNode, domain, relPath)
		xfer, err := manifest.BuildReplicationTransfer(
			recorder.Context.Config.DPN.LocalNode, toNode, link)
		if err != nil {
			manifest.RecordSummary.AddError("Error building ReplicationTransfer: %v ", err)
			return
		}
		manifest.ReplicationTransfers = append(manifest.ReplicationTransfers, xfer)
	}
}

// saveReplicationRequests saves the ReplicationTransfer requests associated
// with our new DPN bag. As of late 2016, we create two replication requests
// for each bag, so the bag is replicated to two remote nodes.
func (recorder *DPNIngestRecorder) saveDPNReplicationRequests(manifest *models.DPNIngestManifest) {
	if len(manifest.ReplicationTransfers) == 0 {
		recorder.buildTransferRequests(manifest)
	}
	for _, xfer := range manifest.ReplicationTransfers {
		recorder.Context.MessageLog.Info("Saving ReplicationTransfer %s with ToNode %s "+
			"for bag %s (%s), link %s",
			xfer.ReplicationId, xfer.ToNode, manifest.DPNBag.UUID,
			manifest.WorkItem.ObjectIdentifier, xfer.Link)
		resp := recorder.LocalClient.ReplicationTransferGet(xfer.ReplicationId)
		if resp.Error != nil {
			data, _ := resp.RawResponseData()
			manifest.RecordSummary.AddError("When checking for existence of replication %s, "+
				"Local DPN server returned error: %v Server response: %s",
				xfer.ReplicationId, resp.Error, string(data))
			return
		}
		// Save this transfer request only if it was not saved before.
		// There's no need to update it if it already exists, because
		// it will not have changed.
		if resp.ReplicationTransfer() == nil {
			saveResp := recorder.LocalClient.ReplicationTransferCreate(xfer)
			if saveResp.Error != nil {
				data, _ := resp.RawResponseData()
				manifest.RecordSummary.AddError("When saving new replication %s, "+
					"Local DPN server returned error: %v Server response: %s",
					xfer.ReplicationId, resp.Error, string(data))
				return
			} else {
				xfer = saveResp.ReplicationTransfer()
			}
		} else {
			recorder.Context.MessageLog.Info("Replication %s for bag %s already exists "+
				"on our DPN server. Not saving.", xfer.ReplicationId, xfer.Bag)
		}
	}
}

// addEventsToManifest adds PremisEvents to the ingest manifest, if they
// don't already exist.
func (recorder *DPNIngestRecorder) addEventsToManifest(manifest *models.DPNIngestManifest) {
	if manifest.DPNIdentifierEvent == nil {
		event, err := manifest.BuildDPNIdentifierEvent()
		if err != nil {
			manifest.RecordSummary.AddError(err.Error())
		}
		manifest.DPNIdentifierEvent = event
	}
	if manifest.DPNIngestEvent == nil {
		event, err := manifest.BuildDPNIngestEvent()
		if err != nil {
			manifest.RecordSummary.AddError(err.Error())
		}
		manifest.DPNIngestEvent = event
	}
}

// saveDPNPremisEvents saves PremisEvents in Pharos saying that the original
// APTrust bag was 1) assigned a DPN identifier, and 2) ingested into DPN.
func (recorder *DPNIngestRecorder) saveDPNPremisEvents(manifest *models.DPNIngestManifest) {
	// If DPN ingest events don't already exist on the manifest, create them
	// For each ingest event:
	//   Check if event already exists in Pharos
	//   If not, create it
	recorder.addEventsToManifest(manifest)
	if manifest.RecordSummary.HasErrors() {
		return
	}
	recorder.saveEvent(manifest, manifest.DPNIdentifierEvent)
	recorder.saveEvent(manifest, manifest.DPNIngestEvent)
}

// saveEvent saves a single PremisEvent to Pharos.
func (recorder *DPNIngestRecorder) saveEvent(manifest *models.DPNIngestManifest, event *apt_models.PremisEvent) {
	recorder.Context.MessageLog.Info("Saving event %s DPN bag %s (%s)",
		event.EventType, manifest.DPNBag.UUID, manifest.WorkItem.ObjectIdentifier)
	resp := recorder.Context.PharosClient.PremisEventSave(event)
	if resp.Error != nil {
		data, _ := resp.RawResponseData()
		manifest.RecordSummary.AddError("Error saving PremisEvent %s (%s): %v Server response: %s",
			event.Identifier, event.EventType, resp.Error, string(data))
	}
	event = resp.PremisEvent()
}

// saveIntellectualObject re-saves the IntellectualObject record in Pharos,
// setting the new DPN UUID on that object.
func (recorder *DPNIngestRecorder) saveIntellectualObject(manifest *models.DPNIngestManifest) {
	resp := recorder.Context.PharosClient.IntellectualObjectGet(manifest.WorkItem.ObjectIdentifier, false, false)
	if resp.Error != nil {
		manifest.RecordSummary.AddError("Error retrieving IntellectualObject %s: %v",
			manifest.WorkItem.ObjectIdentifier, resp.Error)
		return
	}
	obj := resp.IntellectualObject()
	if obj == nil {
		// Impossible
		manifest.RecordSummary.AddError("Pharos returned nil IntellectualObject for %s.",
			manifest.WorkItem.ObjectIdentifier)
		return
	}
	obj.DPNUUID = manifest.DPNBag.UUID
	resp = recorder.Context.PharosClient.IntellectualObjectSave(obj)
	if resp.Error != nil {
		manifest.RecordSummary.AddError("Could not save IntellectualObject.DPNUUID: %v", resp.Error)
		return
	}
}

// finishWithError logs JSON data and tells Pharos and NSQ about this
// failed attempt to record ingest data. The WorkItem will be requeued
// if the errors are not fatal and we have not exceeded the maximum number
// of attempts.
func (recorder *DPNIngestRecorder) finishWithError(manifest *models.DPNIngestManifest) {
	note := "Could not record all necessary info in DPN and/or Pharos."
	maxAttempts := recorder.Context.Config.DPN.DPNIngestRecordWorker.MaxAttempts
	if manifest.RecordSummary.AttemptNumber > maxAttempts {
		note = fmt.Sprintf("Failed after %d attempts. Last error: %s",
			maxAttempts,
			manifest.RecordSummary.Errors[0])
		manifest.RecordSummary.ErrorIsFatal = true
		manifest.RecordSummary.Retry = false
		manifest.WorkItem.Status = constants.StatusFailed
	}
	manifest.RecordSummary.Finish()
	manifest.WorkItem.Note = note
	manifest.WorkItem.Node = ""
	manifest.WorkItem.Pid = 0
	SaveWorkItem(recorder.Context, manifest, manifest.RecordSummary)
	SaveWorkItemState(recorder.Context, manifest, manifest.RecordSummary)
	recorder.Context.MessageLog.Error("Failed to record %s (%s): %s",
		manifest.DPNBag.UUID,
		manifest.DPNBag.LocalId,
		manifest.RecordSummary.AllErrorsAsString())
	if manifest.RecordSummary.ErrorIsFatal {
		manifest.NsqMessage.Finish()
	} else {
		manifest.NsqMessage.Requeue(1 * time.Minute)
	}
}

// finishWithSuccess tells Pharos and NSQ that this item has successfully
// completed the DPN ingest process.
func (recorder *DPNIngestRecorder) finishWithSuccess(manifest *models.DPNIngestManifest) {
	manifest.WorkItem.Note = "DPN ingest complete"
	manifest.WorkItem.Stage = constants.StageResolve
	manifest.WorkItem.StageStartedAt = nil
	manifest.WorkItem.Status = constants.StatusSuccess
	manifest.WorkItem.Node = ""
	manifest.WorkItem.Pid = 0
	SaveWorkItem(recorder.Context, manifest, manifest.RecordSummary)
	manifest.RecordSummary.Finish()
	SaveWorkItemState(recorder.Context, manifest, manifest.RecordSummary)
	manifest.NsqMessage.Finish()
	recorder.Context.MessageLog.Info("Ingest complete for bag %s (%s)",
		manifest.DPNBag.UUID,
		manifest.DPNBag.LocalId)
}
