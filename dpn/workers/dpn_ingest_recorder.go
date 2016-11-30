package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	//	apt_network "github.com/APTrust/exchange/network"
	//	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nsqio/go-nsq"
	"os"
	"time"
)

// dpn_ingest_recorder records information about locally-ingested
// DPN bags in both APTrust and DPN.

type DPNIngestRecorder struct {
	RecordChannel      chan *models.DPNIngestManifest
	PostProcessChannel chan *models.DPNIngestManifest
	Context            *context.Context
	LocalClient        *network.DPNRestClient
	RemoteClients      map[string]*network.DPNRestClient
}

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

func (recorder *DPNIngestRecorder) record() {
	for manifest := range recorder.RecordChannel {
		manifest.NsqMessage.Touch()

		// Create bag record in DPN
		// Create replication requests in DPN
		// Create DPN replication event in APTrust

		manifest.NsqMessage.Touch()
		recorder.PostProcessChannel <- manifest
	}
}

func (recorder *DPNIngestRecorder) postProcess() {
	for manifest := range recorder.PostProcessChannel {
		if manifest.RecordSummary.HasErrors() {
			recorder.finishWithError(manifest)
		} else {
			recorder.finishWithSuccess(manifest)
		}
	}
}

func (recorder *DPNIngestRecorder) saveDPNBagRecord(manifest *models.DPNIngestManifest) {
	// Save the DPNBag record to our local DPN REST server
	resp := recorder.LocalClient.DPNBagGet(manifest.DPNBag.UUID)
	if resp.Error != nil {
		manifest.RecordSummary.AddError("Error checking local DPN node for bag %s: %v",
			manifest.DPNBag.UUID, resp.Error)
		return
	}
	if resp.Bag() == nil {
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
		} else {
			manifest.DPNBag = resp.Bag()
		}
	} else {
		recorder.Context.MessageLog.Info("DPN Bag %s is already in the registry",
			manifest.DPNBag.UUID)
	}
}

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

func (recorder *DPNIngestRecorder) chooseReplicationNodes(manifest *models.DPNIngestManifest, localNode *models.Node) []string {
	howMany := recorder.Context.Config.DPN.ReplicateToNumNodes
	replicateToNodes, err := localNode.ChooseNodesForReplication(howMany)
	if err != nil {
		manifest.RecordSummary.AddError("Cannot choose nodes for replication: %v", err)
		return nil
	}
	return replicateToNodes
}

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
		link := fmt.Sprintf("dpn.%s@%s:outbound/%s.tar",
			toNode, domain, manifest.DPNBag.UUID)
		xfer, err := manifest.BuildReplicationTransfer(
			recorder.Context.Config.DPN.LocalNode, toNode, link)
		if err != nil {
			manifest.RecordSummary.AddError("Error building ReplicationTransfer: %v ", err)
			return
		}
		manifest.ReplicationTransfers = append(manifest.ReplicationTransfers, xfer)
	}
}

func (recorder *DPNIngestRecorder) saveDPNReplicationRequests(manifest *models.DPNIngestManifest) {
	if len(manifest.ReplicationTransfers) == 0 {
		recorder.buildTransferRequests(manifest)
	}
	// -------- START HERE ------------
	// For each replication request
	//   Check if it exists in Pharos
	//   If not, create it
}

func (recorder *DPNIngestRecorder) saveDPNIngestEvents(manifest *models.DPNIngestManifest) {
	// If DPN ingest events don't already exist on the manifest, create them
	// For each ingest event:
	//   Check if event already exists in Pharos
	//   If not, create it
}

func (recorder *DPNIngestRecorder) finishWithError(manifest *models.DPNIngestManifest) {
	// If fatal error or exceeded max attempts
	//    set failed, set note & retry false
	//    finish NSQ message
	// If non-fatal error and not max attempts
	//    set note
	//    requeue NSQ message
	// Save WorkItem
	// Save WorkItemState
}

func (recorder *DPNIngestRecorder) finishWithSuccess(manifest *models.DPNIngestManifest) {
	// Save WorkItem
	// Save WorkItemState
	// Finish NSQ message
}
