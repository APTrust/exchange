package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	apt_models "github.com/APTrust/exchange/models"
	"net/url"
	"strconv"
	"time"
)

type DPNQueue struct {
	// LocalClient is the DPN REST client that talks to our own
	// local DPN REST server.
	LocalClient    *network.DPNRestClient
	// RemoteNodes is a map of remote nodes. Key is the namespace
	// and value is the node.
	RemoteNodes    map[string]*models.Node
	// RemoteClients is a collection of clients that talk to the
	// DPN REST servers on other nodes. The key is the namespace
	// of the remote node, and the value is the client that talks
	// to that node.
	RemoteClients   map[string]*network.DPNRestClient
	// Context provides access to information about our environment
	// and config settings, and access to basic services like
	// logging and a Pharos client.
	Context         *context.Context
	// ExamineItemsSince is a timestamp. We will examine any items
	// updated since this timestamp to see if they need to be queued.
	ExamineItemsSince time.Time
	// QueueResult contains information about which items were
	// queued during this run of the program.
	QueueResult      *models.QueueResult
}

// NewDPNQueue creates a new DPNQueue object. Param _context is a Context
// object, and param hours tells the code to examine all Replication,
// Restore and DPN Ingest requests from the past N hours.
func NewDPNQueue(_context *context.Context, hours int) (*DPNQueue, error) {
	if _context == nil {
		return nil, fmt.Errorf("Param _context cannot be nil.")
	}
	localClient, err := network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.DPNAPIVersion,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	if err != nil {
		return nil, fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	remoteClients, err := localClient.GetRemoteClients()
	if err != nil {
		return nil, fmt.Errorf("Error creating remote DPN REST client: %v", err)
	}
	sinceWhen := time.Now().UTC().Add(time.Duration(-1 * hours) * time.Hour)
	_context.MessageLog.Info("Checking records since %d hours ago (%s)",
		hours, sinceWhen.Format(time.RFC3339))
	dpnQueue := DPNQueue{
		LocalClient: localClient,
		RemoteNodes: make(map[string]*models.Node),
		RemoteClients: remoteClients,
		Context: _context,
		ExamineItemsSince: sinceWhen,
		QueueResult: models.NewQueueResult(),
	}
	return &dpnQueue, nil
}

// Run checks for ReplicationTransfers, RestoreTransfers and IngestRequests
// that need to be queued. It creates DPNWorkItems and NSQ entries for each
// request that needs to be queued.
func (dpnQueue *DPNQueue) Run() {
	dpnQueue.QueueResult.StartTime = time.Now().UTC()
	dpnQueue.queueReplicationRequests()
	dpnQueue.queueRestoreRequests()
	dpnQueue.queueIngestRequests()
	dpnQueue.QueueResult.EndTime = time.Now().UTC()
	dpnQueue.logJsonResults()
}

/***************************************************************************
 ReplicationTransfer Methods
***************************************************************************/

// queueReplicationRequests collects ReplicationTransfer requests from
// the local DPN server and if necessary 1) creates a DPNWorkItem record
// in our Pharos server for the replication request, and 2) creates an
// entry in NSQ telling our replication workers to copy the bag.
//
// We query our local DPN node after synching data from other nodes, and
// we're looking for ReplicationTransfers where the to_node is our node.
// We want to skip transfers that are cancelled or already stored. We
// also want to skip transfers where store_requested is true, because
// those transfers are already in progress. (The remote node sets
// store_requested to true only after it sees that the replicating node
// has returned a valid fixity value for the bag's tag manifest.)
func (dpnQueue *DPNQueue) queueReplicationRequests() {
	pageNumber := 1
	params := dpnQueue.replicationParams(pageNumber)
	for {
		dpnResp := dpnQueue.LocalClient.ReplicationTransferList(params)
		if dpnResp.Error != nil {
			dpnQueue.err("Error getting ReplicationTransfers from local node: %v", dpnResp.Error)
			break
		}
		xfers := dpnResp.ReplicationTransfers()
		for _, xfer := range xfers {
			queueItem := models.NewQueueItem(xfer.ReplicationId)
			dpnWorkItem := dpnQueue.getOrCreateWorkItem(xfer.ReplicationId, constants.DPNTaskReplication)
			queueItem.ItemId = dpnWorkItem.Id
			if dpnWorkItem.QueuedAt.IsZero() {
				dpnQueue.queueReplication(dpnWorkItem, xfer)
			}
			queueItem.QueuedAt = *dpnWorkItem.QueuedAt
			dpnQueue.QueueResult.AddReplication(queueItem)
		}
		if dpnResp.Next == nil {
			break
		} else {
			pageNumber += 1
			params = dpnQueue.replicationParams(pageNumber)
		}
	}
}

// replicationParams returns the URL parameters we need to query our local
// DPN REST server for ReplicationTransfer requests that we will need to
// service.
func (dpnQueue *DPNQueue) replicationParams(pageNumber int) (url.Values) {
	params := url.Values{}
	params.Set("after", dpnQueue.ExamineItemsSince.Format(time.RFC3339))
	params.Set("to_node", dpnQueue.Context.Config.DPN.LocalNode)
	params.Set("cancelled", "false")
	params.Set("stored", "false")
	params.Set("store_requested", "false")
	params.Set("order_by", "updated_at")
	params.Set("page_size", "100")
	params.Set("page", strconv.Itoa(pageNumber))
	return params
}

// queueReplication adds a ReplicationTransfer to NSQ and records info about
// when the item was queued in DPNWorkItem.QueuedAt, which is saved to Pharos.
func (dpnQueue *DPNQueue) queueReplication(dpnWorkItem *apt_models.DPNWorkItem, xfer *models.ReplicationTransfer) {
	err := dpnQueue.Context.NSQClient.Enqueue(
		dpnQueue.Context.Config.DPN.DPNCopyWorker.NsqTopic,
		dpnWorkItem.Id)
	if err != nil {
		dpnQueue.err("Error getting DPNWorkItemList from Pharos: %v", err)
	} else {
		*dpnWorkItem.QueuedAt = time.Now().UTC()
		resp := dpnQueue.Context.PharosClient.DPNWorkItemSave(dpnWorkItem)
		if resp.Error != nil {
			dpnQueue.err("Error updating DPNWorkItem for ReplicationXfer %s from %s: %v",
				xfer.ReplicationId, xfer.FromNode, resp.Error)
			return
		}
		dpnWorkItem = resp.DPNWorkItem()
	}
}

/***************************************************************************
 RestoreTransfer Methods
***************************************************************************/

// queueRestoreRequests collects RestoreTransfer requests from
// the local DPN server and if necessary 1) creates a DPNWorkItem record
// in our Pharos server for the replication request, and 2) creates an
// entry in NSQ telling our replication workers to copy the bag.
//
// We query our local DPN node after synching data from other nodes, and
// we're looking for RestoreTransfers where the from_node is our node.
// We want to skip transfers that are cancelled or already finished. We
// also want to skip transfers where accepted is true, because
// those transfers are already in progress.
func (dpnQueue *DPNQueue) queueRestoreRequests() {
	pageNumber := 1
	params := dpnQueue.restoreParams(pageNumber)
	for {
		dpnResp := dpnQueue.LocalClient.RestoreTransferList(params)
		if dpnResp.Error != nil {
			dpnQueue.err("Error getting RestoreTransfers from local node: %v", dpnResp.Error)
			break
		}
		xfers := dpnResp.RestoreTransfers()
		for _, xfer := range xfers {
			queueItem := models.NewQueueItem(xfer.RestoreId)
			dpnWorkItem := dpnQueue.getOrCreateWorkItem(xfer.RestoreId, constants.DPNTaskRestore)
			queueItem.ItemId = dpnWorkItem.Id
			if dpnWorkItem.QueuedAt.IsZero() {
				dpnQueue.queueRestore(dpnWorkItem, xfer)
			}
			queueItem.QueuedAt = *dpnWorkItem.QueuedAt
			dpnQueue.QueueResult.AddRestore(queueItem)
		}
		if dpnResp.Next == nil {
			break
		} else {
			pageNumber += 1
			params = dpnQueue.restoreParams(pageNumber)
		}
	}
}

// restoreParams returns the URL parameters we need to query our local
// DPN REST server for RestoreTransfer requests that we will need to
// service.
func (dpnQueue *DPNQueue) restoreParams(pageNumber int) (url.Values) {
	params := url.Values{}
	params.Set("after", dpnQueue.ExamineItemsSince.Format(time.RFC3339))
	params.Set("from_node", dpnQueue.Context.Config.DPN.LocalNode)
	params.Set("cancelled", "false")
	params.Set("finished", "false")
	params.Set("accepted", "false")
	params.Set("order_by", "updated_at")
	params.Set("page_size", "100")
	params.Set("page", strconv.Itoa(pageNumber))
	return params
}

// queueRestore adds a RestoreTransfer to NSQ and records info about
// when the item was queued in DPNWorkItem.QueuedAt, which is saved to Pharos.
func (dpnQueue *DPNQueue) queueRestore(dpnWorkItem *apt_models.DPNWorkItem, xfer *models.RestoreTransfer) {
	err := dpnQueue.Context.NSQClient.Enqueue(
		dpnQueue.Context.Config.DPN.DPNRestoreWorker.NsqTopic,
		dpnWorkItem.Id)
	if err != nil {
		dpnQueue.err("Error getting DPNWorkItemList from Pharos: %v", err)
	} else {
		*dpnWorkItem.QueuedAt = time.Now().UTC()
		resp := dpnQueue.Context.PharosClient.DPNWorkItemSave(dpnWorkItem)
		if resp.Error != nil {
			dpnQueue.err("Error updating DPNWorkItem for RestoreXfer %s to %s: %v",
				xfer.RestoreId, xfer.ToNode, resp.Error)
			return
		}
		dpnWorkItem = resp.DPNWorkItem()
	}
}

/***************************************************************************
 Ingest Methods
***************************************************************************/

// queueIngestRequests queues ingest requests that are sitting in Pharos'
// WorkItems table. Users request DPN ingest directly in Pharos, and that's
// the only place those requests exist. (You won't find them in the DPN REST
// server.) We want to queue DPN ingest requests that have no QueuedAt timestamp.
// The DPN REST server will have no record of these items until we finish
// ingesting them into DPN.
func (dpnQueue *DPNQueue) queueIngestRequests() {

}

// ingestParams returns the URL params we need to query Pharos about
// unserviced DPN Ingest requests. These are WorkItems where the action
// is "DPN" and the queued_at timestamp is empty.
func (dpnQueue *DPNQueue) ingestParams(pageNumber int) (url.Values) {
	return nil
}

// queueIngest adds the id of a DPN Ingest WorkItem to NSQ and records info about
// when the item was queued in WorkItem.QueuedAt, which is saved to Pharos.
func (dpnQueue *DPNQueue) queueIngest(workItem *apt_models.WorkItem) {

}

/***************************************************************************
 Misc Utility Methods
***************************************************************************/

// getOrCreateWorkItem returns the DPNWorkItem for the specified
// replication/restore transfer from Pharos. If no DPNWorkItem for the specified
// transfer exists, this creates it in Pharos and returns a copy of it.
func (dpnQueue *DPNQueue) getOrCreateWorkItem(identifier, taskType string) (*apt_models.DPNWorkItem) {
	params := url.Values{}
	params.Set("identifier", identifier)
	params.Set("task", taskType)
	getResp := dpnQueue.Context.PharosClient.DPNWorkItemList(params)
	if getResp.Error != nil {
		dpnQueue.err("Error getting DPNWorkItemList from Pharos: %v", getResp.Error)
		return nil
	}
	existingItem := getResp.DPNWorkItem()
	if existingItem != nil {
		return existingItem
	} else {
		dpnWorkItem := &apt_models.DPNWorkItem{
			Task: taskType,
			Identifier: identifier,
		}
		createResp := dpnQueue.Context.PharosClient.DPNWorkItemSave(dpnWorkItem)
		if createResp.Error != nil {
			dpnQueue.err("Error creating DPNWorkItem for %s Xfer %s: %v",
				taskType, identifier, getResp.Error)
			return nil
		}
		newItem := createResp.DPNWorkItem()
		if newItem == nil {
			dpnQueue.err("DPNWorkItemSave returned nil for %s Xfer %s: %v",
				taskType, identifier, getResp.Error)
		}
		return newItem
	}
}

// err logs an error and adds it to the QueueResult.Errors list.
func (dpnQueue *DPNQueue) err(format string, a ...interface{}) {
	errMsg := fmt.Sprintf(format, a...)
	dpnQueue.Context.MessageLog.Error(errMsg)
	dpnQueue.QueueResult.AddError(errMsg)
}

// logJsonResults dumps the results of this queue run to a machine-readable
// JSON file. This is used in integration post tests to verify that certain
// items were processed, and can be used in production to run automated
// audits and spot checks.
func (dpnQueue *DPNQueue) logJsonResults() {
	jsonData, err := json.MarshalIndent(dpnQueue.QueueResult, "", "  ")
	if err != nil {
		dpnQueue.Context.MessageLog.Warning("Could not dump QueueResult to JSON log: %v", err)
	} else {
		dpnQueue.Context.JsonLog.Println(jsonData)
	}
}
