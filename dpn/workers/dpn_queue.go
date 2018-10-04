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
	"strings"
	"time"
)

// As we're still in the probationary period, limit the number
// fixity checks queued on each run.
const MAX_FIXITY_CHECKS_PER_RUN = 5

// DPNQueue queues DPN ingest requests (found in the Pharos WorkItems table)
// and DPN replication requests (found in the Pharos DPNWorkItems table).
// These items will go into the proper NSQ topics for DPN ingest or
// replication.
type DPNQueue struct {
	// LocalClient is the DPN REST client that talks to our own
	// local DPN REST server.
	LocalClient *network.DPNRestClient
	// RemoteNodes is a map of remote nodes. Key is the namespace
	// and value is the node.
	RemoteNodes map[string]*models.Node
	// RemoteClients is a collection of clients that talk to the
	// DPN REST servers on other nodes. The key is the namespace
	// of the remote node, and the value is the client that talks
	// to that node.
	RemoteClients map[string]*network.DPNRestClient
	// Context provides access to information about our environment
	// and config settings, and access to basic services like
	// logging and a Pharos client.
	Context *context.Context
	// ExamineItemsSince is a timestamp. We will examine any items
	// updated since this timestamp to see if they need to be queued.
	ExamineItemsSince time.Time
	// QueueResult contains information about which items were
	// queued during this run of the program.
	QueueResult *models.QueueResult
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
		message := fmt.Sprintf("Error creating local DPN REST client: %v", err)
		_context.MessageLog.Error(message)
		return nil, fmt.Errorf(message)
	}
	remoteClients, err := localClient.GetRemoteClients()
	if err != nil {
		message := fmt.Sprintf("Error creating remote DPN REST client: %v", err)
		_context.MessageLog.Error(message)
		return nil, fmt.Errorf(message)
	}
	sinceWhen := time.Now().UTC().Add(time.Duration(-1*hours) * time.Hour)
	_context.MessageLog.Info("Checking records since %d hours ago (%s)",
		hours, sinceWhen.Format(time.RFC3339))
	dpnQueue := DPNQueue{
		LocalClient:       localClient,
		RemoteNodes:       make(map[string]*models.Node),
		RemoteClients:     remoteClients,
		Context:           _context,
		ExamineItemsSince: sinceWhen,
		QueueResult:       models.NewQueueResult(),
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
	// *********************************************
	// A.D. 2018-10-04: Stop queuing until we figure
	// out what NSQ is doing with requeues.
	// *********************************************
	// dpnQueue.queueItemsNeedingFixity()
	dpnQueue.QueueResult.EndTime = time.Now().UTC()
	dpnQueue.logResults()
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
			dpnWorkItem := dpnQueue.getOrCreateWorkItem(xfer.ReplicationId, xfer.FromNode,
				constants.DPNTaskReplication)
			if dpnWorkItem == nil {
				dpnQueue.Context.MessageLog.Error("Count not create DPNWorkItem for replication %s from %s",
					xfer.ReplicationId, xfer.FromNode)
				continue
			}
			queueItem.ItemId = dpnWorkItem.Id
			if dpnWorkItem.QueuedAt == nil || dpnWorkItem.QueuedAt.IsZero() {
				dpnQueue.queueDPNWorkItem(dpnWorkItem, constants.DPNTaskReplication)
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
func (dpnQueue *DPNQueue) replicationParams(pageNumber int) url.Values {
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
			dpnWorkItem := dpnQueue.getOrCreateWorkItem(xfer.RestoreId, xfer.ToNode, constants.DPNTaskRestore)
			queueItem.ItemId = dpnWorkItem.Id
			if dpnWorkItem.QueuedAt == nil || dpnWorkItem.QueuedAt.IsZero() {
				dpnQueue.queueDPNWorkItem(dpnWorkItem, constants.DPNTaskRestore)
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
func (dpnQueue *DPNQueue) restoreParams(pageNumber int) url.Values {
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
	pageNumber := 1
	params := dpnQueue.ingestParams(pageNumber)
	for {
		resp := dpnQueue.Context.PharosClient.WorkItemList(params)
		if resp.Error != nil {
			dpnQueue.err("Error getting ingest WorkItems from Pharos: %v", resp.Error)
			break
		}
		ingestItems := resp.WorkItems()
		for _, ingestItem := range ingestItems {
			queueItem := models.NewQueueItem(ingestItem.ObjectIdentifier)
			queueItem.ItemId = ingestItem.Id
			if ingestItem.QueuedAt == nil || ingestItem.QueuedAt.IsZero() {
				dpnQueue.queueIngest(ingestItem)
			}
			queueItem.QueuedAt = *ingestItem.QueuedAt
			dpnQueue.QueueResult.AddIngest(queueItem)
		}
		if resp.Next == nil {
			break
		} else {
			pageNumber += 1
			params = dpnQueue.ingestParams(pageNumber)
		}
	}
}

// ingestParams returns the URL params we need to query Pharos about
// unserviced DPN Ingest requests. These are WorkItems where the action
// is "DPN" and the queued_at timestamp is empty.
func (dpnQueue *DPNQueue) ingestParams(pageNumber int) url.Values {
	params := url.Values{}
	params.Set("item_action", constants.ActionDPN)
	params.Set("queued", "false")
	params.Set("sort", "updated_at")
	params.Set("per_page", "100")
	params.Set("created_after", dpnQueue.ExamineItemsSince.Format(time.RFC3339))
	params.Set("page", strconv.Itoa(pageNumber))
	return params
}

// queueIngest adds the id of a DPN Ingest WorkItem to NSQ and records info about
// when the item was queued in WorkItem.QueuedAt, which is saved to Pharos.
func (dpnQueue *DPNQueue) queueIngest(workItem *apt_models.WorkItem) {
	// Put the item into NSQ. First step for DPN ingest is DPNPackage.
	err := dpnQueue.Context.NSQClient.Enqueue(
		dpnQueue.Context.Config.DPN.DPNPackageWorker.NsqTopic,
		workItem.Id)
	if err != nil {
		dpnQueue.err("Error queueing DPNWorkItem %d for ingest: %v", workItem.Id, err)
	} else {
		// Let Pharos know this item has been queued
		utcNow := time.Now().UTC()
		workItem.QueuedAt = &utcNow
		resp := dpnQueue.Context.PharosClient.WorkItemSave(workItem)
		if resp.Error != nil {
			dpnQueue.err("Error updating WorkItem %d for ingest: %v",
				workItem.Id, resp.Error)
			return
		}
		workItem = resp.WorkItem()
	}
}

/***************************************************************************
 Fixity Methods
***************************************************************************/
func (dpnQueue *DPNQueue) queueItemsNeedingFixity() {
	if strings.Contains(dpnQueue.Context.Config.PharosURL, "demo.aptrust.org") {
		dpnQueue.Context.MessageLog.Info("NOT queuing DPN fixity items because we're running on demo.")
		return
	}
	pageNumber := 1
	params := dpnQueue.fixityBagParams(pageNumber)
	for {
		dpnResp := dpnQueue.LocalClient.DPNBagList(params)
		if dpnResp.Error != nil {
			dpnQueue.err("Error getting Bags from local node: %v", dpnResp.Error)
			break
		}
		bags := dpnResp.Bags()
		for _, bag := range bags {
			// See if our node has run a fixity check on this
			// bag in the past two years.
			fixityParams := dpnQueue.fixityCheckParams(bag.UUID)
			fixityResp := dpnQueue.LocalClient.FixityCheckList(fixityParams)
			if fixityResp.Error != nil {
				dpnQueue.err("Error getting FixityCheck for bag %s from local node: %v",
					bag.UUID, fixityResp.Error)
				continue
			}
			if fixityResp.FixityCheck() == nil {
				// Our node has not done a check in two years.
				// Queue this for checking now.
				queueItem := models.NewQueueItem(bag.UUID)
				dpnWorkItem := dpnQueue.getOrCreateWorkItem(bag.UUID, "", constants.DPNTaskFixity)
				queueItem.ItemId = dpnWorkItem.Id
				if dpnWorkItem.QueuedAt == nil || dpnWorkItem.QueuedAt.IsZero() {
					dpnQueue.queueDPNWorkItem(dpnWorkItem, constants.DPNTaskFixity)
					queueItem.QueuedAt = *dpnWorkItem.QueuedAt
					dpnQueue.QueueResult.AddFixity(queueItem)
				} else {
					// This item already exists in Pharos
					dpnQueue.Context.MessageLog.Info("Skipping: Bag %s is already queued for fixity check",
						bag.UUID)
					continue
				}
			}
			if len(dpnQueue.QueueResult.Fixities) >= MAX_FIXITY_CHECKS_PER_RUN {
				break
			}
		}
		if dpnResp.Next == nil {
			dpnQueue.Context.MessageLog.Info("Reached end of fixity results from DPN REST server")
			break
		} else if len(dpnQueue.QueueResult.Fixities) >= MAX_FIXITY_CHECKS_PER_RUN {
			dpnQueue.Context.MessageLog.Info("Queued max %d fixity checks for this run",
				MAX_FIXITY_CHECKS_PER_RUN)
			break
		} else {
			pageNumber += 1
			params = dpnQueue.fixityBagParams(pageNumber)
		}
	}
}

func (dpnQueue *DPNQueue) fixityBagParams(pageNumber int) url.Values {
	params := url.Values{}
	twoYearsAgo := time.Now().UTC().AddDate(-2, 0, 0)
	params.Set("before", twoYearsAgo.Format(time.RFC3339))
	params.Set("order_by", "created_at")
	params.Set("page_size", "25")
	params.Set("page", strconv.Itoa(pageNumber))
	return params
}

func (dpnQueue *DPNQueue) fixityCheckParams(bagUUID string) url.Values {
	params := url.Values{}
	twoYearsAgo := time.Now().UTC().AddDate(-2, 0, 0)
	params.Set("after", twoYearsAgo.Format(time.RFC3339))
	params.Set("bag", bagUUID)
	params.Set("order_by", "created_at")
	params.Set("node", dpnQueue.Context.Config.DPN.LocalNode)
	params.Set("page_size", "1")
	return params
}

/***************************************************************************
 Misc Utility Methods
***************************************************************************/

// queueDPNWorkItem adds a transfer task to NSQ and records info about
// when the item was queued in DPNWorkItem.QueuedAt, which is saved to Pharos.
func (dpnQueue *DPNQueue) queueDPNWorkItem(dpnWorkItem *apt_models.DPNWorkItem, taskType string) {
	queueTopic := ""
	if taskType == constants.DPNTaskReplication {
		// Copy is first step of replication
		queueTopic = dpnQueue.Context.Config.DPN.DPNCopyWorker.NsqTopic
	} else if taskType == constants.DPNTaskRestore {
		queueTopic = dpnQueue.Context.Config.DPN.DPNRestoreWorker.NsqTopic
	} else if taskType == constants.DPNTaskFixity {
		queueTopic = dpnQueue.Context.Config.DPN.DPNGlacierRestoreWorker.NsqTopic
	} else {
		dpnQueue.Context.MessageLog.Error("Illegal taskType '%s'", taskType)
		return
	}
	// Put the item into NSQ
	err := dpnQueue.Context.NSQClient.Enqueue(queueTopic, dpnWorkItem.Id)
	if err != nil {
		dpnQueue.err("Error queueing DPNWorkItem %d, %s %s: %v",
			dpnWorkItem.Id, taskType, dpnWorkItem.Identifier, err)
	} else {
		// Let Pharos know this item has been queued
		dpnQueue.Context.MessageLog.Info("Added %s %s (DPNWorkItem %d) to NSQ topic %s",
			taskType, dpnWorkItem.Identifier, dpnWorkItem.Id, queueTopic)
		utcNow := time.Now().UTC()
		dpnWorkItem.QueuedAt = &utcNow
		resp := dpnQueue.Context.PharosClient.DPNWorkItemSave(dpnWorkItem)
		if resp.Error != nil {
			dpnQueue.err("Error marking DPNWorkItem %d for %s %s as queued: %v",
				dpnWorkItem.Id, taskType, dpnWorkItem.Identifier, resp.Error)
			return
		}
		dpnWorkItem = resp.DPNWorkItem()
		dpnQueue.Context.MessageLog.Info("Set QueuedAt for %s %s (DPNWorkItem %d) to %s",
			taskType, dpnWorkItem.Identifier, dpnWorkItem.Id,
			dpnWorkItem.QueuedAt.Format(time.RFC3339))
	}
}

// getOrCreateWorkItem returns the DPNWorkItem for the specified
// replication/restore transfer from Pharos. If no DPNWorkItem for
// the specified transfer exists, this creates it in Pharos and
// returns a copy of it.
//
// This code is cluttered with logging to help diagnose issues in
// integration tests.
func (dpnQueue *DPNQueue) getOrCreateWorkItem(identifier, remoteNode, taskType string) *apt_models.DPNWorkItem {
	params := url.Values{}
	params.Set("identifier", identifier)
	params.Set("task", taskType)
	if taskType == constants.DPNTaskFixity {
		twoYearsAgo := time.Now().UTC().AddDate(-2, 0, 0)
		params.Set("queued_after", twoYearsAgo.Format(time.RFC3339))
	}
	getResp := dpnQueue.Context.PharosClient.DPNWorkItemList(params)
	if getResp.Error != nil {
		dpnQueue.err("Error getting DPNWorkItemList from Pharos: %v", getResp.Error)
		return nil
	}
	existingItem := getResp.DPNWorkItem()
	if existingItem != nil {
		queuedAt := "[never]"
		if existingItem.QueuedAt != nil {
			queuedAt = existingItem.QueuedAt.Format(time.RFC3339)
		}
		dpnQueue.Context.MessageLog.Info("Found DPNWorkItem %d for %s %s with QueuedAt = %s",
			existingItem.Id, taskType, existingItem.Identifier, queuedAt)
		return existingItem
	}
	// If we get this far, there's no existing item, so we have to create one.
	dpnWorkItem := &apt_models.DPNWorkItem{
		Task:       taskType,
		Identifier: identifier,
		RemoteNode: remoteNode,
		QueuedAt:   nil,
		Status:     constants.StatusPending,
		Stage:      constants.StageRequested,
		Retry:      true,
	}
	createResp := dpnQueue.Context.PharosClient.DPNWorkItemSave(dpnWorkItem)
	if createResp.Error != nil {
		dpnQueue.err("Error creating DPNWorkItem for %s Xfer %s: %v",
			taskType, identifier, createResp.Error)
		return nil
	}
	newItem := createResp.DPNWorkItem()
	if newItem == nil {
		dpnQueue.err("DPNWorkItemSave returned nil for %s Xfer %s",
			taskType, identifier)
	} else {
		queuedAt := "[never]"
		if newItem.QueuedAt != nil {
			queuedAt = newItem.QueuedAt.Format(time.RFC3339)
		}
		dpnQueue.Context.MessageLog.Info("Created DPNWorkItem %d for %s %s with QueuedAt = %s",
			newItem.Id, taskType, newItem.Identifier, queuedAt)
	}
	return newItem
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
// audits and spot checks. Also dumps a very brief summary to the application
// log.
func (dpnQueue *DPNQueue) logResults() {
	jsonData, err := json.MarshalIndent(dpnQueue.QueueResult, "", "  ")
	if err != nil {
		dpnQueue.Context.MessageLog.Warning("Could not dump QueueResult to JSON log: %v", err)
	} else {
		dpnQueue.Context.JsonLog.Println(string(jsonData))
	}
	dpnQueue.Context.MessageLog.Info("Processed %d replication requests", len(dpnQueue.QueueResult.Replications))
	dpnQueue.Context.MessageLog.Info("Processed %d restore requests", len(dpnQueue.QueueResult.Restores))
	dpnQueue.Context.MessageLog.Info("Processed %d ingest requests", len(dpnQueue.QueueResult.Ingests))
	dpnQueue.Context.MessageLog.Info("Processed %d items needing fixity", len(dpnQueue.QueueResult.Fixities))
}
