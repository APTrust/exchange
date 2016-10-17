package dpn

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	"net/url"
	"strconv"
	"time"
)

// SYNC_BATCH_SIZE describes how many records should request
// per page from remote nodes when we're synching bags,
// replication requests, etc.
const SYNC_BATCH_SIZE = 50

type DPNSync struct {
	// LocalClient is the DPN REST client that talks to our own
	// local DPN REST server.
	LocalClient    *DPNRestClient
	// RemoteClients is a collection of clients that talk to the
	// DPN REST servers on other nodes. The key is the namespace
	// of the remote node, and the value is the client that talks
	// to that node.
	RemoteClients  map[string]*DPNRestClient
	// Context provides access to information about our environment
	// and config settings, and access to basic services like
	// logging and a Pharos client.
	Context        *context.Context
}

// SyncResult describes the result of an operation where we pull
// info about all updated bags, replication requests and restore
// requests from a remote node and copy that data into our own
// local DPN registry.
type SyncResult struct {
	// RemoteNode is the node we are pulling information from.
	RemoteNode            *Node
	// Bags is a list of bags successfully synched.
	Bags                  []*DPNBag
	// ReplicationTransfers successfully synched.
	ReplicationTransfers  []*ReplicationTransfer
	// RestoreTransfers successfully synched.
	RestoreTransfers      []*RestoreTransfer
	// BagSyncError contains the error (if any) that occurred
	// during the bag sync process. The first error will stop
	// the synching of all subsquent bags.
	BagSyncError          error
	// ReplicationSyncError contains the error (if any) that occurred
	// during the synching of Replication Transfers. The first error
	// will stop the synching of all subsquent replication requests.
	ReplicationSyncError  error
	// RestoreSyncError contains the error (if any) that occurred
	// during the synching of Restore Transfers. The first error
	// will stop the synching of all subsquent restore requests.
	RestoreSyncError      error
}

// HasSyncErrors returns true if any errors occurred during the synching
// of bags, replication transfers or restore transfers.
func (syncResult *SyncResult) HasSyncErrors() (bool) {
	return (syncResult.BagSyncError != nil ||
		syncResult.ReplicationSyncError != nil ||
		syncResult.RestoreSyncError != nil)
}

// NewDPNSync creates a new DPNSync object.
func NewDPNSync(_context *context.Context) (*DPNSync, error) {
	if _context == nil {
		return nil, fmt.Errorf("Param _context cannot be nil.")
	}
	localClient, err := NewDPNRestClient(
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
	sync := DPNSync{
		LocalClient: localClient,
		RemoteClients: remoteClients,
		Context: _context,
	}
	return &sync, nil
}

// Run runs all sync operations against all nodes. This is the only function
// your cron job needs to call.
func (dpnSync *DPNSync) Run() (error) {
	nodes, err := dpnSync.GetAllNodes()
	if err != nil {
		return fmt.Errorf("Error getting node info. Nothing synched. %v", err)
	}
	for _, node := range nodes {
		if node.Namespace != dpnSync.LocalNodeName() {
			result := dpnSync.SyncEverythingFromNode(node)
			dpnSync.PrintAndLogResult(result)
		}
	}
	return nil
}

// GetAllNodes returns a list of all the nodes that our node knows about.
func (dpnSync *DPNSync) GetAllNodes()([]*Node, error) {
	result := dpnSync.LocalClient.NodeList(nil)
	if result.Error != nil {
		return nil, result.Error
	}
	return result.Nodes(), nil
}

// LocalNodeName returns the namespace of our local DPN node.
func (dpnSync *DPNSync) LocalNodeName() (string) {
	return dpnSync.LocalClient.Node
}

// RemoteNodeNames returns the namespaces of all known remote
// DPN nodes.
func (dpnSync *DPNSync) RemoteNodeNames() ([]string) {
	remoteNodeNames := make([]string, 0)
	for namespace := range dpnSync.RemoteClients {
		remoteNodeNames = append(remoteNodeNames, namespace)
	}
	return remoteNodeNames
}

// SyncEverythingFromNode syncs all bags, replication requests
// and restore requests from the specified remote node. Note that
// this is a pull-only sync.We are not writing any data to other
// nodes, just reading what they have and updating our own registry
// with their info.
func (dpnSync *DPNSync) SyncEverythingFromNode(remoteNode *Node) (*SyncResult) {
	syncResult := &SyncResult {
		RemoteNode: remoteNode,
	}

	bags, err := dpnSync.SyncBags(remoteNode)
	syncResult.Bags = bags
	syncResult.BagSyncError = err

	replXfers, err := dpnSync.SyncReplicationRequests(remoteNode)
	syncResult.ReplicationTransfers = replXfers
	syncResult.ReplicationSyncError = err

	restoreXfers, err := dpnSync.SyncRestoreRequests(remoteNode)
	syncResult.RestoreTransfers = restoreXfers
	syncResult.RestoreSyncError = err

	return syncResult
}

// SyncBags syncs bags from the specified node to our own local DPN
// registry if the bags match these critieria:
//
// 1. The node we are querying is the admin node for the bag.
// 2. The bag was updated since the last time we queried the node.
//
// Returns a list of the bags that were successfully updated.
// Even on error, this may still return a list with whatever bags
// were updated before the error occurred.
func (dpnSync *DPNSync) SyncBags(remoteNode *Node) ([]*DPNBag, error) {
	pageNumber := 1
	bagsProcessed := make([]*DPNBag, 0)
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.Context.MessageLog.Error("Skipping bag sync for node %s: REST client is nil",
			remoteNode.Namespace)
		return bagsProcessed, fmt.Errorf("No client available for node %s",
			remoteNode.Namespace)
	}
	for {
		dpnSync.Context.MessageLog.Debug("Getting page %d of bags from %s",
			pageNumber, remoteNode.Namespace)
		resp := dpnSync.getBags(remoteClient, remoteNode, pageNumber)
		if resp.Error != nil {
			return bagsProcessed, resp.Error
		}
		dpnSync.Context.MessageLog.Debug("Got %d bags from %s", resp.Count,
			remoteNode.Namespace)
		processed, err := dpnSync.syncBags(resp.Bags())
		if err != nil {
			return bagsProcessed, err
		}
		bagsProcessed = append(bagsProcessed, processed...)
		if resp.Next == nil || *resp.Next == "" {
			dpnSync.Context.MessageLog.Debug("No more bags to get from %s",
				remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	dpnSync.Context.MessageLog.Debug("Processed %d bags from remote node %s",
		len(bagsProcessed), remoteNode.Namespace)
	return bagsProcessed, nil
}

func (dpnSync *DPNSync) syncBags(bags []*DPNBag) ([]*DPNBag, error) {
	bagsProcessed := make([]*DPNBag, 0)
	for _, bag := range(bags) {
		dpnSync.Context.MessageLog.Debug("Processing bag %s from %s", bag.UUID, bag.AdminNode)
		resp := dpnSync.LocalClient.DPNBagGet(bag.UUID)
		existingBag := resp.Bag()
		err := resp.Error
		if existingBag == nil {
			dpnSync.Context.MessageLog.Debug("Bag %s does not exist", bag.UUID)
		} else {
			dpnSync.Context.MessageLog.Debug("%v", existingBag)
		}
		var processedBag *DPNBag
		if existingBag != nil {
			if !existingBag.UpdatedAt.Before(bag.UpdatedAt) {
				dpnSync.Context.MessageLog.Debug("Not updating bag %s, because timestamp is not newer: " +
					"Remote updated_at = %s, Local updated_at = %s", bag.UUID,
					bag.UpdatedAt, existingBag.UpdatedAt)
				continue
			} else {
				dpnSync.Context.MessageLog.Debug("Bag %s exists... updating", bag.UUID)
				resp = dpnSync.LocalClient.DPNBagUpdate(bag)
				processedBag = resp.Bag()
				err = resp.Error
			}
		} else {  // New bag
			dpnSync.Context.MessageLog.Debug("Bag %s not in local registry... creating", bag.UUID)
			resp = dpnSync.LocalClient.DPNBagCreate(bag)
			processedBag = resp.Bag()
			err = resp.Error
		}
		if err != nil {
			dpnSync.Context.MessageLog.Error("Oops! Bag %s: %v", bag.UUID, err)
			dpnSync.Context.MessageLog.Error("%s %s", resp.Request.Method, resp.Request.URL.String())
			dpnSync.Context.MessageLog.Error("Status Code: %d", resp.Response.StatusCode)
			return bagsProcessed, err
		}
		bagsProcessed = append(bagsProcessed, processedBag)
	}
	return bagsProcessed, nil
}

func (dpnSync *DPNSync) getBags(remoteClient *DPNRestClient, remoteNode *Node, pageNumber int) (*DPNResponse) {
	// We want to get all bags updated since the last time we pulled
	// from this node, and only those bags for which the node we're
	// querying is the admin node.
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("admin_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	params.Set("per_page", strconv.Itoa(SYNC_BATCH_SIZE))
	return remoteClient.DPNBagList(&params)
}

// SyncReplicationRequests copies ReplicationTransfer records from
// remote nodes to our own local node.
func (dpnSync *DPNSync) SyncReplicationRequests(remoteNode *Node) ([]*ReplicationTransfer, error) {
	xfersProcessed := make([]*ReplicationTransfer, 0)
	pageNumber := 1
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.Context.MessageLog.Error(
			"Skipping replication sync for node %s: REST client is nil",
			remoteNode.Namespace)
		return xfersProcessed, fmt.Errorf("No client available for node %s",
			remoteNode.Namespace)
	}
	for {
		dpnSync.Context.MessageLog.Debug("Getting page %d of replication requests from %s",
			pageNumber, remoteNode.Namespace)
		resp := dpnSync.getReplicationRequests(remoteClient, remoteNode, pageNumber)
		if resp.Error != nil {
			return xfersProcessed, resp.Error
		}
		dpnSync.Context.MessageLog.Debug("Got %d replication requests from %s",
			resp.Count, remoteNode.Namespace)
		updated, err := dpnSync.syncReplicationRequests(resp.ReplicationTransfers())
		if err != nil {
			return xfersProcessed, err
		}
		xfersProcessed = append(xfersProcessed, updated...)
		if resp.Next == nil || *resp.Next == "" {
			dpnSync.Context.MessageLog.Debug("No more replication requests to get from %s",
				remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	dpnSync.Context.MessageLog.Debug("Processed %d replication requests from node %s",
		len(xfersProcessed), remoteNode.Namespace)
	return xfersProcessed, nil
}

func (dpnSync *DPNSync) syncReplicationRequests(xfers []*ReplicationTransfer) ([]*ReplicationTransfer, error) {
	xfersProcessed := make([]*ReplicationTransfer, 0)
	for _, xfer := range(xfers) {
		dpnSync.Context.MessageLog.Debug("Processing transfer %s in local registry", xfer.ReplicationId)
		resp := dpnSync.LocalClient.ReplicationTransferGet(xfer.ReplicationId)
		existingXfer := resp.ReplicationTransfer()
		err := resp.Error
		var updatedXfer *ReplicationTransfer
		if existingXfer != nil {
			if !existingXfer.UpdatedAt.Before(xfer.UpdatedAt) {
				dpnSync.Context.MessageLog.Debug("Not updating replication request %s, " +
					"because timestamp is not newer: " +
					"Remote updated_at = %s, Local updated_at = %s",
					xfer.ReplicationId, xfer.UpdatedAt, existingXfer.UpdatedAt)
			} else {
				dpnSync.Context.MessageLog.Debug("Replication request %s exists... updating",
					xfer.ReplicationId)
				resp = dpnSync.LocalClient.ReplicationTransferUpdate(xfer)
				updatedXfer = resp.ReplicationTransfer()
				err = resp.Error
			}
		} else {
			dpnSync.Context.MessageLog.Debug("Replication request %s not in local registry... creating",
				xfer.ReplicationId)
			resp = dpnSync.LocalClient.ReplicationTransferCreate(xfer)
			updatedXfer = resp.ReplicationTransfer()
			err = resp.Error
		}
		if err != nil {
			dpnSync.Context.MessageLog.Debug("Oops! Replication request %s: %v", xfer.ReplicationId, err)
			return xfersProcessed, err
		}
		xfersProcessed = append(xfersProcessed, updatedXfer)
	}
	return xfersProcessed, nil
}

func (dpnSync *DPNSync) getReplicationRequests(remoteClient *DPNRestClient, remoteNode *Node, pageNumber int) (*DPNResponse) {
	// Get requests updated since the last time we pulled
	// from this node, where this node is the from_node.
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("from_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	params.Set("per_page", strconv.Itoa(SYNC_BATCH_SIZE))
	return remoteClient.ReplicationList(&params)
}

// SyncRestoreRequests copies RestoreTransfer records from remote
// nodes to our local node.
func (dpnSync *DPNSync) SyncRestoreRequests(remoteNode *Node) ([]*RestoreTransfer, error) {
	xfersProcessed := make([]*RestoreTransfer, 0)
	pageNumber := 1
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.Context.MessageLog.Error("Skipping restore sync for node %s: REST client is nil",
			remoteNode.Namespace)
		return xfersProcessed, fmt.Errorf("No client available for node %s", remoteNode.Namespace)
	}
	for {
		dpnSync.Context.MessageLog.Debug("Getting page %d of restore requests from %s",
			pageNumber, remoteNode.Namespace)
		resp := dpnSync.getRestoreRequests(remoteClient, remoteNode, pageNumber)
		if resp.Error != nil {
			return xfersProcessed, resp.Error
		}
		dpnSync.Context.MessageLog.Debug("Got %d restore requests from %s",
			resp.Count, remoteNode.Namespace)
		updated, err := dpnSync.syncRestoreRequests(resp.RestoreTransfers())
		if err != nil {
			return xfersProcessed, err
		}
		xfersProcessed = append(xfersProcessed, updated...)
		if resp.Next == nil || *resp.Next == "" {
			dpnSync.Context.MessageLog.Debug("No more restore requests to get from %s",
				remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	dpnSync.Context.MessageLog.Debug("Processed %d restore requests in local registry",
		len(xfersProcessed))
	return xfersProcessed, nil
}

func (dpnSync *DPNSync) syncRestoreRequests(xfers []*RestoreTransfer) ([]*RestoreTransfer, error) {
	xfersProcessed := make([]*RestoreTransfer, 0)
	for _, xfer := range(xfers) {
		dpnSync.Context.MessageLog.Debug("Processing restore transfer %s", xfer.RestoreId)
		resp := dpnSync.LocalClient.RestoreTransferGet(xfer.RestoreId)
		existingXfer := resp.RestoreTransfer()
		err := resp.Error
		var updatedXfer *RestoreTransfer
		if existingXfer != nil {
			if !existingXfer.UpdatedAt.Before(xfer.UpdatedAt) {
				dpnSync.Context.MessageLog.Debug("Not updating restore request %s, " +
					"because timestamp is not newer: " +
					"Remote updated_at = %s, Local updated_at = %s",
					xfer.RestoreId, xfer.UpdatedAt, existingXfer.UpdatedAt)
			} else {
				dpnSync.Context.MessageLog.Debug("Restore request %s exists... updating",
					xfer.RestoreId)
				resp = dpnSync.LocalClient.RestoreTransferUpdate(xfer)
				updatedXfer = resp.RestoreTransfer()
				err = resp.Error
			}
		} else {
			dpnSync.Context.MessageLog.Debug("Restore request %s not in local registry... creating",
				xfer.RestoreId)
			resp = dpnSync.LocalClient.RestoreTransferCreate(xfer)
			updatedXfer = resp.RestoreTransfer()
			err = resp.Error
		}
		if err != nil {
			dpnSync.Context.MessageLog.Debug("Oops! Restore request %s: %v", xfer.RestoreId, err)
			return xfersProcessed, err
		}
		xfersProcessed = append(xfersProcessed, updatedXfer)
	}
	return xfersProcessed, nil
}

func (dpnSync *DPNSync) getRestoreRequests(remoteClient *DPNRestClient, remoteNode *Node, pageNumber int) (*DPNResponse) {
	// Get requests updated since the last time we pulled
	// from this node, where this node is the to_node.
	// E.g. We ask TDR for restore requests going TO TDR.
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("to_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	params.Set("per_page", strconv.Itoa(SYNC_BATCH_SIZE))
	return remoteClient.RestoreTransferList(&params)
}

// PrintAndLogResult logs results to both STDOUT and the application's
// message log.
func (dpnSync *DPNSync) PrintAndLogResult(result *SyncResult) {
	// Result summary
	msg := fmt.Sprintf("From %s:\n%d bags\n%d replications\n%d restores\n" +
		"%d digests\n%d fixity checks\n%d ingests",
		result.RemoteNode.Namespace, len(result.Bags),
		len(result.ReplicationTransfers),
		len(result.RestoreTransfers), 0, 0, 0)

	fmt.Println(msg)
	dpnSync.Context.MessageLog.Info(msg)

	// Error messages
	dpnSync.printAndLogErr("BagSyncError", result.BagSyncError)
	dpnSync.printAndLogErr("ReplicationSyncError", result.ReplicationSyncError)
	dpnSync.printAndLogErr("RestoreSyncError", result.RestoreSyncError)
}

func (dpnSync *DPNSync) printAndLogErr(errName string, err error) {
	if err != nil {
		msg := fmt.Sprintf("%s: %v", errName, err)
		fmt.Println(msg)
		dpnSync.Context.MessageLog.Info(msg)
	}
}
