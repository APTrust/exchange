package workers

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"net/url"
	"os"
	"strconv"
	"time"
)

// SYNC_BATCH_SIZE describes how many records should request
// per page from remote nodes when we're synching bags,
// replication requests, etc.
const SYNC_BATCH_SIZE = 50

// DPNSync copies data from remote DPN nodes to our local DPN node.
// Data includes information about bags, replication transfers, etc.
// Each node is the authority on bags where they are listed as the admin
// node, so when synching from DPN node X, we ask for all bags where
// X is the admin node, as well as all ReplicationTransfers, RestoreTransfers,
// FixityChecks, etc. We do NOT ask node X for info about or related to
// bags whose admin node is Y or Z.
type DPNSync struct {
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
	// Results contains information about the results of the sync
	// operations with each node. Key is the node namespace,
	// value is the SyncResult object for that node.
	Results map[string]*models.SyncResult
}

// NewDPNSync creates a new DPNSync object.
func NewDPNSync(_context *context.Context) (*DPNSync, error) {
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
	results := make(map[string]*models.SyncResult)
	for nodeName := range remoteClients {
		results[nodeName] = models.NewSyncResult(nodeName)
	}
	sync := DPNSync{
		LocalClient:   localClient,
		RemoteNodes:   make(map[string]*models.Node),
		RemoteClients: remoteClients,
		Context:       _context,
		Results:       results,
	}
	return &sync, nil
}

// Run runs all sync operations against all nodes. This is the only function
// your cron job needs to call. The boolean return value will be true if all
// sync operations completed without error, false otherwise. For errors, check
// the log.
func (dpnSync *DPNSync) Run() bool {
	nodes, err := dpnSync.GetAllNodes()
	if err != nil {
		msg := fmt.Sprintf("Error getting node info. Nothing synched. %v", err)
		fmt.Fprintf(os.Stderr, msg)
		dpnSync.Context.MessageLog.Error(msg)
		return false
	}
	hasErrors := false
	for _, node := range nodes {
		if node.Namespace != dpnSync.LocalNodeName() {
			dpnSync.RemoteNodes[node.Namespace] = node
			dpnSync.SyncEverythingFromNode(node)
			if dpnSync.Results[node.Namespace].HasErrors("") {
				hasErrors = true
			} else {
				// Update Node's "last updated" timestamp.
			}
			dpnSync.logResult(dpnSync.Results[node.Namespace])
		}
	}
	return hasErrors
}

// GetAllNodes returns a list of all the nodes that our node knows about.
func (dpnSync *DPNSync) GetAllNodes() ([]*models.Node, error) {
	result := dpnSync.LocalClient.NodeList(nil)
	if result.Error != nil {
		return nil, result.Error
	}
	return result.Nodes(), nil
}

// LocalNodeName returns the namespace of our local DPN node.
func (dpnSync *DPNSync) LocalNodeName() string {
	return dpnSync.LocalClient.Node
}

// RemoteNodeNames returns the namespaces of all known remote
// DPN nodes.
func (dpnSync *DPNSync) RemoteNodeNames() []string {
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
func (dpnSync *DPNSync) SyncEverythingFromNode(remoteNode *models.Node) {
	result := dpnSync.Results[remoteNode.Namespace]

	dpnSync.SyncNode(remoteNode)
	if result.HasErrors("") {
		return
	}

	dpnSync.SyncMembers(remoteNode)
	if result.HasErrors("") {
		return
	}

	dpnSync.SyncBags(remoteNode)
	if result.HasErrors("") {
		return
	}

	dpnSync.SyncDigests(remoteNode)
	if result.HasErrors("") {
		return
	}

	dpnSync.SyncFixities(remoteNode)
	if result.HasErrors("") {
		return
	}

	dpnSync.SyncReplicationRequests(remoteNode)
	if result.HasErrors("") {
		return
	}

	dpnSync.SyncRestoreRequests(remoteNode)
	if result.HasErrors("") {
		return
	}
}

// SyncNode copies the latest node record from the node itself
// to our DPN registry. E.g. It copies the SDR record from SDR
// to us, but only if the remote record is newer.
func (dpnSync *DPNSync) SyncNode(remoteNode *models.Node) {
	log := dpnSync.Context.MessageLog
	result := dpnSync.Results[remoteNode.Namespace]
	// Get latest info from the node about itself
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	log.Info("Fetching %s record from %s", remoteNode.Namespace, remoteNode.Namespace)
	resp := remoteClient.NodeGet(remoteNode.Namespace)
	if resp.Error != nil {
		result.AddError(dpn.DPNTypeNode, resp.Error)
		return
	}
	node := resp.Node()
	if node == nil {
		log.Warning("Node %s has no node record of itself", remoteNode.Namespace)
		return
	} else if node.UpdatedAt.After(remoteNode.UpdatedAt) {
		log.Info("Updating node %s because their record is newer", remoteNode.Namespace)
		resp = dpnSync.LocalClient.NodeUpdate(node)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeNode, resp.Error)
		}
		result.AddToSyncCount(dpn.DPNTypeNode, 1)
	} else {
		log.Info("Our record for %s is up to date.", remoteNode.Namespace)
	}
	result.AddToFetchCount(dpn.DPNTypeNode, 1)
}

// SyncMembers copies remote member records to our own node.
// This does not update existing records, it only creates new ones.
func (dpnSync *DPNSync) SyncMembers(remoteNode *models.Node) {
	pageNumber := 1
	log := dpnSync.Context.MessageLog
	result := dpnSync.Results[remoteNode.Namespace]
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.logNoClient(dpn.DPNTypeMember, remoteNode.Namespace)
		return
	}
	for {
		log.Debug("Getting page %d of members from %s", pageNumber, remoteNode.Namespace)
		resp := dpnSync.getMembers(remoteClient, pageNumber)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeMember, resp.Error)
			break
		}
		result.AddToFetchCount(dpn.DPNTypeMember, resp.Count)
		log.Debug("Got %d members from %s", resp.Count, remoteNode.Namespace)
		dpnSync.syncMembers(resp.Members(), result)
		if result.HasErrors(dpn.DPNTypeMember) {
			break
		}
		if resp.Next == nil || *resp.Next == "" {
			log.Debug("No more members to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	log.Debug("Members from %s: fetched %d, synched %d", remoteNode.Namespace,
		result.FetchCounts[dpn.DPNTypeMember], result.SyncCounts[dpn.DPNTypeMember])
}

func (dpnSync *DPNSync) syncMembers(members []*models.Member, result *models.SyncResult) {
	log := dpnSync.Context.MessageLog
	for _, member := range members {
		resp := dpnSync.LocalClient.MemberGet(member.MemberId)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeMember, resp.Error)
			return
		}
		existingMember := resp.Member()
		if existingMember == nil {
			log.Debug("Creating new member %s (%s)", member.Name, member.MemberId)
			resp = dpnSync.LocalClient.MemberCreate(member)
			if resp.Error != nil {
				result.AddError(dpn.DPNTypeMember, resp.Error)
				return
			}
		}
		result.AddToSyncCount(dpn.DPNTypeMember, 1)
	}
}

func (dpnSync *DPNSync) getMembers(remoteClient *network.DPNRestClient, pageNumber int) *network.DPNResponse {
	remoteNode := dpnSync.RemoteNodes[remoteClient.Node]
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	params.Set("per_page", strconv.Itoa(SYNC_BATCH_SIZE))
	return remoteClient.FixityCheckList(params)
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
func (dpnSync *DPNSync) SyncBags(node *models.Node) {
	pageNumber := 1
	log := dpnSync.Context.MessageLog
	result := dpnSync.Results[node.Namespace]
	remoteClient := dpnSync.RemoteClients[node.Namespace]
	if remoteClient == nil {
		dpnSync.logNoClient(dpn.DPNTypeBag, node.Namespace)
		return
	}
	for {
		log.Debug("Getting page %d of bags from %s", pageNumber, node.Namespace)
		resp := dpnSync.getBags(remoteClient, pageNumber)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeBag, resp.Error)
			break
		}
		result.AddToFetchCount(dpn.DPNTypeBag, resp.Count)
		log.Debug("Got %d bags from %s", resp.Count, node.Namespace)
		dpnSync.syncBags(resp.Bags(), result)
		if result.HasErrors(dpn.DPNTypeBag) {
			break
		}
		if resp.Next == nil || *resp.Next == "" {
			log.Debug("No more bags to get from %s", node.Namespace)
			break
		}
		pageNumber += 1
	}
	log.Debug("Bags from %s: fetched %d, synched %d", node.Namespace,
		result.FetchCounts[dpn.DPNTypeBag], result.SyncCounts[dpn.DPNTypeBag])
}

func (dpnSync *DPNSync) syncBags(bags []*models.DPNBag, result *models.SyncResult) {
	log := dpnSync.Context.MessageLog
	if len(bags) == 0 {
		log.Debug("No bags to sync")
		return
	}
	for _, bag := range bags {
		if bag == nil {
			log.Debug("Skipping nil bag")
			continue
		}
		log.Debug("Processing bag %s from %s", bag.UUID, bag.AdminNode)
		resp := dpnSync.LocalClient.DPNBagGet(bag.UUID)
		if resp.Error != nil {
			log.Error(resp.Error.Error())
			result.AddError(dpn.DPNTypeBag, resp.Error)
			return
		}
		existingBag := resp.Bag()
		if existingBag == nil {
			log.Debug("Creating new bag %s", bag.UUID)
			resp = dpnSync.LocalClient.DPNBagCreate(bag)
			if resp.Error != nil {
				log.Error(resp.Error.Error())
				result.AddError(dpn.DPNTypeBag, resp.Error)
				return
			}
		} else if !existingBag.UpdatedAt.Before(bag.UpdatedAt) {
			log.Debug("Skipping bag %s, because ours is same age or newer.", bag.UUID)
		} else {
			log.Debug("Updating bag %s", bag.UUID)
			resp = dpnSync.LocalClient.DPNBagUpdate(bag)
			if resp.Error != nil {
				log.Error(resp.Error.Error())
				result.AddError(dpn.DPNTypeBag, resp.Error)
				return
			}
		}
		dpnSync.SyncIngests(bag)
		result.AddToSyncCount(dpn.DPNTypeBag, 1)
	}
}

func (dpnSync *DPNSync) getBags(remoteClient *network.DPNRestClient, pageNumber int) *network.DPNResponse {
	// We want to get all bags updated since the last time we pulled
	// from this node, and only those bags for which the node we're
	// querying is the admin node.
	remoteNode := dpnSync.RemoteNodes[remoteClient.Node]
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("admin_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	params.Set("per_page", strconv.Itoa(SYNC_BATCH_SIZE))
	return remoteClient.DPNBagList(params)
}

func (dpnSync *DPNSync) SyncDigests(remoteNode *models.Node) {
	pageNumber := 1
	log := dpnSync.Context.MessageLog
	result := dpnSync.Results[remoteNode.Namespace]
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.logNoClient(dpn.DPNTypeDigest, remoteNode.Namespace)
		return
	}
	for {
		log.Debug("Getting page %d of digests from %s", pageNumber, remoteNode.Namespace)
		resp := dpnSync.getDigests(remoteClient, pageNumber)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeDigest, resp.Error)
			break
		}
		result.AddToFetchCount(dpn.DPNTypeDigest, resp.Count)
		log.Debug("Got %d digests from %s", resp.Count, remoteNode.Namespace)
		dpnSync.syncDigests(resp.Digests(), result)
		if result.HasErrors(dpn.DPNTypeDigest) {
			break
		}
		if resp.Next == nil || *resp.Next == "" {
			log.Debug("No more digests to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	log.Debug("Digests from %s: fetched %d, synched %d", remoteNode.Namespace,
		result.FetchCounts[dpn.DPNTypeDigest], result.SyncCounts[dpn.DPNTypeDigest])
}

func (dpnSync *DPNSync) syncDigests(digests []*models.MessageDigest, result *models.SyncResult) {
	log := dpnSync.Context.MessageLog
	if len(digests) == 0 {
		log.Debug("No digests in list")
		return
	}
	for _, digest := range digests {
		if digest == nil {
			log.Debug("Skipping nil digest")
			continue
		}
		resp := dpnSync.LocalClient.DigestGet(digest.Bag, digest.Algorithm)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeDigest, resp.Error)
			return
		}
		existingDigest := resp.Digest()
		if existingDigest == nil {
			log.Debug("Creating new %s digest for bag %s", digest.Algorithm, digest.Bag)
			resp = dpnSync.LocalClient.DigestCreate(digest)
			if resp.Error != nil {
				result.AddError(dpn.DPNTypeDigest, resp.Error)
				return
			}
		}
		result.AddToSyncCount(dpn.DPNTypeDigest, 1)
	}
}

func (dpnSync *DPNSync) getDigests(remoteClient *network.DPNRestClient, pageNumber int) *network.DPNResponse {
	// We want digests only from the node that calculated them.
	remoteNode := dpnSync.RemoteNodes[remoteClient.Node]
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	params.Set("per_page", strconv.Itoa(SYNC_BATCH_SIZE))
	return remoteClient.DigestList(params)
}

func (dpnSync *DPNSync) SyncIngests(bag *models.DPNBag) {
	pageNumber := 1
	log := dpnSync.Context.MessageLog
	result := dpnSync.Results[bag.AdminNode]
	remoteClient := dpnSync.RemoteClients[bag.AdminNode]
	if remoteClient == nil {
		dpnSync.logNoClient(dpn.DPNTypeIngest, bag.AdminNode)
		return
	}
	for {
		log.Debug("Getting page %d of ingests from remote %s for bag %s", pageNumber, bag.AdminNode, bag.UUID)
		resp := dpnSync.getIngests(remoteClient, pageNumber, bag.UUID)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeIngest, resp.Error)
			break
		}
		result.AddToFetchCount(dpn.DPNTypeIngest, resp.Count)
		log.Debug("Got %d ingests for bag %s from %s", resp.Count, bag.UUID, bag.AdminNode)
		dpnSync.syncIngests(resp.Ingests(), result)
		if result.HasErrors(dpn.DPNTypeIngest) {
			break
		}
		if resp.Next == nil || *resp.Next == "" {
			log.Debug("No more ingests to get from %s", bag.AdminNode)
			break
		}
		pageNumber += 1
	}
	log.Debug("Ingests from %s: fetched %d, synched %d", bag.AdminNode,
		result.FetchCounts[dpn.DPNTypeIngest], result.SyncCounts[dpn.DPNTypeIngest])

}

func (dpnSync *DPNSync) syncIngests(ingests []*models.Ingest, result *models.SyncResult) {
	log := dpnSync.Context.MessageLog
	for _, ingest := range ingests {
		resp := dpnSync.LocalClient.IngestCreate(ingest)
		if resp.Response.StatusCode == 409 {
			// Do nothing. This ingest record already exists
			// on our local server.
		} else if resp.Error != nil {
			result.AddError(dpn.DPNTypeIngest, resp.Error)
			return
		} else {
			log.Debug("Created new ingest %s (bag %s)", ingest.IngestId, ingest.Bag)
		}
		result.AddToSyncCount(dpn.DPNTypeIngest, 1)
	}
}

func (dpnSync *DPNSync) getIngests(remoteClient *network.DPNRestClient, pageNumber int, bagUUID string) *network.DPNResponse {
	params := url.Values{}
	params.Set("bag", bagUUID)
	params.Set("latest", "true")
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	params.Set("per_page", strconv.Itoa(SYNC_BATCH_SIZE))
	return remoteClient.IngestList(params)
}

func (dpnSync *DPNSync) SyncFixities(remoteNode *models.Node) {
	pageNumber := 1
	log := dpnSync.Context.MessageLog
	result := dpnSync.Results[remoteNode.Namespace]
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.logNoClient(dpn.DPNTypeFixityCheck, remoteNode.Namespace)
		return
	}
	for {
		log.Debug("Getting page %d of fixities from %s", pageNumber, remoteNode.Namespace)
		resp := dpnSync.getFixities(remoteClient, pageNumber)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeFixityCheck, resp.Error)
			break
		}
		result.AddToFetchCount(dpn.DPNTypeFixityCheck, resp.Count)
		log.Debug("Got %d fixities from %s", resp.Count, remoteNode.Namespace)
		dpnSync.syncFixities(resp.FixityChecks(), result)
		if result.HasErrors(dpn.DPNTypeFixityCheck) {
			break
		}
		if resp.Next == nil || *resp.Next == "" {
			log.Debug("No more fixities to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	log.Debug("Fixities from %s: fetched %d, synched %d", remoteNode.Namespace,
		result.FetchCounts[dpn.DPNTypeFixityCheck], result.SyncCounts[dpn.DPNTypeFixityCheck])
}

func (dpnSync *DPNSync) syncFixities(fixities []*models.FixityCheck, result *models.SyncResult) {
	log := dpnSync.Context.MessageLog
	for _, fixity := range fixities {
		if fixity == nil {
			log.Debug("Skipping nil fixity record")
			continue
		}
		resp := dpnSync.LocalClient.FixityCheckCreate(fixity)
		if resp.Response.StatusCode == 409 {
			// Do nothing. This fixity record already exists
			// on our local server.
		} else if resp.Error != nil {
			result.AddError(dpn.DPNTypeFixityCheck, resp.Error)
			return
		} else {
			log.Debug("Created new fixity %s (bag %s)", fixity.FixityCheckId, fixity.Bag)
		}
		result.AddToSyncCount(dpn.DPNTypeFixityCheck, 1)
	}
}

func (dpnSync *DPNSync) getFixities(remoteClient *network.DPNRestClient, pageNumber int) *network.DPNResponse {
	// Get fixities for the remote node *calculated by that node*
	remoteNode := dpnSync.RemoteNodes[remoteClient.Node]
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	params.Set("per_page", strconv.Itoa(SYNC_BATCH_SIZE))
	return remoteClient.FixityCheckList(params)
}

// SyncReplicationRequests copies ReplicationTransfer records from
// remote nodes to our own local node.
func (dpnSync *DPNSync) SyncReplicationRequests(remoteNode *models.Node) {
	pageNumber := 1
	log := dpnSync.Context.MessageLog
	result := dpnSync.Results[remoteNode.Namespace]
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.logNoClient(dpn.DPNTypeReplication, remoteNode.Namespace)
		return
	}
	for {
		log.Debug("Getting page %d of replication transfers from %s", pageNumber, remoteNode.Namespace)
		resp := dpnSync.getReplicationRequests(remoteClient, pageNumber)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeReplication, resp.Error)
			break
		}
		result.AddToFetchCount(dpn.DPNTypeReplication, resp.Count)
		log.Debug("Got %d replication requests from %s", resp.Count, remoteNode.Namespace)
		dpnSync.syncReplicationRequests(resp.ReplicationTransfers(), result)
		if result.HasErrors(dpn.DPNTypeReplication) {
			break
		}
		if resp.Next == nil || *resp.Next == "" {
			log.Debug("No more replications to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	log.Debug("Replications from %s: fetched %d, synched %d", remoteNode.Namespace,
		result.FetchCounts[dpn.DPNTypeReplication], result.SyncCounts[dpn.DPNTypeReplication])
}

func (dpnSync *DPNSync) syncReplicationRequests(xfers []*models.ReplicationTransfer, result *models.SyncResult) {
	log := dpnSync.Context.MessageLog
	for _, xfer := range xfers {
		if xfer == nil {
			log.Debug("Skipping nil replication transfer record")
			continue
		}
		log.Debug("Processing replication %s from %s (bag %s)", xfer.ReplicationId,
			xfer.FromNode, xfer.Bag)
		resp := dpnSync.LocalClient.ReplicationTransferGet(xfer.ReplicationId)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeReplication, resp.Error)
			return
		}
		existingXfer := resp.ReplicationTransfer()
		if existingXfer == nil {
			log.Debug("Creating new replication request %s", xfer.ReplicationId)
			resp = dpnSync.LocalClient.ReplicationTransferCreate(xfer)
			if resp.Error != nil {
				result.AddError(dpn.DPNTypeReplication, resp.Error)
				return
			}
		} else if !existingXfer.UpdatedAt.Before(xfer.UpdatedAt) {
			log.Debug("Skipping replication %s, because ours is same age or newer.", xfer.ReplicationId)
		} else {
			log.Debug("Updating replication %s", xfer.ReplicationId)
			resp = dpnSync.LocalClient.ReplicationTransferUpdate(xfer)
			if resp.Error != nil {
				result.AddError(dpn.DPNTypeReplication, resp.Error)
				return
			}
		}
		result.AddToSyncCount(dpn.DPNTypeReplication, 1)
	}
}

func (dpnSync *DPNSync) getReplicationRequests(remoteClient *network.DPNRestClient, pageNumber int) *network.DPNResponse {
	// Get requests updated since the last time we pulled
	// from this node, where this node is the from_node.
	remoteNode := dpnSync.RemoteNodes[remoteClient.Node]
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("from_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	params.Set("per_page", strconv.Itoa(SYNC_BATCH_SIZE))
	return remoteClient.ReplicationTransferList(params)
}

// SyncRestoreRequests copies RestoreTransfer records from remote
// nodes to our local node.
func (dpnSync *DPNSync) SyncRestoreRequests(remoteNode *models.Node) {
	pageNumber := 1
	log := dpnSync.Context.MessageLog
	result := dpnSync.Results[remoteNode.Namespace]
	remoteClient := dpnSync.RemoteClients[remoteNode.Namespace]
	if remoteClient == nil {
		dpnSync.logNoClient(dpn.DPNTypeRestore, remoteNode.Namespace)
		return
	}
	for {
		log.Debug("Getting page %d of restore transfers from %s", pageNumber, remoteNode.Namespace)
		resp := dpnSync.getRestoreRequests(remoteClient, pageNumber)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeRestore, resp.Error)
			break
		}
		result.AddToFetchCount(dpn.DPNTypeRestore, resp.Count)
		log.Debug("Got %d restore requests from %s", resp.Count, remoteNode.Namespace)
		dpnSync.syncRestoreRequests(resp.RestoreTransfers(), result)
		if result.HasErrors(dpn.DPNTypeRestore) {
			break
		}
		if resp.Next == nil || *resp.Next == "" {
			log.Debug("No more restores to get from %s", remoteNode.Namespace)
			break
		}
		pageNumber += 1
	}
	log.Debug("Restores from %s: fetched %d, synched %d", remoteNode.Namespace,
		result.FetchCounts[dpn.DPNTypeRestore], result.SyncCounts[dpn.DPNTypeRestore])
}

func (dpnSync *DPNSync) syncRestoreRequests(xfers []*models.RestoreTransfer, result *models.SyncResult) {
	log := dpnSync.Context.MessageLog
	for _, xfer := range xfers {
		if xfer == nil {
			log.Debug("Skipping nil restore transfer record")
			continue
		}
		log.Debug("Processing restore %s from %s (bag %s)", xfer.RestoreId,
			xfer.FromNode, xfer.Bag)
		resp := dpnSync.LocalClient.RestoreTransferGet(xfer.RestoreId)
		if resp.Error != nil {
			result.AddError(dpn.DPNTypeRestore, resp.Error)
			return
		}
		existingXfer := resp.RestoreTransfer()
		if existingXfer == nil {
			log.Debug("Creating new restore request %s", xfer.RestoreId)
			resp = dpnSync.LocalClient.RestoreTransferCreate(xfer)
			if resp.Error != nil {
				result.AddError(dpn.DPNTypeRestore, resp.Error)
				return
			}
		} else if !existingXfer.UpdatedAt.Before(xfer.UpdatedAt) {
			log.Debug("Skipping restore %s, because ours is same age or newer.", xfer.RestoreId)
		} else {
			log.Debug("Updating restore %s", xfer.RestoreId)
			resp = dpnSync.LocalClient.RestoreTransferUpdate(xfer)
			if resp.Error != nil {
				result.AddError(dpn.DPNTypeRestore, resp.Error)
				return
			}
		}
		result.AddToSyncCount(dpn.DPNTypeRestore, 1)
	}
}

func (dpnSync *DPNSync) getRestoreRequests(remoteClient *network.DPNRestClient, pageNumber int) *network.DPNResponse {
	// Get requests updated since the last time we pulled
	// from this node, where this node is the to_node.
	// E.g. We ask TDR for restore requests going TO TDR.
	remoteNode := dpnSync.RemoteNodes[remoteClient.Node]
	params := url.Values{}
	params.Set("after", remoteNode.LastPullDate.Format(time.RFC3339Nano))
	params.Set("to_node", remoteNode.Namespace)
	params.Set("page", fmt.Sprintf("%d", pageNumber))
	params.Set("per_page", strconv.Itoa(SYNC_BATCH_SIZE))
	return remoteClient.RestoreTransferList(params)
}

func (dpnSync *DPNSync) logNoClient(dpnType dpn.DPNObjectType, nodeName string) {
	dpnSync.Context.MessageLog.Error("Skipping %s for node %s: REST client is nil",
		dpnType, nodeName)
}

func (dpnSync *DPNSync) logResult(syncResult *models.SyncResult) {
	for _, dpnType := range dpn.DPNTypes {
		dpnSync.Context.MessageLog.Info("Node %s %s: Fetched %d, Synched %d",
			syncResult.NodeName, dpnType, syncResult.FetchCounts[dpnType],
			syncResult.SyncCounts[dpnType])
	}
	for _, dpnType := range dpn.DPNTypes {
		errors := syncResult.Errors[dpnType]
		if errors != nil {
			for _, err := range errors {
				dpnSync.Context.MessageLog.Error("Node %s %s: %v",
					syncResult.NodeName, dpnType, err)
			}
		}
	}
}
