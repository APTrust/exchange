package dpn_test

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

var skipSyncMessagePrinted = false

const (
	BAG_COUNT     = 1
	REPL_COUNT    = 4
	RESTORE_COUNT = 4
)

func loadConfig(t *testing.T) (*models.Config) {
	configFile := filepath.Join("config", "test.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	return config
}

func runSyncTests(t *testing.T) bool {
	config := loadConfig(t)
	_, err := http.Get(config.DPN.RestClient.LocalServiceURL)
	if !canRunSyncTests("aptrust", config.DPN.RestClient.LocalServiceURL, err) {
		return false
	}
	for nodeNamespace, url := range config.DPN.RemoteNodeURLs {
		if url == "" {
			continue
		}
		_, err := http.Get(url)
		if !canRunSyncTests(nodeNamespace, url, err) {
			return false
		}
	}
	return true
}

func canRunSyncTests(nodeNamespace string, url string, err error) (bool) {
	if err != nil {
		if skipSyncMessagePrinted == false {
			skipSyncMessagePrinted = true
			fmt.Printf("**** Skipping DPN sync integration tests: "+
				"%s server is not running at %s\n", nodeNamespace, url)
			fmt.Println("     Run the run_cluster.sh script in " +
				"DPN-REST/dpnode to get a local cluster running.")
		}
		return false
	}
	return true
}

func newDPNSync(t *testing.T) (*dpn.DPNSync) {
	config := loadConfig(t)
	_context := context.NewContext(config)
	dpnSync, err := dpn.NewDPNSync(_context)
	require.Nil(t, err)
	for namespace, _ := range config.DPN.RemoteNodeTokens {
		require.NotNil(t, dpnSync.RemoteClients[namespace], namespace)
	}
	return dpnSync
}

func TestNewDPNSync(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	if dpnSync == nil {
		return
	}
}

func TestLocalNodeName(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	config := loadConfig(t)
	dpnSync := newDPNSync(t)
	assert.Equal(t, config.DPN.LocalNode, dpnSync.LocalNodeName())
}

func TestRemoteNodeNames(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	config := loadConfig(t)
	dpnSync := newDPNSync(t)
	remoteNodeNames := dpnSync.RemoteNodeNames()
	for name, _ := range config.DPN.RemoteNodeURLs {
		nameIsPresent := false
		for _, remoteName := range remoteNodeNames {
			if name == remoteName {
				nameIsPresent = true
				break
			}
		}
		assert.True(t, nameIsPresent, name)
	}
}

func TestGetAllNodes(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	nodes, err := dpnSync.GetAllNodes()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, 5, len(nodes))
}

func TestSyncBags(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	nodes, err := dpnSync.GetAllNodes()
	require.Nil(t, err)
	for _, node := range nodes {
		if node.Namespace == "aptrust" {
			continue
		}
		aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
		node.LastPullDate = aLongTimeAgo
		bagsSynched, err := dpnSync.SyncBags(node)
		require.Nil(t, err, "Error synching bags for node %s: %v", node.Namespace, err)
		expectedBagCount := BAG_COUNT
		assert.Equal(t, expectedBagCount, len(bagsSynched), node.Namespace)
		for _, remoteBag := range(bagsSynched) {
			require.NotNil(t, remoteBag, node.Namespace)
			resp := dpnSync.LocalClient.DPNBagGet(remoteBag.UUID)
			require.Nil(t, resp.Error)
			require.NotNil(t, resp.Bag)
			assert.Equal(t, remoteBag.UpdatedAt, resp.Bag.UpdatedAt)
		}
	}
}

func TestSyncReplicationRequests(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	nodes, err := dpnSync.GetAllNodes()
	require.Nil(t, err)
	for _, node := range nodes {
		if node.Namespace == "aptrust" {
			continue
		}
		aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
		node.LastPullDate = aLongTimeAgo

		xfersSynched, err := dpnSync.SyncReplicationRequests(node)
		assert.Nil(t, err, node.Namespace)
		assert.Equal(t, REPL_COUNT, len(xfersSynched), node.Namespace)

		for _, xfer := range(xfersSynched) {
			require.NotNil(t, xfer)
			resp := dpnSync.LocalClient.ReplicationTransferGet(xfer.ReplicationId)
			require.Nil(t, resp.Error)
			require.NotNil(t, resp.Xfer)
			assert.Equal(t, xfer.UpdatedAt, resp.Xfer.UpdatedAt, xfer.ReplicationId)
		}
	}
}

func TestSyncRestoreRequests(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	nodes, err := dpnSync.GetAllNodes()
	require.Nil(t, err)
	for _, node := range nodes {
		if node.Namespace == "aptrust" {
			continue
		}
		aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
		node.LastPullDate = aLongTimeAgo
		xfersSynched, err := dpnSync.SyncRestoreRequests(node)
		require.Nil(t, err, node.Namespace)
		assert.Equal(t, RESTORE_COUNT, len(xfersSynched), node.Namespace)
		for _, xfer := range(xfersSynched) {
			require.NotNil(t, xfer)
			resp := dpnSync.LocalClient.RestoreTransferGet(xfer.RestoreId)
			require.Nil(t, resp.Error)
			require.NotNil(t, resp.Xfer)
			assert.Equal(t, xfer.UpdatedAt, resp.Xfer.UpdatedAt, node.Namespace)
		}
	}
}

// func TestSyncEverythingFromNode(t *testing.T) {
// 	if runSyncTests(t) == false {
// 		return  // local test cluster isn't running
// 	}
// 	dpnSync := newDPNSync(t)

// 	// Make 10 bags.
// 	// This will also create 40 replication transfers
// 	// and 40 restore transfers: one for each bag to
// 	// each remote node.
// 	bagCount := 10
// 	xferCount := 64     // 24 from fixtures in DPN REST + 40 that we just created
// 	restoreCount := 44  //  4 from fixtures in DPN REST + 40 that we just created
// 	mock := NewMock(dpnSync)
// 	err := mock.AddRecordsToNodes(dpnSync.RemoteNodeNames(), bagCount)
// 	require.Nil(t, err)
// 	nodes, err := dpnSync.GetAllNodes()
// 	require.Nil(t, err)

// 	for _, node := range nodes {
// 		if node.Namespace == "aptrust" {
// 			continue
// 		}
// 		aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
// 		node.LastPullDate = aLongTimeAgo
// 		syncResult := dpnSync.SyncEverythingFromNode(node)

// 		// Bags
// 		assert.Nil(t, syncResult.BagSyncError)
// 		assert.Equal(t, bagCount, len(syncResult.Bags), node.Namespace)

// 		// Replication Transfers
// 		assert.Nil(t, syncResult.ReplicationSyncError, node.Namespace)
// 		assert.Equal(t, xferCount, len(syncResult.ReplicationTransfers), node.Namespace)

// 		// Bags
// 		assert.Nil(t, syncResult.RestoreSyncError, node.Namespace)
// 		assert.Equal(t, restoreCount, len(syncResult.RestoreTransfers), node.Namespace)

// 		// Timestamp update
// 		resp := dpnSync.LocalClient.NodeGet(node.Namespace)
// 		assert.Nil(t, resp.Error, node.Namespace)
// 		assert.NotEqual(t, aLongTimeAgo, resp.Node.LastPullDate, node.Namespace)
// 	}
// }

func TestSyncWithError(t *testing.T) {
	if runSyncTests(t) == false {
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	nodes, err := dpnSync.GetAllNodes()
	require.Nil(t, err)

	// Pick one node to sync with, and set the API key for that node
	// to a value we know is invalid. This will cause the sync to fail.
	node := nodes[len(nodes) - 1]
	dpnSync.RemoteClients[node.Namespace].APIKey = "0000000000000000"

	aLongTimeAgo := time.Date(1999, time.December, 31, 23, 0, 0, 0, time.UTC)
	node.LastPullDate = aLongTimeAgo

	syncResult := dpnSync.SyncEverythingFromNode(node)
	assert.NotNil(t, syncResult.BagSyncError)
	assert.NotNil(t, syncResult.ReplicationSyncError)
	assert.NotNil(t, syncResult.RestoreSyncError)

	// Because the sync failed (due to the bad API Key), the LastPullDate
	// on the node we tried to pull from should NOT be updated.
	resp := dpnSync.LocalClient.NodeGet(node.Namespace)
	require.Nil(t, resp.Error)
	assert.Equal(t, aLongTimeAgo, resp.Node.LastPullDate, node.Namespace)
}


func TestHasSyncErrors(t *testing.T) {
	syncResult := &dpn.SyncResult{}
	assert.False(t, syncResult.HasSyncErrors())
	syncResult.BagSyncError = fmt.Errorf("Oops.")
	assert.True(t, syncResult.HasSyncErrors())

	syncResult.BagSyncError = nil
	syncResult.ReplicationSyncError = fmt.Errorf("Oops.")
	assert.True(t, syncResult.HasSyncErrors())

	syncResult.ReplicationSyncError = nil
	syncResult.RestoreSyncError = fmt.Errorf("Oops.")
	assert.True(t, syncResult.HasSyncErrors())
}
