package dpn_test

import (
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)
var BAG_IDS = []string{
	"00000000-0000-4000-a000-000000000001",
	"00000000-0000-4000-a000-000000000002",
	"00000000-0000-4000-a000-000000000003",
	"00000000-0000-4000-a000-000000000004",
	"00000000-0000-4000-a000-000000000005",
}
var REPLICATION_IDS = []string{
	"10000000-0000-4111-a000-000000000001",
	"10000000-0000-4111-a000-000000000007",
	"10000000-0000-4111-a000-000000000013",
	"10000000-0000-4111-a000-000000000019",
	"20000000-0000-4000-a000-000000000001",
	"20000000-0000-4000-a000-000000000007",
	"20000000-0000-4000-a000-000000000013",
	"20000000-0000-4000-a000-000000000019",
	"30000000-0000-4000-a000-000000000001",
	"30000000-0000-4000-a000-000000000007",
	"30000000-0000-4000-a000-000000000013",
	"30000000-0000-4000-a000-000000000019",
	"40000000-0000-4000-a000-000000000001",
	"40000000-0000-4000-a000-000000000007",
	"40000000-0000-4000-a000-000000000013",
	"40000000-0000-4000-a000-000000000019",
	"50000000-0000-4000-a000-000000000001",
	"50000000-0000-4000-a000-000000000007",
	"50000000-0000-4000-a000-000000000013",
	"50000000-0000-4000-a000-000000000019",
}
var RESTORE_IDS = []string{
	"11000000-0000-4111-a000-000000000001",
	"11000000-0000-4111-a000-000000000002",
	"11000000-0000-4111-a000-000000000003",
	"11000000-0000-4111-a000-000000000004",
	"21000000-0000-4111-a000-000000000001",
	"21000000-0000-4111-a000-000000000002",
	"21000000-0000-4111-a000-000000000003",
	"21000000-0000-4111-a000-000000000004",
	"31000000-0000-4111-a000-000000000001",
	"31000000-0000-4111-a000-000000000002",
	"31000000-0000-4111-a000-000000000003",
	"31000000-0000-4111-a000-000000000004",
	"41000000-0000-4111-a000-000000000001",
	"41000000-0000-4111-a000-000000000002",
	"41000000-0000-4111-a000-000000000003",
	"41000000-0000-4111-a000-000000000004",
	"51000000-0000-4111-a000-000000000001",
	"51000000-0000-4111-a000-000000000002",
	"51000000-0000-4111-a000-000000000003",
	"51000000-0000-4111-a000-000000000004",
}

func loadConfig(t *testing.T) (*models.Config) {
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	return config
}

func runSyncTests() bool {
	return os.Getenv("RUN_DPN_SYNC_POST_TEST") == "true"
}

// Returns the namespace of the local node, and a list of
// remote node namespaces.
func nodeNamespaces(t *testing.T) (string, []string) {
	config := loadConfig(t)
	remoteNodes := make([]string, 0)
	for namespace, _ := range config.DPN.RemoteNodeTokens {
		remoteNodes = append(remoteNodes, namespace)
	}
	return config.DPN.LocalNode, remoteNodes
}

func allNodeNamespaces(t *testing.T) ([]string) {
	localNode, remoteNodes := nodeNamespaces(t)
	return append([]string { localNode }, remoteNodes...)
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

func getPostTestClient(t *testing.T) (*dpn.DPNRestClient) {
	// If you want to debug, change ioutil.Discard to os.Stdout
	// to see log output from the client.
	config, err := models.LoadConfigFile(filepath.Join("config", "integration.json"))
	require.Nil(t, err)
	client, err := dpn.NewDPNRestClient(
		config.DPN.RestClient.LocalServiceURL,
		config.DPN.RestClient.LocalAPIRoot,
		config.DPN.RestClient.LocalAuthToken,
		config.DPN.LocalNode,
		config.DPN)
	require.Nil(t, err)
	return client
}

func getParams() (*url.Values) {
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	params := url.Values{}
	params.Set("after", oneHourAgo.Format(time.RFC3339Nano))
	return &params
}

func TestNewDPNSync(t *testing.T) {
	if runSyncTests() == false {
		return
	}
	dpnSync := newDPNSync(t)
	if dpnSync == nil {
		return
	}
}

func TestLocalNodeName(t *testing.T) {
	if runSyncTests() == false {
		return
	}
	config := loadConfig(t)
	dpnSync := newDPNSync(t)
	assert.Equal(t, config.DPN.LocalNode, dpnSync.LocalNodeName())
}

func TestRemoteNodeNames(t *testing.T) {
	if runSyncTests() == false {
		return
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

func TestNodesWereSynched(t *testing.T) {
	if runSyncTests() == false {
		t.Skip("Skipping TestNodesWereSynched")
		return
	}
	client := getPostTestClient(t)
	resp := client.NodeList(nil)
	require.Nil(t, resp.Error)
	nodes := resp.Nodes()
	require.Equal(t, 5, len(nodes))
	nodesFound := make(map[string]*dpn.Node)
	for _, node := range nodes {
		nodesFound[node.Namespace] = node
	}
	for _, namespace := range allNodeNamespaces(t) {
		node := nodesFound[namespace]
		assert.NotNil(t, node, "No record for node %s", namespace)
	}
}

func TestMembersWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return
	}
	client := getPostTestClient(t)
	resp := client.MemberList(nil)
	require.Nil(t, resp.Error)
	members := resp.Members()
	require.Equal(t, 5, len(members))
}

func TestBagsWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return
	}
	client := getPostTestClient(t)
	resp := client.DPNBagList(nil)
	require.Nil(t, resp.Error)
	bags := resp.Bags()
	require.Equal(t, 5, len(bags))

	for _, id := range BAG_IDS {
		resp := client.DPNBagGet(id)
		assert.NotNil(t, resp.Bag())
	}
}

func TestIngestsWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return
	}
	client := getPostTestClient(t)
	resp := client.IngestList(nil)
	require.Nil(t, resp.Error)
	ingests := resp.Ingests()
	require.Equal(t, 5, len(ingests))
}

func TestDigestsWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return
	}
	client := getPostTestClient(t)
	resp := client.DigestList(nil)
	require.Nil(t, resp.Error)
	digests := resp.Digests()
	require.Equal(t, 5, len(digests))
}

func TestFixitiesWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return
	}
	client := getPostTestClient(t)
	resp := client.FixityCheckList(nil)
	require.Nil(t, resp.Error)
	fixities := resp.FixityChecks()
	require.Equal(t, 9, len(fixities))

	// We should have two fixities from each node.
	// The first was loaded with the basic fixture
	// set, and the second was loaded in the
	// dpn_sync process.
	params := &url.Values{}
	params.Set("node", "chron")
	resp = client.FixityCheckList(params)
	assert.Equal(t, 2, len(resp.FixityChecks()))

	params.Set("node", "hathi")
	resp = client.FixityCheckList(params)
	assert.Equal(t, 2, len(resp.FixityChecks()))

	params.Set("node", "sdr")
	resp = client.FixityCheckList(params)
	assert.Equal(t, 2, len(resp.FixityChecks()))

	params.Set("node", "tdr")
	resp = client.FixityCheckList(params)
	assert.Equal(t, 2, len(resp.FixityChecks()))
}

func TestReplicationsWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return
	}
	client := getPostTestClient(t)
	resp := client.ReplicationList(nil)
	require.Nil(t, resp.Error)
	xfers:= resp.ReplicationTransfers()
	require.Equal(t, 20, len(xfers))

	for _, id := range REPLICATION_IDS {
		resp := client.ReplicationTransferGet(id)
		assert.NotNil(t, resp.ReplicationTransfer())
	}
}

func TestRestoresWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return
	}
	client := getPostTestClient(t)
	resp := client.RestoreTransferList(nil)
	require.Nil(t, resp.Error)
	xfers:= resp.RestoreTransfers()
	require.Equal(t, 20, len(xfers))

	for _, id := range RESTORE_IDS {
		resp := client.RestoreTransferGet(id)
		assert.NotNil(t, resp.RestoreTransfer())
	}
}
