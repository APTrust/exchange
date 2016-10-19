package dpn_test

import (
	"fmt"
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

func loadConfig(t *testing.T) (*models.Config) {
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	return config
}

func runSyncTests() bool {
	return os.Getenv("RUN_DPN_SYNC_POST_TEST") == "true"
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
		return  // local test cluster isn't running
	}
	dpnSync := newDPNSync(t)
	if dpnSync == nil {
		return
	}
}

func TestLocalNodeName(t *testing.T) {
	if runSyncTests() == false {
		return  // local test cluster isn't running
	}
	config := loadConfig(t)
	dpnSync := newDPNSync(t)
	assert.Equal(t, config.DPN.LocalNode, dpnSync.LocalNodeName())
}

func TestRemoteNodeNames(t *testing.T) {
	if runSyncTests() == false {
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

func TestNodesWereSynched(t *testing.T) {
	if runSyncTests() == false {
		t.Skip("Skipping TestNodesWereSynched")
		return  // local test cluster isn't running
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
	for _, name := range []string{ "aptrust", "chron", "hathi", "sdr", "tdr" } {
		node := nodesFound[name]
		assert.NotNil(t, node, "No record for node %s", name)
	}
}

func TestMembersWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return  // local test cluster isn't running
	}
	client := getPostTestClient(t)
	resp := client.MemberList(nil)
	require.Nil(t, resp.Error)
	members := resp.Members()
	require.Equal(t, 5, len(members))
}

func TestBagsWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return  // local test cluster isn't running
	}
	client := getPostTestClient(t)
	resp := client.DPNBagList(nil)
	require.Nil(t, resp.Error)
	bags := resp.Bags()
	require.Equal(t, 5, len(bags))
	for _, bag := range bags {
		fmt.Println(bag.UUID)
	}
}

func TestIngestsWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return  // local test cluster isn't running
	}
	//dpnSync := newDPNSync(t)

}

func TestDigestsWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return  // local test cluster isn't running
	}
	//dpnSync := newDPNSync(t)

}

func TestFixitiesWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return  // local test cluster isn't running
	}
	//dpnSync := newDPNSync(t)

}

func TestReplicationsWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return  // local test cluster isn't running
	}
	//dpnSync := newDPNSync(t)

}

func TestRestoresWereSynched(t *testing.T) {
	if runSyncTests() == false {
		return  // local test cluster isn't running
	}
	//dpnSync := newDPNSync(t)

}
