package integration_test

import (
	"fmt"
//	"github.com/APTrust/exchange/constants"
//	"github.com/APTrust/exchange/dpn/network"
//	"github.com/APTrust/exchange/models"
//	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestItemsCopiedToStaging(t *testing.T) {
	// Make sure that each of the expected bags has shown
	// up in our test staging area.
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
    }
	for i := 2; i <= 5; i++ {
		_context := getContext(t) // defined in dpn_queue_test.go
		filename := fmt.Sprintf("00000000-0000-4000-a000-00000000000%d.tar", i)
		path := filepath.Join(_context.Config.DPN.StagingDirectory, filename)
		assert.True(t, fileutil.FileExists(path), "File %s was not copied", path)
	}
}

func TestCopyResultSentToRemoteNodes(t *testing.T) {
	// Query the FromNode of each copied bag to make sure that
	// we sent a fixity value back to the ingest node.
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
    }
	// _context := getContext(t)
	// dpnClient, err := network.NewDPNRestClient(
	// 	_context.Config.DPN.RestClient.LocalServiceURL,
	// 	_context.Config.DPN.RestClient.LocalAPIRoot,
	// 	_context.Config.DPN.RestClient.LocalAuthToken,
	// 	_context.Config.DPN.LocalNode,
	// 	_context.Config.DPN)

}

func TestDPNCopyJson(t *testing.T) {
	// Read the JSON log and make sure everything is in the
	// expected state.
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
    }
	// _context := getContext(t)
}
