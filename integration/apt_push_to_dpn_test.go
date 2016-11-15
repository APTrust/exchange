package integration_test

import (
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
)

// test_push_to_dpn is for integration testing only.
// It creates a few WorkItems in Pharos asking that
// a handful of bags be pushed to DPN. Subsequent
// integration tests (such as dpn_queue) depend on
// the WorkItems created by this test.
func TestPushToDPN(t *testing.T) {
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	_context := context.NewContext(config)
	for _, s3Key := range testutil.INTEGRATION_GOOD_BAGS[0:7] {
		identifier := strings.Replace(s3Key, "aptrust.receiving.test.", "", 1)
		identifier = strings.Replace(identifier, ".tar", "", 1)
		resp := _context.PharosClient.IntellectualObjectPushToDPN(identifier)
		workItem := resp.WorkItem()
		require.Nil(t, resp.Error)
		require.NotNil(t, workItem)
		_context.MessageLog.Info("Created DPN work item #%d for %s",
			workItem.Id, workItem.ObjectIdentifier)
	}
}
