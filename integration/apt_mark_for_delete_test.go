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

// apt_mark_for_delete_test is used by scripts/integration_test.rb
// to mark some files for deletion, so that the apt_file_delete
// integration test will have some items to work with.
func TestMarkForDelete(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	_context := context.NewContext(config)
	s3Key := testutil.INTEGRATION_GOOD_BAGS[9]
	identifier := strings.Replace(s3Key, "aptrust.receiving.test.", "", 1)
	identifier = strings.Replace(identifier, ".tar", "", 1)
	resp := _context.PharosClient.IntellectualObjectRequestDelete(identifier)
	require.Nil(t, resp.Error)
	_context.MessageLog.Info("Created delete request for object %s", identifier)
}
