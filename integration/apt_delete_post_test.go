package integration_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
)

/*
These tests check the results of the integration tests for
the app apt_file_delete. See the ingest_test.sh script in
the scripts folder, which sets up an integration context, runs
apt_file_delete.
*/

func TestDeleteResults(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	// Load config
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()
	_context := context.NewContext(config)

	s3Key := testutil.INTEGRATION_GOOD_BAGS[8]
	identifier := strings.Replace(s3Key, "aptrust.receiving.test.", "", 1)
	identifier = strings.Replace(identifier, ".tar", "", 1)

	// Check delete events
	params := url.Values{}
	params.Set("object_identifier", identifier)
	params.Set("event_type", constants.EventDeletion)
	params.Set("page", "1")
	params.Set("per_page", "100")
	resp := _context.PharosClient.PremisEventList(params)
	require.Nil(t, resp.Error)
	events := resp.PremisEvents()
	assert.Equal(t, 6, len(events))

	// Make sure files don't exist in S3
}
