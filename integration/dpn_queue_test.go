package integration_test

import (
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

// getObjectIdentifiers returns the identifiers of objects that
// apps/test_push_to_dpn marks as "Push to DPN". These objects
// should wind up in the dpn_copy queue.
func identifiersPushedToDPN() ([]string) {
	identifiers := make([]string, 7)
	for index, s3Key := range testutil.INTEGRATION_GOOD_BAGS[0:7] {
		identifier := strings.Replace(s3Key, "aptrust.receiving.test.", "", 1)
		identifier = strings.Replace(identifier, ".tar", "", 1)
		identifiers[index] = identifier
	}
	return identifiers
}

func getContext(t *testing.T) (*context.Context) {
	configFile := filepath.Join("config", "integration.json")
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	config.ExpandFilePaths()
	return context.NewContext(config)
}

func TestWorkItemsCreatedAndQueued(t *testing.T) {
	if !testutil.ShouldRunIntegrationTests() {
		t.Skip("Skipping integration test. Set ENV var RUN_EXCHANGE_INTEGRATION=true if you want to run them.")
	}
	expectedIdentifiers := identifiersPushedToDPN()
	_context := getContext(t)
	params := url.Values{}
	params.Set("item_action", "DPN")
	params.Set("page", "1")
	params.Set("per_page", "100")
	resp := _context.PharosClient.WorkItemList(params)
	require.Nil(t, resp.Error)
	assert.Equal(t, len(expectedIdentifiers), resp.Count)
	for _, workItem := range resp.WorkItems() {
		found := false
		queued := false
		currentIdentifier := ""
		for _, identifier := range expectedIdentifiers {
			currentIdentifier = identifier
			if workItem.ObjectIdentifier == identifier {
				found = true
				if workItem.QueuedAt != nil && !workItem.QueuedAt.IsZero() {
					queued = true
				}
				break
			}
		}
		assert.True(t, found, "No WorkItem for object %s", currentIdentifier)
		assert.True(t, queued, "Object %s was not queued", currentIdentifier)
	}
}
