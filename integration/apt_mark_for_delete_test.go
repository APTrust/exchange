package integration_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

	instResp := _context.PharosClient.InstitutionGet("test.edu")
	require.Nil(t, instResp.Error)
	institution := instResp.Institution()
	require.NotNil(t, institution)

	// Mark one object in standard storage for deletion
	s3Key := testutil.INTEGRATION_GOOD_BAGS[9]
	identifier := strings.Replace(s3Key, "aptrust.integration.test", "test.edu", 1)
	identifier = strings.Replace(identifier, ".tar", "", 1)
	markObjectForDeletion(t, _context, identifier, institution.Id)

	// Mark one object in Glacier-only storage for deletion
	s3Key = testutil.INTEGRATION_GLACIER_BAGS[0]
	identifier = strings.Replace(s3Key, "aptrust.integration.test", "test.edu", 1)
	identifier = strings.Replace(identifier, ".tar", "", 1)
	markObjectForDeletion(t, _context, identifier, institution.Id)
}

func markObjectForDeletion(t *testing.T, _context *context.Context, identifier string, institutionId int) {
	// Get the object with all its files (true) but no events (false)
	objResp := _context.PharosClient.IntellectualObjectGet(identifier, true, false)
	require.Nil(t, objResp.Error)
	obj := objResp.IntellectualObject()
	for _, gf := range obj.GenericFiles {
		item := makeDeletionWorkItem(obj.Identifier, gf.Identifier, institutionId)
		itemResp := _context.PharosClient.WorkItemSave(item)
		require.Nil(t, itemResp.Error)
	}
}

func makeDeletionWorkItem(objIdentifier, gfIdentifier string, instId int) *models.WorkItem {
	instApprover := "approver@example.com"
	aptrustApprover := "someone@aptrust.org"
	return &models.WorkItem{
		ObjectIdentifier:      objIdentifier,
		GenericFileIdentifier: gfIdentifier,
		Name:                  "Homer Simpson",
		Bucket:                "fake-bucket",
		ETag:                  "fake-etag",
		Size:                  9999,
		BagDate:               time.Now().UTC(),
		InstitutionId:         instId,
		User:                  "user@example.com",
		InstitutionalApprover: &instApprover,
		APTrustApprover:       &aptrustApprover,
		Action:                constants.ActionDelete,
		Stage:                 constants.StageRequested,
		Status:                constants.StatusPending,
		Retry:                 true,
		Note:                  "Deletion requested",
		Outcome:               "We'll see now, won't we?",
	}
}
