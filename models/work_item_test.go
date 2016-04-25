package models_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/logger"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

var bagDate time.Time = time.Date(2104, 7, 2, 12, 0, 0, 0, time.UTC)
var ingestDate time.Time = time.Date(2014, 9, 10, 12, 0, 0, 0, time.UTC)


func SampleWorkItem() *models.WorkItem {
	return &models.WorkItem{
		Id: 9000,
		ObjectIdentifier: "ncsu.edu/some_object",
		GenericFileIdentifier: "ncsu.edu/some_object/data/doc.pdf",
		Name: "Sample Document",
		Bucket: "aptrust.receiving.ncsu.edu",
		ETag: "12345",
		BagDate: bagDate,
		Institution: "ncsu.edu",
		Date: ingestDate,
		Note: "so many!",
		Action: "Ingest",
		Stage: "Store",
		Status: "Success",
		Outcome: "happy day!",
		Retry: true,
		Reviewed: false,
		Node: "",
		Pid: 0,
		State: "",
		NeedsAdminReview: false,
	}
}

func TestWorkItemSerializeForFluctus(t *testing.T) {
	workItem := SampleWorkItem()
	bytes, err := workItem.SerializeForFluctus()
	if err != nil {
		t.Error(err)
	}
	expected := `{"action":"Ingest","bag_date":"2104-07-02T12:00:00Z","bucket":"aptrust.receiving.ncsu.edu","date":"2014-09-10T12:00:00Z","etag":"12345","generic_file_identifier":"ncsu.edu/some_object/data/doc.pdf","institution":"ncsu.edu","name":"Sample Document","needs_admin_review":false,"node":"","note":"so many!","object_identifier":"ncsu.edu/some_object","outcome":"happy day!","pid":0,"retry":true,"reviewed":false,"stage":"Store","state":"","status":"Success"}`
	assert.Equal(t, expected, string(bytes))
}

func TestWorkItemHasBeenStored(t *testing.T) {
	workItem := models.WorkItem{
		Action: "Ingest",
		Stage: "Record",
		Status: "Success",
	}
	assert.True(t, workItem.HasBeenStored())

	workItem.Stage = constants.StageCleanup
	assert.True(t, workItem.HasBeenStored())

	workItem.Stage = constants.StageStore
	workItem.Status = constants.StatusPending
	assert.True(t, workItem.HasBeenStored())

	workItem.Stage = constants.StageStore
	workItem.Status = constants.StatusStarted
	assert.False(t, workItem.HasBeenStored())

	workItem.Stage = constants.StageFetch
	assert.False(t, workItem.HasBeenStored())

	workItem.Stage = constants.StageUnpack
	assert.False(t, workItem.HasBeenStored())

	workItem.Stage = constants.StageValidate
	assert.False(t, workItem.HasBeenStored())
}

func TestIsStoring(t *testing.T) {
	workItem := models.WorkItem{
		Action: "Ingest",
		Stage: "Store",
		Status: "Started",
	}
	assert.True(t, workItem.IsStoring())
	workItem.Status = "Pending"
	assert.False(t, workItem.IsStoring())

	workItem.Status = "Started"
	workItem.Stage = "Record"
	assert.False(t, workItem.IsStoring())
}

func TestWorkItemShouldTryIngest(t *testing.T) {
	workItem := models.WorkItem{
		Action: "Ingest",
		Stage: "Receive",
		Status: "Pending",
		Retry: true,
	}

	// Test stages
	assert.True(t, workItem.ShouldTryIngest())

	workItem.Stage = "Fetch"
	assert.True(t, workItem.ShouldTryIngest())

	workItem.Stage = "Unpack"
	assert.True(t, workItem.ShouldTryIngest())

	workItem.Stage = "Validate"
	assert.True(t, workItem.ShouldTryIngest())

	workItem.Stage = "Record"
	assert.False(t, workItem.ShouldTryIngest())

	// Test Store/Pending and Store/Started
	workItem.Stage = "Store"
	workItem.Status = "Started"
	assert.False(t, workItem.ShouldTryIngest())

	workItem.Stage = "Store"
	workItem.Status = "Pending"
	assert.False(t, workItem.ShouldTryIngest())

	// Test Retry = false
	workItem.Status = "Started"
	workItem.Retry = false

	workItem.Stage = "Receive"
	assert.False(t, workItem.ShouldTryIngest())

	workItem.Stage = "Fetch"
	assert.False(t, workItem.ShouldTryIngest())

	workItem.Stage = "Unpack"
	assert.False(t, workItem.ShouldTryIngest())

	workItem.Stage = "Validate"
	assert.False(t, workItem.ShouldTryIngest())

	workItem.Stage = "Record"
	assert.False(t, workItem.ShouldTryIngest())
}

func getWorkItems(action constants.ActionType) ([]*models.WorkItem) {
	workItems := make([]*models.WorkItem, 3)
	workItems[0] = &models.WorkItem{
		Action: action,
		Stage: "Resolve",
		Status: constants.StatusSuccess,
	}
	workItems[1] = &models.WorkItem{
		Action: action,
		Stage: "Resolve",
		Status: constants.StatusFailed,
	}
	workItems[2] = &models.WorkItem{
		Action: action,
		Stage: "Requested",
		Status: constants.StatusPending,
	}
	return workItems
}

func TestHasPendingDeleteRequest(t *testing.T) {
	workItems := getWorkItems(constants.ActionDelete)
	assert.True(t, models.HasPendingDeleteRequest(workItems))

	workItems[2].Status = constants.StatusStarted
	assert.True(t, models.HasPendingDeleteRequest(workItems))

	workItems[2].Status = constants.StatusCancelled
	assert.False(t, models.HasPendingDeleteRequest(workItems))
}

func TestHasPendingRestoreRequest(t *testing.T) {
	workItems := getWorkItems(constants.ActionRestore)
	assert.True(t, models.HasPendingRestoreRequest(workItems))

	workItems[2].Status = constants.StatusStarted
	assert.True(t, models.HasPendingRestoreRequest(workItems))

	workItems[2].Status = constants.StatusCancelled
	assert.False(t, models.HasPendingRestoreRequest(workItems))
}

func TestHasPendingIngestRequest(t *testing.T) {
	workItems := getWorkItems(constants.ActionIngest)
	assert.True(t, models.HasPendingIngestRequest(workItems))

	workItems[2].Status = constants.StatusStarted
	assert.True(t, models.HasPendingIngestRequest(workItems))

	workItems[2].Status = constants.StatusCancelled
	assert.False(t, models.HasPendingIngestRequest(workItems))
}

func TestSetNodePidState(t *testing.T) {
	item := SampleWorkItem()
	object := make(map[string]string)
	object["key"] = "value"

	discardLogger := logger.DiscardLogger("workitem_test")
	item.SetNodePidState(object, discardLogger)
	hostname, _ := os.Hostname()
	if hostname == "" {
		assert.Equal(t, "hostname?", item.Node)
	} else {
		assert.Equal(t, hostname, item.Node)
	}
	assert.EqualValues(t, os.Getpid(), item.Pid)
	assert.Equal(t, "{\"key\":\"value\"}", item.State)
}
