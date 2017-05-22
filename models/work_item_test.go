package models_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

var bagDate time.Time = time.Date(2104, 7, 2, 12, 0, 0, 0, time.UTC)
var ingestDate time.Time = time.Date(2014, 9, 10, 12, 0, 0, 0, time.UTC)

func SampleWorkItem() *models.WorkItem {
	return &models.WorkItem{
		Id:                    9000,
		ObjectIdentifier:      "ncsu.edu/some_object",
		GenericFileIdentifier: "ncsu.edu/some_object/data/doc.pdf",
		Name:             "Sample Document",
		Bucket:           "aptrust.receiving.ncsu.edu",
		ETag:             "12345",
		Size:             31337,
		BagDate:          bagDate,
		InstitutionId:    324,
		Date:             ingestDate,
		Note:             "so many!",
		Action:           "Ingest",
		Stage:            "Store",
		Status:           "Success",
		Outcome:          "happy day!",
		Retry:            true,
		Node:             "",
		Pid:              0,
		NeedsAdminReview: false,
		CreatedAt:        ingestDate,
		UpdatedAt:        ingestDate,
	}
}

func TestWorkItemSerializeForPharos(t *testing.T) {
	workItem := SampleWorkItem()
	bytes, err := workItem.SerializeForPharos()
	if err != nil {
		t.Error(err)
	}
	expected := `{"action":"Ingest","bag_date":"2104-07-02T12:00:00Z","bucket":"aptrust.receiving.ncsu.edu","date":"2014-09-10T12:00:00Z","etag":"12345","generic_file_identifier":"ncsu.edu/some_object/data/doc.pdf","institution_id":324,"name":"Sample Document","needs_admin_review":false,"node":"","note":"so many!","object_identifier":"ncsu.edu/some_object","outcome":"happy day!","pid":0,"queued_at":null,"retry":true,"size":31337,"stage":"Store","stage_started_at":null,"status":"Success"}`
	assert.Equal(t, expected, string(bytes))
}

func TestWorkItemHasBeenStored(t *testing.T) {
	workItem := models.WorkItem{
		Action: "Ingest",
		Stage:  "Record",
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
		Stage:  "Store",
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
		Stage:  "Receive",
		Status: "Pending",
		Retry:  true,
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

func getWorkItems(action string) []*models.WorkItem {
	workItems := make([]*models.WorkItem, 3)
	workItems[0] = &models.WorkItem{
		Action: action,
		Stage:  "Resolve",
		Status: constants.StatusSuccess,
	}
	workItems[1] = &models.WorkItem{
		Action: action,
		Stage:  "Resolve",
		Status: constants.StatusFailed,
	}
	workItems[2] = &models.WorkItem{
		Action: action,
		Stage:  "Requested",
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

func TestSetNodeAndPid(t *testing.T) {
	item := SampleWorkItem()
	item.SetNodeAndPid()
	hostname, _ := os.Hostname()
	if hostname == "" {
		assert.Equal(t, "hostname?", item.Node)
	} else {
		assert.Equal(t, hostname, item.Node)
	}
	assert.EqualValues(t, os.Getpid(), item.Pid)
}

func TestBelogsToOtherWorker(t *testing.T) {
	item := SampleWorkItem()
	assert.False(t, item.BelongsToAnotherWorker())
	item.SetNodeAndPid()
	assert.False(t, item.BelongsToAnotherWorker())
	item.Pid = item.Pid + 1
	assert.True(t, item.BelongsToAnotherWorker())
	item.Pid = item.Pid - 1
	assert.False(t, item.BelongsToAnotherWorker())
	item.Node = "some.other.host.kom"
	assert.True(t, item.BelongsToAnotherWorker())
}

func TestIsInProgress(t *testing.T) {
	item := SampleWorkItem()
	item.Node = ""
	item.Pid = 0
	assert.False(t, item.IsInProgress())
	item.SetNodeAndPid()
	assert.True(t, item.IsInProgress())
}

func TestIsPastIngest(t *testing.T) {
	item := SampleWorkItem()
	item.Stage = constants.StageReceive
	assert.False(t, item.IsPastIngest())
	item.Stage = constants.StageFetch
	assert.False(t, item.IsPastIngest())
	item.Stage = constants.StageUnpack
	assert.False(t, item.IsPastIngest())
	item.Stage = constants.StageValidate
	assert.False(t, item.IsPastIngest())

	item.Stage = constants.StageStore
	assert.True(t, item.IsPastIngest())
	item.Stage = constants.StageRecord
	assert.True(t, item.IsPastIngest())
	item.Stage = constants.StageCleanup
	assert.True(t, item.IsPastIngest())
}

func TestMsgSkippingInProgress(t *testing.T) {
	item := SampleWorkItem()
	item.Id = 999
	item.Name = "bag1.tar"
	item.Node = "node1"
	item.Pid = 1234
	timestamp, _ := time.Parse(time.RFC3339, "2017-05-22T17:10:00Z")
	item.UpdatedAt = timestamp
	expected := "Marking NSQ message for WorkItem 999 (bag1.tar) as finished without doing any work, because this item is currently in process by node node1, pid 1234. WorkItem was last updated at 2017-05-22 17:10:00 +0000 UTC."
	assert.Equal(t, expected, item.MsgSkippingInProgress())
}

func TestMsgPastIngest(t *testing.T) {
	item := SampleWorkItem()
	item.Id = 999
	item.Name = "bag1.tar"
	expected := "Marking NSQ Message for WorkItem 999 (bag1.tar) as finished without doing any work, because this item is already past the ingest phase."
	assert.Equal(t, expected, item.MsgPastIngest())
}

func TestMsgAlreadyOnDisk(t *testing.T) {
	item := SampleWorkItem()
	item.Name = "bag1.tar"
	expected := "Bag bag1.tar is already on disk and appears to be complete."
	assert.Equal(t, expected, item.MsgAlreadyOnDisk())
}

func TestMsgAlreadyValidated(t *testing.T) {
	item := SampleWorkItem()
	item.Name = "bag1.tar"
	expected := "Bag bag1.tar has already been validated. Now it's going to the cleanup channel."
	assert.Equal(t, expected, item.MsgAlreadyValidated())
}

func TestMsgGoingToValidation(t *testing.T) {
	item := SampleWorkItem()
	item.Name = "bag1.tar"
	expected := "Bag bag1.tar is going to the validation channel."
	assert.Equal(t, expected, item.MsgGoingToValidation())
}
