package models_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/logger"
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
	actual := string(bytes)
	if actual != expected {
		t.Errorf("WorkItem.SerializeForFluctus expected:\n'%s'\nbut got:\n'%s'", expected, actual)
	}
}

func TestWorkItemHasBeenStored(t *testing.T) {
	workItem := models.WorkItem{
		Action: "Ingest",
		Stage: "Record",
		Status: "Success",
	}
	if workItem.HasBeenStored() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	workItem.Stage = constants.StageCleanup
	if workItem.HasBeenStored() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	workItem.Stage = constants.StageStore
	workItem.Status = constants.StatusPending
	if workItem.HasBeenStored() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	workItem.Stage = constants.StageStore
	workItem.Status = constants.StatusStarted
	if workItem.HasBeenStored() == true {
		t.Error("HasBeenStored() should have returned false")
	}
	workItem.Stage = constants.StageFetch
	if workItem.HasBeenStored() == true {
		t.Error("HasBeenStored() should have returned false")
	}
	workItem.Stage = constants.StageUnpack
	if workItem.HasBeenStored() == true {
		t.Error("HasBeenStored() should have returned false")
	}
	workItem.Stage = constants.StageValidate
	if workItem.HasBeenStored() == true {
		t.Error("HasBeenStored() should have returned false")
	}
}

func TestIsStoring(t *testing.T) {
	workItem := models.WorkItem{
		Action: "Ingest",
		Stage: "Store",
		Status: "Started",
	}
	if workItem.IsStoring() == false {
		t.Error("IsStoring() should have returned true")
	}
	workItem.Status = "Pending"
	if workItem.IsStoring() == true {
		t.Error("IsStoring() should have returned false")
	}
	workItem.Status = "Started"
	workItem.Stage = "Record"
	if workItem.IsStoring() == true {
		t.Error("IsStoring() should have returned false")
	}
}

func TestWorkItemShouldTryIngest(t *testing.T) {
	workItem := models.WorkItem{
		Action: "Ingest",
		Stage: "Receive",
		Status: "Pending",
		Retry: true,
	}

	// Test stages
	if workItem.ShouldTryIngest() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	workItem.Stage = "Fetch"
	if workItem.ShouldTryIngest() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	workItem.Stage = "Unpack"
	if workItem.ShouldTryIngest() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	workItem.Stage = "Validate"
	if workItem.ShouldTryIngest() == false {
		t.Error("HasBeenStored() should have returned true")
	}
	workItem.Stage = "Record"
	if workItem.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}

	// Test Store/Pending and Store/Started
	workItem.Stage = "Store"
	workItem.Status = "Started"
	if workItem.ShouldTryIngest() == true {
		t.Error("ShouldTryIngest() should have returned false")
	}

	workItem.Stage = "Store"
	workItem.Status = "Pending"
	if workItem.ShouldTryIngest() == true {
		t.Error("ShouldTryIngest() should have returned false")
	}

	// Test Retry = false
	workItem.Status = "Started"
	workItem.Retry = false

	workItem.Stage = "Receive"
	if workItem.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}

	workItem.Stage = "Fetch"
	if workItem.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}

	workItem.Stage = "Unpack"
	if workItem.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}

	workItem.Stage = "Validate"
	if workItem.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}

	workItem.Stage = "Record"
	if workItem.ShouldTryIngest() == true {
		t.Error("HasBeenStored() should have returned false")
	}
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
	if models.HasPendingDeleteRequest(workItems) == false {
		t.Error("HasPendingDeleteRequest() should have returned true")
	}
	workItems[2].Status = constants.StatusStarted
	if models.HasPendingDeleteRequest(workItems) == false {
		t.Error("HasPendingDeleteRequest() should have returned true")
	}
	workItems[2].Status = constants.StatusCancelled
	if models.HasPendingDeleteRequest(workItems) == true {
		t.Error("HasPendingDeleteRequest() should have returned false")
	}
}

func TestHasPendingRestoreRequest(t *testing.T) {
	workItems := getWorkItems(constants.ActionRestore)
	if models.HasPendingRestoreRequest(workItems) == false {
		t.Error("HasPendingRestoreRequest() should have returned true")
	}
	workItems[2].Status = constants.StatusStarted
	if models.HasPendingRestoreRequest(workItems) == false {
		t.Error("HasPendingRestoreRequest() should have returned true")
	}
	workItems[2].Status = constants.StatusCancelled
	if models.HasPendingRestoreRequest(workItems) == true {
		t.Error("HasPendingRestoreRequest() should have returned false")
	}
}

func TestHasPendingIngestRequest(t *testing.T) {
	workItems := getWorkItems(constants.ActionIngest)
	if models.HasPendingIngestRequest(workItems) == false {
		t.Error("HasPendingIngestRequest() should have returned true")
	}
	workItems[2].Status = constants.StatusStarted
	if models.HasPendingIngestRequest(workItems) == false {
		t.Error("HasPendingIngestRequest() should have returned true")
	}
	workItems[2].Status = constants.StatusCancelled
	if models.HasPendingIngestRequest(workItems) == true {
		t.Error("HasPendingIngestRequest() should have returned false")
	}
}

func TestSetNodePidState(t *testing.T) {
	item := SampleWorkItem()
	object := make(map[string]string)
	object["key"] = "value"

	discardLogger := logger.DiscardLogger("workitem_test")
	item.SetNodePidState(object, discardLogger)
	hostname, _ := os.Hostname()
	if hostname == "" {
		if item.Node != "hostname?" {
			t.Error("Expected 'hostname?' for node, but got '%s'", item.Node)
		} else if item.Node != hostname {
			t.Error("Expected Node '%s', got '%s'", hostname, item.Node)
		}
	}
	if item.Pid != os.Getpid() {
		t.Error("Expected Pid %d, got %d", os.Getpid(), item.Pid)
	}
	expectedState := "{\"key\":\"value\"}"
	if item.State != expectedState {
		t.Error("Expected State '%s', got '%s'", expectedState, item.State)
	}
}
