package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"net/url"
	"strconv"
	"time"
)

type APTSpotTestRestore struct {
	Context          *context.Context
	CreatedBefore    time.Time
	NotRestoredSince time.Time
	MaxSize          int64
	DryRun           bool
}

// NewAPTSpotTestRestore creates a new restore spot test worker.
// This is meant to run as a cron job.
//
// Param maxSize tells the worker to choose bags no larger than
// maxSize for restoration. Param createdBefore means choose bags
// created before this date. Param notRestoredSince means choose
// bags that have not been restored since this date (which helps
// prevent us restoring the same bag again and again).
func NewAPTSpotTestRestore(_context *context.Context, maxSize int64, createdBefore, notRestoredSince time.Time) *APTSpotTestRestore {

	// Patch for https://trello.com/c/Ep4pKzZB
	err := CacheBucketNames(_context)
	if err != nil {
		panic(fmt.Sprintf("Cannot cache bucket names from Pharos: %v", err))
	}

	return &APTSpotTestRestore{
		Context:          _context,
		CreatedBefore:    createdBefore,
		NotRestoredSince: notRestoredSince,
		MaxSize:          maxSize,
		DryRun:           false,
	}
}

// Run runs the spot test by choosing ONE bag from each institution
// that matches the specified criteria (smaller than maxSize, createdBefore
// the specified date and notRestoredSince the specified date).
// It creates a Restore WorkItem for each bag, and returns the WorkItems
// it created. The caller can get the WorkItem.Id and object identifier from there.
func (restoreTest *APTSpotTestRestore) Run() ([]*models.WorkItem, error) {
	restoreTest.logFacts()
	workItems := make([]*models.WorkItem, 0)
	institutions, err := restoreTest.GetInstitutions()
	if err != nil {
		return workItems, fmt.Errorf("Error getting list of institutions from Pharos: %v", err)
	}
	for _, inst := range institutions {
		if inst.Identifier == "aptrust.org" {
			restoreTest.Context.MessageLog.Info("Skipping aptrust.org")
			continue
		}
		restoreTest.Context.MessageLog.Info("Looking up objects for %s", inst.Identifier)
		obj, err := restoreTest.GetObjectFor(inst.Identifier)
		if err != nil {
			restoreTest.Context.MessageLog.Error(err.Error())
			continue
		}
		if obj == nil {
			restoreTest.Context.MessageLog.Info("No suitable objects for %s", inst.Identifier)
			continue
		}
		workItem, err := restoreTest.CreateWorkItem(obj)
		if err != nil {
			restoreTest.Context.MessageLog.Error("Error creating new Restore WorkItem for %s: %v",
				obj.Identifier, err)
			continue
		}
		workItems = append(workItems, workItem)
	}

	return workItems, nil
}

// logFacts logs our basic working parameters.
func (restoreTest *APTSpotTestRestore) logFacts() {
	restoreTest.Context.MessageLog.Info("MaxSize: %d, CreatedBefore: %s, NotRestoredSince: %s",
		restoreTest.MaxSize,
		restoreTest.CreatedBefore.Format(time.RFC3339),
		restoreTest.NotRestoredSince.Format(time.RFC3339))
	if restoreTest.DryRun {
		restoreTest.Context.MessageLog.Info("This is a DRY RUN, so no WorkItems will be created.")
	}
}

// GetInstitutions returns a list of all depositing institutions from Pharos.
func (restoreTest *APTSpotTestRestore) GetInstitutions() ([]*models.Institution, error) {
	resp := restoreTest.Context.PharosClient.InstitutionList(url.Values{})
	if resp.Error != nil {
		return nil, resp.Error
	}
	institutions := resp.Institutions()
	restoreTest.Context.MessageLog.Info("Got %d institutions from Pharos", len(institutions))
	return institutions, nil
}

// GetObjectFor returns the IntellectualObject we should restore for the
// specified institution. This object will be the first one we find in
// Pharos that matches all of the criteria, which include:
//
// institution - it belongs the specified institution
//
// createdBefore - it was created before the specified date
//
// notRestoredSince - it has not been restored since the specified date
//
// maxSize - it's total size is less than or equal to this many bytes
//
// state - is "A" for active
//
// access - is not "restricted"
func (restoreTest *APTSpotTestRestore) GetObjectFor(institution string) (*models.IntellectualObject, error) {
	var obj *models.IntellectualObject
	var err error
	hasMoreResults := true
	pageNumber := 1
	for hasMoreResults {
		obj, hasMoreResults, err = restoreTest.findOne(institution, pageNumber)
		if obj != nil || err != nil {
			break
		}
		pageNumber++
	}
	return obj, err
}

// findOne finds one IntellectualObject from the specified page of results
// that meets our criteria for restoration spot tests.
//
// Returns an IntellectualObject (or nil if none match our criteria), a boolean
// indicating whether Pharos has more results to fetch, and an error
// if there is one.
func (restoreTest *APTSpotTestRestore) findOne(institution string, pageNumber int) (*models.IntellectualObject, bool, error) {
	var selectedObject *models.IntellectualObject
	params := url.Values{}
	params.Set("institution", institution)
	params.Set("state", "A")
	params.Set("createdBefore", restoreTest.CreatedBefore.Format(time.RFC3339))
	params.Set("page", strconv.Itoa(pageNumber))
	params.Set("per_page", "100")
	resp := restoreTest.Context.PharosClient.IntellectualObjectList(params)
	if resp.Error != nil {
		return nil, false, resp.Error
	}
	objects := resp.IntellectualObjects()
	restoreTest.Context.MessageLog.Info("Found %d object candidates for %s", len(objects), institution)
	for _, obj := range objects {
		if obj.Access == "restricted" {
			restoreTest.Context.MessageLog.Info("Skipping %s: restricted", obj.Identifier)
			continue
		}
		if obj.FileSize == 0 {
			restoreTest.Context.MessageLog.Info("Skipping %s: FileSize zero seems incorrect",
				obj.Identifier)
			continue
		}
		if obj.FileSize > restoreTest.MaxSize {
			restoreTest.Context.MessageLog.Info("Skipping %s: size %d is greater than max %d",
				obj.Identifier, obj.FileSize, restoreTest.MaxSize)
			continue
		}
		hasCompletedRestore, err := restoreTest.HasCompletedRestore(obj.Identifier)
		if err != nil {
			restoreTest.Context.MessageLog.Warning("Error checking restore WorkItem for %s: %v",
				obj.Identifier, err)
			continue
		}
		if !hasCompletedRestore {
			restoreTest.Context.MessageLog.Info("Object %s meets all criteria", obj.Identifier)
			selectedObject = obj
			break
		} else {
			restoreTest.Context.MessageLog.Info("Object %s disqualified by recent restore", obj.Identifier)
		}
	}
	return selectedObject, resp.HasNextPage(), nil
}

// HasCompletedRestore returns true if the object with the specified identifier
// has been successfully restored since NotRestoredSince.
func (restoreTest *APTSpotTestRestore) HasCompletedRestore(objIdentifier string) (bool, error) {
	params := url.Values{}
	params.Set("object_identifier", objIdentifier)
	params.Set("action", constants.ActionRestore)
	params.Set("status", constants.StatusSuccess)
	params.Set("updated_after", restoreTest.NotRestoredSince.Format(time.RFC3339))
	params.Set("page", "1")
	params.Set("per_page", "1")
	restoreTest.Context.MessageLog.Info("Checking recent restorations for %s", objIdentifier)
	resp := restoreTest.Context.PharosClient.WorkItemList(params)
	if resp.Error != nil {
		return false, resp.Error
	}
	hasRestore := resp.WorkItem() != nil
	return hasRestore, nil
}

func (restoreTest *APTSpotTestRestore) GetLastIngestWorkItem(objIdentifier string) (*models.WorkItem, error) {
	params := url.Values{}
	params.Set("object_identifier", objIdentifier)
	params.Set("action", constants.ActionIngest)
	params.Set("status", constants.StatusSuccess)
	params.Set("sort", "date") // Sorts by date_processed desc
	params.Set("page", "1")
	params.Set("per_page", "1")
	resp := restoreTest.Context.PharosClient.WorkItemList(params)
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.WorkItem(), nil
}

// CreateWorkItem creates the Restore WorkItem for the specified object identifier.
func (restoreTest *APTSpotTestRestore) CreateWorkItem(obj *models.IntellectualObject) (*models.WorkItem, error) {
	lastIngestItem, err := restoreTest.GetLastIngestWorkItem(obj.Identifier)
	if err != nil {
		return nil, fmt.Errorf("Cannot find last ingest WorkItem for %s: %v", obj.Identifier, err)
	}
	if lastIngestItem == nil {
		return nil, fmt.Errorf("Last ingest WorkItem is missing for %s", obj.Identifier)
	}
	action := constants.ActionRestore
	if obj.StorageOption != constants.StorageStandard {
		action = constants.ActionGlacierRestore
	}
	workItem := &models.WorkItem{
		ObjectIdentifier: obj.Identifier,
		Name:             lastIngestItem.Name,
		Bucket:           lastIngestItem.Bucket,
		ETag:             lastIngestItem.ETag,
		Size:             obj.FileSize, // may have changed with file deletions or overwrites
		BagDate:          lastIngestItem.BagDate,
		Date:             time.Now().UTC(),
		InstitutionId:    lastIngestItem.InstitutionId,
		User:             "system@aptrust.org", // because this is an automated spot test
		Action:           action,
		Stage:            constants.StageRequested,
		Status:           constants.StatusPending,
		Outcome:          "Pending",
		Retry:            true,
		Note:             "Automated object restoration spot test created by system",
	}
	if !restoreTest.DryRun {
		restoreTest.Context.MessageLog.Info("Creating Restore WorkItem for %s", obj.Identifier)
		resp := restoreTest.Context.PharosClient.WorkItemSave(workItem)
		if resp.Error != nil {
			return nil, resp.Error
		}
		// Returned item includes Id.
		workItem = resp.WorkItem()
	}
	return workItem, nil
}
