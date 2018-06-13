package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/nsqio/go-nsq"
	"os"
	"time"
)

// TODO: Move constants to config file?

// Standard retrieval is 3-5 hours
const RETRIEVAL_OPTION = "Standard"

// Keep the files in S3 up to 5 days, in case we're
// having system problems and we need to attempt the
// restore multiple times.
const DAYS_TO_KEEP_IN_S3 = 5

// Requests that an object be restored from Glacier to S3. This is
// the first step toward restoring a Glacier-only bag.
type APTGlacierRestoreInit struct {
	// Context includes logging, config, network connections, and
	// other general resources for the worker.
	Context *context.Context
	// RequestChannel is for requesting an item be moved from Glacier
	// into S3.
	RequestChannel chan *models.GlacierRestoreState
	// CleanupChannel is for housekeeping, like updating NSQ.
	CleanupChannel chan *models.GlacierRestoreState
	// S3Url is a custom URL that the S3 client should connect to.
	// We use this only in testing, when we want the client to talk
	// to a local test server. This should not be set in demo or
	// production.
	S3Url string
}

func NewGlacierRestore(_context *context.Context) *APTGlacierRestoreInit {
	restorer := &APTGlacierRestoreInit{
		Context: _context,
	}
	// Set up buffered channels
	restorerBufferSize := _context.Config.GlacierRestoreWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.GlacierRestoreWorker.Workers * 10
	restorer.RequestChannel = make(chan *models.GlacierRestoreState, restorerBufferSize)
	restorer.CleanupChannel = make(chan *models.GlacierRestoreState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.GlacierRestoreWorker.NetworkConnections; i++ {
		go restorer.RequestRestore()
	}
	for i := 0; i < _context.Config.GlacierRestoreWorker.Workers; i++ {
		go restorer.Cleanup()
	}
	return restorer
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (restorer *APTGlacierRestoreInit) HandleMessage(message *nsq.Message) error {
	workItem, err := GetWorkItem(message, restorer.Context)
	if err != nil {
		restorer.Context.MessageLog.Error(err.Error())
		return err
	}
	state, err := restorer.GetGlacierRestoreState(message, workItem)
	if err != nil {
		restorer.Context.MessageLog.Error("Error getting WorkItemState for WorkItem %d: %s",
			workItem.Id, err.Error())
		return err
	}
	restorer.RequestChannel <- state
	return nil
}

func (restorer *APTGlacierRestoreInit) GetGlacierRestoreState(message *nsq.Message, workItem *models.WorkItem) (*models.GlacierRestoreState, error) {
	state := models.NewGlacierRestoreState(message, workItem)
	if workItem.WorkItemStateId != nil && *workItem.WorkItemStateId != 0 {
		workItemState, err := GetWorkItemState(workItem, restorer.Context, false)
		if err != nil {
			return nil, err
		}
		if workItemState != nil && workItemState.HasData() {
			state, err = workItemState.GlacierRestoreState()
			if err != nil {
				return nil, err
			}
			state.NSQMessage = message
			state.WorkItem = workItem
		}
	}
	return state, nil
}

func (restorer *APTGlacierRestoreInit) RequestRestore() {
	for state := range restorer.RequestChannel {
		state.WorkSummary.ClearErrors()
		state.WorkSummary.Attempted = true
		state.WorkSummary.AttemptNumber += 1
		state.WorkSummary.Start()

		if state.WorkItem.GenericFileIdentifier != "" {
			gf, err := restorer.GetGenericFile(state)
			if err != nil {
				state.WorkSummary.AddError(err.Error())
				restorer.CleanupChannel <- state
				continue
			}
			state.GenericFile = gf
			restorer.RequestFile(state, gf)
		} else {
			restorer.RequestObject(state)
		}

		state.WorkSummary.Finish()
		restorer.CleanupChannel <- state
	}
}

func (restorer *APTGlacierRestoreInit) RequestObject(state *models.GlacierRestoreState) {
	obj, err := restorer.GetIntellectualObject(state)
	if err != nil {
		state.WorkSummary.AddError(err.Error())
		return
	}
	state.IntellectualObject = obj
	for _, gf := range obj.GenericFiles {
		needsRestoreRequest, err := restorer.RestoreRequestNeeded(state, gf)
		if err != nil {
			state.WorkSummary.AddError(err.Error())
			continue
		}
		if needsRestoreRequest {
			restorer.RequestFile(state, gf)
		}
	}
}

func (restorer *APTGlacierRestoreInit) RestoreRequestNeeded(state *models.GlacierRestoreState, gf *models.GenericFile) (bool, error) {
	needsRestoreRequest := false
	s3Client, err := restorer.GetS3HeadClient(gf.StorageOption)
	if err != nil {
		return needsRestoreRequest, err
	}
	fileUUID, err := gf.PreservationStorageFileName()
	if err != nil {
		return needsRestoreRequest, err
	}
	s3Client.Head(fileUUID)
	if s3Client.ErrorMessage != "" {
		err = fmt.Errorf("S3 HEAD request for file %s (%s) returned error: %s",
			fileUUID, gf.Identifier, s3Client.ErrorMessage)
		return needsRestoreRequest, err
	}
	restoreRequestInfo, err := s3Client.GetRestoreRequestInfo()
	if err != nil {
		return needsRestoreRequest, err
	}

	glacierRestoreRequest := state.FindRequest(gf.Identifier)
	if glacierRestoreRequest == nil {
		glacierRestoreRequest = &models.GlacierRestoreRequest{
			GenericFileIdentifier: gf.Identifier,
			GlacierBucket:         s3Client.BucketName,
			GlacierKey:            fileUUID,
		}
		state.Requests = append(state.Requests, glacierRestoreRequest)
	}

	if restoreRequestInfo.RequestInProgress {
		// Log and go on
		restorer.Context.MessageLog.Info("Already in progress: %s (%s/%s)",
			gf.Identifier, s3Client.BucketName, fileUUID)
	} else if restoreRequestInfo.RequestIsComplete {
		// Log and update expiry date
		glacierRestoreRequest.IsAvailableInS3 = true
		glacierRestoreRequest.EstimatedDeletionFromS3 = restoreRequestInfo.S3ExpiryDate
		restorer.Context.MessageLog.Info("Already restored to S3: %s (%s/%s)",
			gf.Identifier, s3Client.BucketName, fileUUID)
	} else {
		// Not restored yet and not even requested.
		// We need to make a request for this now.
		restorer.Context.MessageLog.Info("Needs Glacier retrieval request: %s (%s/%s)",
			gf.Identifier, s3Client.BucketName, fileUUID)
		needsRestoreRequest = true
	}
	glacierRestoreRequest.LastChecked = time.Now().UTC()
	return needsRestoreRequest, nil
}

func (restorer *APTGlacierRestoreInit) GetS3HeadClient(storageOption string) (*network.S3Head, error) {
	region, bucket, err := restorer.Context.Config.StorageRegionAndBucketFor(storageOption)
	if err != nil {
		return nil, err
	}
	client := network.NewS3Head(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		region,
		bucket)
	// Hack for testing: Tell the client to talk to our own
	// local S3 test server, and clear the bucket name,
	// because that gets prepended to the URL.
	if restorer.S3Url != "" {
		restorer.Context.MessageLog.Warning("Setting S3 URL to %s. This should happen only in testing!",
			restorer.S3Url)
		client.SetSessionEndpoint(restorer.S3Url)
		client.BucketName = ""
	}
	return client, nil
}

func (restorer *APTGlacierRestoreInit) GetIntellectualObject(state *models.GlacierRestoreState) (*models.IntellectualObject, error) {
	// Get object with files (second param) but no events (third param)
	resp := restorer.Context.PharosClient.IntellectualObjectGet(state.WorkItem.ObjectIdentifier, true, false)
	if resp.Error != nil {
		return nil, resp.Error
	}
	obj := resp.IntellectualObject()
	if obj == nil {
		return nil, fmt.Errorf("Pharos returned nil for IntellectualObject %s",
			state.WorkItem.ObjectIdentifier)
	}
	return obj, nil
}

func (restorer *APTGlacierRestoreInit) GetGenericFile(state *models.GlacierRestoreState) (*models.GenericFile, error) {
	resp := restorer.Context.PharosClient.GenericFileGet(state.WorkItem.GenericFileIdentifier, false)
	if resp.Error != nil {
		return nil, resp.Error
	}
	gf := resp.GenericFile()
	if gf == nil {
		return nil, fmt.Errorf("Pharos returned nil for GenericFile %s",
			state.WorkItem.GenericFileIdentifier)
	}
	return gf, nil
}

func (restorer *APTGlacierRestoreInit) Cleanup() {
	for state := range restorer.RequestChannel {
		if state.WorkSummary.HasErrors() {
			restorer.FinishWithError(state)
		} else {
			gfIdentifiers := state.GetFileIdentifiers()
			report := state.GetReport(gfIdentifiers)
			if report.AllItemsInS3() {
				// Can finish this queue item and create a new
				// WorkItem with action = 'Restore'. From there,
				// it will go into the restore queue, where
				// the normal apt_restore worker can handle it.
				restorer.CreateRestoreWorkItem(state)
			} else if report.AllRetrievalsInitiated() {
				restorer.RequeueToCheckState(state)
			} else {
				// Not all restore requests accepted by Glacier.
				restorer.RequeueForAdditionalRequests(state)
			}
		}
		restorer.SaveWorkItemState(state)
		restorer.UpdateWorkItem(state)
	}
}

// updateWorkItem saves the updated WorkItem in Pharos
func (restorer *APTGlacierRestoreInit) UpdateWorkItem(state *models.GlacierRestoreState) {
	// By the time we call this, we've done as much as possible
	// with this WorkItem, and we're telling Pharos the state
	// of this task. One of the methods below should have set
	// all of the WorkItem properties before this is called.
	// Methods: finishWithError, requeueForAdditionalRequests,
	// reqeueToCheckState, createRestoreWorkItem.
	resp := restorer.Context.PharosClient.WorkItemSave(state.WorkItem)
	if resp.Error != nil {
		restorer.Context.MessageLog.Error("Error updating WorkItem %d: %v", state.WorkItem.Id, resp.Error)
	}
}

// saveWorkItemState saves a JSON representation of the GlacierRestoreState
// in Pharos' WorkItemState table. We do this primarily so an admin can
// review this info and trace evidence on problem cases. The WorkItemState
// JSON is visible on the WorkItem detail page of the Pharos UI.
func (restorer *APTGlacierRestoreInit) SaveWorkItemState(state *models.GlacierRestoreState) {
	if state.WorkItem == nil {
		restorer.Context.MessageLog.Warning("Can't set WorkItemState on nil WorkItem")
		return
	}
	jsonData, err := json.Marshal(state)
	if err != nil {
		msg := fmt.Sprintf(" Error converting GlacierRestoreState to JSON for "+
			"WorkItemState (WorkItem %d): %v", state.WorkItem.Id, err)
		restorer.Context.MessageLog.Error(msg)
		state.WorkItem.Note += msg
		return
	}
	workItemState := models.NewWorkItemState(state.WorkItem.Id, constants.ActionGlacierRestore, string(jsonData))
	resp := restorer.Context.PharosClient.WorkItemStateSave(workItemState)
	if resp.Error != nil {
		msg := fmt.Sprintf("Error saving WorkItemState for WorkItem %d: %v", state.WorkItem.Id, err)
		restorer.Context.MessageLog.Error(msg)
		state.WorkItem.Note += msg
	}
}

func (restorer *APTGlacierRestoreInit) FinishWithError(state *models.GlacierRestoreState) {
	errMessage := state.WorkSummary.AllErrorsAsString()
	workItemId := 0
	if state.WorkItem != nil {
		workItemId = state.WorkItem.Id
	}
	restorer.Context.MessageLog.Error("Error processing WorkItem %d: %s", workItemId, errMessage)
	state.WorkItem.Note = errMessage
	state.WorkItem.Status = constants.StatusFailed
	state.WorkItem.Retry = false
	state.WorkItem.NeedsAdminReview = true
	state.NSQMessage.Finish()
}

// requeueForAdditionalRequests: We call this when we know we didn't
// issue Glacier restore requests for some of the files we'll need to
// restore (or maybe we issued the requests, but AWS/Glacier didn't
// accept them). In this case, we put the item back in the current
// queue and reprocess it, requesting Glacier-to-S3 restoration for
// any files still needing to be restored. We can requeue with a
// one-minute timeout.
func (restorer *APTGlacierRestoreInit) RequeueForAdditionalRequests(state *models.GlacierRestoreState) {
	restorer.Context.MessageLog.Error("Requeueing WorkItem %d: Needs additional Glacier restore requests.",
		state.WorkItem.Id)
	state.WorkItem.Note = "Requeued to make additional Glacier restore requests."
	// Don't revert status to Pending, or this may get queued
	// again by apt_queue.
	state.WorkItem.Status = constants.StatusStarted
	state.WorkItem.Retry = true
	state.WorkItem.NeedsAdminReview = false
	state.NSQMessage.Requeue(1 * time.Minute)
}

// requeueToCheckState: We call this when we know we've requested
// Glacier-to-S3 restoration of all required files, and those requests
// have all been accepted.
// It typically takes 3-5 hours to get all the
// files into S3.
func (restorer *APTGlacierRestoreInit) RequeueToCheckState(state *models.GlacierRestoreState) {
	restorer.Context.MessageLog.Error("Requeueing WorkItem %d to check on restoration progress: "+
		"All restore requests accepted.", state.WorkItem.Id)
	state.WorkItem.Note = "Requeued to check on status of Glacier restore requests."
	state.WorkItem.Status = constants.StatusStarted
	state.WorkItem.Retry = true
	state.WorkItem.NeedsAdminReview = false
	state.NSQMessage.Requeue(2 * time.Hour)
}

// createRestoreWorkItem: We call this to create a normal WorkItem
// with action='Restore 'when we know all files have been restored
// from Glacier to S3. Once all files are in S3, the apt_restore
// process can follow the normal S3 restoration process. So we'll
// close out this WorkItem and open a new one, which will go into
// the apt_restore queue.
func (restorer *APTGlacierRestoreInit) CreateRestoreWorkItem(state *models.GlacierRestoreState) {
	restorer.Context.MessageLog.Info("Files for WorkItem %d are all in S3.", state.WorkItem.Id)
	newWorkItem := &models.WorkItem{}
	newWorkItem.ObjectIdentifier = state.WorkItem.ObjectIdentifier
	newWorkItem.GenericFileIdentifier = state.WorkItem.GenericFileIdentifier
	newWorkItem.Name = state.WorkItem.Name
	newWorkItem.Bucket = state.WorkItem.Bucket
	newWorkItem.ETag = state.WorkItem.ETag
	newWorkItem.Size = state.WorkItem.Size
	newWorkItem.BagDate = state.WorkItem.BagDate
	newWorkItem.InstitutionId = state.WorkItem.InstitutionId
	newWorkItem.User = state.WorkItem.User
	newWorkItem.Action = constants.ActionRestore
	newWorkItem.Stage = constants.StageRequested
	newWorkItem.Status = constants.StatusPending
	newWorkItem.Retry = true
	resp := restorer.Context.PharosClient.WorkItemSave(newWorkItem)
	if resp.Error != nil {
		restorer.Context.MessageLog.Error("WorkItem %d: Error creating new Restore WorkItem",
			state.WorkItem.Id, resp.Error)
		state.WorkItem.Note = fmt.Sprintf("All files have been restored from Glacier to S3, "+
			"but received the following error from Pharos when trying to create a new "+
			"Restore WorkItem to finish the restoration job: %v", resp.Error)
		state.WorkItem.Status = constants.StatusFailed
	} else {
		newSavedWorkItem := resp.WorkItem()
		state.WorkItem.Note = fmt.Sprintf("All files have been moved from Glacier to S3. "+
			"Created new WorkItem #%d to finish restoration.", newSavedWorkItem.Id)
		state.WorkItem.Status = constants.StatusSuccess
	}
	state.NSQMessage.Finish()
}

func (restorer *APTGlacierRestoreInit) RequestAllFiles(state *models.GlacierRestoreState) {
	if state.WorkItem.GenericFileIdentifier != "" {
		gfIdentifier := state.WorkItem.GenericFileIdentifier
		resp := restorer.Context.PharosClient.GenericFileGet(gfIdentifier, false)
		if resp.Error != nil {
			state.WorkSummary.AddError("Error getting GenericFile %s from Pharos: %v", gfIdentifier, resp.Error)
			return
		}
		genericFile := resp.GenericFile()
		if genericFile == nil {
			state.WorkSummary.AddError("Pharos returned nil for GenericFile %s", gfIdentifier)
			return
		}
		restorer.RequestFile(state, genericFile)
	} else if state.WorkItem.ObjectIdentifier != "" {
		objIdentifier := state.WorkItem.ObjectIdentifier
		resp := restorer.Context.PharosClient.IntellectualObjectGet(objIdentifier, true, false)
		if resp.Error != nil {
			state.WorkSummary.AddError("Error getting IntellectualObject %s from Pharos: %v", objIdentifier, resp.Error)
			return
		}
		obj := resp.IntellectualObject()
		if obj == nil {
			state.WorkSummary.AddError("Pharos returned nil for IntellectualObject %s", objIdentifier)
			return
		}
		restorer.Context.MessageLog.Info("Object %s has %d files", obj.Identifier, len(obj.GenericFiles))
		for _, genericFile := range obj.GenericFiles {
			restorer.RequestFile(state, genericFile)
		}
	} else {
		state.WorkSummary.AddError("Cannot process WorkItem %d: no file identifier or object identifier.", state.WorkItem.Id)
		return
	}
}

func (restorer *APTGlacierRestoreInit) RequestFile(state *models.GlacierRestoreState, gf *models.GenericFile) {
	details, err := restorer.GetRequestDetails(gf)
	if err != nil {
		state.WorkSummary.AddError(err.Error())
		return
	}

	glacierRestoreRequest := restorer.GetRequestRecord(state, gf, details)
	if glacierRestoreRequest.RequestAccepted {
		// Prior request was accepted and is in progress.
		if glacierRestoreRequest.IsAvailableInS3 {
			restorer.Context.MessageLog.Info("Skipping %s: item is already in S3.", gf.Identifier)
		} else {
			restorer.Context.MessageLog.Info("Skipping %s: retrieval request was accepted earlier.", gf.Identifier)
		}
	} else {
		// Make a note if we're re-attempting.
		if !glacierRestoreRequest.RequestedAt.IsZero() {
			restorer.Context.MessageLog.Info("File %s (%s/%s) was requested from Glacier at %s, "+
				"but that request was not accepted. Trying again.",
				gf.Identifier, details["bucket"], details["fileUUID"],
				glacierRestoreRequest.RequestedAt.Format(time.RFC3339))
		}
		restorer.InitializeRetrieval(state, gf, details, glacierRestoreRequest)
	}
}

// This returns the info we'll need to ask AWS to move
// the file from Glacier to S3.
func (restorer *APTGlacierRestoreInit) GetRequestDetails(gf *models.GenericFile) (map[string]string, error) {
	details := make(map[string]string)
	fileUUID, err := gf.PreservationStorageFileName()
	if err != nil {
		return nil, fmt.Errorf("File %s: %v. URI is %s", gf.Identifier, err, gf.URI)
	}
	details["fileUUID"] = fileUUID
	details["region"] = restorer.Context.Config.GlacierRegionVA
	details["bucket"] = restorer.Context.Config.GlacierBucketVA
	if gf.StorageOption == constants.StorageGlacierOH {
		details["region"] = restorer.Context.Config.GlacierRegionOH
		details["bucket"] = restorer.Context.Config.GlacierBucketOH
	} else if gf.StorageOption == constants.StorageGlacierOR {
		details["region"] = restorer.Context.Config.GlacierRegionOR
		details["bucket"] = restorer.Context.Config.GlacierBucketOR
	} else {
		return nil, fmt.Errorf("Cannot restore file %s because StorageOption is %s", gf.Identifier, gf.StorageOption)
	}
	return details, nil
}

func (restorer *APTGlacierRestoreInit) GetRequestRecord(state *models.GlacierRestoreState, gf *models.GenericFile, details map[string]string) *models.GlacierRestoreRequest {
	glacierRestoreRequest := state.FindRequest(gf.Identifier)
	if glacierRestoreRequest == nil {
		restorer.Context.MessageLog.Info("Creating new request for %s", gf.Identifier)
		glacierRestoreRequest = &models.GlacierRestoreRequest{
			GenericFileIdentifier: gf.Identifier,
			GlacierBucket:         details["bucket"],
			GlacierKey:            details["fileUUID"],
			RequestAccepted:       false,
			SomeoneElseRequested:  false,
		}
		state.Requests = append(state.Requests, glacierRestoreRequest)
	}
	return glacierRestoreRequest
}

func (restorer *APTGlacierRestoreInit) InitializeRetrieval(state *models.GlacierRestoreState, gf *models.GenericFile, details map[string]string, glacierRestoreRequest *models.GlacierRestoreRequest) {

	restorer.Context.MessageLog.Info("Requesting Glacier retrieval of %s at %s (%s)",
		gf.Identifier, gf.URI, gf.StorageOption)

	restoreClient := network.NewS3Restore(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		details["region"],
		details["bucket"],
		details["fileUUID"],
		RETRIEVAL_OPTION,
		DAYS_TO_KEEP_IN_S3)
	now := time.Now().UTC()
	estimatedDeletionFromS3 := now.AddDate(0, 0, DAYS_TO_KEEP_IN_S3)
	restoreClient.Restore()
	if restoreClient.ErrorMessage != "" {
		state.WorkSummary.AddError("Glacier retrieval request returned an error for %s at %s: %v",
			gf.Identifier, gf.URI, restoreClient.ErrorMessage)
	}

	// Update this info. It's a pointer, so it will be saved with GlacierRestoreState.
	glacierRestoreRequest.RequestAccepted = (restoreClient.ErrorMessage == "")
	glacierRestoreRequest.RequestedAt = now
	glacierRestoreRequest.EstimatedDeletionFromS3 = estimatedDeletionFromS3

	// If we're requesting this now, it's because we think
	// we haven't requested it yet. But if it's already in
	// progress, someone else (or some other process) must
	// have requested the restoration. Do we still need to
	// track this bit of info? Do we care who requested it?
	glacierRestoreRequest.SomeoneElseRequested = restoreClient.RestoreAlreadyInProgress
}
