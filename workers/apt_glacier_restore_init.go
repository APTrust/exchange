package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	//	"github.com/APTrust/exchange/util"
	//	"github.com/APTrust/exchange/util/fileutil"
	//	"github.com/APTrust/exchange/util/storage"
	//	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"os"
	//	"strings"
	"time"
)

// Standard retrieval is 3-5 hours
const RETRIEVAL_OPTION = "Standard"

// Keep the files in S3 up to 5 days, in case we're
// having system problems and we need to attempt the
// restore multiple times.
const DAYS_TO_KEEP_IN_S3 = 5

// Requests that an object be restored from Glacier to S3. This is
// the first step toward restoring a Glacier-only bag.
type APTGlacierRestore struct {
	Context        *context.Context
	RequestChannel chan *models.GlacierRestoreState
	CleanupChannel chan *models.GlacierRestoreState
}

func NewGlacierRestore(_context *context.Context) *APTGlacierRestore {
	restorer := &APTGlacierRestore{
		Context: _context,
	}
	// Set up buffered channels
	restorerBufferSize := _context.Config.GlacierRestoreWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.GlacierRestoreWorker.Workers * 10
	restorer.RequestChannel = make(chan *models.GlacierRestoreState, restorerBufferSize)
	restorer.CleanupChannel = make(chan *models.GlacierRestoreState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.GlacierRestoreWorker.NetworkConnections; i++ {
		go restorer.requestRestore()
	}
	for i := 0; i < _context.Config.GlacierRestoreWorker.Workers; i++ {
		go restorer.cleanup()
	}
	return restorer
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (restorer *APTGlacierRestore) HandleMessage(message *nsq.Message) error {
	// TODO: Set up GlacierRestoreState
	workItem, err := GetWorkItem(message, restorer.Context)
	if err != nil {
		restorer.Context.MessageLog.Error(err.Error())
		return err
	}
	var state *models.GlacierRestoreState
	if workItem.WorkItemStateId != nil && *workItem.WorkItemStateId != 0 {
		workItemState, err := GetWorkItemState(workItem, restorer.Context, false)
		if err != nil {
			restorer.Context.MessageLog.Error(err.Error())
			return err
		}
		if workItemState != nil && workItemState.HasData() {
			state, err := workItemState.GlacierRestoreState()
			if err != nil {
				restorer.Context.MessageLog.Error(err.Error())
				return err
			}
			state.NSQMessage = message
			state.WorkItem = workItem
		}
	} else {
		state = models.NewGlacierRestoreState(message, workItem)
	}
	restorer.RequestChannel <- state
	return nil
}

func (restorer *APTGlacierRestore) requestRestore() {
	for state := range restorer.RequestChannel {
		state.WorkSummary.ClearErrors()
		state.WorkSummary.Attempted = true
		state.WorkSummary.AttemptNumber += 1
		state.WorkSummary.Start()
		// if WorkItem has a GenericFileIdentifier, this is a
		// single-file restore. Otherwise, it's an object restore.
		// Request retrieval from Glacier
		// Update GlacierRestoreState
		// Push to CleanupChannel
	}
}

func (restorer *APTGlacierRestore) cleanup() {
	//for restoreState := range restorer.RequestChannel {
	// Update WorkItem in Pharos
	// Push to NSQ's restoration channel for packaging, etc.
	//}
}

func (restorer *APTGlacierRestore) requestAllFiles(state *models.GlacierRestoreState) {
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
		restorer.requestFile(state, genericFile)
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
			restorer.requestFile(state, genericFile)
		}
	} else {
		state.WorkSummary.AddError("Cannot process WorkItem %d: no file identifier or object identifier.", state.WorkItem.Id)
		return
	}
}

func (restorer *APTGlacierRestore) requestFile(state *models.GlacierRestoreState, gf *models.GenericFile) {
	details, err := restorer.getRequestDetails(gf)
	if err != nil {
		state.WorkSummary.AddError(err.Error())
		return
	}

	glacierRestoreRequest := restorer.getRequestRecord(state, gf, details)
	if glacierRestoreRequest == nil {
		// Prior request was accepted and is in progress
		return
	}
	restorer.initializeRetrieval(state, gf, details, glacierRestoreRequest)
}

func (restorer *APTGlacierRestore) getRequestDetails(gf *models.GenericFile) (map[string]string, error) {
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

func (restorer *APTGlacierRestore) getRequestRecord(state *models.GlacierRestoreState, gf *models.GenericFile, details map[string]string) *models.GlacierRestoreRequest {
	glacierRestoreRequest := state.FindRequest(gf.Identifier)
	if glacierRestoreRequest != nil {
		if glacierRestoreRequest.RequestAccepted {
			restorer.Context.MessageLog.Info("Skipping %s: retrieval request was accepted earlier.", gf.Identifier)
			return nil
		} else {
			restorer.Context.MessageLog.Info("Retrying existing request for %s, which was not previously accepted", gf.Identifier)
		}
	} else {
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

func (restorer *APTGlacierRestore) initializeRetrieval(state *models.GlacierRestoreState, gf *models.GenericFile, details map[string]string, glacierRestoreRequest *models.GlacierRestoreRequest) {

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
	someoneElseRequested := false
	restoreClient.Restore()
	if restoreClient.ErrorMessage != "" {
		if restoreClient.RestoreAlreadyInProgress {
			// Although we checked for this above, this case can occur when
			// an outside service requests Glacier retrieval.
			restorer.Context.MessageLog.Info("Retrieval of %s was requested earlier (probably by someone else) and is already in progress.", gf.Identifier)
			someoneElseRequested = true
		} else {
			state.WorkSummary.AddError("Glacier retrieval request returned an error for %s at %s: %v", gf.Identifier, gf.URI, restoreClient.ErrorMessage)
			return
		}
	}

	// Update this info. It's a pointer, so it will be saved with GlacierRestoreState.
	glacierRestoreRequest.RequestAccepted = (restoreClient.ErrorMessage == "")
	glacierRestoreRequest.RequestedAt = now
	glacierRestoreRequest.EstimatedDeletionFromS3 = estimatedDeletionFromS3
	glacierRestoreRequest.SomeoneElseRequested = someoneElseRequested
}