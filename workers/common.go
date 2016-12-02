package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nsqio/go-nsq"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Creates and returns an NSQ consumer for a worker process.
func CreateNsqConsumer(config *models.Config, workerConfig *models.WorkerConfig) (*nsq.Consumer, error) {
	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", workerConfig.MaxInFlight)
	nsqConfig.Set("heartbeat_interval", workerConfig.HeartbeatInterval)
	nsqConfig.Set("max_attempts", workerConfig.MaxAttempts)
	nsqConfig.Set("read_timeout", workerConfig.ReadTimeout)
	nsqConfig.Set("write_timeout", workerConfig.WriteTimeout)
	nsqConfig.Set("msg_timeout", workerConfig.MessageTimeout)
	return nsq.NewConsumer(workerConfig.NsqTopic, workerConfig.NsqChannel, nsqConfig)
}

// Set up the basic pieces of data we'll need to process a request.
// Param initIfEmpty says we should initialize an IntellectualObject
// if we can't find one in the IngestManifest. That should only happen
// in apt_fetcher, where we're often fetching new bags that Pharos has
// never seen before. All other workers should pass in false for initIfEmpty.
func GetIngestState(message *nsq.Message, _context *context.Context, initIfEmpty bool) (*models.IngestState, error) {
	workItem, err := GetWorkItem(message, _context)
	if err != nil {
		return nil, err
	}
	_context.MessageLog.Info("Loaded WorkItem %d (%s/%s)",
		workItem.Id, workItem.Bucket, workItem.Name)

	workItemState, err := GetWorkItemState(workItem, _context, initIfEmpty)
	if err != nil {
		return nil, err
	}

	_context.MessageLog.Info("Loaded WorkItemState for WorkItem %d (%s/%s)",
		workItem.Id, workItem.Bucket, workItem.Name)

	// We expect an empty IngestManifest when we are making the
	// first attempt to ingest a new bag, so let's not throw an
	// error below if we happen to run into this condition.
	expectingEmptyManifest := (workItemState.Id == 0 && initIfEmpty == false)

	ingestManifest, err := workItemState.IngestManifest()
	if err != nil && !expectingEmptyManifest {
		_context.MessageLog.Error(
			"Error unmarshalling IngestManifest for WorkItem %d (%s/%s): %v",
			workItem.Id, workItem.Bucket, workItem.Name)
		return nil, err
	}
	ingestState := &models.IngestState{
		NSQMessage:     message,
		WorkItem:       workItem,
		WorkItemState:  workItemState,
		IngestManifest: ingestManifest,
	}

	// If this is a new WorkItemState, we didn't load it from Pharos,
	// and we have no IntelObj data. So set the basic IntelObj data now.
	// This should ONLY happen in the apt_fetcher. All other processes
	// MUST have an IntellectualObject to proceed.
	if workItemState.Id == 0 {
		if initIfEmpty {
			SetBasicObjectInfo(ingestState, _context)
		} else {
			return ingestState, fmt.Errorf("IngestState is missing IntellectualObject.")
		}
	}

	_context.MessageLog.Info("Loaded IngestState for WorkItem %d (%s/%s)",
		workItem.Id, workItem.Bucket, workItem.Name)

	return ingestState, err
}

// Gets the WorkItem with the specified Id from Pharos.
func GetWorkItem(message *nsq.Message, _context *context.Context) (*models.WorkItem, error) {
	workItemId, err := strconv.Atoi(string(message.Body))
	if err != nil {
		return nil, fmt.Errorf("Could not get WorkItemId from NSQ message body: %v", err)
	}
	resp := _context.PharosClient.WorkItemGet(workItemId)
	if resp.Error != nil {
		return nil, fmt.Errorf("Error getting WorkItem %d from Pharos: %v", workItemId, resp.Error)
	}
	workItem := resp.WorkItem()
	if workItem == nil {
		return nil, fmt.Errorf("Pharos returned nil for WorkItem %d", workItemId)
	}
	return workItem, nil
}

// Gets the WorkItemState associated with the specified WorkItem from Pharos.
// Param initIfEmpty should be true ONLY when calling from apt_fetcher, which
// is working with objects that are not yet in the system.
func GetWorkItemState(workItem *models.WorkItem, _context *context.Context, initIfEmpty bool) (*models.WorkItemState, error) {
	var workItemState *models.WorkItemState
	var err error
	workItemStateId := 0
	if workItem.WorkItemStateId != nil {
		workItemStateId = *workItem.WorkItemStateId
	}
	resp := _context.PharosClient.WorkItemStateGet(workItemStateId)
	if resp.Response.StatusCode == http.StatusNotFound {
		if initIfEmpty {
			// Record has not been created yet, so build a new one now.
			workItemState, err = InitWorkItemState(workItem)
			if err != nil {
				return nil, err
			}
		} else {
			// Not found and we're not supposed to init. That's trouble.
			// It means we're being called from some worker other than
			// apt_fetcher, and those workers require that a WorkItemState
			// record exist.
			return nil, fmt.Errorf("HTTP 404. Pharos has no WorkItemState with WorkItemState id %d", workItem.WorkItemStateId)
		}
	} else if resp.Error != nil {
		// We got some other 4xx/5xx error from the Pharos REST service.
		return nil, fmt.Errorf("Error getting WorkItemState for WorkItem %d from Pharos: %v", workItem.Id, resp.Error)
	} else {
		// We didn't get a 404 or any other error. The WorkItemState should be in
		// the response.
		workItemState = resp.WorkItemState()
		if workItemState == nil {
			return nil, fmt.Errorf("Pharos returned nil for WorkItemState with WorkItemState id %d", workItem.WorkItemStateId)
		}
	}
	return workItemState, nil
}

// This is used only by apt_fetcher, when we're working on a brand new
// ingest bag that doesn't yet have a WorkItemState record.
func InitWorkItemState(workItem *models.WorkItem) (*models.WorkItemState, error) {
	ingestManifest := models.NewIngestManifest()
	ingestManifest.WorkItemId = workItem.Id
	ingestManifest.S3Bucket = workItem.Bucket
	ingestManifest.S3Key = workItem.Name
	ingestManifest.ETag = workItem.ETag
	workItemState := models.NewWorkItemState(workItem.Id, constants.ActionIngest, "")
	err := workItemState.SetStateFromIngestManifest(ingestManifest)
	if err != nil {
		return nil, err
	}
	return workItemState, nil
}

// This is really only used by apt_fetcher to initialize some barebones data on
// an empty IntellectualObject. This is only ever called during the fetch stage.
func SetBasicObjectInfo(ingestState *models.IngestState, _context *context.Context) {
	// instIdentifier is, e.g., virginia.edu, ncsu.edu, etc.
	// We'll download the tar file from the receiving bucket to
	// something like /mnt/apt/data/virginia.edu/name_of_bag.tar
	// See IngestTarFilePath below.
	obj := ingestState.IngestManifest.Object
	instIdentifier := util.OwnerOf(ingestState.IngestManifest.S3Bucket)
	obj.BagName = util.CleanBagName(ingestState.IngestManifest.S3Key)
	obj.Institution = instIdentifier
	obj.InstitutionId = ingestState.WorkItem.InstitutionId
	obj.IngestS3Bucket = ingestState.IngestManifest.S3Bucket
	obj.IngestS3Key = ingestState.IngestManifest.S3Key
	obj.IngestTarFilePath = filepath.Join(
		_context.Config.TarDirectory,
		instIdentifier, ingestState.IngestManifest.S3Key)

	// If this IntellectualObject was created by our validator and VirtualBag,
	// the identifier will be the bag name (minus the .tar extension).
	// That's fine for cases where depositors or other organizations are
	// using the validator outside of APTrust's repository environment, but
	// APTrust requires that we add the Institution name and a slash to
	// the beginning of the identifier. So make sure it's there, and propagate
	// the change all the way down to the GenericFiles.
	if !strings.HasPrefix(obj.Identifier, obj.Institution+"/") {
		obj.Identifier = fmt.Sprintf("%s/%s", obj.Institution, obj.Identifier)
		for _, gf := range obj.GenericFiles {
			if !strings.HasPrefix(gf.Identifier, obj.Identifier) {
				gf.IntellectualObjectIdentifier = obj.Identifier
				gf.Identifier = fmt.Sprintf("%s/%s", obj.Institution, gf.Identifier)
			}
		}
	}
}

// Record the WorkItemState for this task. We drop a copy into our
// JSON log as a backup, and updated the WorkItemState in Pharos,
// so the next worker knows what to do with this item.
//
// Param activeResult will change, depending on what stage of processing
// we're in. It could be the IngestState.FetchResult, IngestState.RecordResult,
// etc.
func RecordWorkItemState(ingestState *models.IngestState, _context *context.Context, activeResult *models.WorkSummary) {
	// Serialize the IngestManifest to JSON, and stuff it into the
	// WorkItemState.State. Subsequent workers need this info to
	// store the object's files in S3 and Glacier, and to record
	// results in Pharos.
	err := ingestState.WorkItemState.SetStateFromIngestManifest(ingestState.IngestManifest)
	if err != nil {
		// If we couldn't serialize the IngestManifest, subsequent workers
		// won't have the info they need to process this bag. We'll have to
		// requeue this item and start all over.
		_context.MessageLog.Error(err.Error())
		activeResult.AddError("Could not convert Ingest Manifest "+
			"to JSON. This item will have to be re-processed. Error was: %v", err)
	} else {
		// OK. We serialized the IngestManifest. Dump a copy into the
		// file system for backup and troubleshooting, and send a copy
		// over to Pharos, so the next worker in the chain (the save worker)
		// can access it.
		LogJson(ingestState, _context.JsonLog)
		resp := _context.PharosClient.WorkItemStateSave(ingestState.WorkItemState)
		if resp.Error != nil {
			// Could not send a copy of the WorkItemState to Pharos.
			// That means subsequent workers won't have the info they
			// need to work on this bag. We'll have to start processing
			// all over again.
			_context.MessageLog.Error(resp.Error.Error())
			activeResult.AddError("Could not save WorkItemState "+
				"to Pharos. This item will have to be re-processed. Error was: %v", resp.Error)
		} else {
			// Saved to Pharos!
			_context.MessageLog.Info("Saved WorkItemState for WorkItem %d (%s/%s) to Pharos",
				ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
				ingestState.WorkItem.Name)
			ingestState.WorkItemState = resp.WorkItemState()
		}
	}
}

// Tell Pharos that this item failed processing due to a fatal error.
func MarkWorkItemFailed(ingestState *models.IngestState, _context *context.Context) error {
	_context.MessageLog.Info("Telling Pharos processing failed for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Node = ""
	ingestState.WorkItem.Pid = 0
	ingestState.WorkItem.StageStartedAt = nil
	ingestState.WorkItem.Retry = false
	ingestState.WorkItem.NeedsAdminReview = true
	ingestState.WorkItem.Status = constants.StatusFailed
	ingestState.WorkItem.Note = "Processing failed. " + ingestState.IngestManifest.AllErrorsAsString()
	resp := _context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		_context.MessageLog.Error("Could not mark WorkItem failed for %s/%s: %v",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}

// Tell Pharos that this item has been requeued due to transient errors.
func MarkWorkItemRequeued(ingestState *models.IngestState, _context *context.Context) error {
	_context.MessageLog.Info("Telling Pharos we are requeueing %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Node = ""
	ingestState.WorkItem.Pid = 0
	ingestState.WorkItem.StageStartedAt = nil
	ingestState.WorkItem.Retry = true
	ingestState.WorkItem.NeedsAdminReview = false
	ingestState.WorkItem.Status = constants.StatusStarted
	ingestState.WorkItem.Note = "Item has been requeued due to transient errors. " +
		ingestState.IngestManifest.AllErrorsAsString()
	resp := _context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		_context.MessageLog.Error("Could not mark WorkItem requeued for %s/%s: %v",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}

// Tell Pharos that we've started work on this item.
func MarkWorkItemStarted(ingestState *models.IngestState, _context *context.Context, stage, message string) error {
	_context.MessageLog.Info("Telling Pharos we're starting %s for %s/%s",
		stage, ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	utcNow := time.Now().UTC()
	ingestState.WorkItem.SetNodeAndPid()
	ingestState.WorkItem.Stage = stage
	ingestState.WorkItem.StageStartedAt = &utcNow
	ingestState.WorkItem.Status = constants.StatusStarted
	ingestState.WorkItem.Note = message
	resp := _context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		_context.MessageLog.Error("Could not mark WorkItem started for %s for %s/%s: %v",
			stage, ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}

// Tell Pharos that this item was processed successfully.
func MarkWorkItemSucceeded(ingestState *models.IngestState, _context *context.Context, nextStage string) error {
	_context.MessageLog.Info("Telling Pharos processing can proceed for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Node = ""
	ingestState.WorkItem.Pid = 0
	ingestState.WorkItem.Retry = true
	ingestState.WorkItem.StageStartedAt = nil
	ingestState.WorkItem.NeedsAdminReview = false
	ingestState.WorkItem.Stage = nextStage
	if nextStage == constants.StageCleanup {
		ingestState.WorkItem.Status = constants.StatusSuccess
	} else {
		ingestState.WorkItem.Status = constants.StatusPending
	}
	ingestState.WorkItem.Note = fmt.Sprintf("Item is ready for %s", nextStage)
	resp := _context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		_context.MessageLog.Error("Could not mark WorkItem ready for %s for %s/%s: %v",
			nextStage, ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}

// Push this item into the specified NSQ topic.
func PushToQueue(ingestState *models.IngestState, _context *context.Context, queueTopic string) {
	err := _context.NSQClient.Enqueue(
		queueTopic,
		ingestState.WorkItem.Id)
	if err != nil {
		msg := fmt.Sprintf("Error adding WorkItem %d (%s/%s) to NSQ record topic: %v",
			ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
			ingestState.WorkItem.Name, err)
		ingestState.IngestManifest.FetchResult.AddError(msg)
		_context.MessageLog.Error(msg)
		// Record work item state again, to capture the
		// cannot-be-queued error.
		RecordWorkItemState(ingestState, _context, ingestState.IngestManifest.FetchResult)
	}
}

// Dump the WorkItemState.State into the JSON log, surrounded my markers that
// make it easy to find. This log gets big.
func LogJson(ingestState *models.IngestState, jsonLog *log.Logger) {
	timestamp := time.Now().UTC().Format(time.RFC3339)
	startMessage := fmt.Sprintf("-------- BEGIN %s/%s | Etag: %s | Time: %s --------",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, ingestState.WorkItem.ETag,
		timestamp)
	endMessage := fmt.Sprintf("-------- END %s/%s | Etag: %s | Time: %s --------",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, ingestState.WorkItem.ETag,
		timestamp)
	jsonLog.Println(startMessage, "\n",
		ingestState.WorkItemState.State, "\n",
		endMessage)
}

// Deletes the bag from the staging area, and releases the reserved
// storage from the volume manager.
func DeleteBagFromStaging(ingestState *models.IngestState, _context *context.Context, activeResult *models.WorkSummary) {
	tarFile := ingestState.IngestManifest.Object.IngestTarFilePath
	if tarFile != "" && fileutil.FileExists(tarFile) {
		_context.MessageLog.Info("Deleting %s", tarFile)
		err := os.Remove(tarFile)
		if err != nil {
			_context.MessageLog.Warning(err.Error())
		}
		err = _context.VolumeClient.Release(tarFile)
		if err != nil {
			_context.MessageLog.Warning(err.Error())
		}
	} else {
		_context.MessageLog.Info("Skipping deletion of %s: file does not exist", tarFile)
	}

	untarredBagPath := ingestState.IngestManifest.Object.IngestUntarredPath
	looksSafeToDelete := fileutil.LooksSafeToDelete(untarredBagPath, 12, 3)
	if fileutil.FileExists(untarredBagPath) && looksSafeToDelete {
		_context.MessageLog.Info("Deleting untarred bag at %s", untarredBagPath)
		err := os.RemoveAll(untarredBagPath)
		if err != nil {
			_context.MessageLog.Warning(err.Error())
		}
		err = _context.VolumeClient.Release(untarredBagPath)
		if err != nil {
			_context.MessageLog.Warning(err.Error())
		}
	} else {
		_context.MessageLog.Info("Skipping deletion of untarred bag dir at %s: "+
			"Directory does not exist, or is unsafe to delete.", untarredBagPath)
	}

}
