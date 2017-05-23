package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var TAR_SUFFIX = regexp.MustCompile("\\.tar$")

// CreateNSQConsumer creates and returns an NSQ consumer for a worker process.
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

// --------------------------------------------------------------------------------
// TODO - Remove this
// --------------------------------------------------------------------------------
// GetIngestState sets up the basic pieces of data we'll need to process a
// request. Param initIfEmpty says we should initialize an IntellectualObject
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
	// Special case for handling WorkItems imported from Fluctus.
	if ingestManifest != nil && ingestManifest.FetchResult == nil {
		_context.MessageLog.Info("Created new IngestManifest for old Fluctus item WorkItem %d (%s/%s)",
			workItem.Id, workItem.Bucket, workItem.Name)
		ingestManifest = models.NewIngestManifest()
	}

	// Save memory. We don't need this after loading the
	// IngestManifest from the WorkItemState. For bags with
	// thousands of files, the state JSON can be many MB.
	// We reference the IngestManifest while running, and we
	// keep it up to date. When we occasionally send the state
	// data back to Pharos, RecordWorkItemState regenerates
	// the JSON from the current, up-to-date IngestManifest.
	workItemState.State = ""

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

// GetWorkItem returns the WorkItem with the specified Id from Pharos,
// or nil.
func GetWorkItem(message *nsq.Message, _context *context.Context) (*models.WorkItem, error) {
	msgBody := strings.TrimSpace(string(message.Body))
	_context.MessageLog.Info("NSQ Message body: '%s'", msgBody)
	workItemId, err := strconv.Atoi(string(msgBody))
	if err != nil || workItemId == 0 {
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

// GetWorkItemState returns the WorkItemState associated with the specified
// WorkItem from Pharos, or nil if none exists. Param initIfEmpty should be
// true ONLY when calling from apt_fetcher, which is working with objects
// that are not yet in the system.
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

// InitWorkItemState returns a new WorkItemState object.
// This is used only by apt_fetcher, when we're working on a brand new
// ingest bag that doesn't yet have a WorkItemState record.
// Param workItem is the workItem to be associated with the
// WorkItemState.
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

// --------------------------------------------------------------------------------
// TODO - Remove this
// --------------------------------------------------------------------------------
// SetBasicObjectInfo sets initial essential properties on the
// IntellectualObject associated with an ingestState
// (ingestState.IngestManifest.Object). This is only used by
// apt_fetcher and is only ever called during the fetch stage.
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
	obj.ETag = ingestState.WorkItem.ETag

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

// RecordWorkItemState saves the WorkItemState for this task. We drop a
// copy into our JSON log as a backup, and update the WorkItemState in
// Pharos, so the next worker knows what to do with this item.
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
	// Get rid of this, to conserve memory. We will regenerate it
	// from IngestState each time we post state back to Pharos.
	ingestState.WorkItemState.State = ""
}

// Loads the bag validation config file specified in the general config
// options. This will die if the bag validation config cannot be loaded
// or is invalid.
func LoadAPTrustBagValidationConfig(_context *context.Context) *validation.BagValidationConfig {
	bagValidationConfig, errors := validation.LoadBagValidationConfig(
		_context.Config.BagValidationConfigFile)
	if errors != nil && len(errors) > 0 {
		msg := fmt.Sprintf("Could not load bag validation config from %s",
			_context.Config.BagValidationConfigFile)
		for _, err := range errors {
			msg += fmt.Sprintf("%s ... ", err.Error())
		}
		fmt.Fprintln(os.Stderr, msg)
		_context.MessageLog.Fatal(msg)
	} else {
		_context.MessageLog.Info("Loaded bag validation config file %s",
			_context.Config.BagValidationConfigFile)
	}
	return bagValidationConfig
}

// MarkWorkItemFailed tells Pharos that this item failed processing
// due to a fatal error or too many unsuccessful attempts.
func MarkWorkItemFailed(ingestState *models.IngestState, _context *context.Context) error {
	_context.MessageLog.Info("Telling Pharos processing failed for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Date = time.Now().UTC()
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

// MarkWorkItemRequeued tells Pharos that this item has been requeued
// due to transient errors.
func MarkWorkItemRequeued(ingestState *models.IngestState, _context *context.Context) error {
	_context.MessageLog.Info("Telling Pharos we are requeueing %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Date = time.Now().UTC()
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

// MarkWorkItemStarted tells Pharos that we've started work on this item.
func MarkWorkItemStarted(ingestState *models.IngestState, _context *context.Context, stage, message string) error {
	_context.MessageLog.Info("Telling Pharos we're starting %s for %s/%s",
		stage, ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	utcNow := time.Now().UTC()
	ingestState.WorkItem.Date = utcNow
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

// MarkWorkItemSucceeded tells Pharos that this item was processed successfully.
func MarkWorkItemSucceeded(ingestState *models.IngestState, _context *context.Context, nextStage string) error {
	if nextStage == constants.StageCleanup {
		_context.MessageLog.Info("Ingest complete for %s/%s",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
		ingestState.WorkItem.Note = fmt.Sprintf("Item was successfully ingested")
	} else {
		_context.MessageLog.Info("Telling Pharos processing can proceed for %s/%s",
			ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
		ingestState.WorkItem.Note = fmt.Sprintf("Item is ready for %s", nextStage)
	}
	ingestState.WorkItem.Date = time.Now().UTC()
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
	resp := _context.PharosClient.WorkItemSave(ingestState.WorkItem)
	if resp.Error != nil {
		_context.MessageLog.Error("Could not mark WorkItem ready for %s for %s/%s: %v",
			nextStage, ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, resp.Error)
		return resp.Error
	}
	ingestState.WorkItem = resp.WorkItem()
	return nil
}

// PushToQueue pushes the WorkItem in ingestState into the specified
// NSQ topic.
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

// LogJson dumps the WorkItemState.State into the JSON log, surrounded by
// markers that make it easy to find. This log gets big.
func LogJson(ingestState *models.IngestState, jsonLog *log.Logger) {
	timestamp := time.Now().UTC().Format(time.RFC3339)
	jsonString := `{"ErrorMessage": "Cannot mashal Json for this item."}`
	jsonBytes, err := json.MarshalIndent(ingestState.IngestManifest, "", "  ")
	if err == nil {
		jsonString = string(jsonBytes)
	}
	startMessage := fmt.Sprintf("-------- BEGIN %s/%s | Etag: %s | Time: %s --------",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, ingestState.WorkItem.ETag,
		timestamp)
	endMessage := fmt.Sprintf("-------- END %s/%s | Etag: %s | Time: %s --------",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, ingestState.WorkItem.ETag,
		timestamp)
	jsonLog.Println(startMessage, "\n", jsonString, "\n", endMessage)
}

// DeleteFileFromStaging deletes the bag from the staging area, and releases
// the reserved storage from the volume manager. This deletes both the tarred
// and untarred version of the bag, if they both exist.
func DeleteFileFromStaging(pathToFile string, _context *context.Context) {
	looksSafeToDelete := fileutil.LooksSafeToDelete(pathToFile, 12, 3)
	if pathToFile != "" && fileutil.FileExists(pathToFile) && looksSafeToDelete {
		_context.MessageLog.Info("Deleting %s", pathToFile)
		err := os.Remove(pathToFile)
		if err != nil {
			_context.MessageLog.Warning(err.Error())
		}
		if _context.Config.UseVolumeService && strings.HasSuffix(pathToFile, ".tar") {
			err = _context.VolumeClient.Release(pathToFile)
			if err != nil {
				_context.MessageLog.Warning(err.Error())
			}
		}
	} else {
		_context.MessageLog.Info("Skipping deletion of %s: file does not exist or deletion is unsafe", pathToFile)
	}
}

// SetupIngestState sets up the IngestState object that the
// workers use during the ingest process.
func SetupIngestState(message *nsq.Message, _context *context.Context) (*models.IngestState, error) {
	workItem, err := GetWorkItem(message, _context)
	if err != nil {
		return nil, err
	}
	_context.MessageLog.Info("Loaded WorkItem %d (%s/%s)",
		workItem.Id, workItem.Bucket, workItem.Name)

	manifest := models.NewIngestManifest()
	manifest.WorkItemId = workItem.Id

	// -----------------------------------
	// TODO: get rid of these
	manifest.S3Bucket = workItem.Bucket
	manifest.S3Key = workItem.Name
	manifest.ETag = workItem.ETag
	//
	// -----------------------------------

	instIdentifier := util.OwnerOf(workItem.Bucket)

	manifest.BagPath = filepath.Join(_context.Config.TarDirectory,
		instIdentifier, workItem.Name)
	manifest.DBPath = TAR_SUFFIX.ReplaceAllString(manifest.BagPath, ".valdb")

	ingestState := &models.IngestState{}
	ingestState.NSQMessage = message
	ingestState.WorkItem = workItem
	ingestState.IngestManifest = manifest

	return ingestState, nil
}
