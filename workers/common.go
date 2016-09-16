package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
	"log"
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
		activeResult.AddError("Could not convert Ingest Manifest " +
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
			activeResult.AddError("Could not save WorkItemState " +
				"to Pharos. This item will have to be re-processed. Error was: %v", resp.Error)
		} else {
			// Saved to Pharos!
			ingestState.WorkItemState = resp.WorkItemState()
		}
	}
}

// Tell Pharos that this item failed processing due to a fatal error.
func MarkWorkItemFailed (ingestState *models.IngestState, _context *context.Context) (error) {
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
func MarkWorkItemRequeued (ingestState *models.IngestState, _context *context.Context) (error) {
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

// Tell Pharos that this item was processed successfully.
func MarkWorkItemSucceeded (ingestState *models.IngestState, _context *context.Context, nextStage string) (error) {
	_context.MessageLog.Info("Telling Pharos processing can proceed for %s/%s",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name)
	ingestState.WorkItem.Node = ""
	ingestState.WorkItem.Pid = 0
	ingestState.WorkItem.Retry = true
	ingestState.WorkItem.StageStartedAt = nil
	ingestState.WorkItem.NeedsAdminReview = false
	ingestState.WorkItem.Stage = nextStage
	ingestState.WorkItem.Status = constants.StatusPending
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
func PushToQueue (ingestState *models.IngestState, _context *context.Context, queueTopic string) {
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
func LogJson (ingestState *models.IngestState, jsonLog *log.Logger) {
	timestamp := time.Now().UTC().Format(time.RFC3339)
	startMessage := fmt.Sprintf("-------- BEGIN %s/%s | Etag: %s | Time: %s --------",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, ingestState.WorkItem.ETag,
		timestamp)
	endMessage := fmt.Sprintf("-------- END %s/%s | Etag: %s | Time: %s --------",
		ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, ingestState.WorkItem.ETag,
		timestamp)
	jsonLog.Println(startMessage, "\n",
		ingestState.WorkItemState.State, "\n",
		endMessage, "\n")
}
