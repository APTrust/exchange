package workers

import (
	"fmt"
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
