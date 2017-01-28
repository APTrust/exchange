package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/stats"
	"net/url"
	"time"
)

const UNKNOWN_TOPIC = "unknown_topic"

type APTQueue struct {
	Context      *context.Context
	NSQClient    *network.NSQClient
	topic        string
	stats        *stats.APTQueueStats
	dryRun       bool
	statsEnabled bool
}

// NewAPTQueue creates a new queue worker to push WorkItems from
// Pharos into NSQ, and marked them as queued. If param topic is
// specified, this will queue items destined for the specified topic;
// otherwise, it will queue items for all topics. If param enableStats
// is true, it will dump stats about what was queued to a JSON file.
// If param dryRun is true, it will log all the items it would have
// queued, without actually pushing anything to NSQ.
func NewAPTQueue(_context *context.Context, topic string, enableStats, dryRun bool) *APTQueue {
	_context.MessageLog.Info("NSQ address: %s", _context.Config.NsqdHttpAddress)
	nsqClient := network.NewNSQClient(_context.Config.NsqdHttpAddress)
	aptQueue := &APTQueue{
		Context:      _context,
		NSQClient:    nsqClient,
		topic:        topic,
		statsEnabled: enableStats,
		dryRun:       dryRun,
	}
	if enableStats {
		aptQueue.stats = stats.NewAPTQueueStats()
	}
	return aptQueue
}

// Run retrieves all unqueued work items from Pharos and pushes
// them into the appropriate NSQ topic.
func (aptQueue *APTQueue) Run() {
	aptQueue.printLogHeader()
	params := url.Values{}
	params.Set("queued", "false")
	params.Set("status", constants.StatusPending)
	params.Set("retry", "true")
	params.Set("node_empty", "true")
	params.Set("page", "1")
	params.Set("per_page", "100")
	for {
		resp := aptQueue.Context.PharosClient.WorkItemList(params)
		aptQueue.Context.MessageLog.Info("GET %s", resp.Request.URL)
		if resp.Error != nil {
			aptQueue.recordError(
				"Error getting WorkItem list from Pharos: %s",
				resp.Error)
		}
		for _, item := range resp.WorkItems() {
			if aptQueue.addToNSQ(item) {
				aptQueue.markAsQueued(item)
			}
		}
		if resp.HasNextPage() == false {
			break
		}
		params = resp.ParamsForNextPage()
	}
}

func (aptQueue *APTQueue) addToNSQ(workItem *models.WorkItem) bool {
	identifier := workItem.Name
	if workItem.ObjectIdentifier != "" {
		identifier = workItem.ObjectIdentifier
	}
	if workItem.GenericFileIdentifier != "" {
		identifier = workItem.GenericFileIdentifier
	}
	topic := aptQueue.getNSQTopic(workItem)
	if aptQueue.topic != "" && topic != aptQueue.topic {
		aptQueue.Context.MessageLog.Info(
			"Skipping WorkItem id %d - %s (%s/%s/%s) because topic would be %s",
			workItem.Id, identifier, workItem.Action,
			workItem.Stage, workItem.Status, topic)
		return false
	}
	if topic == UNKNOWN_TOPIC {
		aptQueue.recordError(
			"Unknown topic for WorkItem %d - %s (%s/%s/%s)",
			workItem.Id, identifier, workItem.Action,
			workItem.Stage, workItem.Status)
		return false
	}
	if aptQueue.dryRun {
		aptQueue.Context.MessageLog.Info(
			"[DRY RUN] Would add WorkItem id %d - %s (%s/%s/%s) - to %s",
			workItem.Id, identifier, workItem.Action,
			workItem.Stage, workItem.Status, topic)
		return false
	}
	err := aptQueue.NSQClient.Enqueue(topic, workItem.Id)
	if err != nil {
		aptQueue.recordError("Error sending WorkItem %d %s (%s/%s/%s) - to %s: %v",
			workItem.Id, identifier, workItem.Action,
			workItem.Stage, workItem.Status, topic, err)
		return false
	}
	aptQueue.Context.MessageLog.Info("Added WorkItem id %d - %s (%s/%s/%s) - to %s",
		workItem.Id, identifier, workItem.Action, workItem.Stage, workItem.Status, topic)
	if aptQueue.stats != nil {
		aptQueue.stats.AddWorkItem(topic, workItem)
	}
	return true
}

func (aptQueue *APTQueue) markAsQueued(workItem *models.WorkItem) *models.WorkItem {
	utcNow := time.Now().UTC()
	workItem.Date = utcNow
	workItem.QueuedAt = &utcNow
	resp := aptQueue.Context.PharosClient.WorkItemSave(workItem)
	if resp.Error != nil {
		aptQueue.recordError("Error setting QueuedAt for WorkItem with id %d: %v",
			workItem.Id, resp.Error)
		return nil
	}
	if resp.Response.StatusCode != 200 {
		aptQueue.processPharosError(resp)
		return nil
	}
	aptQueue.Context.MessageLog.Info("Marked WorkItem id %d (%s/%s/%s) as queued in Pharos",
		workItem.Id, workItem.Action, workItem.Stage, workItem.Status)
	if aptQueue.stats != nil {
		aptQueue.stats.AddItemMarkedAsQueued(workItem)
	}
	return resp.WorkItem()
}

func (aptQueue *APTQueue) processPharosError(resp *network.PharosResponse) {
	respBody := ""
	bytesRead, aptQueuer := resp.RawResponseData()
	if aptQueuer == nil {
		respBody = string(bytesRead)
	} else {
		respBody = fmt.Sprintf("[Could not read response body: %v]", aptQueuer)
	}
	aptQueue.recordError("%s %s returned status code %d. Response body: %s",
		resp.Request.Method, resp.Request.URL, resp.Response.StatusCode, respBody)
}

func (aptQueue *APTQueue) recordError(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	if aptQueue.stats != nil {
		aptQueue.stats.AddError(msg)
	}
	aptQueue.Context.MessageLog.Error(msg)
}

func (aptQueue *APTQueue) getNSQTopic(workItem *models.WorkItem) string {
	config := aptQueue.Context.Config
	topic := UNKNOWN_TOPIC
	if workItem.Action == constants.ActionIngest {
		if workItem.Stage == constants.StageReceive {
			topic = config.FetchWorker.NsqTopic
		} else if workItem.Stage == constants.StageStore {
			topic = config.StoreWorker.NsqTopic
		} else if workItem.Stage == constants.StageRecord {
			topic = config.RecordWorker.NsqTopic
		}
	} else if workItem.Action == constants.ActionFixityCheck {
		topic = config.FixityWorker.NsqTopic
	} else if workItem.Action == constants.ActionRestore {
		topic = config.RestoreWorker.NsqTopic
	} else if workItem.Action == constants.ActionDelete {
		topic = config.FileDeleteWorker.NsqTopic
	} else if workItem.Action == constants.ActionDPN {
		topic = config.DPN.DPNPackageWorker.NsqTopic
	}
	return topic
}

func (aptQueue *APTQueue) GetStats() *stats.APTQueueStats {
	return aptQueue.stats
}

func (aptQueue *APTQueue) printLogHeader() {
	topic := aptQueue.topic
	if aptQueue.topic == "" {
		topic = "ALL"
	}
	aptQueue.Context.MessageLog.Info("apt_queue started with the following params:")
	aptQueue.Context.MessageLog.Info("Topic = %s", topic)
	aptQueue.Context.MessageLog.Info("Enable Stats = %t", aptQueue.statsEnabled)
	aptQueue.Context.MessageLog.Info("Dry Run = %t", aptQueue.dryRun)
}
