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
	stats        *stats.APTQueueStats
	statsEnabled bool
}

func NewAPTQueue(_context *context.Context, enableStats bool) *APTQueue {
	nsqClient := network.NewNSQClient(_context.Config.NsqdHttpAddress)
	aptQueue := &APTQueue{
		Context:      _context,
		NSQClient:    nsqClient,
		statsEnabled: enableStats,
	}
	if enableStats {
		aptQueue.stats = stats.NewAPTQueueStats()
	}
	return aptQueue
}

// Run retrieves all unqueued work items from Pharos and pushes
// them into the appropriate NSQ topic.
func (aptQueue *APTQueue) Run() {
	params := url.Values{}
	params.Set("queued", "false")
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
	topic := aptQueue.getNSQTopic(workItem)
	if topic == UNKNOWN_TOPIC {
		aptQueue.recordError("Unknown topic for WorkItem %d: %s/%s",
			workItem.Id, workItem.Action, workItem.Stage)
		return false
	}
	err := aptQueue.NSQClient.Enqueue(topic, workItem.Id)
	if err != nil {
		aptQueue.recordError("Error sending WorkItem %d to NSQ topic %s: %v",
			workItem.Id, topic, err)
		return false
	}
	aptQueue.Context.MessageLog.Info("Added WorkItem id %d (%s/%s/%s) to NSQ topic %s",
		workItem.Id, workItem.Action, workItem.Stage, workItem.Status, topic)
	if aptQueue.stats != nil {
		aptQueue.stats.AddWorkItem(topic, workItem)
	}
	return true
}

func (aptQueue *APTQueue) markAsQueued(workItem *models.WorkItem) *models.WorkItem {
	utcNow := time.Now().UTC()
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
