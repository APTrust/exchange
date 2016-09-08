package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
	"net/http"
	"os"
	"strconv"
	"sync"
//	"time"
)

type APTFetcher struct {
	Context        *context.Context
	FetchChannel   chan *FetchData
	RecordChannel  chan *FetchData
	CleanupChannel chan *FetchData
	WaitGroup      sync.WaitGroup
}

type FetchData struct {
	WorkItem        *models.WorkItem
	WorkItemState   *models.WorkItemState
	IngestManifest  *models.IngestManifest
}

func NewATPFetcher(_context *context.Context) (*APTFetcher) {
	fetcher := &APTFetcher{
		Context: _context,
	}
	// Set up buffered channels
	fetcherBufferSize := _context.Config.FetchWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.FetchWorker.Workers * 10
	fetcher.FetchChannel = make(chan *FetchData, fetcherBufferSize)
	fetcher.RecordChannel = make(chan *FetchData, workerBufferSize)
	fetcher.CleanupChannel = make(chan *FetchData, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.FetchWorker.NetworkConnections; i++ {
		go fetcher.fetch()
	}
	for i := 0; i < _context.Config.FetchWorker.Workers; i++ {
		go fetcher.cleanup()
		go fetcher.record()
	}
	return fetcher
}

func (fetcher *APTFetcher) HandleMessage(message *nsq.Message) (error) {
	message.DisableAutoResponse()
	fetchData, err := fetcher.initFetchData(message)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return err
	}
	resp := fetcher.Context.PharosClient.WorkItemStateSave(fetchData.WorkItemState)
	if resp.Error != nil {
		return resp.Error
	}
	fetchData.WorkItem, err = fetcher.markWorkItemAsStarted(fetchData.WorkItem)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return err
	}
	fetcher.FetchChannel <- fetchData
	return nil
}

func (fetcher *APTFetcher) fetch() {
//	for manifest := range fetcher.FetchChannel {
//
//	}
}

func (fetcher *APTFetcher) cleanup() {
//	for manifest := range fetcher.FetchChannel {
//
//	}
}

func (fetcher *APTFetcher) record() {
	// for manifest := range fetcher.FetchChannel {

	// }
}

func (fetcher *APTFetcher) initFetchData (message *nsq.Message) (*FetchData, error) {
	workItem, err := fetcher.getWorkItem(message)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return nil, err
	}
	workItemState, err := fetcher.getWorkItemState(workItem)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return nil, err
	}
	ingestManifest, err := workItemState.IngestManifest()
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return nil, err
	}
	fetchData := &FetchData{
		WorkItem: workItem,
		WorkItemState: workItemState,
		IngestManifest: ingestManifest,
	}
	return fetchData, err
}


// Returns the WorkItem record from Pharos that has the WorkItemId
// specified in the NSQ message.
func (fetcher *APTFetcher) getWorkItem(message *nsq.Message) (*models.WorkItem, error) {
	workItemId, err := strconv.Atoi(string(message.Body))
	if err != nil {
		return nil, fmt.Errorf("Could not get WorkItemId from NSQ message body: %v", err)
	}
	resp := fetcher.Context.PharosClient.WorkItemGet(workItemId)
	if resp.Error != nil {
		return nil, fmt.Errorf("Error getting WorkItem %d from Pharos: %v", err)
	}
	workItem := resp.WorkItem()
	if workItem == nil {
		return nil, fmt.Errorf("Pharos returned nil for WorkItem %d", workItemId)
	}
	return workItem, nil
}

// Returns the WorkItemState record from Pharos with the specified workItem.Id,
// or creates a new WorkItemState (if necessary) and returns that. If this is
// the first time we've attempted to ingest this item, we'll have to crate a
// new WorkItemState.
func (fetcher *APTFetcher) getWorkItemState(workItem *models.WorkItem) (*models.WorkItemState, error) {
	var workItemState *models.WorkItemState
	var err error
	resp := fetcher.Context.PharosClient.WorkItemStateGet(workItem.Id)
	if resp.Response.StatusCode == http.StatusNotFound {
		// Record has not been created yet, so build a new one now.
		workItemState, err = fetcher.initWorkItemState(workItem)
		if err != nil {
			return nil, err
		}
	} else if resp.Error != nil {
		// We got some other 4xx/5xx error from the Pharos REST service.
		return nil, fmt.Errorf("Error getting WorkItemState for WorkItem %d from Pharos: %v", resp.Error)
	} else {
		// We didn't get a 404 or any other error. The WorkItemState should be in
		// the response.
		workItemState = resp.WorkItemState()
		if workItemState == nil {
			return nil, fmt.Errorf("Pharos returned nil for WorkItemState with WorkItem id %d", workItem.Id)
		}
	}
	return workItemState, nil
}

func (fetcher *APTFetcher) initWorkItemState (workItem *models.WorkItem) (*models.WorkItemState, error) {
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

func (fetcher *APTFetcher) markWorkItemAsStarted (workItem *models.WorkItem) (*models.WorkItem, error) {
	hostname, _ := os.Hostname()
	if hostname == "" { hostname = "apt_fetcher_host" }
	workItem.Node = hostname
	workItem.Status = constants.StatusStarted
	workItem.Pid = os.Getpid()
	workItem.Note = "Fetching bag from receiving bucket."
	resp := fetcher.Context.PharosClient.WorkItemSave(workItem)
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.WorkItem(), nil
}

// This is for direct testing without NSQ.
func (fetcher *APTFetcher) RunWithoutNsq(fetchData *FetchData) {
	fetcher.WaitGroup.Add(1)
	fetcher.FetchChannel <- fetchData
	fetcher.Context.MessageLog.Debug("Put %s into Fluctus channel", fetchData.IngestManifest.S3Key)
	fetcher.WaitGroup.Wait()
}
