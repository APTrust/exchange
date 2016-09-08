package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
	"net/http"
	"strconv"
	"sync"
//	"time"
)

type APTFetcher struct {
	Context        *context.Context
	InitChannel    chan *models.IngestManifest
	FetchChannel   chan *models.IngestManifest
	RecordChannel  chan *models.IngestManifest
	CleanupChannel chan *models.IngestManifest
	WaitGroup      sync.WaitGroup
}

func NewATPFetcher(_context *context.Context) (*APTFetcher) {
	fetcher := &APTFetcher{
		Context: _context,
	}
	// Set up buffered channels
	fetcherBufferSize := _context.Config.FetchWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.FetchWorker.Workers * 10
	fetcher.InitChannel = make(chan *models.IngestManifest, workerBufferSize)
	fetcher.FetchChannel = make(chan *models.IngestManifest, fetcherBufferSize)
	fetcher.RecordChannel = make(chan *models.IngestManifest, workerBufferSize)
	fetcher.CleanupChannel = make(chan *models.IngestManifest, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.FetchWorker.NetworkConnections; i++ {
		go fetcher.fetch()
	}
	for i := 0; i < _context.Config.FetchWorker.Workers; i++ {
		go fetcher.init()
		go fetcher.cleanup()
		go fetcher.record()
	}
	return fetcher
}

func (fetcher *APTFetcher) HandleMessage(message *nsq.Message) (error) {
	message.DisableAutoResponse()
	workItem, err := fetcher.getWorkItem(message)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return err
	}
	workItemState, err := fetcher.getWorkItemState(workItem)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return err
	}
	ingestManifest, err := workItemState.IngestManifest()
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return err
	}
	fetcher.FetchChannel <- ingestManifest
	return nil
}

func (fetcher *APTFetcher) init() {
//	for manifest := range fetcher.FetchChannel {
//
//	}
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
		workItemState, err = fetcher.InitWorkItemState(workItem)
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

func (fetcher *APTFetcher) InitWorkItemState (workItem *models.WorkItem) (*models.WorkItemState, error) {
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

// This is for direct testing without NSQ.
func (fetcher *APTFetcher) RunWithoutNsq(manifest *models.IngestManifest) {
	fetcher.WaitGroup.Add(1)
	fetcher.InitChannel <- manifest
	fetcher.Context.MessageLog.Debug("Put %s into Fluctus channel", manifest.S3Key)
	fetcher.WaitGroup.Wait()
}
