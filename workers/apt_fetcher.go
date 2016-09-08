package workers

import (
//	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
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

func (fetcher *APTFetcher) RunWithoutNsq(manifest *models.IngestManifest) {
	fetcher.WaitGroup.Add(1)
	fetcher.InitChannel <- manifest
	fetcher.Context.MessageLog.Debug("Put %s into Fluctus channel", manifest.S3Key)
	fetcher.WaitGroup.Wait()
}
