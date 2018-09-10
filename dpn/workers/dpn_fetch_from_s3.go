package workers

// import (
// 	"fmt"
// 	"github.com/APTrust/exchange/constants"
// 	"github.com/APTrust/exchange/context"
// 	"github.com/APTrust/exchange/models"
// 	"github.com/APTrust/exchange/network"
// 	"github.com/APTrust/exchange/util"
// 	"github.com/APTrust/exchange/util/fileutil"
// 	"github.com/APTrust/exchange/util/storage"
// 	"github.com/APTrust/exchange/validation"
// 	"github.com/nsqio/go-nsq"
// 	"os"
// 	"strings"
// 	"time"
// )

// // Fetches from S3 to local storage.
// type DPNFetchFromS3 struct {
// 	Context        *context.Context
// 	FetchChannel   chan *models.DPNS3FetchState
// 	CleanupChannel chan *models.DPNS3FetchState
// }

// func NewDPNFetchFromS3(_context *context.Context) *DPNFetchFromS3 {
// 	fetcher := &DPNFetchFromS3{
// 		Context: _context,
// 	}

// 	// Set up buffered channels
// 	fetcherBufferSize := _context.Config.FetchWorker.NetworkConnections * 4
// 	workerBufferSize := _context.Config.FetchWorker.Workers * 10
// 	fetcher.FetchChannel = make(chan *models.DPNS3FetchState, fetcherBufferSize)
// 	fetcher.CleanupChannel = make(chan *models.DPNS3FetchState, workerBufferSize)
// 	// Set up a limited number of go routines
// 	for i := 0; i < _context.Config.FetchWorker.NetworkConnections; i++ {
// 		go fetcher.fetch()
// 	}
// 	for i := 0; i < _context.Config.FetchWorker.Workers; i++ {
// 		go fetcher.cleanup()
// 	}
// 	return fetcher
// }

// // This is the callback that NSQ workers use to handle messages from NSQ.
// func (fetcher *DPNFetchFromS3) HandleMessage(message *nsq.Message) error {

// 	// Set up *models.DPNRetriavalManifest

// 	fetcher.FetchChannel <- ingestState
// 	return nil
// }

// func (fetcher *DPNFetchFromS3) fetch() {
// 	for state := range fetcher.FetchChannel {

// 	}
// }

// func (fetcher *DPNFetchFromS3) cleanup() {
// 	for state := range fetcher.CleanupChannel {

// 	}
// }

// func (fetcher *DPNFetchFromS3) Download(state *models.DPNRetriavalManifest) {

// }

// func (fetcher *DPNFetchFromS3) FinishWithSuccess(state *models.DPNRetriavalManifest) {

// }

// func (fetcher *DPNFetchFromS3) FinishWithError(state *models.DPNRetriavalManifest) {

// }

// func (fetcher *DPNFetchFromS3) GetRetrievalManifest() *models.DPNRetriavalManifest {
// 	// Get the WorkItem

// 	// Get the DPN Bag
// }
