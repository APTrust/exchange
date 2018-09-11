package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	dpn_network "github.com/APTrust/exchange/dpn/network"
	//	"github.com/APTrust/exchange/models"
	//	"github.com/APTrust/exchange/network"
	//	"github.com/APTrust/exchange/util"
	//	"github.com/APTrust/exchange/util/fileutil"
	//	"github.com/APTrust/exchange/util/storage"
	//	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	//	"os"
	//	"strings"
	// "time"
)

// Fetches from S3 to local storage.
type DPNS3Retriever struct {
	// Context includes logging, config, network connections, and
	// other general resources for the worker.
	Context *context.Context
	// LocalDPNRestClient lets us talk to our local DPN server.
	LocalDPNRestClient *dpn_network.DPNRestClient
	// FetchChannel is for fetching files from S3.
	FetchChannel chan *DPNRestoreHelper
	// CleanupChannel is for post-fetch processing.
	CleanupChannel chan *DPNRestoreHelper
	// PostTestChannel is for testing only. In production, nothing listens
	// on this channel.
	PostTestChannel chan *DPNRestoreHelper
}

func NewDPNS3Retriever(_context *context.Context) (*DPNS3Retriever, error) {
	retriever := &DPNS3Retriever{
		Context: _context,
	}

	// Set up buffered channels
	fetcherBufferSize := _context.Config.FetchWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.FetchWorker.Workers * 10
	retriever.FetchChannel = make(chan *DPNRestoreHelper, fetcherBufferSize)
	retriever.CleanupChannel = make(chan *DPNRestoreHelper, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.FetchWorker.NetworkConnections; i++ {
		go retriever.fetch()
	}
	for i := 0; i < _context.Config.FetchWorker.Workers; i++ {
		go retriever.cleanup()
	}

	// Set up a client to talk to our local DPN server.
	var err error
	retriever.LocalDPNRestClient, err = dpn_network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	return retriever, err
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (retriever *DPNS3Retriever) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	helper, err := NewDPNRestoreHelper(message, retriever.Context,
		retriever.LocalDPNRestClient, constants.ActionFixityCheck,
		"LocalCopySummary")
	if err != nil {
		retriever.Context.MessageLog.Error(err.Error())
		return err
	}
	helper.Manifest.LocalCopySummary.ClearErrors()
	helper.Manifest.LocalCopySummary.Start()
	helper.Manifest.DPNWorkItem.Status = constants.StatusStarted
	helper.SaveDPNWorkItem()
	if helper.Manifest.LocalCopySummary.HasErrors() {
		retriever.Context.MessageLog.Error("Error setting up manifest for WorkItem %s: %s",
			string(message.Body), helper.Manifest.LocalCopySummary.AllErrorsAsString())
		// No use proceeding...
		retriever.CleanupChannel <- helper
		return fmt.Errorf(helper.Manifest.LocalCopySummary.AllErrorsAsString())
	}
	if helper.Manifest.DPNWorkItem.IsCompletedOrCancelled() {
		retriever.Context.MessageLog.Info("Skipping WorkItem %d because status is %s",
			helper.Manifest.DPNWorkItem.Id, helper.Manifest.DPNWorkItem.Status)
		retriever.CleanupChannel <- helper
		return nil
	}

	// OK, we're good. Retrieve the file from S3.
	retriever.FetchChannel <- helper
	return nil
}

func (retriever *DPNS3Retriever) fetch() {
	for helper := range retriever.FetchChannel {
		// Retrieve the tar file if it's not already on disk.

		retriever.CleanupChannel <- helper
	}
}

func (retriever *DPNS3Retriever) cleanup() {
	for helper := range retriever.CleanupChannel {
		helper.Manifest.LocalCopySummary.Finish()
		if helper.Manifest.LocalCopySummary.HasErrors() {
			retriever.FinishWithError(helper)
		} else {
			retriever.FinishWithSuccess(helper)
		}
		// For testing only. The test code creates the PostTestChannel.
		// When running in demo & production, this channel is nil.
		if retriever.PostTestChannel != nil {
			retriever.PostTestChannel <- helper
		}
	}
}

func (fetcher *DPNS3Retriever) Download(helper *DPNRestoreHelper) {

}

func (fetcher *DPNS3Retriever) FinishWithSuccess(helper *DPNRestoreHelper) {

}

func (fetcher *DPNS3Retriever) FinishWithError(helper *DPNRestoreHelper) {

}
