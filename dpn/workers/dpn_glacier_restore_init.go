package workers

import (
	//	"encoding/json"
	"fmt"
	//	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/nsqio/go-nsq"
	//	"net/url"
	"strconv"
	"strings"
	//	"time"
)

// Keep the files in S3 up to 60 days, in case we're
// having system problems and we need to attempt the
// restore multiple times. We'll have other processes
// clean out the S3 bucket when necessary.
const DAYS_TO_KEEP_IN_S3 = 5

// Requests that an object be restored from Glacier to S3. This is
// the first step toward performing fixity checks on DPN bags, and
// restoring DPN bags, all of which are stored in Glacier.
type DPNGlacierRestoreInit struct {
	// Context includes logging, config, network connections, and
	// other general resources for the worker.
	Context *context.Context
	// LocalDPNRestClient lets us talk to our local DPN server.
	LocalDPNRestClient *network.DPNRestClient
	// RequestChannel is for requesting an item be moved from Glacier
	// into S3.
	RequestChannel chan *models.DPNGlacierRestoreState
	// CleanupChannel is for housekeeping, like updating NSQ.
	CleanupChannel chan *models.DPNGlacierRestoreState
	// PostTestChannel is for testing only. In production, nothing listens
	// on this channel.
	PostTestChannel chan *models.DPNGlacierRestoreState
	// S3Url is a custom URL that the S3 client should connect to.
	// We use this only in testing, when we want the client to talk
	// to a local test server. This should not be set in demo or
	// production.
	S3Url string
}

func DPNNewGlacierRestoreInit(_context *context.Context) (*DPNGlacierRestoreInit, error) {
	restorer := &DPNGlacierRestoreInit{
		Context: _context,
	}
	// Set up buffered channels
	restorerBufferSize := _context.Config.DPN.DPNGlacierRestoreWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.DPN.DPNGlacierRestoreWorker.Workers * 10
	restorer.RequestChannel = make(chan *models.DPNGlacierRestoreState, restorerBufferSize)
	restorer.CleanupChannel = make(chan *models.DPNGlacierRestoreState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.DPN.DPNGlacierRestoreWorker.NetworkConnections; i++ {
		go restorer.RequestRestore()
	}
	for i := 0; i < _context.Config.DPN.DPNGlacierRestoreWorker.Workers; i++ {
		go restorer.Cleanup()
	}
	var err error
	restorer.LocalDPNRestClient, err = network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	return restorer, err
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (restorer *DPNGlacierRestoreInit) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	state := restorer.GetRestoreState(message)
	restorer.SaveDPNWorkItem(state)
	if state.ErrorMessage != "" {
		restorer.Context.MessageLog.Error("Error setting up state for WorkItem %d: %s",
			string(message.Body), state.ErrorMessage)
		// No use proceeding...
		restorer.CleanupChannel <- state
		return fmt.Errorf(state.ErrorMessage)
	}
	// OK, we're good
	restorer.RequestChannel <- state
	return nil
}

func (restorer *DPNGlacierRestoreInit) RequestRestore() {
	// for state := range restorer.RequestChannel {
	// 	// Request restore from Glacier
	// }
}

func (restorer *DPNGlacierRestoreInit) Cleanup() {
	for state := range restorer.CleanupChannel {
		if state.ErrorMessage != "" {
			restorer.FinishWithError(state)
		} else {
			restorer.FinishWithSuccess(state)
		}
		// For testing only. The test code creates the PostTestChannel.
		// When running in demo & production, this channel is nil.
		if restorer.PostTestChannel != nil {
			restorer.PostTestChannel <- state
		}
	}
}

func (restorer *DPNGlacierRestoreInit) FinishWithSuccess(state *models.DPNGlacierRestoreState) {
	state.DPNWorkItem.ClearNodeAndPid()
	note := "Awaiting availability in S3 for fixity check"
	if state.IsAvailableInS3 {
		note = "Item is available in S3 for fixity check"
	}
	state.DPNWorkItem.Note = &note
	restorer.SaveDPNWorkItem(state)
	state.NSQMessage.Finish()

	// Move to download queue
}

func (restorer *DPNGlacierRestoreInit) FinishWithError(state *models.DPNGlacierRestoreState) {
	state.DPNWorkItem.ClearNodeAndPid()
	state.DPNWorkItem.Note = &state.ErrorMessage
	restorer.SaveDPNWorkItem(state)
	state.NSQMessage.Finish()
}

// GetWorkItem returns the WorkItem with the specified Id from Pharos,
// or nil.
func (restorer *DPNGlacierRestoreInit) GetRestoreState(message *nsq.Message) *models.DPNGlacierRestoreState {
	msgBody := strings.TrimSpace(string(message.Body))
	restorer.Context.MessageLog.Info("NSQ Message body: '%s'", msgBody)
	state := &models.DPNGlacierRestoreState{}

	// Get the DPN work item
	dpnWorkItemId, err := strconv.Atoi(string(msgBody))
	if err != nil || dpnWorkItemId == 0 {
		state.ErrorMessage = fmt.Sprintf("Could not get DPNWorkItem Id from NSQ message body: %v", err)
		return state
	}
	resp := restorer.Context.PharosClient.DPNWorkItemGet(dpnWorkItemId)
	if resp.Error != nil {
		state.ErrorMessage = fmt.Sprintf("Error getting DPNWorkItem %d from Pharos: %v", dpnWorkItemId, resp.Error)
		return state
	}
	dpnWorkItem := resp.DPNWorkItem()
	if dpnWorkItem == nil {
		state.ErrorMessage = fmt.Sprintf("Pharos returned nil for WorkItem %d", dpnWorkItemId)
		return state
	}
	state.DPNWorkItem = dpnWorkItem
	state.DPNWorkItem.SetNodeAndPid()
	note := "Requesting Glacier restoration for fixity"
	state.DPNWorkItem.Note = &note

	// Get the DPN Bag from the DPN REST server.
	dpnResp := restorer.LocalDPNRestClient.DPNBagGet(dpnWorkItem.Identifier)
	if dpnResp.Error != nil {
		state.ErrorMessage = fmt.Sprintf("Error getting DPN bag %s from %s: %v", dpnWorkItem.Identifier,
			restorer.Context.Config.DPN.RestClient.LocalServiceURL, resp.Error)
		return state
	}
	dpnBag := dpnResp.Bag()
	if dpnBag == nil {
		state.ErrorMessage = fmt.Sprintf("DPN REST server returned nil for bag %s", dpnWorkItem.Identifier)
		return state
	}
	state.DPNBag = dpnBag

	// Although this is duplicate info, we record it in the state object
	// so we can see it in the Pharos UI when we're checking on the state
	// of an item.
	state.GlacierBucket = restorer.Context.Config.DPN.DPNGlacierRegion
	state.GlacierKey = dpnBag.UUID

	return state
}

func (restorer *DPNGlacierRestoreInit) SaveDPNWorkItem(state *models.DPNGlacierRestoreState) {
	jsonData, err := state.ToJson()
	if err != nil {
		msg := fmt.Sprintf("Could not marshal DPNGlacierRestoreState "+
			"for DPNWorkItem %d: %v", state.DPNWorkItem.Id, err)
		restorer.Context.MessageLog.Error(msg)
		note := "[JSON serialization error]"
		state.DPNWorkItem.Note = &note
	}

	// Update the DPNWorkItem
	state.DPNWorkItem.State = &jsonData
	state.DPNWorkItem.Retry = !state.ErrorIsFatal

	resp := restorer.Context.PharosClient.DPNWorkItemSave(state.DPNWorkItem)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not save DPNWorkItem %d "+
			"for fixity on bag %s to Pharos: %v",
			state.DPNWorkItem.Id, state.DPNWorkItem.Identifier, err)
		restorer.Context.MessageLog.Error(msg)
		if state.ErrorMessage == "" {
			state.ErrorMessage = msg
		}
	}
}
