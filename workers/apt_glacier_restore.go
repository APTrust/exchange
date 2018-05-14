package workers

import (
	//	"fmt"
	//	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	//	"github.com/APTrust/exchange/network"
	//	"github.com/APTrust/exchange/util"
	//	"github.com/APTrust/exchange/util/fileutil"
	//	"github.com/APTrust/exchange/util/storage"
	//	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	//	"os"
	//	"strings"
	//	"time"
)

// Requests that an object be restored from Glacier to S3. This is
// the first step toward restoring a Glacier-only bag.
type APTGlacierRestore struct {
	Context        *context.Context
	RequestChannel chan *models.GlacierRestoreState
	CleanupChannel chan *models.GlacierRestoreState
}

func NewGlacierRestore(_context *context.Context) *APTGlacierRestore {
	restorer := &APTGlacierRestore{
		Context: _context,
	}
	// Set up buffered channels
	restorerBufferSize := _context.Config.GlacierRestoreWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.GlacierRestoreWorker.Workers * 10
	restorer.RequestChannel = make(chan *models.GlacierRestoreState, restorerBufferSize)
	restorer.CleanupChannel = make(chan *models.GlacierRestoreState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.GlacierRestoreWorker.NetworkConnections; i++ {
		go restorer.requestRestore()
	}
	for i := 0; i < _context.Config.GlacierRestoreWorker.Workers; i++ {
		go restorer.cleanup()
	}
	return restorer
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (restorer *APTGlacierRestore) HandleMessage(message *nsq.Message) error {
	// TODO: Set up GlacierRestoreState
	workItem, err := GetWorkItem(message, restorer.Context)
	if err != nil {
		restorer.Context.MessageLog.Error(err.Error())
		return err
	}
	var glacierRestoreState *models.GlacierRestoreState
	if workItem.WorkItemStateId != nil && *workItem.WorkItemStateId != 0 {
		workItemState, err := GetWorkItemState(workItem, restorer.Context, false)
		if err != nil {
			restorer.Context.MessageLog.Error(err.Error())
			return err
		}
		if workItemState != nil && workItemState.HasData() {
			glacierRestoreState, err := workItemState.GlacierRestoreState()
			if err != nil {
				restorer.Context.MessageLog.Error(err.Error())
				return err
			}
			glacierRestoreState.NSQMessage = message
			glacierRestoreState.WorkItem = workItem
		}
	} else {
		glacierRestoreState = models.NewGlacierRestoreState(message, workItem)
	}
	restorer.RequestChannel <- glacierRestoreState
	return nil
}

func (restorer *APTGlacierRestore) requestRestore() {
	for glacierRestoreState := range restorer.RequestChannel {
		glacierRestoreState.WorkSummary.ClearErrors()
		glacierRestoreState.WorkSummary.Attempted = true
		glacierRestoreState.WorkSummary.AttemptNumber += 1
		glacierRestoreState.WorkSummary.Start()
		// if WorkItem has a GenericFileIdentifier, this is a
		// single-file restore. Otherwise, it's an object restore.
		// Request retrieval from Glacier
		// Update GlacierRestoreState
		// Push to CleanupChannel
	}
}

func (restorer *APTGlacierRestore) cleanup() {
	//for restoreState := range restorer.RequestChannel {
	// Update WorkItem in Pharos
	// Push to NSQ's restoration channel for packaging, etc.
	//}
}

func (restorer *APTGlacierRestore) requestAllFiles(glacierRestoreState *models.GlacierRestoreState) {

}
