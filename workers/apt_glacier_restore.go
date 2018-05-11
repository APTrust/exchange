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

	// restorer.RequestChannel <- glacierRestoreState

	// Return no error, so NSQ knows we're OK.
	return nil
}

func (restorer *APTGlacierRestore) requestRestore() {
	//for restoreState := range restorer.RequestChannel {
	// Request retrieval from Glacier
	// Update GlacierRestoreState
	// Push to CleanupChannel
	//}
}

func (restorer *APTGlacierRestore) cleanup() {
	//for restoreState := range restorer.RequestChannel {
	// Update WorkItem in Pharos
	// Push to NSQ's restoration channel for packaging, etc.
	//}
}
