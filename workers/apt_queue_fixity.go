package workers

import (
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
	"net/url"
	"strconv"
	"time"
)

type APTQueueFixity struct {
	Context        *context.Context
	NSQClient      *network.NSQClient
	maxFiles       int
	identifierLike string
	nsqTopic       string
}

// NewAPTQueueFixity creates a new worker to push files needing
// a fixity check into the NSQ apt_fixity_topic. Param _context
// is a Context object and maxFiles is the maximum number of files
// to queue. Param identifierLike is used in integration testing
// to select files we know exist.
func NewAPTQueueFixity(_context *context.Context, identifierLike string, maxFiles int) *APTQueueFixity {
	_context.MessageLog.Info("NSQ address: %s", _context.Config.NsqdHttpAddress)
	nsqClient := network.NewNSQClient(_context.Config.NsqdHttpAddress)
	aptQueue := &APTQueueFixity{
		Context:        _context,
		NSQClient:      nsqClient,
		maxFiles:       maxFiles,
		identifierLike: identifierLike,
		nsqTopic:       _context.Config.FixityWorker.NsqTopic,
	}
	return aptQueue
}

// Run retrieves a list of GenericFiles needing fixity checks and
// adds the Identifier of each file to the NSQ apt_fixity_check topic.
// It stops after queuing maxFiles.
func (aptQueue *APTQueueFixity) Run() {

	// Set up basic params
	hours := aptQueue.Context.Config.MaxDaysSinceFixityCheck * 24 * -1
	sinceWhen := time.Now().Add(time.Duration(hours) * time.Hour).UTC()
	perPage := util.Min(100, aptQueue.maxFiles)

	// Log what we're doing
	aptQueue.Context.MessageLog.Info(
		"Queuing up to %d files not checked since %s "+
			"to topic %s", aptQueue.maxFiles, sinceWhen.Format(time.RFC3339),
		aptQueue.nsqTopic)

	// Get to work
	params := url.Values{}
	itemsAdded := 0
	params.Set("not_checked_since", sinceWhen.Format(time.RFC3339))
	params.Set("per_page", strconv.Itoa(perPage))
	params.Set("page", "1")
	params.Set("sort", "identifier")
	if aptQueue.identifierLike != "" {
		params.Set("identifier_like", aptQueue.identifierLike)
		aptQueue.Context.MessageLog.Info(
			"Queuing only files whose identifier contains %s",
			aptQueue.identifierLike)
	}
	for {
		resp := aptQueue.Context.PharosClient.GenericFileList(params)
		aptQueue.Context.MessageLog.Info("GET %s", resp.Request.URL)
		if resp.Error != nil {
			aptQueue.Context.MessageLog.Error(
				"Error getting GenericFile list from Pharos: %s",
				resp.Error)
		}
		for _, gf := range resp.GenericFiles() {
			if aptQueue.addToNSQ(gf) {
				itemsAdded += 1
			}
		}
		if resp.HasNextPage() == false || itemsAdded >= aptQueue.maxFiles {
			break
		}
		params = resp.ParamsForNextPage()
	}
}

func (aptQueue *APTQueueFixity) addToNSQ(gf *models.GenericFile) bool {
	err := aptQueue.NSQClient.EnqueueString(aptQueue.nsqTopic, gf.Identifier)
	if err != nil {
		aptQueue.Context.MessageLog.Error("Error sending '%s' to %s: %v",
			gf.Identifier, aptQueue.nsqTopic, err)
		return false
	}
	aptQueue.Context.MessageLog.Info("Added '%s' to %s",
		gf.Identifier, aptQueue.nsqTopic)
	return true
}
