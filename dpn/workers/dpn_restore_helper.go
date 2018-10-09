package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nsqio/go-nsq"
	"os"
	"strconv"
	"strings"
	"time"
)

type DPNRestoreHelper struct {
	Manifest      *models.DPNRetrievalManifest
	WorkSummary   *apt_models.WorkSummary
	context       *context.Context
	dpnRestClient *network.DPNRestClient
	summaryName   string
	workItemId    int
}

func NewDPNRestoreHelper(message *nsq.Message, _context *context.Context, dpnRestClient *network.DPNRestClient, action, summaryName string) (*DPNRestoreHelper, error) {
	helper := &DPNRestoreHelper{
		context:       _context,
		dpnRestClient: dpnRestClient,
		summaryName:   summaryName,
	}
	// action should be constants.ActionFixityCheck or constants.ActionRestore
	err := helper.initManifest(message, action)
	return helper, err
}

func (helper *DPNRestoreHelper) initManifest(message *nsq.Message, action string) error {
	msgBody := strings.TrimSpace(string(message.Body))
	helper.context.MessageLog.Info("NSQ Message body: '%s'", msgBody)
	helper.Manifest = models.NewDPNRetrievalManifest(message)
	helper.Manifest.TaskType = action
	helper.Manifest.GlacierBucket = helper.context.Config.DPN.DPNPreservationBucket

	// Set the WorkSummary to match the current action. E.g. If we're
	// working on the GlacierRestore phase, we should record errors in
	// the GlacierRestore WorkSummary
	helper.WorkSummary = helper.Manifest.GetSummary(helper.summaryName)
	if helper.WorkSummary == nil {
		return fmt.Errorf("Manifest has no WorkSummary for %s", helper.summaryName)
	}

	var err error
	helper.workItemId, err = strconv.Atoi(string(msgBody))
	if err != nil || helper.workItemId == 0 {
		helper.WorkSummary.AddError(
			"Could not get DPNWorkItem Id from NSQ message body: %v", err)
		return err
	}

	err = helper.getDPNWorkItem()
	if err != nil {
		return err
	}

	if helper.Manifest.DPNWorkItem.State != nil && *helper.Manifest.DPNWorkItem.State != "" {
		restoredManifest, err := models.DPNRetrievalManifestFromJson(*helper.Manifest.DPNWorkItem.State)
		if err != nil {
			helper.context.MessageLog.Warning("Error restoring manifest state "+
				"for DPNWorkItem %d: %v JSON:\n%s", helper.Manifest.DPNWorkItem.Id, err,
				helper.Manifest.DPNWorkItem.Identifier, *helper.Manifest.DPNWorkItem.State)
			helper.context.MessageLog.Warning("Starting with new manifest for "+
				"for DPNWorkItem %d (%s)", helper.Manifest.DPNWorkItem.Id,
				helper.Manifest.DPNWorkItem.Identifier)
		} else {
			restoredManifest.NsqMessage = helper.Manifest.NsqMessage
			restoredManifest.DPNWorkItem = helper.Manifest.DPNWorkItem
			helper.Manifest = restoredManifest
		}
	}

	// This will be nil for new jobs, and should be non-nil if we're reattempting
	// an existing job. DPN bag records are immutable, so we don't need to reload
	// them each time we reattempt a task. (Unlike DPNWorkItem, which can be
	// cancelled between attempts.)
	if helper.Manifest.DPNBag == nil {
		err = helper.getDPNBag()
		if err != nil {
			return err
		}
	}
	if helper.Manifest.ExpectedFixityValue == "" {
		err = helper.getBagDigest()
		if err != nil {
			return err
		}
	}
	if helper.Manifest.GlacierKey == "" {
		helper.Manifest.GlacierKey = fmt.Sprintf("%s.tar", helper.Manifest.DPNBag.UUID)
	}

	return nil
}

func (helper *DPNRestoreHelper) getDPNWorkItem() error {
	resp := helper.context.PharosClient.DPNWorkItemGet(helper.workItemId)
	if resp.Error != nil {
		helper.WorkSummary.AddError(
			"Error getting DPNWorkItem %d from Pharos: %v",
			helper.workItemId, resp.Error)
		return resp.Error
	}
	dpnWorkItem := resp.DPNWorkItem()
	if dpnWorkItem == nil {
		errMessage := fmt.Sprintf("Pharos returned nil for WorkItem %d", helper.workItemId)
		helper.WorkSummary.AddError(errMessage)
		return fmt.Errorf(errMessage)
	}

	helper.Manifest.DPNWorkItem = dpnWorkItem
	helper.Manifest.DPNWorkItem.SetNodeAndPid()
	if helper.Manifest.DPNWorkItem.Note == nil {
		note := "Requesting Glacier restoration for fixity"
		helper.Manifest.DPNWorkItem.Note = &note
	}
	queuedAt := time.Time{}
	if helper.Manifest.DPNWorkItem.QueuedAt != nil {
		queuedAt = *helper.Manifest.DPNWorkItem.QueuedAt
	}
	helper.context.MessageLog.Info("Loaded DPNWorkItem %d with QueuedAt = %s",
		helper.Manifest.DPNWorkItem.Id, queuedAt.Format(time.RFC3339))
	return nil
}

func (helper *DPNRestoreHelper) getDPNBag() error {
	// Get the DPN Bag from the DPN REST server.
	resp := helper.dpnRestClient.DPNBagGet(helper.Manifest.DPNWorkItem.Identifier)
	if resp.Error != nil {
		errMsg := fmt.Sprintf("Error getting DPN bag %s from %s: %v",
			helper.Manifest.DPNWorkItem.Identifier,
			helper.context.Config.DPN.RestClient.LocalServiceURL, resp.Error)
		helper.WorkSummary.AddError(errMsg)
		return fmt.Errorf(errMsg)
	}
	dpnBag := resp.Bag()
	if dpnBag == nil {
		errMsg := fmt.Sprintf("DPN REST server returned nil for bag %s",
			helper.Manifest.DPNWorkItem.Identifier)
		helper.WorkSummary.AddError(errMsg)
		return fmt.Errorf(errMsg)
	}
	helper.Manifest.DPNBag = dpnBag
	return nil
}

func (helper *DPNRestoreHelper) getBagDigest() error {
	resp := helper.dpnRestClient.DigestGet(helper.Manifest.DPNWorkItem.Identifier, constants.AlgSha256)
	if resp.Error != nil {
		errMsg := fmt.Sprintf("Error getting sha256 digest for DPN bag %s from %s: %v",
			helper.Manifest.DPNWorkItem.Identifier,
			helper.context.Config.DPN.RestClient.LocalServiceURL, resp.Error)
		helper.WorkSummary.AddError(errMsg)
		return fmt.Errorf(errMsg)
	}
	digest := resp.Digest()
	if digest == nil {
		errMsg := fmt.Sprintf("DPN REST server returned nil digest for bag %s",
			helper.Manifest.DPNWorkItem.Identifier)
		helper.WorkSummary.AddError(errMsg)
		return fmt.Errorf(errMsg)
	}
	helper.Manifest.ExpectedFixityValue = digest.Value
	return nil
}

func (helper *DPNRestoreHelper) SaveDPNWorkItem() {
	jsonData, err := helper.Manifest.ToJson()
	if err != nil {
		msg := fmt.Sprintf("Could not marshal DPNRetrievalManifest "+
			"for DPNWorkItem %d: %v", helper.Manifest.DPNWorkItem.Id, err)
		helper.context.MessageLog.Error(msg)
		note := "JSON serialization error while trying to save DPNWorkItemState."
		helper.Manifest.DPNWorkItem.Note = &note
	}

	// Update the DPNWorkItem
	helper.Manifest.DPNWorkItem.State = &jsonData
	helper.Manifest.DPNWorkItem.Retry = !helper.WorkSummary.ErrorIsFatal

	queuedAt := time.Time{}
	if helper.Manifest.DPNWorkItem.QueuedAt != nil {
		queuedAt = *helper.Manifest.DPNWorkItem.QueuedAt
	}
	helper.context.MessageLog.Info("Saving DPNWorkItem %d with QueuedAt = %s",
		helper.Manifest.DPNWorkItem.Id, queuedAt.Format(time.RFC3339))
	resp := helper.context.PharosClient.DPNWorkItemSave(helper.Manifest.DPNWorkItem)
	if resp.Error != nil {
		rawResponse := "[Unavailable]"
		data, _ := resp.RawResponseData()
		if data != nil {
			rawResponse = string(data)
		}
		msg := fmt.Sprintf("Could not save DPNWorkItem %d "+
			"for fixity on bag %s to Pharos: %v ... Raw Response: %s",
			helper.Manifest.DPNWorkItem.Id, helper.Manifest.DPNWorkItem.Identifier,
			err, rawResponse)
		helper.context.MessageLog.Error(msg)
		helper.WorkSummary.AddError(msg)
	}
}

func (helper *DPNRestoreHelper) FileExistsAndIsComplete() bool {
	if helper.Manifest.LocalPath == "" {
		helper.context.MessageLog.Info("No file path is set yet for WorkItem %d (Bag %s).",
			helper.Manifest.DPNWorkItem.Id, helper.Manifest.DPNWorkItem.Identifier)
	}
	if helper.Manifest.LocalPath != "" && fileutil.FileExists(helper.Manifest.LocalPath) {
		file, err := os.Open(helper.Manifest.LocalPath)
		if err != nil {
			helper.context.MessageLog.Warning("Error opening file %s. Telling worker this "+
				"file is not complete on disk. %v", helper.Manifest.LocalPath, err)
			return false
		}
		defer file.Close()
		fileInfo, err := file.Stat()
		if err != nil {
			helper.context.MessageLog.Warning("Error getting stats for file %s. "+
				"Telling worker this file is not complete on disk. %v", helper.Manifest.LocalPath, err)
			return false
		}
		if uint64(fileInfo.Size()) == helper.Manifest.DPNBag.Size {
			helper.context.MessageLog.Info("File %s is already on disk and same size as bag (%d bytes)",
				helper.Manifest.LocalPath, helper.Manifest.DPNBag.Size)
			return true
		} else {
			helper.context.MessageLog.Info("File %s is on disk, but size doesn't match. "+
				"Disk has %d bytes, DPN Bag is %d bytes", helper.Manifest.LocalPath,
				fileInfo.Size(), helper.Manifest.DPNBag.Size)
			return false
		}
	}
	helper.context.MessageLog.Info("File %s is not on disk yet.", helper.Manifest.LocalPath)
	return false
}
