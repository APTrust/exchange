package workers

// import (
// 	"github.com/APTrust/exchange/constants"
// 	"github.com/APTrust/exchange/context"
// 	"github.com/APTrust/exchange/dpn/models"
// 	"github.com/APTrust/exchange/dpn/network"
// 	apt_models "github.com/APTrust/exchange/models"
// 	"github.com/APTrust/exchange/util"
// 	"github.com/APTrust/exchange/validation"
// 	"github.com/nsqio/go-nsq"
// 	"log"
// 	"os"
// 	"path/filepath"
// 	"strconv"
// 	"strings"
// 	"time"
// )

// // GetWorkItem returns the WorkItem with the specified Id from Pharos,
// // or nil.
// func GetRetrievalManifest(message *nsq.Message, summaryName string) *models.DPNRetrievalManifest {
// 	msgBody := strings.TrimSpace(string(message.Body))
// 	restorer.Context.MessageLog.Info("NSQ Message body: '%s'", msgBody)
// 	manifest := models.NewDPNRetrievalManifest(message)
// 	manifest.TaskType = constants.ActionFixityCheck
// 	manifest.GlacierBucket = restorer.Context.Config.DPN.DPNPreservationBucket

// 	dpnWorkItemId, err := strconv.Atoi(string(msgBody))
// 	if err != nil || dpnWorkItemId == 0 {
// 		manifest.GetSummary(summaryName).AddError(
// 			"Could not get DPNWorkItem Id from NSQ message body: %v", err)
// 		return manifest
// 	}

// 	restorer.GetDPNWorkItem(manifest, dpnWorkItemId)

// 	if manifest.DPNWorkItem.State != nil && *manifest.DPNWorkItem.State != "" {
// 		restoredManifest, err := models.DPNRetrievalManifestFromJson(*manifest.DPNWorkItem.State)
// 		if err != nil {
// 			restorer.Context.MessageLog.Warning("Error restoring manifest state "+
// 				"for DPNWorkItem %d: %v JSON:\n%s", manifest.DPNWorkItem.Id, err,
// 				manifest.DPNWorkItem.Identifier, *manifest.DPNWorkItem.State)
// 			restorer.Context.MessageLog.Warning("Starting with new manifest for "+
// 				"for DPNWorkItem %d (%s)", manifest.DPNWorkItem.Id,
// 				manifest.DPNWorkItem.Identifier)
// 		} else {
// 			restoredManifest.NsqMessage = manifest.NsqMessage
// 			restoredManifest.DPNWorkItem = manifest.DPNWorkItem
// 			manifest = restoredManifest
// 		}
// 	}

// 	// This will be nil for new jobs, and should be non-nil if we're reattempting
// 	// an existing job. DPN bag records are immutable, so we don't need to reload
// 	// them each time we reattempt a task. (Unlike DPNWorkItem, which can be
// 	// cancelled between attempts.)
// 	if manifest.DPNBag == nil {
// 		restorer.GetDPNBag(manifest)
// 	}
// 	if manifest.ExpectedFixityValue == "" {
// 		restorer.GetBagDigest(manifest)
// 	}

// 	return manifest
// }

// func (restorer *DPNGlacierRestoreInit) GetDPNWorkItem(manifest *models.DPNRetrievalManifest, summaryName string, dpnWorkItemId int) {
// 	// Get the DPN work item
// 	resp := restorer.Context.PharosClient.DPNWorkItemGet(dpnWorkItemId)
// 	if resp.Error != nil {
// 		manifest.GetSummary(summaryName).AddError(
// 			"Error getting DPNWorkItem %d from Pharos: %v",
// 			dpnWorkItemId, resp.Error)
// 		return
// 	}
// 	dpnWorkItem := resp.DPNWorkItem()
// 	if dpnWorkItem == nil {
// 		manifest.GetSummary(summaryName).AddError(
// 			"Pharos returned nil for WorkItem %d", dpnWorkItemId)
// 		return
// 	}

// 	manifest.DPNWorkItem = dpnWorkItem
// 	manifest.DPNWorkItem.SetNodeAndPid()
// 	note := "Requesting Glacier restoration for fixity"
// 	manifest.DPNWorkItem.Note = &note
// }

// func (restorer *DPNGlacierRestoreInit) GetDPNBag(manifest *models.DPNRetrievalManifest, summaryName string) {
// 	// Get the DPN Bag from the DPN REST server.
// 	resp := restorer.LocalDPNRestClient.DPNBagGet(manifest.DPNWorkItem.Identifier)
// 	if resp.Error != nil {
// 		manifest.GetSummary(summaryName).AddError(
// 			"Error getting DPN bag %s from %s: %v",
// 			manifest.DPNWorkItem.Identifier,
// 			restorer.Context.Config.DPN.RestClient.LocalServiceURL, resp.Error)
// 		return
// 	}
// 	dpnBag := resp.Bag()
// 	if dpnBag == nil {
// 		manifest.GetSummary(summaryName).AddError(
// 			"DPN REST server returned nil for bag %s",
// 			manifest.DPNWorkItem.Identifier)
// 		return
// 	}
// 	manifest.DPNBag = dpnBag
// }

// func (restorer *DPNGlacierRestoreInit) GetBagDigest(manifest *models.DPNRetrievalManifest, summaryName string) {
// 	resp := restorer.LocalDPNRestClient.DigestGet(manifest.DPNWorkItem.Identifier, constants.AlgSha256)
// 	if resp.Error != nil {
// 		manifest.GetSummary(summaryName).AddError(
// 			"Error getting sha256 digest for DPN bag %s from %s: %v",
// 			manifest.DPNWorkItem.Identifier,
// 			restorer.Context.Config.DPN.RestClient.LocalServiceURL, resp.Error)
// 		return
// 	}
// 	digest := resp.Digest()
// 	if digest == nil {
// 		manifest.GetSummary(summaryName).AddError(
// 			"DPN REST server returned nil digest for bag %s",
// 			manifest.DPNWorkItem.Identifier)
// 		return
// 	}
// 	manifest.ExpectedFixityValue = digest.Value
// }

// func (restorer *DPNGlacierRestoreInit) SaveDPNWorkItem(manifest *models.DPNRetrievalManifest, summaryName string) {
// 	jsonData, err := manifest.ToJson()
// 	if err != nil {
// 		msg := fmt.Sprintf("Could not marshal DPNRetrievalManifest "+
// 			"for DPNWorkItem %d: %v", manifest.DPNWorkItem.Id, err)
// 		restorer.Context.MessageLog.Error(msg)
// 		note := "JSON serialization error while trying to save DPNWorkItemState."
// 		manifest.DPNWorkItem.Note = &note
// 	}

// 	// Update the DPNWorkItem
// 	manifest.DPNWorkItem.State = &jsonData
// 	manifest.DPNWorkItem.Retry = !manifest.GetSummary(summaryName).ErrorIsFatal

// 	resp := restorer.Context.PharosClient.DPNWorkItemSave(manifest.DPNWorkItem)
// 	if resp.Error != nil {
// 		msg := fmt.Sprintf("Could not save DPNWorkItem %d "+
// 			"for fixity on bag %s to Pharos: %v",
// 			manifest.DPNWorkItem.Id, manifest.DPNWorkItem.Identifier, err)
// 		restorer.Context.MessageLog.Error(msg)
// 		manifest.GetSummary(summaryName).AddError(msg)
// 	}
// }
