package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"strconv"
)

// GetDPNWorkItem returns the DPNWorkItem associated with this message.
// Param _context is a context object, manifest is a ReplicationManifest,
// and workSummary should be the WorkSummary pertinent to the current
// operation. So, on copy, workSummary should be manifest.CopySummary;
// on validation, it should be manifest.ValidationSummary; and on store
// it should be manifest.StoreSummary.
func GetDPNWorkItem(_context *context.Context, manifest *models.ReplicationManifest, workSummary *apt_models.WorkSummary) {
	workItemId, err := strconv.Atoi(string(manifest.NsqMessage.Body))
	if err != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItemId from" +
			"NSQ message body '%s': %v", manifest.NsqMessage.Body, err)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	resp := _context.PharosClient.DPNWorkItemGet(workItemId)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItem (id %d) " +
			"from Pharos: %v", workItemId, resp.Error)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	dpnWorkItem := resp.DPNWorkItem()
	manifest.DPNWorkItem = dpnWorkItem
	if dpnWorkItem == nil {
		msg := fmt.Sprintf("Pharos returned nil for DPNWorkItem %d",
			workItemId)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	if dpnWorkItem.Task != constants.DPNTaskReplication {
		msg := fmt.Sprintf("DPNWorkItem %d has task type %s, " +
			"and does not belong in this queue!", workItemId, dpnWorkItem.Task)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
	}
	if !util.LooksLikeUUID(dpnWorkItem.Identifier) {
		msg := fmt.Sprintf("DPNWorkItem %d has identifier '%s', " +
			"which does not look like a UUID", workItemId, dpnWorkItem.Identifier)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
	}
}

// GetXferRequest gets the ReplicationTransfer request from our local
// DPN REST server that describes the replication we're about to
// perform. Param _context is a context object, manifest is a ReplicationManifest,
// and workSummary should be the WorkSummary pertinent to the current
// operation. So, on copy, workSummary should be manifest.CopySummary;
// on validation, it should be manifest.ValidationSummary; and on store
// it should be manifest.StoreSummary.
func GetXferRequest(localClient *network.DPNRestClient, manifest *models.ReplicationManifest, workSummary *apt_models.WorkSummary) {
	if manifest == nil || manifest.DPNWorkItem == nil {
		msg := fmt.Sprintf("getXferRequest: ReplicationManifest.DPNWorkItem cannot be nil.")
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	resp := localClient.ReplicationTransferGet(manifest.DPNWorkItem.Identifier)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get ReplicationTransfer %s " +
			"from DPN server: %v", manifest.DPNWorkItem.Identifier, resp.Error)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	xfer := resp.ReplicationTransfer()
	manifest.ReplicationTransfer = xfer
	if xfer == nil {
		msg := fmt.Sprintf("DPN server returned nil for ReplicationId %s",
			manifest.DPNWorkItem.Identifier)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	if xfer.Stored {
		msg := fmt.Sprintf("ReplicationId %s is already marked as Stored. Nothing left to do.",
			manifest.DPNWorkItem.Identifier)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	if xfer.Cancelled {
		msg := fmt.Sprintf("ReplicationId %s was cancelled. Nothing left to do.",
			manifest.DPNWorkItem.Identifier)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
	}
}

// GetDPNBag gets the bag record fom the local DPN REST server that
// describes the bag we are being asked to copy.
// Param _context is a context object, manifest is a ReplicationManifest,
// and workSummary should be the WorkSummary pertinent to the current
// operation. So, on copy, workSummary should be manifest.CopySummary;
// on validation, it should be manifest.ValidationSummary; and on store
// it should be manifest.StoreSummary.
func GetDPNBag(localClient *network.DPNRestClient, manifest *models.ReplicationManifest, workSummary *apt_models.WorkSummary) {
	if manifest == nil || manifest.ReplicationTransfer == nil {
		msg := fmt.Sprintf("getDPNBag: ReplicationManifest.ReplicationTransfer cannot be nil.")
		workSummary.ErrorIsFatal = true
		workSummary.AddError(msg)
		return
	}
	resp := localClient.DPNBagGet(manifest.ReplicationTransfer.Bag)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get ReplicationTransfer %s " +
			"from DPN server: %v", manifest.DPNWorkItem.Identifier, resp.Error)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	dpnBag := resp.Bag()
	manifest.DPNBag = dpnBag
	if dpnBag == nil {
		msg := fmt.Sprintf("DPN server returned nil for Bag %s",
			manifest.ReplicationTransfer.Bag)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
}

// reserveSpaceOnVolume does just what it says.
// Make sure we have space to copy this item from the remote node.
// We will be validating this bag in a later step without untarring it,
// so we just have to reserve enough room for the tar file.
func ReserveSpaceOnVolume(_context *context.Context, manifest *models.ReplicationManifest) (bool) {
	okToCopy := false
	err := _context.VolumeClient.Ping(500)
	if err == nil {
		path := manifest.LocalPath
		ok, err := _context.VolumeClient.Reserve(path, uint64(manifest.DPNBag.Size))
		if err != nil {
			_context.MessageLog.Warning("Volume service returned an error. " +
				"Will requeue ReplicationTransfer %s bag (%s) because we may not " +
				"have enough space to copy %d bytes from %s.",
				manifest.ReplicationTransfer.ReplicationId,
				manifest.ReplicationTransfer.Bag,
				manifest.DPNBag.Size,
				manifest.ReplicationTransfer.FromNode)
		} else if ok {
			// VolumeService says we have enough space for this.
			okToCopy = ok
		}
	} else {
		_context.MessageLog.Warning("Volume service is not running or returned an error. " +
			"Continuing as if we have enough space to download %d bytes.",
			manifest.DPNBag.Size,)
		okToCopy = true
	}
	return okToCopy
}
