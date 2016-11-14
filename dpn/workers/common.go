package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/nsqio/go-nsq"
	"log"
	"path/filepath"
	"strconv"
	"time"
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

// GetXferRequest gets the ReplicationTransfer request from the
// DPN REST server that describes the replication we're about to
// perform. Param _context is a context object, manifest is a ReplicationManifest,
// and workSummary should be the WorkSummary pertinent to the current
// operation. So, on copy, workSummary should be manifest.CopySummary;
// on validation, it should be manifest.ValidationSummary; and on store
// it should be manifest.StoreSummary.
func GetXferRequest(dpnClient *network.DPNRestClient, manifest *models.ReplicationManifest, workSummary *apt_models.WorkSummary) {
	if manifest == nil || manifest.DPNWorkItem == nil {
		msg := fmt.Sprintf("getXferRequest: ReplicationManifest.DPNWorkItem cannot be nil.")
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	resp := dpnClient.ReplicationTransferGet(manifest.DPNWorkItem.Identifier)
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

// GetDPNBag gets the bag record fom the DPN REST server that
// describes the bag we are being asked to copy.
// Param _context is a context object, manifest is a ReplicationManifest,
// and workSummary should be the WorkSummary pertinent to the current
// operation. So, on copy, workSummary should be manifest.CopySummary;
// on validation, it should be manifest.ValidationSummary; and on store
// it should be manifest.StoreSummary.
func GetDPNBag(dpnClient *network.DPNRestClient, manifest *models.ReplicationManifest, workSummary *apt_models.WorkSummary) {
	if manifest == nil || manifest.ReplicationTransfer == nil {
		msg := fmt.Sprintf("getDPNBag: ReplicationManifest.ReplicationTransfer cannot be nil.")
		workSummary.ErrorIsFatal = true
		workSummary.AddError(msg)
		return
	}
	resp := dpnClient.DPNBagGet(manifest.ReplicationTransfer.Bag)
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

// UpdateReplicationTransfer updates manifest.ReplicationTransfer at the
// remote DPN node that remoteClient is connected to. That must be the
// FromNode of the ReplicationTransfer.
func UpdateReplicationTransfer(_context *context.Context, remoteClient *network.DPNRestClient, manifest *models.ReplicationManifest) {
	if manifest.ReplicationTransfer != nil {
		if remoteClient == nil {
			msg := fmt.Sprintf("Cannot update ReplicationTransfer %s " +
				"because REST client for node %s is nil.",
				manifest.ReplicationTransfer.ReplicationId,
				manifest.ReplicationTransfer.FromNode)
			manifest.CopySummary.AddError(msg)
			_context.MessageLog.Error(msg)
		} else {
			resp := remoteClient.ReplicationTransferUpdate(manifest.ReplicationTransfer)
			rawRespData, _ := resp.RawResponseData()
			respBody := "[response body not available]"
			if rawRespData != nil {
				respBody = string(rawRespData)
			}
			if resp.Error != nil {
				msg := fmt.Sprintf("When trying to update ReplicationTransfer %s," +
					"got error %v. Response body: %s",
					manifest.ReplicationTransfer.ReplicationId,
					resp.Error, respBody)
				manifest.CopySummary.AddError(msg)
				_context.MessageLog.Error(msg)
			} else if resp.ReplicationTransfer() == nil {
				msg := fmt.Sprintf("When updating ReplicationTransfer %s, " +
					"remote server did not return an updated transfer record. " +
					"Response body: %s",
					manifest.ReplicationTransfer.ReplicationId,
					respBody)
				manifest.CopySummary.AddError(msg)
				_context.MessageLog.Error(msg)
			} else {
				// Update succeeded. Update our manifest with the transfer
				// record that the remote DPN server returned. That record
				// will say whether or not the remote server wants us to
				// store the bag.
				manifest.ReplicationTransfer = resp.ReplicationTransfer()
				_context.MessageLog.Info("Updated xfer %s at %s",
					manifest.ReplicationTransfer.ReplicationId,
					manifest.ReplicationTransfer.FromNode)
			}
		}
	} else {
		_context.MessageLog.Warning("Cannot update nil ReplicationTransfer.")
	}
}

// Dump the WorkItemState.State into the JSON log, surrounded my markers that
// make it easy to find. This log gets big.
func LogReplicationJson (manifest *models.ReplicationManifest, jsonLog *log.Logger) {
	timestamp := time.Now().UTC().Format(time.RFC3339)
	startMessage := fmt.Sprintf("-------- BEGIN DPNWorkItem %d | XferId: %s | Time: %s --------",
		manifest.DPNWorkItem.Id, manifest.DPNWorkItem.Identifier, timestamp)
	endMessage := fmt.Sprintf("-------- END DPNWorkItem %d | XferId: %s | Time: %s --------",
		manifest.DPNWorkItem.Id, manifest.DPNWorkItem.Identifier, timestamp)
	state := "{}"
	if manifest.DPNWorkItem.State != nil {
		state = *manifest.DPNWorkItem.State
	}
	jsonLog.Println(startMessage, "\n",
		state, "\n",
		endMessage, "\n")
}

// SaveDPNWorkItemState saves the manifest.DPNWorkItem to Pharos,
// after it's State property to a JSON serialization of the manifest.
func SaveDPNWorkItemState(_context *context.Context, manifest *models.ReplicationManifest, workSummary *apt_models.WorkSummary) {
	dpnWorkItem := manifest.DPNWorkItem
	priorState := dpnWorkItem.State
	empty := ""
	dpnWorkItem.State = &empty
	jsonData, err := json.Marshal(manifest)
	if err != nil {
		msg := fmt.Sprintf("Could not marshal ReplicationManifest " +
			"for replication %s to JSON: %v", manifest.DPNWorkItem.Identifier,  err)
		_context.MessageLog.Error(msg)
		workSummary.AddError(msg)
		dpnWorkItem.State = priorState
		if dpnWorkItem.Note == nil {
			note := ""
			dpnWorkItem.Note = &note
		}
		*dpnWorkItem.Note += "[JSON serialization error]"
	}
	newState := string(jsonData)
	dpnWorkItem.State = &newState
	resp := _context.PharosClient.DPNWorkItemSave(dpnWorkItem)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not save DPNWorkItem %d " +
			"for replication %s to Pharos: %v",
			manifest.DPNWorkItem.Id, manifest.DPNWorkItem.Identifier, err)
		_context.MessageLog.Error(msg)
		workSummary.AddError(msg)
	}
}

// SetupReplicationManifest creates a ReplicationManifest for this job.
func SetupReplicationManifest(message *nsq.Message, stage string, _context *context.Context, localClient *network.DPNRestClient, remoteClients map[string]*network.DPNRestClient) (*models.ReplicationManifest) {
	manifest := models.NewReplicationManifest(message)
	var activeSummary *apt_models.WorkSummary
	if stage == "copy" {
		activeSummary = manifest.CopySummary
	} else if stage == "validate" {
		activeSummary = manifest.ValidateSummary
	} else if stage == "store" {
		activeSummary = manifest.StoreSummary
	} else {
		panic(fmt.Sprintf("Unknown stage %s", stage))
	}

	// Get the DPNWorkItem that describes this replication.
	workItemId, err := strconv.Atoi(string(manifest.NsqMessage.Body))
	if err != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItemId from" +
			"NSQ message body '%s': %v", manifest.NsqMessage.Body, err)
		activeSummary.AddError(msg)
		activeSummary.ErrorIsFatal = true
		activeSummary.Finish()
		return manifest
	}
	_context.MessageLog.Info("Requesting DPNWorkItem %d from Pharos", workItemId)
	resp := _context.PharosClient.DPNWorkItemGet(workItemId)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItem (id %d) " +
			"from Pharos: %v", workItemId, resp.Error)
		activeSummary.AddError(msg)
		activeSummary.ErrorIsFatal = true
		activeSummary.Finish()
		return manifest
	}
	manifest.DPNWorkItem = resp.DPNWorkItem()

	// Restore the manifest from the saved state. If we can recover
	// the saved state from the DPNWorkItem, it will replace the
	// manifest we created above. Note that that manifest is still
	// empty at this point. If we can't recover the manifest, we'll
	// rebuild it, but it will be missing WorkSummary records from
	// previous stages.
	savedState := ""
	fromNode := ""
	if manifest.DPNWorkItem.State != nil {
		savedState = *manifest.DPNWorkItem.State
	}
	savedManifest := &models.ReplicationManifest{}
	unmarshaFailed := false
	err = json.Unmarshal([]byte(savedState), &savedManifest)
	if err != nil {
		_context.MessageLog.Warning(
			"Cannot unmarshal saved manifest for ReplicationId %s: %v\n" +
				"Will re-fetch bag from remote node. Manifest data: %s",
			err, manifest.DPNWorkItem.Identifier, savedState)
		unmarshaFailed = true
	} else {
		manifest = savedManifest
		manifest.DPNWorkItem = resp.DPNWorkItem()
		fromNode = manifest.ReplicationTransfer.FromNode
	}

	// Make sure we attach this, so we can touch, finish, and requeue
	// the NSQ message.
	manifest.NsqMessage = message

	// Clear prior errors for this stage of processing, since we're
	// about to try again.
	if stage == "copy" {
		manifest.CopySummary.ClearErrors()
	} else if stage == "validate" {
		manifest.ValidateSummary.ClearErrors()
	} else if stage == "store" {
		manifest.StoreSummary.ClearErrors()
	}

	// Get the latest copy of the ReplicationTransfer from
	// the remote node. There's a chance this replication may
	// have been cancelled since we copied the bag from the
	// remote node. Unfortunately, we have to do this twice.
	// If we don't know the FromNode, we have to first get
	// the ReplicationTransfer from our node, then check with
	// the node that originated the request, to ensure it hasn't
	// been cancelled.
	if fromNode == "" {
		_context.MessageLog.Info("Requesting ReplicationTransfer %s from local DPN server",
			manifest.DPNWorkItem.Identifier)
		GetXferRequest(localClient, manifest, activeSummary)
		if activeSummary.HasErrors() {
			return manifest
		}
		fromNode = manifest.ReplicationTransfer.FromNode
	}

	// Now we get the transfer record from the originating node.
	remoteClient := remoteClients[fromNode]
	if remoteClient == nil {
		activeSummary.AddError("Cannot get remote DPN client for %s",
			manifest.ReplicationTransfer.FromNode)
	}
	_context.MessageLog.Info("Requesting ReplicationTransfer %s from remote DPN server %s",
		manifest.DPNWorkItem.Identifier, manifest.ReplicationTransfer.FromNode)
	GetXferRequest(remoteClient, manifest, activeSummary)
	if activeSummary.HasErrors() {
		return manifest
	}

	// A little more manifest-building, in case we were unable to
	// restore the original manifest above.
	if unmarshaFailed {
		// This is where we have stored our local copy of this bag.
		manifest.LocalPath = filepath.Join(
			_context.Config.DPN.StagingDirectory,
			manifest.ReplicationTransfer.Bag + ".tar")
		_context.MessageLog.Info("Set manifest.LocalPath to %s", manifest.LocalPath)
		// Get the DPN bag from the remote node.
		GetDPNBag(remoteClient, manifest, activeSummary)
	}

	// In the copy stage, StoreRequested will always be false. After the copy
	// stage, when we send the fixity value back to the remote server and the
	// remote knows whether or not we received a valid copy of the bag, the
	// remote server (FromNode) will set StoreRequested to true. We don't want
	// to proceed past the copy stage if StoreRequested is false.
	if stage != "copy" && manifest.ReplicationTransfer.StoreRequested == false {
		activeSummary.AddError("Aborting replication because StoreRequested is false.")
		manifest.Cancelled = true
	}
	// Remote node (FromNode) can cancel the replication at any time, so always
	// check this.
	if manifest.ReplicationTransfer.Cancelled == true {
		activeSummary.AddError("Aborting replication because Cancelled is true.")
		manifest.Cancelled = true
	}

	return manifest
}
