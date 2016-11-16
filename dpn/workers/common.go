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
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// GetDPNWorkItem fetches the DPNWorkItem associated with this message
// and attaches it to the manifest.
//
// Param _context is a context object, manifest is a ReplicationManifest,
// and workSummary should be the WorkSummary pertinent to the current
// operation. So, on copy, workSummary should be manifest.CopySummary;
// on validation, it should be manifest.ValidationSummary; and on store
// it should be manifest.StoreSummary.
func GetDPNWorkItem(_context *context.Context, manifest *models.ReplicationManifest, workSummary *apt_models.WorkSummary) {
	workItemId, err := strconv.Atoi(string(manifest.NsqMessage.Body))
	if err != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItemId from"+
			"NSQ message body '%s': %v", manifest.NsqMessage.Body, err)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	resp := _context.PharosClient.DPNWorkItemGet(workItemId)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItem (id %d) "+
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
		msg := fmt.Sprintf("DPNWorkItem %d has task type %s, "+
			"and does not belong in this queue!", workItemId, dpnWorkItem.Task)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
	}
	if !util.LooksLikeUUID(dpnWorkItem.Identifier) {
		msg := fmt.Sprintf("DPNWorkItem %d has identifier '%s', "+
			"which does not look like a UUID", workItemId, dpnWorkItem.Identifier)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
	}
}

// GetWorkItem fetches the WorkItem associated with this message
// and attaches it to the manifest.
//
// Param _context is a context object, manifest is an IngestManifest,
// and workSummary should be the WorkSummary pertinent to the current
// operation. So, on package, workSummary should be manifest.PackageSummary;
// on store it should be manifest.StoreSummary, and on record, it should be
// manifest.RecordSummary.
func GetWorkItem(_context *context.Context, manifest *models.DPNIngestManifest, workSummary *apt_models.WorkSummary) {
	workItemId, err := strconv.Atoi(string(manifest.NsqMessage.Body))
	if err != nil {
		msg := fmt.Sprintf("Could not get WorkItemId from"+
			"NSQ message body '%s': %v", manifest.NsqMessage.Body, err)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	resp := _context.PharosClient.WorkItemGet(workItemId)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get WorkItem (id %d) "+
			"from Pharos: %v", workItemId, resp.Error)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	workItem := resp.WorkItem()
	manifest.WorkItem = workItem
	if workItem == nil {
		msg := fmt.Sprintf("Pharos returned nil for WorkItem %d", workItemId)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	if workItem.Action != constants.ActionDPN {
		msg := fmt.Sprintf("WorkItem %d has action type %s, "+
			"and does not belong in this queue!", workItemId, workItem.Action)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
	}
}

// GetWorkItemState fetches the WorkItemState associated with this message
// and attaches it to the manifest.
//
// Param _context is a context object, manifest is an IngestManifest,
// and workSummary should be the WorkSummary pertinent to the current
// operation. So, on package, workSummary should be manifest.PackageSummary;
// on store it should be manifest.StoreSummary, and on record, it should be
// manifest.RecordSummary.
func GetWorkItemState(_context *context.Context, manifest *models.DPNIngestManifest, workSummary *apt_models.WorkSummary) {
	if manifest.WorkItem == nil {
		msg := fmt.Sprintf("Can't get WorkItemState: WorkItem is nil or WorkItemStateId is missing")
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	if manifest.WorkItem.WorkItemStateId == nil || *manifest.WorkItem.WorkItemStateId == 0 {
		// If this is our first attempt at packaging, this item will have no state.
		_context.MessageLog.Info("No WorkItemState for WorkItem %d",
			manifest.WorkItem.Id, manifest.WorkItem.ObjectIdentifier)
		return
	}
	resp := _context.PharosClient.WorkItemStateGet(*manifest.WorkItem.WorkItemStateId)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get WorkItemState (id %d) "+
			"from Pharos: %v", manifest.WorkItem.WorkItemStateId, resp.Error)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	workItemState := resp.WorkItemState()
	manifest.WorkItemState = workItemState
	if workItemState == nil {
		msg := fmt.Sprintf("Pharos returned nil for WorkItemState %d", manifest.WorkItem.WorkItemStateId)
		workSummary.AddError(msg)
		workSummary.ErrorIsFatal = true
		return
	}
	if workItemState.Action != constants.ActionDPN {
		msg := fmt.Sprintf("WorkItem %d has action type %s, "+
			"and does not belong in this queue!", manifest.WorkItem.WorkItemStateId,
			workItemState.Action)
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
		msg := fmt.Sprintf("Could not get ReplicationTransfer %s "+
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
		msg := fmt.Sprintf("Could not get ReplicationTransfer %s "+
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
func ReserveSpaceOnVolume(_context *context.Context, manifest *models.ReplicationManifest) bool {
	okToCopy := false
	err := _context.VolumeClient.Ping(500)
	if err == nil {
		path := manifest.LocalPath
		ok, err := _context.VolumeClient.Reserve(path, uint64(manifest.DPNBag.Size))
		if err != nil {
			_context.MessageLog.Warning("Volume service returned an error. "+
				"Will requeue ReplicationTransfer %s bag (%s) because we may not "+
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
		_context.MessageLog.Warning("Volume service is not running or returned an error. "+
			"Continuing as if we have enough space to download %d bytes.",
			manifest.DPNBag.Size)
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
			msg := fmt.Sprintf("Cannot update ReplicationTransfer %s "+
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
				msg := fmt.Sprintf("When trying to update ReplicationTransfer %s,"+
					"got error %v. Response body: %s",
					manifest.ReplicationTransfer.ReplicationId,
					resp.Error, respBody)
				manifest.CopySummary.AddError(msg)
				_context.MessageLog.Error(msg)
			} else if resp.ReplicationTransfer() == nil {
				msg := fmt.Sprintf("When updating ReplicationTransfer %s, "+
					"remote server did not return an updated transfer record. "+
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
func LogReplicationJson(manifest *models.ReplicationManifest, jsonLog *log.Logger) {
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
		msg := fmt.Sprintf("Could not marshal ReplicationManifest "+
			"for replication %s to JSON: %v", manifest.DPNWorkItem.Identifier, err)
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
		msg := fmt.Sprintf("Could not save DPNWorkItem %d "+
			"for replication %s to Pharos: %v",
			manifest.DPNWorkItem.Id, manifest.DPNWorkItem.Identifier, err)
		_context.MessageLog.Error(msg)
		workSummary.AddError(msg)
	}
}

// SetupReplicationManifest loads the existing ReplicationManifest associated with
// the NSQ message, or creates a new one if necessary. Param message should
// be the NSQ message we're working on. Param stage should be one of "copy",
// "validate" or "store". Param _context is the context of the worker calling
// this fuction.
func SetupReplicationManifest(message *nsq.Message, stage string, _context *context.Context, localClient *network.DPNRestClient, remoteClients map[string]*network.DPNRestClient) *models.ReplicationManifest {

	manifest, activeSummary := initManifest(message, stage)
	GetDPNWorkItem(_context, manifest, activeSummary)
	restoreSucceeded := restoreReplicationState(manifest, _context)

	// We may have previously attempted this stage of processing.
	// If so, reset errors and start/end time, because we're about
	// to try it again.
	activeSummary.ClearErrors()
	activeSummary.StartedAt = time.Time{}
	activeSummary.FinishedAt = time.Time{}

	// Now we get the transfer record from the originating node.
	remoteClient := remoteClients[manifest.DPNWorkItem.RemoteNode]
	if remoteClient == nil {
		activeSummary.AddError("Cannot get remote DPN client for %s",
			manifest.ReplicationTransfer.FromNode)
	}
	GetXferRequest(remoteClient, manifest, activeSummary)
	if activeSummary.HasErrors() {
		return manifest
	}

	// A little more manifest-building, in case we were unable to
	// restore the original manifest above.
	if !restoreSucceeded {
		// This is where we have stored our local copy of this bag.
		manifest.LocalPath = filepath.Join(
			_context.Config.DPN.StagingDirectory,
			manifest.ReplicationTransfer.Bag+".tar")
		_context.MessageLog.Info("Set manifest.LocalPath to %s", manifest.LocalPath)
		// Get the DPN bag from the remote node.
		GetDPNBag(remoteClient, manifest, activeSummary)
	}

	cancelIfNecessary(manifest, stage, activeSummary)
	return manifest
}

// initManifest creates an empty ReplicationManifest, returning that and
// a pointer to the WorkSummary that will record information about the
// current operations.
func initManifest(message *nsq.Message, stage string) (*models.ReplicationManifest, *apt_models.WorkSummary) {
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
	return manifest, activeSummary
}

// Restores the ReplicationManifest from the saved version in DPNWorkItem.State,
// if possible. This acts directly on the manifest pointer param, and returns
// a boolean indicating whether the restore was successful.
func restoreReplicationState(manifest *models.ReplicationManifest, _context *context.Context) bool {
	savedState := ""
	if manifest.DPNWorkItem.State != nil {
		savedState = *manifest.DPNWorkItem.State
	}
	savedManifest := &models.ReplicationManifest{}
	restoreSucceeded := false
	err := json.Unmarshal([]byte(savedState), &savedManifest)
	if err != nil {
		_context.MessageLog.Warning(
			"Cannot unmarshal saved manifest for ReplicationId %s: %v\n"+
				"Will re-fetch bag from remote node. Manifest data: %s",
			manifest.DPNWorkItem.Identifier, err, savedState)
	} else {
		manifest.ReplicationTransfer = savedManifest.ReplicationTransfer
		manifest.DPNBag = savedManifest.DPNBag
		manifest.CopySummary = savedManifest.CopySummary
		manifest.ValidateSummary = savedManifest.ValidateSummary
		manifest.StoreSummary = savedManifest.StoreSummary
		manifest.LocalPath = savedManifest.LocalPath
		manifest.StorageURL = savedManifest.StorageURL
		manifest.RsyncOutput = savedManifest.RsyncOutput
		manifest.Cancelled = savedManifest.Cancelled
		restoreSucceeded = true
	}
	return restoreSucceeded
}

// cancelIfNecessary cancels this job if the ReplicationTransfer
// from the remote node has changed to indicate that we should not
// proceed.
func cancelIfNecessary(manifest *models.ReplicationManifest, stage string, activeSummary *apt_models.WorkSummary) {
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
}

// restoreIngestState restores an IngestManifest from the serialized version
// in WorkItemState.State.
func restoreIngestState(_context *context.Context, manifest *models.DPNIngestManifest) {
	savedState := ""
	if manifest.WorkItemState != nil {
		savedState = manifest.WorkItemState.State
	}
	savedManifest := &models.DPNIngestManifest{}
	err := json.Unmarshal([]byte(savedState), &savedManifest)
	if err == nil {
		manifest.LocalDir = savedManifest.LocalDir
		manifest.LocalTarFile = savedManifest.LocalTarFile
		manifest.StorageURL = savedManifest.StorageURL
		manifest.DPNBag = savedManifest.DPNBag
		manifest.PackageSummary = savedManifest.PackageSummary
		manifest.ValidateSummary = savedManifest.ValidateSummary
		manifest.StoreSummary = savedManifest.StoreSummary
		manifest.RecordSummary = savedManifest.RecordSummary
	} else {
		_context.MessageLog.Warning(
			"Cannot unmarshal saved manifest for %s: %v\n"+
				"Will build state instead. Manifest data: %s",
			manifest.WorkItem.ObjectIdentifier, err, savedState)
		manifest.LocalDir = filepath.Join(
			_context.Config.DPN.StagingDirectory,
			manifest.WorkItem.ObjectIdentifier)
		manifest.LocalTarFile = fmt.Sprintf("%s.tar", manifest.LocalDir)
	}
}

// LoadBagValidationConfig loads the bag validation config file specified
// in the general config options. This will die if the bag validation
// config cannot be loaded or is invalid.
func LoadBagValidationConfig(_context *context.Context) *validation.BagValidationConfig {
	bagValidationConfig, errors := validation.LoadBagValidationConfig(
		_context.Config.DPN.BagValidationConfigFile)
	if errors != nil && len(errors) > 0 {
		msg := fmt.Sprintf("Could not load bag validation config from %s",
			_context.Config.BagValidationConfigFile)
		for _, err := range errors {
			msg += fmt.Sprintf("%s ... ", err.Error())
		}
		fmt.Fprintln(os.Stderr, msg)
		_context.MessageLog.Fatal(msg)
	} else {
		_context.MessageLog.Info("Loaded bag validation config file %s",
			_context.Config.DPN.BagValidationConfigFile)
	}
	return bagValidationConfig
}

// SetupIngestManifest loads the existing DPNIngestManifest associated with
// the NSQ message, or creates a new one if necessary. Param message should
// be the NSQ message we're working on. Param stage should be one of "package",
// "store" or "record". Param _context is the context of the worker calling
// this fuction.
func SetupIngestManifest(message *nsq.Message, stage string, _context *context.Context) *models.DPNIngestManifest {
	// TODO: Implement this.
	//
	// 1. Fetch the WorkItem
	// 2. Fetch the WorkItemState
	// 3. Unmarshal DPNIngestManifest from WorkItemState, or create a new IngestManifest.
	//    If creating a new manifest:
	//    - fetch the IntellectualObject
	//    - set LocalDir and LocalTarFile
	manifest := models.NewDPNIngestManifest(message)

	var activeSummary *apt_models.WorkSummary
	if stage == "package" {
		activeSummary = manifest.PackageSummary
	} else if stage == "store" {
		activeSummary = manifest.StoreSummary
	} else if stage == "record" {
		activeSummary = manifest.RecordSummary
	}

	GetWorkItem(_context, manifest, activeSummary)
	GetWorkItemState(_context, manifest, activeSummary)
	if activeSummary.HasErrors() {
		return manifest
	}
	restoreIngestState(_context, manifest)
	//loadIntellectualObject(_context, manifest)

	return nil
}
