package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/util"
	"github.com/nsqio/go-nsq"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// dpn_copier copies tarred bags from other nodes via rsync.
// This is used when replicating content from other nodes.
// For putting together DPN bags from APTrust files, see fetcher.go.

type Copier struct {
	CopyChannel         chan *models.ReplicationManifest
	ChecksumChannel     chan *models.ReplicationManifest
	PostProcessChannel  chan *models.ReplicationManifest
	Context             *context.Context
	LocalClient         *network.DPNRestClient
	RemoteClients       map[string]*network.DPNRestClient
}

func NewCopier(_context *context.Context) (*Copier, error) {
	localClient, err := network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	if err != nil {
		return nil, fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	remoteClients, err := localClient.GetRemoteClients()
	if err != nil {
		return nil, err
	}
	copier := &Copier {
		Context: _context,
		LocalClient: localClient,
		RemoteClients: remoteClients,
	}
	workerBufferSize := _context.Config.DPN.DPNCopyWorker.Workers * 4
	copier.CopyChannel = make(chan *models.ReplicationManifest, workerBufferSize)
	copier.ChecksumChannel = make(chan *models.ReplicationManifest, workerBufferSize)
	copier.PostProcessChannel = make(chan *models.ReplicationManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNCopyWorker.Workers; i++ {
		go copier.doCopy()
		go copier.verifyChecksum()
		go copier.postProcess()
	}
	return copier, nil
}

func (copier *Copier) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	// Get the DPNWorkItem, the ReplicationTransfer, and the DPNBag
	manifest := copier.buildReplicationManifest(message)
	if manifest.CopySummary.HasErrors() {
		copier.PostProcessChannel <- manifest
		return nil
	}

	if !copier.reserveSpaceOnVolume(manifest) {
		manifest.CopySummary.AddError("Cannot reserve disk space to process this bag.")
		manifest.CopySummary.Finish()
		message.Requeue(10 * time.Minute)
	}

	// Start processing.
	copier.CopyChannel <- manifest
	copier.Context.MessageLog.Info("Put xfer request %s (bag %s) from %s " +
		" into the copy channel", manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer, manifest.ReplicationTransfer.FromNode)
	return nil
}

// Copy the file from the remote node to our local staging area.
func (copier *Copier) doCopy() {
	for manifest := range copier.CopyChannel {
		rsyncCommand := GetRsyncCommand(manifest.ReplicationTransfer.Link,
			manifest.LocalPath, copier.Context.Config.DPN.UseSSHWithRsync)
		copier.Context.MessageLog.Info("Starting copy of ReplicationTransfer %s " +
			"with command %s %s", manifest.ReplicationTransfer.ReplicationId,
			rsyncCommand.Path, strings.Join(rsyncCommand.Args, ""))

		// Touch message on both sides of rsync, so NSQ doesn't time out.
		if manifest.NsqMessage != nil {
			manifest.NsqMessage.Touch()
		}
		output, err := rsyncCommand.CombinedOutput()
		copier.Context.MessageLog.Info("Rsync Output: %s", string(output))
		if manifest.NsqMessage != nil {
			manifest.NsqMessage.Touch()
		}
		if err != nil {
			// Copy failed. Don't cancel replication on rsync error.
			// This is usually a network problem or a config problem.
			msg := fmt.Sprintf("ReplicationTransfer %s failed with rsync error '%s'",
				manifest.ReplicationTransfer.ReplicationId, err.Error())
			manifest.CopySummary.AddError(msg)
			copier.PostProcessChannel <- manifest
		} else {
			// Copy succeeded.
			copier.ChecksumChannel <- manifest
		}
	}
}

// Run a checksum on the tag manifest and send that back to the
// FromNode. If the checksum is good, the FromNode will set
// the ReplicationTransfer's StoreRequested attribute to true,
// and we should store the bag. If the checksum is bad, the remote
// node will set StoreRequested to false, and we should delete
// the tar file.
func (copier *Copier) verifyChecksum() {
	//for manifest := range copier.ChecksumChannel {
		// 1. Calculate the sha256 digest of the tag manifest.
		// 2. Send the result the ReplicationTransfer.FromNode.
		// 3. If the updated ReplicationTransfer.StoreRequested is true,
		//    push this item into the validation queue. Otherwise,
		//    delete the bag from the local staging area.
	//}
}

func (copier *Copier) postProcess() {
	for manifest := range copier.PostProcessChannel {
		if manifest.CopySummary.HasErrors() {
			copier.finishWithError(manifest)
		} else {
			copier.finishWithSuccess(manifest)
		}
	}
}

// buildReplicationManifest creates a ReplicationManifest for this job.
func (copier *Copier) buildReplicationManifest(message *nsq.Message) (*models.ReplicationManifest) {
	manifest := models.NewReplicationManifest(message)
	manifest.NsqMessage = message
	manifest.CopySummary.Attempted = true
	manifest.CopySummary.AttemptNumber = 1
	manifest.CopySummary.Start()
	copier.getDPNWorkItem(manifest)
	if manifest.CopySummary.HasErrors() {
		return manifest
	}
	copier.getXferRequest(manifest)
	if manifest.CopySummary.HasErrors() {
		return manifest
	}
	// This is where we will store our local copy of this bag.
	manifest.LocalPath = filepath.Join(
		copier.Context.Config.DPN.StagingDirectory,
		manifest.ReplicationTransfer.Bag)

	copier.getDPNBag(manifest)
	return manifest
}

// getDPNWorkItem returns the DPNWorkItem associated with this message,
// and a boolean indicating whether or not processing should continue.
func (copier *Copier) getDPNWorkItem(manifest *models.ReplicationManifest) {
	workItemId, err := strconv.Atoi(string(manifest.NsqMessage.Body))
	if err != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItemId from" +
			"NSQ message body '%s': %v", manifest.NsqMessage.Body, err)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
		return
	}
	resp := copier.Context.PharosClient.DPNWorkItemGet(workItemId)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItem (id %d) " +
			"from Pharos: %v", workItemId, resp.Error)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
		return
	}
	dpnWorkItem := resp.DPNWorkItem()
	manifest.DPNWorkItem = dpnWorkItem
	if dpnWorkItem == nil {
		msg := fmt.Sprintf("Pharos returned nil for DPNWorkItem %d",
			workItemId)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
		return
	}
	if dpnWorkItem.Task != constants.DPNTaskReplication {
		msg := fmt.Sprintf("DPNWorkItem %d has task type %s, " +
			"and does not belong in this queue!", workItemId, dpnWorkItem.Task)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
	}
	if !util.LooksLikeUUID(dpnWorkItem.Identifier) {
		msg := fmt.Sprintf("DPNWorkItem %d has identifier '%s', " +
			"which does not look like a UUID", workItemId, dpnWorkItem.Identifier)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
	}
}

// getXferRequest gets the ReplicationTransfer request from our local
// DPN REST server that describes the replication we're about to
// perform.
func (copier *Copier) getXferRequest(manifest *models.ReplicationManifest) {
	if manifest == nil || manifest.DPNWorkItem == nil {
		msg := fmt.Sprintf("getXferRequest: ReplicationManifest.DPNWorkItem cannot be nil.")
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
		return
	}
	resp := copier.LocalClient.ReplicationTransferGet(manifest.DPNWorkItem.Identifier)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get ReplicationTransfer %s " +
			"from DPN server: %v", manifest.DPNWorkItem.Identifier, resp.Error)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
		return
	}
	xfer := resp.ReplicationTransfer()
	manifest.ReplicationTransfer = xfer
	if xfer == nil {
		msg := fmt.Sprintf("DPN server returned nil for ReplicationId %s",
			manifest.DPNWorkItem.Identifier)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
		return
	}
	if xfer.Stored {
		msg := fmt.Sprintf("ReplicationId %s is already marked as Stored. Nothing left to do.",
			manifest.DPNWorkItem.Identifier)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
		return
	}
	if xfer.Cancelled {
		msg := fmt.Sprintf("ReplicationId %s was cancelled. Nothing left to do.",
			manifest.DPNWorkItem.Identifier)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
	}
}

// getDPNBag gets the bag record fom the local DPN REST server that
// describes the bag we are being asked to copy.
func (copier *Copier) getDPNBag(manifest *models.ReplicationManifest) {
	if manifest == nil || manifest.ReplicationTransfer == nil {
		msg := fmt.Sprintf("getDPNBag: ReplicationManifest.ReplicationTransfer cannot be nil.")
		manifest.CopySummary.ErrorIsFatal = true
		manifest.CopySummary.AddError(msg)
		return
	}
	resp := copier.LocalClient.DPNBagGet(manifest.ReplicationTransfer.Bag)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get ReplicationTransfer %s " +
			"from DPN server: %v", manifest.DPNWorkItem.Identifier, resp.Error)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
		return
	}
	dpnBag := resp.Bag()
	manifest.DPNBag = dpnBag
	if dpnBag == nil {
		msg := fmt.Sprintf("DPN server returned nil for Bag %s",
			manifest.ReplicationTransfer.Bag)
		manifest.CopySummary.AddError(msg)
		manifest.CopySummary.ErrorIsFatal = true
		return
	}
}

// reserveSpaceOnVolume does just what it says.
// Make sure we have space to copy this item from the remote node.
// We will be validating this bag in a later step without untarring it,
// so we just have to reserve enough room for the tar file.
func (copier *Copier) reserveSpaceOnVolume(manifest *models.ReplicationManifest) (bool) {
	okToCopy := false
	err := copier.Context.VolumeClient.Ping(500)
	if err == nil {
		path := manifest.LocalPath
		ok, err := copier.Context.VolumeClient.Reserve(path, uint64(manifest.DPNBag.Size))
		if err != nil {
			copier.Context.MessageLog.Warning("Volume service returned an error. " +
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
		copier.Context.MessageLog.Warning("Volume service is not running or returned an error. " +
			"Continuing as if we have enough space to download %d bytes.",
			manifest.DPNBag.Size,)
		okToCopy = true
	}
	return okToCopy
}

func (copier *Copier) finishWithError(manifest *models.ReplicationManifest) {
	xferId := "[unknown]"
	if manifest.ReplicationTransfer != nil {
		xferId = manifest.ReplicationTransfer.ReplicationId
	} else if manifest.DPNWorkItem != nil {
		xferId = manifest.DPNWorkItem.Identifier
	}
	if manifest.CopySummary.ErrorIsFatal {
		msg := fmt.Sprintf("Xfer %s has fatal error: %s",
			xferId, manifest.CopySummary.Errors[0])
		copier.Context.MessageLog.Error(msg)
		copier.cancelTransfer(manifest)
		manifest.NsqMessage.Finish()
	} else {
		msg := fmt.Sprintf("Requeueing xfer %s due to non-fatal error: %s",
			xferId, manifest.CopySummary.Errors[0])
		copier.Context.MessageLog.Warning(msg)
		manifest.NsqMessage.Requeue(1 * time.Minute)
	}
	manifest.CopySummary.Finish()
}

func (copier *Copier) finishWithSuccess(manifest *models.ReplicationManifest) {

	manifest.CopySummary.Finish()
}


func (copier *Copier) cancelTransfer(manifest *models.ReplicationManifest) {
	if manifest.ReplicationTransfer != nil {
		manifest.ReplicationTransfer.Cancelled = true
		manifest.ReplicationTransfer.CancelReason = manifest.CopySummary.Errors[0]
		client := copier.RemoteClients[manifest.ReplicationTransfer.FromNode]
		if client == nil {
			msg := fmt.Sprintf("Cannot cancel ReplicationTransfer %s " +
				"because no REST client exists for node %s.",
				manifest.ReplicationTransfer.ReplicationId,
				manifest.ReplicationTransfer.FromNode)
			manifest.CopySummary.AddError(msg)
			copier.Context.MessageLog.Error(msg)
		} else {
			resp := copier.LocalClient.ReplicationTransferUpdate(manifest.ReplicationTransfer)
			if resp.Error != nil {
				rawRespData, _ := resp.RawResponseData()
				respBody := "[response body not available]"
				if rawRespData != nil {
					respBody = string(rawRespData)
				}
				msg := fmt.Sprintf("When trying to cancel ReplicationTransfer %s," +
					"got error %v. Response body: %s",
					manifest.ReplicationTransfer.ReplicationId,
					resp.Error, respBody)
				manifest.CopySummary.AddError(msg)
				copier.Context.MessageLog.Error(msg)
			} else {
				// Cancellation succeeded.
				copier.Context.MessageLog.Info("Cancelled xfer %s at %s",
					manifest.ReplicationTransfer.ReplicationId,
					manifest.ReplicationTransfer.FromNode)
			}
		}
	} else {
		copier.Context.MessageLog.Warning("Cannot cancel nil ReplicationTransfer.")
	}
}

// GetRsyncCommand returns a command object for copying from the remote
// location to the local filesystem. The copy is done via rsync over ssh,
// and the command will capture stdout and stderr. The copyFrom param
// should be a valid scp target in this format:
//
// remoteuser@remotehost:/remote/dir/bag.tar
//
// The copyTo param should be an absolute path on a locally-accessible
// file system, such as:
//
// /mnt/dpn/data/bag.tar
//
// Using this assumes a few things:
//
// 1. You have rsync installed.
// 2. You have an ssh client installed.
// 3. You have an entry in your ~/.ssh/config file specifying
//    connection and key information for the remote host.
//
// Usage:
//
// command := GetRsyncCommand("aptrust@tdr:bag.tar", "/mnt/dpn/bag.tar")
// err := command.Run()
// if err != nil {
//    ... do something ...
// }
//
// -- OR --
//
// output, err := command.CombinedOutput()
// if err != nil {
//    fmt.Println(err.Error())
//    fmt.Println(string(output))
// }
func GetRsyncCommand(copyFrom, copyTo string, useSSH bool) (*exec.Cmd) {
	//rsync -avz -e ssh remoteuser@remotehost:/remote/dir /this/dir/
	if useSSH {
		return exec.Command("rsync", "-avzW", "-e",  "ssh", copyFrom, copyTo, "--inplace")
	}
	return exec.Command("rsync", "-avzW", "--inplace", copyFrom, copyTo)
}
