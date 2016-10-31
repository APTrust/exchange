package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/util"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
//	"os"
	"os/exec"
//	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// dpn_copier copies tarred bags from other nodes via rsync.
// This is used when replicating content from other nodes.
// For putting together DPN bags from APTrust files, see fetcher.go.

type Copier struct {
	CopyChannel         chan *CopyManifest
	ChecksumChannel     chan *CopyManifest
	Context             *context.Context
	LocalClient         *network.DPNRestClient
	RemoteClients       map[string]*network.DPNRestClient
}

type CopyManifest struct {
	NsqMessage          *nsq.Message `json:"-"`
	DPNWorkItem         *apt_models.DPNWorkItem
	ReplicationTransfer *models.ReplicationTransfer
	DPNBag              *models.DPNBag
	WorkSummary         *apt_models.WorkSummary
	LocalPath           string
	RsyncStdout         string
	RsyncStderr         string
}

func NewCopyManifest() (*CopyManifest) {
	return &CopyManifest{
		WorkSummary: apt_models.NewWorkSummary(),
	}
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
	copier.CopyChannel = make(chan *CopyManifest, workerBufferSize)
	copier.ChecksumChannel = make(chan *CopyManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNCopyWorker.Workers; i++ {
		go copier.doCopy()
		go copier.verifyChecksum()
	}
	return copier, nil
}

func (copier *Copier) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	// Get the DPNWorkItem, the ReplicationTransfer, and the DPNBag
	copyManifest := copier.buildCopyManifest(message)
	if copyManifest.WorkSummary.HasErrors() {
		copier.finishWithError(copyManifest)
		return nil
	}

	if !copier.reserveSpaceOnVolume(copyManifest) {
		copyManifest.WorkSummary.AddError("Cannot reserve disk space to process this bag.")
		copyManifest.WorkSummary.Finish()
		message.Requeue(10 * time.Minute)
	}

	// Start processing.
	copier.CopyChannel <- copyManifest
	copier.Context.MessageLog.Info("Put xfer request %s (bag %s) from %s " +
		" into the copy channel", copyManifest.ReplicationTransfer.ReplicationId,
		copyManifest.ReplicationTransfer, copyManifest.ReplicationTransfer.FromNode)
	return nil
}

// Copy the file from the remote node to our local staging area.
func (copier *Copier) doCopy() {
	for copyManifest := range copier.CopyChannel {
		localPath := "?"
		rsyncCommand := GetRsyncCommand(copyManifest.ReplicationTransfer.Link,
			localPath, copier.Context.Config.DPN.UseSSHWithRsync)

		copier.Context.MessageLog.Info("Starting copy of ReplicationTransfer %s " +
			"with command %s %s", copyManifest.ReplicationTransfer.ReplicationId,
			rsyncCommand.Path, strings.Join(rsyncCommand.Args, ""))

		// Touch message on both sides of rsync, so NSQ doesn't time out.
		if copyManifest.NsqMessage != nil {
			copyManifest.NsqMessage.Touch()
		}
		output, err := rsyncCommand.CombinedOutput()
		if err != nil {
			msg := fmt.Sprintf("ReplicationTransfer %s failed with rsync error '%s'",
				copyManifest.ReplicationTransfer.ReplicationId, err.Error())
			copyManifest.WorkSummary.AddError(msg)
			// TODO: copier.finishWithError() or move to next channel
		}
		copier.Context.MessageLog.Info("Rsync Output: %s", string(output))
		if copyManifest.NsqMessage != nil {
			copyManifest.NsqMessage.Touch()
		}
		if err != nil {
			// Something went wrong
		} else {
			// OK
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
	//for copyManifest := range copier.ChecksumChannel {
		// 1. Calculate the sha256 digest of the tag manifest.
		// 2. Send the result the ReplicationTransfer.FromNode.
		// 3. If the updated ReplicationTransfer.StoreRequested is true,
		//    push this item into the validation queue. Otherwise,
		//    delete the bag from the local staging area.
	//}
}

// buildCopyManifest creates a CopyManifest for this job.
func (copier *Copier) buildCopyManifest(message *nsq.Message) (*CopyManifest) {
	copyManifest := NewCopyManifest()
	copyManifest.NsqMessage = message
	copyManifest.WorkSummary.Attempted = true
	copyManifest.WorkSummary.AttemptNumber = 1
	copyManifest.WorkSummary.Start()
	copier.getDPNWorkItem(copyManifest)
	if copyManifest.WorkSummary.HasErrors() {
		return copyManifest
	}
	copier.getXferRequest(copyManifest)
	if copyManifest.WorkSummary.HasErrors() {
		return copyManifest
	}
	copier.getDPNBag(copyManifest)
	return copyManifest
}

// getDPNWorkItem returns the DPNWorkItem associated with this message,
// and a boolean indicating whether or not processing should continue.
func (copier *Copier) getDPNWorkItem(copyManifest *CopyManifest) {
	workItemId, err := strconv.Atoi(string(copyManifest.NsqMessage.Body))
	if err != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItemId from" +
			"NSQ message body '%s': %v", copyManifest.NsqMessage.Body, err)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
		return
	}
	resp := copier.Context.PharosClient.DPNWorkItemGet(workItemId)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get DPNWorkItem (id %d) " +
			"from Pharos: %v", workItemId, resp.Error)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
		return
	}
	dpnWorkItem := resp.DPNWorkItem()
	copyManifest.DPNWorkItem = dpnWorkItem
	if dpnWorkItem == nil {
		msg := fmt.Sprintf("Pharos returned nil for DPNWorkItem %d",
			workItemId)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
		return
	}
	if dpnWorkItem.Task != constants.DPNTaskReplication {
		msg := fmt.Sprintf("DPNWorkItem %d has task type %s, " +
			"and does not belong in this queue!", workItemId, dpnWorkItem.Task)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
	}
	if !util.LooksLikeUUID(dpnWorkItem.Identifier) {
		msg := fmt.Sprintf("DPNWorkItem %d has identifier '%s', " +
			"which does not look like a UUID", workItemId, dpnWorkItem.Identifier)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
	}
}

// getXferRequest gets the ReplicationTransfer request from our local
// DPN REST server that describes the replication we're about to
// perform.
func (copier *Copier) getXferRequest(copyManifest *CopyManifest) {
	if copyManifest == nil || copyManifest.DPNWorkItem == nil {
		msg := fmt.Sprintf("getXferRequest: CopyManifest.DPNWorkItem cannot be nil.")
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
		return
	}
	resp := copier.LocalClient.ReplicationTransferGet(copyManifest.DPNWorkItem.Identifier)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get ReplicationTransfer %s " +
			"from DPN server: %v", copyManifest.DPNWorkItem.Identifier, resp.Error)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
		return
	}
	xfer := resp.ReplicationTransfer()
	copyManifest.ReplicationTransfer = xfer
	if xfer == nil {
		msg := fmt.Sprintf("DPN server returned nil for ReplicationId %s",
			copyManifest.DPNWorkItem.Identifier)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
		return
	}
	if xfer.Stored {
		msg := fmt.Sprintf("ReplicationId %s is already marked as Stored. Nothing left to do.",
			copyManifest.DPNWorkItem.Identifier)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
		return
	}
	if xfer.Cancelled {
		msg := fmt.Sprintf("ReplicationId %s was cancelled. Nothing left to do.",
			copyManifest.DPNWorkItem.Identifier)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
	}
}

// getDPNBag gets the bag record fom the local DPN REST server that
// describes the bag we are being asked to copy.
func (copier *Copier) getDPNBag(copyManifest *CopyManifest) {
	if copyManifest == nil || copyManifest.ReplicationTransfer == nil {
		msg := fmt.Sprintf("getDPNBag: CopyManifest.ReplicationTransfer cannot be nil.")
		copyManifest.WorkSummary.ErrorIsFatal = true
		copyManifest.WorkSummary.AddError(msg)
		return
	}
	resp := copier.LocalClient.DPNBagGet(copyManifest.ReplicationTransfer.Bag)
	if resp.Error != nil {
		msg := fmt.Sprintf("Could not get ReplicationTransfer %s " +
			"from DPN server: %v", copyManifest.DPNWorkItem.Identifier, resp.Error)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
		return
	}
	dpnBag := resp.Bag()
	copyManifest.DPNBag = dpnBag
	if dpnBag == nil {
		msg := fmt.Sprintf("DPN server returned nil for Bag %s",
			copyManifest.ReplicationTransfer.Bag)
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.WorkSummary.ErrorIsFatal = true
		return
	}
}

// reserveSpaceOnVolume does just what it says.
// Make sure we have space to copy this item from the remote node.
// We will be validating this bag in a later step without untarring it,
// so we just have to reserve enough room for the tar file.
func (copier *Copier) reserveSpaceOnVolume(copyManifest *CopyManifest) (bool) {
	okToCopy := false
	err := copier.Context.VolumeClient.Ping(500)
	if err == nil {
		path := copyManifest.LocalPath
		ok, err := copier.Context.VolumeClient.Reserve(path, uint64(copyManifest.DPNBag.Size))
		if err != nil {
			copier.Context.MessageLog.Warning("Volume service returned an error. " +
				"Will requeue ReplicationTransfer %s bag (%s) because we may not " +
				"have enough space to copy %d bytes from %s.",
				copyManifest.ReplicationTransfer.ReplicationId,
				copyManifest.ReplicationTransfer.Bag,
				copyManifest.DPNBag.Size,
				copyManifest.ReplicationTransfer.FromNode)
		} else if ok {
			// VolumeService says we have enough space for this.
			okToCopy = ok
		}
	} else {
		copier.Context.MessageLog.Warning("Volume service is not running or returned an error. " +
			"Continuing as if we have enough space to download %d bytes.",
			copyManifest.DPNBag.Size,)
		okToCopy = true
	}
	return okToCopy
}

func (copier *Copier) finishWithError(copyManifest *CopyManifest) {
	xferId := "[unknown]"
	if copyManifest.ReplicationTransfer != nil {
		xferId = copyManifest.ReplicationTransfer.ReplicationId
	} else if copyManifest.DPNWorkItem != nil {
		xferId = copyManifest.DPNWorkItem.Identifier
	}
	if copyManifest.WorkSummary.ErrorIsFatal {
		msg := fmt.Sprintf("Xfer %s has fatal error: %s",
			xferId, copyManifest.WorkSummary.Errors[0])
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.NsqMessage.Finish()
	} else {
		msg := fmt.Sprintf("Xfer %s has non-fatal error: %s",
			xferId, copyManifest.WorkSummary.Errors[0])
		copyManifest.WorkSummary.AddError(msg)
		copyManifest.NsqMessage.Requeue(1 * time.Minute)
	}
	copyManifest.WorkSummary.Finish()
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
