package workers

import (
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nsqio/go-nsq"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// dpn_copier copies tarred bags from other nodes via rsync.
// This is used when replicating content from other nodes.
// For putting together DPN bags from APTrust files, see fetcher.go.

type DPNCopier struct {
	CopyChannel         chan *models.ReplicationManifest
	ChecksumChannel     chan *models.ReplicationManifest
	PostProcessChannel  chan *models.ReplicationManifest
	Context             *context.Context
	LocalClient         *network.DPNRestClient
	RemoteClients       map[string]*network.DPNRestClient
}

func NewDPNCopier(_context *context.Context) (*DPNCopier, error) {
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
	copier := &DPNCopier {
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

func (copier *DPNCopier) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	copier.Context.MessageLog.Info("Checking NSQ message %s", string(message.Body))

	// Get the DPNWorkItem, the ReplicationTransfer, and the DPNBag
	manifest := copier.buildReplicationManifest(message)
	if manifest.CopySummary.HasErrors() {
		copier.PostProcessChannel <- manifest
		return nil
	}

	if !ReserveSpaceOnVolume(copier.Context, manifest) {
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
func (copier *DPNCopier) doCopy() {
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
func (copier *DPNCopier) verifyChecksum() {
	//for manifest := range copier.ChecksumChannel {
		// 1. Calculate the sha256 digest of the tag manifest.
		// 2. Send the result the ReplicationTransfer.FromNode.
		// 3. If the updated ReplicationTransfer.StoreRequested is true,
		//    push this item into the validation queue. Otherwise,
		//    delete the bag from the local staging area.
	//}
}

func (copier *DPNCopier) postProcess() {
	for manifest := range copier.PostProcessChannel {
		if manifest.CopySummary.HasErrors() {
			copier.finishWithError(manifest)
		} else {
			copier.finishWithSuccess(manifest)
		}
	}
}

// buildReplicationManifest creates a ReplicationManifest for this job.
func (copier *DPNCopier) buildReplicationManifest(message *nsq.Message) (*models.ReplicationManifest) {
	manifest := models.NewReplicationManifest(message)
	manifest.NsqMessage = message
	manifest.CopySummary.Attempted = true
	manifest.CopySummary.AttemptNumber = 1
	manifest.CopySummary.Start()
	GetDPNWorkItem(copier.Context, manifest, manifest.CopySummary)
	if manifest.CopySummary.HasErrors() {
		return manifest
	}
	GetXferRequest(copier.LocalClient, manifest, manifest.CopySummary)
	if manifest.CopySummary.HasErrors() {
		return manifest
	}
	// This is where we will store our local copy of this bag.
	manifest.LocalPath = filepath.Join(
		copier.Context.Config.DPN.StagingDirectory,
		manifest.ReplicationTransfer.Bag + ".tar")

	GetDPNBag(copier.LocalClient, manifest, manifest.CopySummary)
	return manifest
}

func (copier *DPNCopier) finishWithError(manifest *models.ReplicationManifest) {
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
		remoteClient := copier.RemoteClients[xferId]
		CancelTransfer(copier.Context, remoteClient, manifest)
		manifest.NsqMessage.Finish()
	} else {
		msg := fmt.Sprintf("Requeueing xfer %s due to non-fatal error: %s",
			xferId, manifest.CopySummary.Errors[0])
		copier.Context.MessageLog.Warning(msg)
		manifest.NsqMessage.Requeue(1 * time.Minute)
	}
	// Delete the file, which may not even have been completely copied.
	if fileutil.LooksSafeToDelete(manifest.LocalPath, 12, 3) {
		os.Remove(manifest.LocalPath)
	}
	manifest.CopySummary.Finish()

	// Tell Pharos what happened, and then dump the JSON to a log file.
	*manifest.DPNWorkItem.Note = "Copy failed."
	SaveWorkItemState(copier.Context, manifest, manifest.CopySummary)
	LogReplicationJson(manifest, copier.Context.JsonLog)
}

func (copier *DPNCopier) finishWithSuccess(manifest *models.ReplicationManifest) {
	manifest.CopySummary.Finish()
	*manifest.DPNWorkItem.Note = "Copy succeeded."

	// Save DPNWorkItem.State BEFORE pushing to next queue, because
	// the next worker may pick this up immediately, and it needs
	// up-to-date info.
	SaveWorkItemState(copier.Context, manifest, manifest.CopySummary)
	topic := copier.Context.Config.DPN.DPNValidationWorker.NsqTopic
	err := copier.Context.NSQClient.Enqueue(topic, manifest.DPNWorkItem.Id)
	if err != nil {
		msg := fmt.Sprintf("Error pushing DPNWorkItem %d (replication %s) into NSQ topic %s: %v",
			manifest.DPNWorkItem.Id, manifest.DPNWorkItem.Identifier, topic, err)
		manifest.CopySummary.AddError(msg)
		copier.Context.MessageLog.Error(msg)
		*manifest.DPNWorkItem.Note = "Copy succeeded but could not push to validation queue."
		SaveWorkItemState(copier.Context, manifest, manifest.CopySummary)
	}
	LogReplicationJson(manifest, copier.Context.JsonLog)
	manifest.NsqMessage.Finish()
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
