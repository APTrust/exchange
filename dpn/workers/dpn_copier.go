package workers

import (
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nsqio/go-nsq"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// DPNCopier copies tarred bags from other nodes via rsync.
// This is used when replicating content from other nodes.
// For putting together DPN bags from APTrust files, see fetcher.go.
type DPNCopier struct {
	CopyChannel        chan *models.ReplicationManifest
	ChecksumChannel    chan *models.ReplicationManifest
	PostProcessChannel chan *models.ReplicationManifest
	Context            *context.Context
	LocalClient        *network.DPNRestClient
	RemoteClients      map[string]*network.DPNRestClient
}

// NewDPNCopier returns a new DPNCopier object.
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
	copier := &DPNCopier{
		Context:       _context,
		LocalClient:   localClient,
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

// HandleMessage is the NSQ message handler. The NSQ consumer will pass each
// message in the subscribed channel to this function.
func (copier *DPNCopier) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	copier.Context.MessageLog.Info("Checking NSQ message %s", string(message.Body))

	// Get the DPNWorkItem, the ReplicationTransfer, and the DPNBag
	// manifest := copier.buildReplicationManifest(message)
	manifest := SetupReplicationManifest(message, "copy", copier.Context,
		copier.LocalClient, copier.RemoteClients)

	if !copier.copyShouldProceed(manifest, message) {
		message.Finish()
		return nil
	}

	manifest.CopySummary.Start()
	manifest.CopySummary.Attempted = true
	manifest.CopySummary.AttemptNumber += 1

	// TODO: Where is the corresponding Release for this Reserve?
	if copier.Context.Config.UseVolumeService && !ReserveSpaceOnVolume(copier.Context, manifest) {
		manifest.CopySummary.AddError("Cannot reserve disk space to process this bag.")
		manifest.CopySummary.Finish()
		message.Requeue(10 * time.Minute)
	}

	// Start processing.
	copier.CopyChannel <- manifest
	copier.Context.MessageLog.Info("Put xfer request %s (bag %s) from %s "+
		" into the copy channel", manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag, manifest.ReplicationTransfer.FromNode)
	return nil
}

// Copy the file from the remote node to our local staging area.
func (copier *DPNCopier) doCopy() {
	for manifest := range copier.CopyChannel {
		rsyncCommand := GetRsyncCommand(manifest.ReplicationTransfer.Link,
			manifest.LocalPath, copier.Context.Config.DPN.UseSSHWithRsync)
		copier.logStartOfCopy(manifest, rsyncCommand)

		// Touch message on both sides of rsync, so NSQ doesn't time out.
		// The copy process may take a few hours, depending on the size
		// of the bag.
		if manifest.NsqMessage != nil {
			manifest.NsqMessage.Touch()
		}

		// Tell Pharos that we've started to copy item.
		hostname, _ := os.Hostname()
		note := "Copying bag from remote node"
		manifest.DPNWorkItem.Note = &note
		manifest.DPNWorkItem.ProcessingNode = &hostname
		manifest.DPNWorkItem.Pid = os.Getpid()
		SaveDPNWorkItemState(copier.Context, manifest, manifest.CopySummary)

		output, err := rsyncCommand.CombinedOutput()
		copier.Context.MessageLog.Info("Rsync Output: %s", string(output))
		manifest.RsyncOutput = string(output)
		if manifest.NsqMessage != nil {
			manifest.NsqMessage.Touch()
		}
		if err != nil {
			// Copy failed. Don't cancel replication on rsync error.
			// This is usually a network problem or a config problem.
			copier.errCopyFailure(manifest, err)
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
	for manifest := range copier.ChecksumChannel {
		copier.calculateTagManifestDigest(manifest)
		if !manifest.CopySummary.HasErrors() {
			remoteClient := copier.RemoteClients[manifest.ReplicationTransfer.FromNode]
			UpdateReplicationTransfer(copier.Context, remoteClient, manifest)
		}
		copier.PostProcessChannel <- manifest
	}
}

// postProcess tells Pharos, NSQ, and DPN REST server whether the copy
// succeeded or failed.
func (copier *DPNCopier) postProcess() {
	for manifest := range copier.PostProcessChannel {
		if manifest.CopySummary.HasErrors() {
			copier.finishWithError(manifest)
		} else {
			copier.finishWithSuccess(manifest)
		}
	}
}

// calculateTagManifestDigest calculates the sha256 digest of the bag's
// tagmanifest-sha256.txt file.
func (copier *DPNCopier) calculateTagManifestDigest(manifest *models.ReplicationManifest) {
	tarFileIterator, err := fileutil.NewTarFileIterator(manifest.LocalPath)
	if err != nil {
		manifest.CopySummary.AddError("Can't get TarFileIterator for %s: %v",
			manifest.LocalPath, err.Error())
		return
	}
	// DPN BagIt spec says that the top-level dir inside the bag should
	// have the same name as the bag itself (a UUID).
	// https://wiki.duraspace.org/display/DPN/BagIt+Specification#BagItSpecification-DPNBagitStructure
	tagManifestPath := filepath.Join(manifest.ReplicationTransfer.Bag, "tagmanifest-sha256.txt")
	readCloser, err := tarFileIterator.Find(tagManifestPath)
	if readCloser != nil {
		defer readCloser.Close()
	}
	if err != nil {
		manifest.CopySummary.AddError("Can't get tagmanifest from bag: %v", err.Error())
		return
	}
	nonce := ""
	if manifest.ReplicationTransfer.FixityNonce != nil && *manifest.ReplicationTransfer.FixityNonce != "" {
		nonce = *manifest.ReplicationTransfer.FixityNonce
		copier.logFixityNonce(manifest, nonce)
	} else {
		copier.logNoNonce(manifest)
	}
	digest, err := copier.calculateSha256(readCloser, nonce)
	if err != nil {
		manifest.CopySummary.AddError("Error calculating tagmanifest digest: %v",
			err.Error())
		return
	}
	manifest.ReplicationTransfer.FixityValue = digest
	copier.logDigest(manifest)
}

// calculateSha256 calculates the sha256 digest of the contents of the
// supplied reader. It returns the digest as a hex-encoded string.
func (copier *DPNCopier) calculateSha256(reader io.Reader, nonce string) (*string, error) {
	sha256Hash := sha256.New()
	_, err := io.Copy(sha256Hash, reader)
	if err != nil {
		return nil, err
	}
	digest := ""
	if nonce == "" {
		digest = fmt.Sprintf("%x", sha256Hash.Sum(nil))
	} else {
		digest = fmt.Sprintf("%x", sha256Hash.Sum([]byte(nonce)))
	}
	return &digest, nil
}

func (copier *DPNCopier) finishWithError(manifest *models.ReplicationManifest) {
	xferId := "[unknown]"
	fromNode := ""
	if manifest.ReplicationTransfer != nil {
		xferId = manifest.ReplicationTransfer.ReplicationId
		fromNode = manifest.ReplicationTransfer.FromNode
	} else if manifest.DPNWorkItem != nil {
		xferId = manifest.DPNWorkItem.Identifier
	}
	if manifest.CopySummary.ErrorIsFatal {
		msg := fmt.Sprintf("Xfer %s has fatal error: %s",
			xferId, manifest.CopySummary.Errors[0])
		copier.Context.MessageLog.Error(msg)
		manifest.ReplicationTransfer.Cancelled = true
		manifest.ReplicationTransfer.CancelReason = &manifest.CopySummary.Errors[0]
		remoteClient := copier.RemoteClients[fromNode]
		UpdateReplicationTransfer(copier.Context, remoteClient, manifest)
		manifest.NsqMessage.Finish()
	} else if manifest.CopySummary.AttemptNumber > copier.Context.Config.DPN.DPNCopyWorker.MaxAttempts {
		msg := fmt.Sprintf("Attempt to copy Replication %s failed %d times. %s",
			xferId, manifest.CopySummary.AttemptNumber, manifest.CopySummary.Errors[0])
		copier.Context.MessageLog.Error(msg)
		manifest.ReplicationTransfer.Cancelled = true
		manifest.ReplicationTransfer.CancelReason = &msg
		remoteClient := copier.RemoteClients[fromNode]
		UpdateReplicationTransfer(copier.Context, remoteClient, manifest)
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
	manifest.DPNWorkItem.ProcessingNode = nil
	manifest.DPNWorkItem.Pid = 0
	SaveDPNWorkItemState(copier.Context, manifest, manifest.CopySummary)
	LogReplicationJson(manifest, copier.Context.JsonLog)
}

func (copier *DPNCopier) finishWithSuccess(manifest *models.ReplicationManifest) {
	manifest.CopySummary.Finish()
	note := "Copy succeeded."
	manifest.DPNWorkItem.Note = &note
	manifest.DPNWorkItem.ProcessingNode = nil
	manifest.DPNWorkItem.Pid = 0
	copier.Context.MessageLog.Info("Copy succeeded for Replication %s",
		manifest.ReplicationTransfer.ReplicationId)

	// Save DPNWorkItem.State BEFORE pushing to next queue, because
	// the next worker may pick this up immediately, and it needs
	// up-to-date info.
	SaveDPNWorkItemState(copier.Context, manifest, manifest.CopySummary)
	topic := copier.Context.Config.DPN.DPNValidationWorker.NsqTopic
	err := copier.Context.NSQClient.Enqueue(topic, manifest.DPNWorkItem.Id)
	if err != nil {
		msg := fmt.Sprintf("Error pushing DPNWorkItem %d (replication %s) into NSQ topic %s: %v",
			manifest.DPNWorkItem.Id, manifest.DPNWorkItem.Identifier, topic, err)
		manifest.CopySummary.AddError(msg)
		copier.Context.MessageLog.Error(msg)
		warning := "Copy succeeded but could not push to validation queue."
		manifest.DPNWorkItem.Note = &warning
		copier.Context.MessageLog.Info("Copy succeeded for Replication %s "+
			"but could not push to validation queue.",
			manifest.ReplicationTransfer.ReplicationId)
		SaveDPNWorkItemState(copier.Context, manifest, manifest.CopySummary)
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
func GetRsyncCommand(copyFrom, copyTo string, useSSH bool) *exec.Cmd {
	//rsync -avz -e ssh remoteuser@remotehost:/remote/dir /this/dir/
	if useSSH {
		return exec.Command("rsync", "-avzW", "-e", "ssh", copyFrom, copyTo, "--inplace")
	}
	return exec.Command("rsync", "-avzW", "--inplace", copyFrom, copyTo)
}

func (copier *DPNCopier) copyShouldProceed(manifest *models.ReplicationManifest, message *nsq.Message) bool {
	shouldProceed := true
	if manifest.DPNWorkItem.IsBeingProcessed() {
		copier.logItemAlreadyInProcess(manifest)
		shouldProceed = false
	} else if manifest.ReplicationTransfer.Stored {
		EnsureItemIsMarkedComplete(copier.Context, manifest)
		copier.logReplicationStored(manifest)
		shouldProceed = false
	} else if manifest.ReplicationTransfer.Cancelled {
		EnsureItemIsMarkedCancelled(copier.Context, manifest)
		copier.logReplicationCancelled(manifest)
		shouldProceed = false
	} else if fileutil.FileExists(manifest.LocalPath) {
		// If we got this far, we know the file is not currently
		// being processed. See if we have a *complete* copy of
		// the file on disk.
		stat, err := os.Stat(manifest.LocalPath)
		if err == nil && uint64(stat.Size()) == manifest.DPNBag.Size {
			copier.logThatFileIsOnDisk(manifest, message)
			copier.finishWithSuccess(manifest)
			shouldProceed = false
		}
	}
	return shouldProceed
}

func (copier *DPNCopier) logThatFileIsOnDisk(manifest *models.ReplicationManifest, message *nsq.Message) {
	copier.Context.MessageLog.Info("Message %s: Bag %s for replication %s is already on disk",
		string(message.Body), manifest.DPNBag.UUID, manifest.ReplicationTransfer.ReplicationId)
}

func (copier *DPNCopier) logReplicationStored(manifest *models.ReplicationManifest) {
	copier.Context.MessageLog.Info("Replication %s for bag %s has already been stored",
		manifest.ReplicationTransfer.ReplicationId, manifest.DPNBag.UUID)
}

func (copier *DPNCopier) logReplicationCancelled(manifest *models.ReplicationManifest) {
	bagUUID := "<UUID unknown>"
	if manifest.DPNBag != nil && manifest.DPNBag.UUID != "" {
		bagUUID = manifest.DPNBag.UUID
	}
	copier.Context.MessageLog.Info("Replication %s for bag %s was cancelled",
		manifest.ReplicationTransfer.ReplicationId, bagUUID)
}

func (copier *DPNCopier) logStartOfCopy(manifest *models.ReplicationManifest, cmd *exec.Cmd) {
	copier.Context.MessageLog.Info("Starting copy of ReplicationTransfer %s "+
		"with command %s %s", manifest.ReplicationTransfer.ReplicationId,
		cmd.Path, strings.Join(cmd.Args, " "))
}

func (copier *DPNCopier) errCopyFailure(manifest *models.ReplicationManifest, err error) {
	msg := fmt.Sprintf("ReplicationTransfer %s failed with rsync error '%s'",
		manifest.ReplicationTransfer.ReplicationId, err.Error())
	manifest.CopySummary.AddError(msg)
}

func (copier *DPNCopier) logFixityNonce(manifest *models.ReplicationManifest, nonce string) {
	copier.Context.MessageLog.Info("FixityNonce for replication %s is %s",
		manifest.ReplicationTransfer.ReplicationId, nonce)
}

func (copier *DPNCopier) logNoNonce(manifest *models.ReplicationManifest) {
	copier.Context.MessageLog.Info("No FixityNonce for replication %s",
		manifest.ReplicationTransfer.ReplicationId)
}

func (copier *DPNCopier) logDigest(manifest *models.ReplicationManifest) {
	copier.Context.MessageLog.Info("Xfer %s has digest %s",
		manifest.ReplicationTransfer.ReplicationId,
		*manifest.ReplicationTransfer.FixityValue)
}

func (copier *DPNCopier) logItemAlreadyInProcess(manifest *models.ReplicationManifest) {
	node := "unknown"
	if manifest.DPNWorkItem.ProcessingNode != nil {
		node = *manifest.DPNWorkItem.ProcessingNode
	}
	copier.Context.MessageLog.Info("Skipping xfer request %s (bag %s): item is already "+
		" being processed by node %s, pid %d.", manifest.ReplicationTransfer.ReplicationId,
		manifest.ReplicationTransfer.Bag, node, manifest.DPNWorkItem.Pid)
}
