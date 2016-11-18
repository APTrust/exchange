package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	// dpn_util "github.com/APTrust/exchange/dpn/util"
	apt_network "github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"os"
	"path/filepath"
	"time"
)

// dpn_packager repackages APTrust bags as DPN bags so they
// can be copied into DPN.

type DPNPackager struct {
	PackageChannel      chan *models.DPNIngestManifest
	ValidationChannel   chan *models.DPNIngestManifest
	PostProcessChannel  chan *models.DPNIngestManifest
	BagValidationConfig *validation.BagValidationConfig
	Context             *context.Context
	LocalClient         *network.DPNRestClient
	RemoteClients       map[string]*network.DPNRestClient
}

func NewDPNPackager(_context *context.Context) (*DPNPackager, error) {
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
	packager := &DPNPackager{
		Context:       _context,
		LocalClient:   localClient,
		RemoteClients: remoteClients,
	}
	packager.BagValidationConfig = LoadBagValidationConfig(packager.Context)
	workerBufferSize := _context.Config.DPN.DPNPackageWorker.Workers * 4
	packager.PackageChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	packager.ValidationChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	packager.PostProcessChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNPackageWorker.Workers; i++ {
		go packager.buildBag()
		go packager.validate()
		go packager.postProcess()
	}
	return packager, nil
}

func (packager *DPNPackager) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	packager.Context.MessageLog.Info("Packager is checking NSQ message %s", string(message.Body))

	manifest := SetupIngestManifest(message, "package", packager.Context)
	manifest.PackageSummary.Start()
	manifest.PackageSummary.Attempted = true
	manifest.PackageSummary.AttemptNumber += 1

	// Handle the case where we cannot get the WorkItem whose id
	// is specified in the NSQ message.
	if manifest.PackageSummary.HasErrors() {
		packager.Context.MessageLog.Info("Cannot process NSQ message %s", string(message.Body))
		packager.PostProcessChannel <- manifest
		return nil
	}

	now := time.Now().UTC()
	hostname, _ := os.Hostname()
	manifest.WorkItem.Stage = constants.StagePackage
	manifest.WorkItem.StageStartedAt = &now
	manifest.WorkItem.Status = constants.StatusStarted
	manifest.WorkItem.Node = hostname
	manifest.WorkItem.Pid = os.Getpid()
	SaveWorkItem(packager.Context, manifest, manifest.PackageSummary)

	// Start processing.
	packager.Context.MessageLog.Info("Putting bag %s into the package channel",
		manifest.WorkItem.ObjectIdentifier)
	packager.PackageChannel <- manifest
	return nil
}

func (packager *DPNPackager) buildBag() {
	for manifest := range packager.PackageChannel {
		// Create the manifest.LocalDir, with data subdir
		// Create a new BagBuilder
		// Fetch all of the IntelObj's GenericFile into the data subdir
		// Add each file to the BagBuilder
		// Add manifests and other required files
		packager.createBagDirectory(manifest)
		if manifest.PackageSummary.HasErrors() {
			packager.PostProcessChannel <- manifest
			continue
		}
		packager.buildDPNBag(manifest)
		if manifest.PackageSummary.HasErrors() {
			packager.PostProcessChannel <- manifest
			continue
		}
		packager.ValidationChannel <- manifest
	}
}

func (packager *DPNPackager) validate() {
	for manifest := range packager.ValidationChannel {
		// Validate the bag
		// Tar it up
		// Send it into the PostProcessChannel
		packager.PostProcessChannel <- manifest
	}
}

func (packager *DPNPackager) postProcess() {
	for manifest := range packager.PostProcessChannel {
		if manifest.PackageSummary.HasErrors() {
			packager.finishWithError(manifest)
		} else {
			packager.finishWithSuccess(manifest)
		}
	}
}

// Build the DPN bag by fetching the APTrust files, writing manifests, etc.
func (packager *DPNPackager) buildDPNBag(manifest *models.DPNIngestManifest) {
	// builder, err := dpn_util.NewBagBuilder(
	// 	manifest.LocalDir,
	// 	manifest.IntellectualObject,
	// 	packager.Context.Config.DPN.DefaultMetadata)
	// if err != nil {
	// 	manifest.PackageSummary.AddError("Cannot create BagBuilder: %v", err)
	// 	return
	// }
	packager.fetchAllFiles(manifest)

	// Write tag files and manifests
	// Validate the bag
	// Tar it up
}

// ISSUE: See https://www.pivotaltracker.com/story/show/134540309
// TODO: Don't even try to solve the issue above without a thorough plan.
func (packager *DPNPackager) fetchAllFiles(manifest *models.DPNIngestManifest) {
	downloader := apt_network.NewS3Download(
		constants.AWSVirginia,
		packager.Context.Config.PreservationBucket,
		"",    // s3 key to fetch - to be set below
		"",    // local path at which to save the s3 file - set below
		false, // no need to calculate md5
		true)  // calculate sha256 for fixity verification
	packager.Context.MessageLog.Info("Object %s has %d saved files",
		manifest.IntellectualObject.Identifier,
		len(manifest.IntellectualObject.GenericFiles))
	for _, gf := range manifest.IntellectualObject.GenericFiles {
		downloader.Sha256Digest = ""
		downloader.ErrorMessage = ""

		// -------------------------------------------------------------------
		// TODO: Separate tag files from data files
		// TODO: Omit deleted files
		// -------------------------------------------------------------------

		// We're going to want to confirm the sha256 digest of the download...
		existingSha256 := gf.GetChecksumByAlgorithm(constants.AlgSha256)
		if existingSha256 == nil {
			manifest.PackageSummary.AddError("Cannot find sha256 digest for file %s", gf.Identifier)
			break
		}
		// Figure out what the key name is for this file. It's a UUID.
		s3KeyName, err := gf.PreservationStorageFileName()
		if err != nil {
			manifest.PackageSummary.AddError("File %s: %v", gf.Identifier, err)
			break
		}

		// Tell the downloader what we're downloading, and where to put it.
		downloader.KeyName = s3KeyName
		downloader.LocalPath = filepath.Join(manifest.LocalDir, gf.OriginalPath())

		// Make sure the target directory exists in the local file system.
		packager.ensureDirectory(manifest, downloader.LocalPath)
		if manifest.PackageSummary.HasErrors() {
			break
		}

		// Fetch is the expensive part, so we don't even want to get to this
		// point if we don't have the info above.
		packager.Context.MessageLog.Info("Downloading %s (%s)", gf.Identifier, s3KeyName)
		downloader.Fetch()
		if downloader.ErrorMessage != "" {
			msg := fmt.Sprintf("Error fetching %s from S3: %s", gf.Identifier, downloader.ErrorMessage)
			packager.Context.MessageLog.Error(msg)
			manifest.PackageSummary.AddError(msg)
			break
		}
		if downloader.Sha256Digest != existingSha256.Digest {
			msg := fmt.Sprintf("sha256 digest mismatch for for file %s."+
				"Our digest: %s. Digest of fetched file: %s",
				gf.Identifier, existingSha256, downloader.Sha256Digest)
			packager.Context.MessageLog.Error(msg)
			manifest.PackageSummary.AddError(msg)
			break
		}
	}
}

func (packager *DPNPackager) ensureDirectory(manifest *models.DPNIngestManifest, filePath string) {
	dir := filepath.Dir(filePath)
	if !fileutil.FileExists(dir) {
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			manifest.PackageSummary.AddError("Cannot create directory %s: %v", dir, err)
			manifest.PackageSummary.ErrorIsFatal = true
		}
	}
}

func (packager *DPNPackager) createBagDirectory(manifest *models.DPNIngestManifest) {
	if manifest.LocalDir == "" {
		manifest.PackageSummary.AddError("LocalDirectory is not set for bag %s",
			manifest.IntellectualObject.Identifier)
		manifest.PackageSummary.ErrorIsFatal = true
		manifest.PackageSummary.Retry = false
		return
	}
	err := os.MkdirAll(manifest.LocalDir, 0755)
	if err != nil {
		manifest.PackageSummary.AddError("Cannot create directory to build bag: %v", err)
		manifest.PackageSummary.ErrorIsFatal = true
	} else {
		packager.Context.MessageLog.Info("Created dir %s", manifest.LocalDir)
	}
}

func (packager *DPNPackager) finishWithSuccess(manifest *models.DPNIngestManifest) {
	packager.Context.MessageLog.Info("Packaging succeeded for %s", manifest.WorkItem.ObjectIdentifier)
	manifest.WorkItem.Status = constants.StageStore
	manifest.WorkItem.Status = constants.StatusPending
	manifest.WorkItem.Note = "Packaging completed, awaiting storage"
	manifest.WorkItem.Node = ""    // no worker is working on this now
	manifest.WorkItem.Pid = 0      // no process is working on this
	manifest.WorkItem.Retry = true // just in case this had been false
	SaveWorkItem(packager.Context, manifest, manifest.PackageSummary)
	SaveWorkItemState(packager.Context, manifest, manifest.PackageSummary)
	if fileutil.LooksSafeToDelete(manifest.LocalDir, 12, 3) {
		os.RemoveAll(manifest.LocalDir)
	}
	PushToQueue(packager.Context, manifest, manifest.PackageSummary,
		packager.Context.Config.DPN.DPNStoreWorker.NsqTopic)
	manifest.NsqMessage.Finish()
}

func (packager *DPNPackager) finishWithError(manifest *models.DPNIngestManifest) {
	// Log what happened.
	msg := ""
	packager.Context.MessageLog.Error(manifest.PackageSummary.AllErrorsAsString())
	if manifest.PackageSummary.ErrorIsFatal {
		msg := fmt.Sprintf("Ingest error for %s is fatal. Will not retry.",
			manifest.WorkItem.ObjectIdentifier)
		packager.Context.MessageLog.Error(msg)
		manifest.NsqMessage.Finish()
		manifest.WorkItem.Status = constants.StatusFailed
		manifest.WorkItem.Outcome = "Failed due to fatal error"
		manifest.WorkItem.Retry = false
		manifest.WorkItem.NeedsAdminReview = true
	} else if manifest.PackageSummary.AttemptNumber > packager.Context.Config.DPN.DPNPackageWorker.MaxAttempts {
		msg := fmt.Sprintf("Giving up on ingest for %s after %d attempts.",
			manifest.WorkItem.ObjectIdentifier, manifest.PackageSummary.AttemptNumber)
		packager.Context.MessageLog.Error(msg)
		manifest.NsqMessage.Finish()
		manifest.WorkItem.Status = constants.StatusFailed
		manifest.WorkItem.Outcome = "Failed after too many attempts with transient errors"
		manifest.WorkItem.Retry = false
		manifest.WorkItem.NeedsAdminReview = true
	} else {
		msg := fmt.Sprintf("Will retry ingest for %s",
			manifest.WorkItem.ObjectIdentifier)
		packager.Context.MessageLog.Warning(msg)
		manifest.NsqMessage.Requeue(1 * time.Minute)
		manifest.WorkItem.Status = constants.StatusPending
		manifest.WorkItem.Outcome = "Pending retry after transient errors"
		manifest.WorkItem.Retry = true
	}

	// Set the WorkItem fields and save to Pharos
	manifest.WorkItem.Note = msg
	manifest.WorkItem.Node = "" // no worker is working on this now
	manifest.WorkItem.Pid = 0   // no process is working on this

	// Delete the folder containing the bag we were building,
	// And delete the tar file too, if it exists.
	if fileutil.LooksSafeToDelete(manifest.LocalDir, 12, 3) {
		err := os.RemoveAll(manifest.LocalDir)
		if err != nil {
			manifest.PackageSummary.AddError("Could not delete bag directory %s: %v",
				manifest.LocalDir, err)
		}
	}
	err := os.Remove(manifest.LocalTarFile)
	if err != nil {
		manifest.PackageSummary.AddError("Could not delete tar file %s: %v",
			manifest.LocalTarFile, err)
	}

	// Save info to Pharos so the next worker knows what's what.
	SaveWorkItem(packager.Context, manifest, manifest.PackageSummary)
	SaveWorkItemState(packager.Context, manifest, manifest.PackageSummary)
}
