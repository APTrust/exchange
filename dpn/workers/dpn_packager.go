package workers

import (
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	dpn_util "github.com/APTrust/exchange/dpn/util"
	apt_models "github.com/APTrust/exchange/models"
	apt_network "github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/tarfile"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DPNPackager repackages APTrust bags as DPN bags so they
// can be copied into DPN.
type DPNPackager struct {
	PackageChannel      chan *models.DPNIngestManifest
	TarChannel          chan *models.DPNIngestManifest
	ValidationChannel   chan *models.DPNIngestManifest
	PostProcessChannel  chan *models.DPNIngestManifest
	BagValidationConfig *validation.BagValidationConfig
	Context             *context.Context
	LocalClient         *network.DPNRestClient
	RemoteClients       map[string]*network.DPNRestClient
}

// NewDPNPackager creates a new DPNPackager.
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
	packager.BagValidationConfig = LoadDPNBagValidationConfig(packager.Context)
	workerBufferSize := _context.Config.DPN.DPNPackageWorker.Workers * 4
	packager.PackageChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	packager.TarChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	packager.ValidationChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	packager.PostProcessChannel = make(chan *models.DPNIngestManifest, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNPackageWorker.Workers; i++ {
		go packager.buildBag()
		go packager.tarBag()
		go packager.validate()
		go packager.postProcess()
	}
	return packager, nil
}

// HandleMessage is the NSQ message handler. The NSQ consumer will pass each
// message in the subscribed channel to this function.
func (packager *DPNPackager) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	packager.Context.MessageLog.Info("Packager is checking NSQ message %s", string(message.Body))

	// Set up the manifest WITH the IntellectualObject
	manifest := SetupIngestManifest(message, "package", packager.Context, true)
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
	manifest.WorkItem.Note = "Starting DPN ingest"
	SaveWorkItem(packager.Context, manifest, manifest.PackageSummary)

	// Start processing.
	packager.Context.MessageLog.Info("Putting bag %s into the package channel",
		manifest.WorkItem.ObjectIdentifier)
	packager.PackageChannel <- manifest
	return nil
}

func (packager *DPNPackager) buildBag() {
	for manifest := range packager.PackageChannel {
		// Download and assemble all the files.
		packager.assembleFilesAndManifests(manifest)
		if !manifest.PackageSummary.HasErrors() {
			// Build the DPN bag record that will go into
			// the DPN registry.
			packager.buildDPNBag(manifest)
		}
		if manifest.PackageSummary.HasErrors() {
			packager.PostProcessChannel <- manifest
		} else {
			packager.TarChannel <- manifest
		}
	}
}

// getInstitution gets the Pharos record of the institution that owns the
// IntellectualObject we're currently working with. From that record, we
// can get the institution's DPN UUID. If an institution does not have a
// DPN UUID, they will not be able to push bags to DPN. So if we wind up
// with an institution with no DPN UUID here, that means some access
// restrictions were not properly enforced in Pharos.
func (packager *DPNPackager) getInstitution(manifest *models.DPNIngestManifest) *apt_models.Institution {
	instIdentifier := manifest.IntellectualObject.Institution
	if instIdentifier == "" {
		instIdentifier = strings.Split(manifest.IntellectualObject.Identifier, "/")[0]
	}
	if instIdentifier == "" {
		manifest.PackageSummary.AddError("Cannot get institution identifier from object %s",
			manifest.IntellectualObject.Identifier)
		return nil
	}
	resp := packager.Context.PharosClient.InstitutionGet(instIdentifier)
	if resp.Error != nil {
		manifest.PackageSummary.AddError("Can't get institution '%s' from Pharos: %v",
			instIdentifier, resp.Error.Error())
	}
	inst := resp.Institution()
	if inst == nil {
		manifest.PackageSummary.AddError("Pharos returned nil for institution '%s'", instIdentifier)
	}
	return inst
}

// buildDPNBag bags the DPNBag object that will eventually be entered into
// the DPN registry. This is not the bag itself, just the DPN registry entry.
func (packager *DPNPackager) buildDPNBag(manifest *models.DPNIngestManifest) {
	packager.Context.MessageLog.Info("Building DPN bag record for %s",
		manifest.IntellectualObject.Identifier)
	depositingInstitution := packager.getInstitution(manifest)
	if depositingInstitution == nil {
		return
	}
	manifest.DPNBag = models.NewDPNBag(
		manifest.IntellectualObject.Identifier,
		depositingInstitution.DPNUUID,
		packager.Context.Config.DPN.LocalNode)

	// Calculate the sha256 digest of the tag manifest. This is used for
	// validating bag transfers in DPN. Note that we are NOT using a
	// nonce when we call shaHash.Sum(nil). Though the DPN spec allows
	// us to use a nonce, no nodes are using nonces as of late 2016.
	tagManifestFile := filepath.Join(manifest.LocalDir, "tagmanifest-sha256.txt")
	if !fileutil.FileExists(tagManifestFile) {
		manifest.PackageSummary.AddError("Cannot find tag manifest %s", tagManifestFile)
		return
	}
	reader, err := os.Open(tagManifestFile)
	if err != nil {
		manifest.PackageSummary.AddError("Cannot read tag manifest at %s: %v",
			tagManifestFile, err)
		return
	}
	defer reader.Close()
	shaHash := sha256.New()
	io.Copy(shaHash, reader)
	tagManifestDigest := fmt.Sprintf("%x", shaHash.Sum(nil))

	// Now create the MessageDigest for this bag, with the tag manifest
	// checksum that will be used to verify transfers. When a remote
	// node copies this bag to fulfill a replication request, we expect
	// the node to return this fixity value as proof that it received
	// a valid copy of the bag.
	digest := &models.MessageDigest{
		Bag:       manifest.DPNBag.UUID,
		Algorithm: constants.AlgSha256,
		Node:      packager.Context.Config.DPN.LocalNode,
		Value:     tagManifestDigest,
		CreatedAt: time.Now().UTC(),
	}
	manifest.DPNBag.MessageDigests = append(manifest.DPNBag.MessageDigests, digest)

	// Now that we have a valid DPN bag object, we can name the tar file.
	// According to the DPN spec, the tar file name should be the bag's
	// UUID plus a ".tar" extension.
	parentOfBagDir := filepath.Dir(manifest.LocalDir)
	manifest.LocalTarFile = filepath.Join(parentOfBagDir, manifest.DPNBag.UUID+".tar")
}

func (packager *DPNPackager) tarBag() {
	for manifest := range packager.TarChannel {
		packager.Context.MessageLog.Info("Tarring %s", manifest.LocalDir)
		manifest.NsqMessage.Touch()

		files, err := fileutil.RecursiveFileList(manifest.LocalDir)
		if err != nil {
			manifest.PackageSummary.AddError("Cannot get list of files in directory %s: %s",
				manifest.LocalDir, err.Error())
			packager.PostProcessChannel <- manifest
			continue
		}

		// Set up our tar writer...
		tarWriter := tarfile.NewWriter(manifest.LocalTarFile)
		err = tarWriter.Open()
		if err != nil {
			manifest.PackageSummary.AddError("Error creating tar file %s for bag %s: %v",
				manifest.LocalTarFile, manifest.IntellectualObject.Identifier, err)
			packager.PostProcessChannel <- manifest
			continue
		}

		// ... and start filling it up.
		for _, filePath := range files {
			// The DPN spec at https://wiki.duraspace.org/display/DPN/BagIt+Specification
			// says the top-level folder within the bag should have the name of the DPN
			// Object Identifier (the UUID). So we replace <bag_name>/ with <uuid>/.
			//
			// Splitting filePath on object identifier looks like this:
			// /mnt/dpn/staging/test.edu/bag1/data/file1
			// pathInBag = "data/file1"
			// pathWithinArchive = "7a27db64-cea6-4602-a6c5-d8b2a7f6c02b/data/file1"
			pathInBag := strings.Split(filePath, manifest.IntellectualObject.Identifier)[1]
			pathWithinArchive := filepath.Join(manifest.DPNBag.UUID, pathInBag)

			err = tarWriter.AddToArchive(filePath, pathWithinArchive)
			if err != nil {
				manifest.PackageSummary.AddError("Error adding file %s to archive %s: %v",
					filePath, pathWithinArchive, err)
				tarWriter.Close()
				packager.PostProcessChannel <- manifest
				break
			}
		}
		tarWriter.Close()
		manifest.NsqMessage.Touch()

		// Finish the DPN bag record by setting the file size
		file, err := os.Stat(manifest.LocalTarFile)
		if err != nil {
			manifest.PackageSummary.AddError("Cannot get file size of %s: %v",
				manifest.LocalTarFile, err)
			packager.PostProcessChannel <- manifest
			break
		}
		manifest.DPNBag.Size = uint64(file.Size())

		// We want to validate everything AFTER tarring, because
		// the tarred bag is ultimately what will go into DPN.
		packager.ValidationChannel <- manifest
	}
}

func (packager *DPNPackager) validate() {
	for manifest := range packager.ValidationChannel {
		packager.Context.MessageLog.Info("Validating %s", manifest.LocalTarFile)
		manifest.NsqMessage.Touch()
		validator, err := validation.NewValidator(manifest.LocalTarFile, packager.BagValidationConfig, false)
		if err != nil {
			manifest.PackageSummary.AddError(err.Error())
		} else {
			// Validation can take a long time for large bags.
			summary, err := validator.Validate()
			if err != nil {
				manifest.PackageSummary.AddError(err.Error())
			} else {
				manifest.ValidateSummary = summary
			}
		}
		manifest.NsqMessage.Touch()
		if fileutil.LooksSafeToDelete(validator.DBName(), 12, 3) {
			os.Remove(validator.DBName())
		}
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
func (packager *DPNPackager) assembleFilesAndManifests(manifest *models.DPNIngestManifest) {
	packager.Context.MessageLog.Info("Assembling %s", manifest.IntellectualObject.Identifier)
	manifest.NsqMessage.Touch()
	// A little problem with BagBuilder here is that it creates a directory
	// with the bag name under the directory you give it. So if LocalDir
	// is /mnt/dpn/staging/test.edu/bag1, the BagBuilder starts putting files
	// into /mnt/dpn/staging/test.edu/bag1/bag1. That behavior makes sense in
	// other contexts, but causes problems here. So we have to remove the
	// bag name from the path.
	pathParts := strings.Split(manifest.LocalDir, string(os.PathSeparator))
	builderDir := strings.Join(pathParts[0:len(pathParts)-1], string(os.PathSeparator))
	packager.Context.MessageLog.Info("BuilderDir is %s", builderDir)

	builder, err := dpn_util.NewBagBuilder(
		builderDir, // should be absolute path
		manifest.IntellectualObject,
		packager.Context.Config.DPN.DefaultMetadata)
	if err != nil {
		manifest.PackageSummary.AddError("Cannot create BagBuilder: %v", err)
		return
	}
	packager.fetchAllFiles(manifest)

	for _, gf := range manifest.IntellectualObject.GenericFiles {
		localPath := filepath.Join(manifest.LocalDir, gf.OriginalPath())
		var err error
		if strings.HasPrefix(gf.OriginalPath(), "data/") {
			pathMinusDataPrefix := strings.Replace(gf.OriginalPath(), "data/", "", 1)
			packager.Context.MessageLog.Info("Adding %s as data file at %s", localPath, pathMinusDataPrefix)
			err = builder.Bag.AddFile(localPath, pathMinusDataPrefix)
		} else {
			// Using Sprintf instead of filepath.Join because, for our purposes,
			// tar file paths are supposed to contain forward slashes only.
			// See https://tools.ietf.org/html/draft-kunze-bagit-14, sections
			// 2.1.3 and 7.2. The GNU tar file standard also states that
			// directory names are separated by slashes (not backslashes).
			// http://www.gnu.org/software/tar/manual/html_node/Standard.html
			targetPath := fmt.Sprintf("aptrust-tags/%s", gf.OriginalPath())
			localPath = filepath.Join(manifest.LocalDir, "aptrust-tags", gf.OriginalPath())
			packager.Context.MessageLog.Info("Adding %s as tag file at %s", localPath, targetPath)
			err = builder.Bag.AddCustomTagfile(localPath, targetPath, true)
		}
		if err != nil {
			manifest.PackageSummary.AddError("Cannot add %s to bag: %v", localPath, err)
			return
		}
	}
	manifest.NsqMessage.Touch()

	// Write tag files and manifests
	errors := builder.Bag.Save()
	if errors != nil && len(errors) > 0 {
		for _, err = range errors {
			manifest.PackageSummary.AddError("Bagging error: %v", err)
		}
	}
	manifest.NsqMessage.Touch()
}

// ISSUE: See https://www.pivotaltracker.com/story/show/134540309
// TODO: Don't even try to solve the issue above without a thorough plan.
func (packager *DPNPackager) fetchAllFiles(manifest *models.DPNIngestManifest) {
	downloader := apt_network.NewS3Download(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		packager.Context.Config.PreservationBucket,
		"",    // s3 key to fetch - to be set below
		"",    // local path at which to save the s3 file - set below
		false, // no need to calculate md5
		true)  // calculate sha256 for fixity verification
	packager.Context.MessageLog.Info("Object %s has %d saved files",
		manifest.IntellectualObject.Identifier,
		len(manifest.IntellectualObject.GenericFiles))
	downloaded := 0
	for _, gf := range manifest.IntellectualObject.GenericFiles {
		downloader.Sha256Digest = ""
		downloader.ErrorMessage = ""

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
		// Any files outside the data directory are tag files, and per the DPN
		// spec, tag files from APTrust bags have to go into a dir called
		// aptrust-tags. (Actually, <anything>-tags, but we're going with
		// aptrust-tags.) See the DPN bagging spec here:
		// https://wiki.duraspace.org/display/DPNC/BagIt+Specification
		downloader.KeyName = s3KeyName
		targetPath := gf.OriginalPath()
		if !strings.HasPrefix(gf.OriginalPath(), "data/") {
			targetPath = filepath.Join("aptrust-tags", gf.OriginalPath())
		}
		downloader.LocalPath = filepath.Join(manifest.LocalDir, targetPath)

		// Fetch is the expensive part, so we don't even want to get to this
		// point if we don't have the info above.
		packager.Context.MessageLog.Info("Downloading %s (%s) to %s", gf.Identifier,
			s3KeyName, downloader.LocalPath)
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
		downloaded += 1
	}
	totalFileCount := len(manifest.IntellectualObject.GenericFiles)
	if downloaded == totalFileCount {
		packager.Context.MessageLog.Info("Downloaded all %d files for %s",
			downloaded, manifest.IntellectualObject.Identifier)
	} else {
		packager.Context.MessageLog.Error("Downloaded only %d of %d files for %s",
			downloaded, totalFileCount, manifest.IntellectualObject.Identifier)
	}
}

func (packager *DPNPackager) finishWithSuccess(manifest *models.DPNIngestManifest) {
	// Tell Pharos we're done with this item and save the work state.
	packager.Context.MessageLog.Info("Packaging succeeded for %s", manifest.WorkItem.ObjectIdentifier)
	manifest.WorkItem.Stage = constants.StageStore
	manifest.WorkItem.Status = constants.StatusPending
	manifest.WorkItem.Note = "Packaging completed, awaiting storage"
	manifest.WorkItem.Node = ""    // no worker is working on this now
	manifest.WorkItem.Pid = 0      // no process is working on this
	manifest.WorkItem.Retry = true // just in case this had been false
	SaveWorkItem(packager.Context, manifest, manifest.PackageSummary)
	manifest.PackageSummary.Finish()
	SaveWorkItemState(packager.Context, manifest, manifest.PackageSummary)

	// Delete the working directory where we built the bag
	if fileutil.LooksSafeToDelete(manifest.LocalDir, 12, 3) {
		os.RemoveAll(manifest.LocalDir)
	}

	// Push this WorkItem to the next NSQ topic.
	packager.Context.MessageLog.Info("Pushing %s (DPN bag %s) to NSQ topic %s",
		manifest.IntellectualObject.Identifier, manifest.DPNBag.UUID,
		packager.Context.Config.DPN.DPNIngestStoreWorker.NsqTopic)
	PushToQueue(packager.Context, manifest, manifest.PackageSummary,
		packager.Context.Config.DPN.DPNIngestStoreWorker.NsqTopic)
	if manifest.PackageSummary.HasErrors() {
		packager.Context.MessageLog.Error(manifest.PackageSummary.Errors[0])
	}

	// Tell NSQ we're done packaging this.
	manifest.NsqMessage.Finish()
}

func (packager *DPNPackager) finishWithError(manifest *models.DPNIngestManifest) {
	// Log what happened.
	msg := "Ingest failed. See ingest manifest."
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
	if manifest.WorkItem != nil {
		SaveWorkItem(packager.Context, manifest, manifest.PackageSummary)
	}
	manifest.PackageSummary.Finish()
	if manifest.WorkItemState != nil {
		SaveWorkItemState(packager.Context, manifest, manifest.PackageSummary)
	}
}
