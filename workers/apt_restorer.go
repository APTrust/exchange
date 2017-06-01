package workers

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/tarfile"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// APTRestorer restores bags by reassmbling their contents and
// pushing them into the depositor's restoration bucket.
type APTRestorer struct {
	// Context contains basic information required to run,
	// connect to Pharos, S3, etc.
	Context *context.Context
	// PackageChannel is for the go routines that reassemble
	// the S3 files into a new bag.
	PackageChannel chan *models.RestoreState
	// ValidateChannel is for go routines that validate the
	// newly assembled bag before sending it off to the restoration bucket.
	ValidateChannel chan *models.RestoreState
	// CopyChannel is for the goroutines that copy the newly
	// packaged bag to the depositor's restoration bucket in S3.
	CopyChannel chan *models.RestoreState
	// PostProcess channel is for the goroutines that record
	// the outcome of the restoration in Pharos and NSQ, and
	// do any other required cleanup.
	PostProcessChannel chan *models.RestoreState
	// BagValidationConfig is loaded from a JSON file in the
	// config directory. It describes what constitutes a valid
	// APTrust bag.
	BagValidationConfig *validation.BagValidationConfig
}

func NewAPTRestorer(_context *context.Context) *APTRestorer {
	restorer := &APTRestorer{
		Context: _context,
	}

	restorer.BagValidationConfig = LoadAPTrustBagValidationConfig(restorer.Context)

	// Set up buffered channels
	workerBufferSize := _context.Config.RestoreWorker.Workers * 10
	restorer.PackageChannel = make(chan *models.RestoreState, workerBufferSize)
	restorer.ValidateChannel = make(chan *models.RestoreState, workerBufferSize)
	restorer.CopyChannel = make(chan *models.RestoreState, workerBufferSize)
	restorer.PostProcessChannel = make(chan *models.RestoreState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.RestoreWorker.Workers; i++ {
		go restorer.buildBag()
		go restorer.validateBag()
		go restorer.copyToRestorationBucket()
		go restorer.postProcess()
	}
	return restorer
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (restorer *APTRestorer) HandleMessage(message *nsq.Message) error {
	// Build the RestoreState object by fetching WorkItem and IntellectualObject
	// from Pharos.
	restoreState, err := restorer.buildState(message)
	if err != nil {
		restorer.Context.MessageLog.Error(err.Error())
		message.Finish()
		return nil
	}

	// If this item was queued more than once, and this process or any
	// other is currently working on it, just finish the message and
	// assume that the in-progress worker will take care of the original.
	if restoreState.WorkItem.Node != "" && restoreState.WorkItem.Pid != 0 {
		restorer.Context.MessageLog.Info("Marking WorkItem %d (%s/%s) as finished "+
			"without doing any work, because this item is currently in process by "+
			"node %s, pid %d. WorkItem was last updated at %s.",
			restoreState.WorkItem.Id, restoreState.WorkItem.Bucket,
			restoreState.WorkItem.Name, restoreState.WorkItem.Node,
			restoreState.WorkItem.Pid, restoreState.WorkItem.UpdatedAt)
		message.Finish()
		return nil
	}

	// Disable auto response, so we can tell NSQ when we need to
	// that we're still working on this item.
	message.DisableAutoResponse()

	// Tell Pharos that we're building the bag: constants.StagePackage, constants.StatusStarted
	restorer.Context.MessageLog.Info("Marking %s as started", restoreState.WorkItem.ObjectIdentifier)
	restorer.markWorkItemStarted(restoreState)

	// We may have partially processed this item before and then been
	// forced to quit due to some transient error like not being able
	// to contact Pharos or S3. Figure out how far we got on the last
	// attempt to process, and resume there. Clear errors from prior
	// processing before resuming.
	if restoreState.CopySummary.Finished() && !restoreState.CopySummary.HasErrors() {
		restorer.logWhereThisIsGoing(restoreState, "PostProcessChannel")
		restoreState.RecordSummary.ClearErrors()
		restorer.PostProcessChannel <- restoreState
	} else if restoreState.ValidateSummary.Finished() && !restoreState.ValidateSummary.HasErrors() &&
		fileutil.FileExists(restoreState.LocalTarFile) {
		restorer.logWhereThisIsGoing(restoreState, "CopyChannel")
		restoreState.CopySummary.ClearErrors()
		restorer.CopyChannel <- restoreState
	} else if restoreState.PackageSummary.Finished() && !restoreState.PackageSummary.HasErrors() &&
		fileutil.FileExists(restoreState.LocalTarFile) {
		restorer.logWhereThisIsGoing(restoreState, "ValidateChannel")
		restoreState.ValidateSummary.ClearErrors()
		restorer.ValidateChannel <- restoreState
	} else {
		restorer.logWhereThisIsGoing(restoreState, "PackageChannel")
		restoreState.PackageSummary.ClearErrors()
		restoreState.CancelReason = ""
		restorer.PackageChannel <- restoreState
	}

	// Return no error, so NSQ knows we're OK.
	return nil
}

func (restorer *APTRestorer) buildBag() {
	for restoreState := range restorer.PackageChannel {
		restoreState.TouchNSQ()
		restoreState.PackageSummary.Attempted = true
		restoreState.PackageSummary.AttemptNumber += 1
		restoreState.PackageSummary.Start()

		// Download all of the IntellectualObject's files to the
		// local bag directory.
		restorer.fetchAllFiles(restoreState)
		if restoreState.PackageSummary.HasErrors() {
			restorer.PostProcessChannel <- restoreState
			continue
		}
		restoreState.TouchNSQ()

		// Write info files and  md5 and sha256 manifests
		restorer.writeAPTrustInfoFile(restoreState)
		restorer.writeBagitFile(restoreState)
		restorer.writeBagInfoFile(restoreState)
		restorer.writeManifest(constants.PAYLOAD_MANIFEST, constants.AlgMd5, restoreState)
		restorer.writeManifest(constants.PAYLOAD_MANIFEST, constants.AlgSha256, restoreState)
		restorer.writeManifest(constants.TAG_MANIFEST, constants.AlgMd5, restoreState)
		restorer.writeManifest(constants.TAG_MANIFEST, constants.AlgSha256, restoreState)
		if restoreState.PackageSummary.HasErrors() {
			restorer.PostProcessChannel <- restoreState
			continue
		}
		restoreState.TouchNSQ()

		// Tar the bag.
		restorer.tarBag(restoreState)
		if restoreState.PackageSummary.HasErrors() {
			restorer.PostProcessChannel <- restoreState
			continue
		}
		restoreState.TouchNSQ()

		// Done with packaging. On to validation...
		restoreState.PackageSummary.Finish()
		restorer.Context.MessageLog.Info("Putting %s into the validation channel",
			restoreState.WorkItem.ObjectIdentifier)
		restorer.ValidateChannel <- restoreState
	}
}

func (restorer *APTRestorer) validateBag() {
	for restoreState := range restorer.ValidateChannel {
		restoreState.TouchNSQ()
		restoreState.ValidateSummary.Attempted = true
		restoreState.ValidateSummary.AttemptNumber += 1
		restoreState.ValidateSummary.Start()
		validator, err := validation.NewValidator(
			restoreState.LocalTarFile,
			restorer.BagValidationConfig,
			false) // false means don't preserve ingest attributes in db
		if err != nil {
			restoreState.ValidateSummary.AddError(err.Error())
		} else {
			// Validation can take a long time for large bags.
			restorer.Context.MessageLog.Info("Validating %s", restoreState.LocalTarFile)
			validator.ObjIdentifier = restoreState.WorkItem.ObjectIdentifier
			summary, err := validator.Validate()
			restorer.Context.MessageLog.Info("Finished validating %s", restoreState.LocalTarFile)
			if err != nil {
				summary := models.NewWorkSummary()
				summary.Attempted = true
				summary.StartedAt = time.Now().UTC()
				summary.AddError(err.Error())
				summary.FinishedAt = time.Now().UTC()
			} else if summary != nil && summary.HasErrors() {
				for _, errMsg := range summary.Errors {
					restoreState.ValidateSummary.AddError("Validation error: %s", errMsg)
				}
				restoreState.ValidateSummary = summary
			}
		}
		restoreState.ValidateSummary.Finish()
		restoreState.TouchNSQ()
		if restoreState.ValidateSummary.HasErrors() {
			restorer.Context.MessageLog.Info("Putting %s into PostProcess channel",
				restoreState.WorkItem.ObjectIdentifier)
			restorer.PostProcessChannel <- restoreState
		} else {
			restorer.Context.MessageLog.Info("Putting %s into Copy channel",
				restoreState.WorkItem.ObjectIdentifier)
			restorer.CopyChannel <- restoreState
		}
	}
}

func (restorer *APTRestorer) copyToRestorationBucket() {
	for restoreState := range restorer.CopyChannel {
		restoreState.TouchNSQ()
		restoreState.CopySummary.Attempted = true
		restoreState.CopySummary.AttemptNumber += 1
		restoreState.CopySummary.Start()
		restorer.uploadBag(restoreState)
		restoreState.CopySummary.Finish()
		restorer.PostProcessChannel <- restoreState
	}
}

func (restorer *APTRestorer) postProcess() {
	for restoreState := range restorer.PostProcessChannel {
		// Mark item completed in Pharos and finish NSQ.
		if restoreState.HasErrors() {
			restorer.finishWithError(restoreState)
		} else {
			restorer.finishWithSuccess(restoreState)
		}
	}
}

func (restorer *APTRestorer) finishWithError(restoreState *models.RestoreState) {
	mostRecentSummary := restoreState.MostRecentSummary()
	note := fmt.Sprintf("Bag could not be restored: %s", mostRecentSummary.AllErrorsAsString())
	maxAttempts := restorer.Context.Config.RestoreWorker.MaxAttempts
	if mostRecentSummary.AttemptNumber > maxAttempts && restoreState.CancelReason == "" {
		note = fmt.Sprintf("Too many failed restore attempts (%d). "+
			"Errors: %s",
			maxAttempts,
			mostRecentSummary.AllErrorsAsString())
		mostRecentSummary.ErrorIsFatal = true
	}

	// Delete the tar file & valdb no matter what...
	restorer.deleteFiles(restoreState)

	// ...but delete the bag directory only if this is a fatal error.
	// We may have cases where we've downloaded 99,000 of a bag's 100,000
	// files, and if we're going to retry, we should leave those files
	// on disk, so we don't have to re-download them. The fetchAllFiles
	// function includes logic to check for existing files in the
	// LocalBagDir.
	//
	// Fatal errors can be due to 1) exceeding max processing attempts,
	// 2) inability to fetch all of the bag's files, 3) inability to
	// create a valid bag.
	if restoreState.HasFatalErrors() || restoreState.CancelReason != "" {
		restoreState.RecordSummary.Attempted = true
		restoreState.RecordSummary.AttemptNumber += 1
		restoreState.RecordSummary.Start()
		restorer.deleteBagDir(restoreState)
		mostRecentSummary.Retry = false
		restoreState.WorkItem.Retry = false
		if restoreState.CancelReason != "" {
			restoreState.WorkItem.Status = constants.StatusCancelled
			note = restoreState.CancelReason
		} else {
			restoreState.WorkItem.Status = constants.StatusFailed
		}
	} else {
		// Set this back to pending, and we'll try again.
		restoreState.WorkItem.Status = constants.StatusPending
	}

	restoreState.WorkItem.Date = time.Now().UTC()
	restoreState.WorkItem.Note = note
	restoreState.WorkItem.Node = ""
	restoreState.WorkItem.Pid = 0
	restoreState.WorkItem.StageStartedAt = nil

	if restoreState.HasFatalErrors() {
		restoreState.RecordSummary.Finish()
	}
	restorer.saveWorkItem(restoreState)
	restorer.saveWorkItemState(restoreState)

	if restoreState.CancelReason != "" {
		restorer.Context.MessageLog.Warning(restoreState.CancelReason)
	} else {
		restorer.Context.MessageLog.Error("Failed to restore %s: %s",
			restoreState.WorkItem.ObjectIdentifier,
			mostRecentSummary.AllErrorsAsString())
	}
	if mostRecentSummary.ErrorIsFatal {
		restorer.Context.MessageLog.Error("Error for %s is fatal",
			restoreState.WorkItem.ObjectIdentifier)
		restoreState.NSQMessage.Finish()
	} else {
		restorer.Context.MessageLog.Info("Requeuing WorkItem %d (%s)",
			restoreState.WorkItem.Id,
			restoreState.WorkItem.ObjectIdentifier)
		restoreState.NSQMessage.Requeue(1 * time.Minute)
	}
}

func (restorer *APTRestorer) finishWithSuccess(restoreState *models.RestoreState) {
	restoreState.RecordSummary.Attempted = true
	restoreState.RecordSummary.AttemptNumber += 1
	restoreState.RecordSummary.Start()
	message := fmt.Sprintf("Bag %s restored to %s",
		restoreState.WorkItem.ObjectIdentifier,
		restoreState.RestoredToUrl)
	restorer.Context.MessageLog.Info(message)

	restoreState.WorkItem.Date = time.Now().UTC()
	restoreState.WorkItem.Note = message
	restoreState.WorkItem.Stage = constants.StageResolve
	restoreState.WorkItem.StageStartedAt = nil
	restoreState.WorkItem.Status = constants.StatusSuccess
	restoreState.WorkItem.Node = ""
	restoreState.WorkItem.Pid = 0

	restorer.deleteFiles(restoreState)
	restorer.deleteBagDir(restoreState)
	restoreState.RecordSummary.Finish()
	restorer.saveWorkItem(restoreState)
	restorer.saveWorkItemState(restoreState)

	// Tell NSQ we're done storing this.
	restoreState.NSQMessage.Finish()
}

func (restorer *APTRestorer) deleteFiles(restoreState *models.RestoreState) {
	dbPath := TAR_SUFFIX.ReplaceAllString(restoreState.LocalTarFile, ".valdb")
	restorer.deleteFile(restoreState, restoreState.LocalTarFile)
	restorer.deleteFile(restoreState, dbPath)
}

func (restorer *APTRestorer) deleteFile(restoreState *models.RestoreState, filename string) {
	if fileutil.FileExists(filename) &&
		fileutil.LooksSafeToDelete(filename, 12, 3) {
		err := os.Remove(filename)
		if err != nil && !os.IsNotExist(err) {
			message := fmt.Sprintf("Failed to delete %s", filename)
			restorer.Context.MessageLog.Error(message)
		} else {
			if filename == restoreState.LocalTarFile {
				restoreState.TarFileDeletedAt = time.Now().UTC()
			}
		}
	}
}

func (restorer *APTRestorer) deleteBagDir(restoreState *models.RestoreState) {
	if fileutil.FileExists(restoreState.LocalBagDir) &&
		fileutil.LooksSafeToDelete(restoreState.LocalBagDir, 12, 3) {
		err := os.RemoveAll(restoreState.LocalBagDir)
		if err != nil && !os.IsNotExist(err) {
			message := fmt.Sprintf("Failed to delete %s", restoreState.LocalBagDir)
			//restoreState.MostRecentSummary().AddError(message)
			restorer.Context.MessageLog.Error(message)
		} else {
			restoreState.BagDirDeletedAt = time.Now().UTC()
		}
	}
}

func (restorer *APTRestorer) uploadBag(restoreState *models.RestoreState) {
	// Each institution has its own restoration bucket.
	restorationBucket := util.RestorationBucketFor(restoreState.IntellectualObject.Institution)
	// In certain environments, such as test and integration,
	// the config specifies a custom restoration bucket so that
	// we don't mix test bags in with production bags.
	if restorer.Context.Config.CustomRestoreBucket != "" {
		restorationBucket = fmt.Sprintf("%s.%s",
			restorer.Context.Config.CustomRestoreBucket,
			restoreState.IntellectualObject.Institution)
	}
	s3Key := fmt.Sprintf("%s.tar", restoreState.IntellectualObject.BagName)
	restorer.Context.MessageLog.Info("Uploading %s to %s/%s",
		restoreState.LocalTarFile, restorationBucket, s3Key)
	upload := network.NewS3Upload(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		restorationBucket,
		s3Key,
		"application/x-tar")

	// Open a reader for the tarred bag.
	reader, err := os.Open(restoreState.LocalTarFile)
	if reader != nil {
		defer reader.Close()
	}
	if err != nil {
		restoreState.CopySummary.AddError("Upload: error opening reader for tar file %s: %v",
			restoreState.LocalTarFile, err)
		return
	}

	// Send the tarred bag to the depositor's restoration bucket.
	upload.Send(reader)
	if upload.ErrorMessage != "" {
		restoreState.CopySummary.AddError("Error uploading tar file %s: %s",
			restoreState.LocalTarFile, upload.ErrorMessage)
		return
	}
	restoreState.RestoredToUrl = upload.Response.Location
	restoreState.CopiedToRestorationAt = time.Now().UTC()
}

// buildState builds the RestoreState object, which keeps track of which
// parts of the restore operation have been completed.
func (restorer *APTRestorer) buildState(message *nsq.Message) (*models.RestoreState, error) {
	restoreState := models.NewRestoreState(message)
	restorer.Context.MessageLog.Info("Asking Pharos for WorkItem %s", string(message.Body))
	workItem, err := GetWorkItem(message, restorer.Context)
	if err != nil {
		return nil, err
	}
	restoreState.WorkItem = workItem
	restorer.Context.MessageLog.Info("Got WorkItem %d", workItem.Id)

	// Get the saved state of this item, if there is one.
	if workItem.WorkItemStateId != nil {
		restorer.Context.MessageLog.Info("Asking Pharos for WorkItemState %d", *workItem.WorkItemStateId)
		resp := restorer.Context.PharosClient.WorkItemStateGet(*workItem.WorkItemStateId)
		if resp.Error != nil {
			restorer.Context.MessageLog.Warning("Could not retrieve WorkItemState with id %d: %v",
				*workItem.WorkItemStateId, resp.Error)
		} else {
			workItemState := resp.WorkItemState()
			savedState := &models.RestoreState{}
			err = json.Unmarshal([]byte(workItemState.State), savedState)
			if err != nil {
				return nil, fmt.Errorf("Could not unmarshal WorkItemState.State: %v", err)
			}
			restoreState.PackageSummary = savedState.PackageSummary
			restoreState.ValidateSummary = savedState.ValidateSummary
			restoreState.CopySummary = savedState.CopySummary
			restoreState.RecordSummary = savedState.RecordSummary
			restoreState.LocalBagDir = savedState.LocalBagDir
			restoreState.LocalTarFile = savedState.LocalTarFile
			restoreState.RestoredToUrl = savedState.RestoredToUrl
			restoreState.CopiedToRestorationAt = savedState.CopiedToRestorationAt
			restorer.Context.MessageLog.Info("Got WorkItemState %d", *workItem.WorkItemStateId)
		}
	}

	// Get the intellectual object. This should not have changed
	// during the processing of this request, because Pharos does
	// not permit delete operations while a restore is pending.
	restorer.Context.MessageLog.Info("Asking Pharos for IntellectualObject %s",
		restoreState.WorkItem.ObjectIdentifier)
	response := restorer.Context.PharosClient.IntellectualObjectGet(
		restoreState.WorkItem.ObjectIdentifier, true, false)
	if response.Error != nil {
		return nil, err
	}
	restoreState.IntellectualObject = response.IntellectualObject()
	restorer.Context.MessageLog.Info("Got IntellectualObject %s",
		restoreState.WorkItem.ObjectIdentifier)

	// LocalBagDir will not be set if we were unable to retrieve
	// WorkItemState above.
	if restoreState.LocalBagDir == "" {
		restoreState.LocalBagDir = filepath.Join(
			restorer.Context.Config.RestoreDirectory,
			restoreState.IntellectualObject.Identifier)
	}
	restorer.Context.MessageLog.Info("Set local bag dir to %s", restoreState.LocalBagDir)
	return restoreState, nil
}

// markWorkItemStarted tells Pharos that we're starting work on this.
func (restorer *APTRestorer) markWorkItemStarted(restoreState *models.RestoreState) {
	now := time.Now().UTC()
	restoreState.WorkItem.Date = now
	restoreState.WorkItem.Stage = constants.StagePackage
	restoreState.WorkItem.Status = constants.StatusStarted
	restoreState.WorkItem.Node, _ = os.Hostname()
	restoreState.WorkItem.Pid = os.Getpid()
	restoreState.WorkItem.StageStartedAt = &now
	restorer.saveWorkItem(restoreState)
}

func (restorer *APTRestorer) saveWorkItem(restoreState *models.RestoreState) {
	resp := restorer.Context.PharosClient.WorkItemSave(restoreState.WorkItem)
	// We can proceed if this call fails. Pharos just won't show users
	// the current state of processing for this item.
	if resp.Error != nil {
		restorer.Context.MessageLog.Warning(
			"Error marking WorkItem %d as %s/%s for object %s: %v",
			restoreState.WorkItem.Id,
			restoreState.WorkItem.Stage,
			restoreState.WorkItem.Status,
			restoreState.WorkItem.ObjectIdentifier,
			resp.Error)
	}
}

func (restorer *APTRestorer) saveWorkItemState(restoreState *models.RestoreState) {
	stateJson, err := json.Marshal(restoreState)
	if err != nil {
		errMessage := fmt.Sprintf("Cannot marshal restoreState JSON: %v", err)
		restorer.Context.MessageLog.Error(errMessage)
		restoreState.MostRecentSummary().AddError(errMessage)
		return
	}
	restorer.logJson(restoreState, string(stateJson))
	workItemState := models.NewWorkItemState(restoreState.WorkItem.Id,
		constants.ActionRestore, string(stateJson))
	if restoreState.WorkItem.WorkItemStateId != nil {
		workItemState.Id = *restoreState.WorkItem.WorkItemStateId
	}
	resp := restorer.Context.PharosClient.WorkItemStateSave(workItemState)
	if resp.Error != nil {
		restorer.Context.MessageLog.Warning(
			"Error saving WorkItemState for object %s: %v",
			restoreState.IntellectualObject.Identifier,
			resp.Error)
	}
}

// Log a message saying which channel we're putting this into.
func (restorer *APTRestorer) logWhereThisIsGoing(restoreState *models.RestoreState, channelName string) {
	restorer.Context.MessageLog.Info("Putting %s into %s channel",
		restoreState.WorkItem.ObjectIdentifier, channelName)
}

// tarBag tars up the entire bag, after all files have been downloaded
// and manifests written.
func (restorer *APTRestorer) tarBag(restoreState *models.RestoreState) {
	restorer.Context.MessageLog.Info("Tarring %s", restoreState.LocalBagDir)
	files, err := fileutil.RecursiveFileList(restoreState.LocalBagDir)
	if err != nil {
		restoreState.PackageSummary.AddError("Cannot get list of files in directory %s: %s",
			restoreState.LocalBagDir, err.Error())
		return
	}

	// Set up our tar writer...
	restoreState.LocalTarFile = fmt.Sprintf("%s.tar", restoreState.LocalBagDir)
	tarWriter := tarfile.NewWriter(restoreState.LocalTarFile)
	err = tarWriter.Open()
	if err != nil {
		restoreState.PackageSummary.AddError("Error creating tar file %s for bag %s: %v",
			restoreState.LocalTarFile, restoreState.IntellectualObject.Identifier, err)
		return
	}

	// ... and start filling it up.
	for _, filePath := range files {
		// We want to transform filePath to pathWithinArchive, like so:
		// /mnt/aptrust/restore/ncsu.edu/bag123/bagit.txt -> bag123/bagit.txt
		pathInBag := strings.Split(filePath, restoreState.IntellectualObject.Identifier)[1]
		pathWithinArchive := filepath.Join(restoreState.IntellectualObject.BagName, pathInBag)
		err = tarWriter.AddToArchive(filePath, pathWithinArchive)
		if err != nil {
			restoreState.PackageSummary.AddError("Error adding file %s to archive %s: %v",
				filePath, pathWithinArchive, err)
			tarWriter.Close()
			return
		}
	}
	tarWriter.Close()
}

// writeBagitFile creates the bagit.txt file for this bag.
func (restorer *APTRestorer) writeBagitFile(restoreState *models.RestoreState) {
	bagitPath := filepath.Join(restoreState.LocalBagDir, "bagit.txt")
	bagitFile, err := os.Create(bagitPath)
	if err != nil {
		restoreState.PackageSummary.AddError("Cannot create bagit.txt file %s: %v",
			bagitPath, err)
		return
	}
	fmt.Fprintln(bagitFile, "BagIt-Version:", restorer.Context.Config.BagItVersion)
	fmt.Fprintln(bagitFile, "Tag-File-Character-Encoding:", restorer.Context.Config.BagItEncoding)
	bagitFile.Close()
	restorer.addFile(restoreState, bagitPath, "bagit.txt")
}

// writeAPTrustInfo creates and writes a basic aptrust-info file into the
// bag directory, if that file does not exist. Prior to 2016 (?), we did not
// save the aptrust-info.txt file to long-term storage, so we do need to
// rebuild this file for some older bags. For newer bags, we omit this step.
func (restorer *APTRestorer) writeAPTrustInfoFile(restoreState *models.RestoreState) {
	aptInfoPath := filepath.Join(restoreState.LocalBagDir, "aptrust-info.txt")
	if fileutil.FileExists(aptInfoPath) {
		restorer.Context.MessageLog.Info("aptrust-info.txt already exists for %s",
			restoreState.IntellectualObject.Identifier)
		return
	} else {
		restorer.Context.MessageLog.Info("Creating aptrust-info.txt for older bag %s",
			restoreState.IntellectualObject.Identifier)
	}
	aptInfoFile, err := os.Create(aptInfoPath)
	if err != nil {
		restoreState.PackageSummary.AddError("Cannot create aptrust-info.txt file %s: %v",
			aptInfoPath, err)
		return
	}
	fmt.Fprintln(aptInfoFile, "Title:", restoreState.IntellectualObject.Title)
	fmt.Fprintln(aptInfoFile, "Access:", strings.Title(restoreState.IntellectualObject.Access))
	fmt.Fprintln(aptInfoFile, "Description:", restoreState.IntellectualObject.Description)
	aptInfoFile.Close()
	restorer.addFile(restoreState, aptInfoPath, "aptrust-info.txt")
}

// writeBagInfoFile creates the bag-info.txt file, if it does not already
// exist. We started saving bag-info.txt in mid(?) 2016. Bags ingested
// prior to that date need a reconstructed bag-info.txt file.
func (restorer *APTRestorer) writeBagInfoFile(restoreState *models.RestoreState) {
	bagInfoPath := filepath.Join(restoreState.LocalBagDir, "bag-info.txt")
	if fileutil.FileExists(bagInfoPath) {
		restorer.Context.MessageLog.Info("bag-info.txt already exists for %s",
			restoreState.IntellectualObject.Identifier)
		return
	} else {
		restorer.Context.MessageLog.Info("Creating bag-info.txt for older bag %s",
			restoreState.IntellectualObject.Identifier)
	}
	bagInfoFile, err := os.Create(bagInfoPath)
	if err != nil {
		restoreState.PackageSummary.AddError("Cannot create bag-info.txt file %s: %v",
			bagInfoPath, err)
		return
	}

	// In APTrust 1.0, we had a bag size limit of 250GB. When we restored a multi-part
	// bag that exceeded that size, we broke it up into multiple bags, and the
	// Bag-Count tag might say something like "1 of 2". In APTrust 2.0, we're moving
	// away from size limits, and we're going to restore bags all in one piece.
	// So Bag-Count will be "1 of 1".

	fmt.Fprintln(bagInfoFile, "Source-Organization:", restoreState.IntellectualObject.Institution)
	fmt.Fprintln(bagInfoFile, "Bagging-Date:", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintln(bagInfoFile, "Bag-Count:", "1 of 1")
	fmt.Fprintln(bagInfoFile, "Bag-Group-Identifier:", "")
	fmt.Fprintln(bagInfoFile, "Internal-Sender-Description:", restoreState.IntellectualObject.Description)
	fmt.Fprintln(bagInfoFile, "Internal-Sender-Identifier:", restoreState.IntellectualObject.AltIdentifier)
	bagInfoFile.Close()
	restorer.addFile(restoreState, bagInfoPath, "bag-info.txt")
}

// addFile temporarily adds a file to the intellectual object's GenericFiles
// list, so that writeManifest can include the bagit file and its
// checksum in the manifests. We do not save this GenericFile record
// to Pharos. We do this for bagit.txt, and if necessary, for bag-info.txt and
// aptrust-info.txt.
func (restorer *APTRestorer) addFile(restoreState *models.RestoreState, absPath, relativePath string) {
	gf := models.NewGenericFile()
	gf.IntellectualObjectIdentifier = restoreState.IntellectualObject.Identifier
	gf.Identifier = fmt.Sprintf("%s/%s", restoreState.IntellectualObject.Identifier, relativePath)
	md5, err := fileutil.CalculateChecksum(absPath, constants.AlgMd5)
	if err != nil {
		restoreState.PackageSummary.AddError("Can't get md5 digest of %s: %v", absPath, err)
	}
	sha256, err := fileutil.CalculateChecksum(absPath, constants.AlgSha256)
	if err != nil {
		restoreState.PackageSummary.AddError("Can't get sha256 digest of %s: %v", absPath, err)
	}
	checksumMd5 := &models.Checksum{
		Algorithm: constants.AlgMd5,
		Digest:    md5,
	}
	checksumSha256 := &models.Checksum{
		Algorithm: constants.AlgSha256,
		Digest:    sha256,
	}
	gf.Checksums = append(gf.Checksums, checksumMd5)
	gf.Checksums = append(gf.Checksums, checksumSha256)
	restorer.Context.MessageLog.Info("Added file %s to bag. Abs path: %s. Relative path: %s",
		gf.Identifier, absPath, relativePath)
	//restorer.Context.MessageLog.Info("ObjIdentifer: %s, gf.OriginalPath: %s",
	//	restoreState.IntellectualObject.Identifier, gf.OriginalPath())
	restoreState.IntellectualObject.GenericFiles = append(
		restoreState.IntellectualObject.GenericFiles, gf)
}

// writeManifest writes the manifest-md5.txt file or the manifest-sha256.txt file
// for this bag.
func (restorer *APTRestorer) writeManifest(manifestType, algorithm string, restoreState *models.RestoreState) {
	if algorithm != constants.AlgMd5 && algorithm != constants.AlgSha256 {
		restorer.Context.MessageLog.Fatal("writeManifest: Unsupported algorithm: %s", algorithm)
	}
	manifestPath := restorer.getManifestPath(manifestType, algorithm, restoreState)
	manifestFile, err := os.Create(manifestPath)
	if err != nil {
		restoreState.PackageSummary.AddError("Cannot create manifest file %s: %v",
			manifestPath, err)
		return
	}
	defer manifestFile.Close()
	for _, gf := range restoreState.IntellectualObject.GenericFiles {
		if !restorer.fileBelongsInManifest(gf, manifestType) {
			restorer.Context.MessageLog.Info("Skipping file '%s' for manifest type %s (%s)",
				gf.Identifier, manifestType, algorithm)
			continue
		}
		checksum := gf.GetChecksumByAlgorithm(algorithm)
		if checksum == nil {
			restoreState.PackageSummary.AddError("Cannot find %s checksum for file %s",
				algorithm, gf.OriginalPath())
			return
		}
		_, err := fmt.Fprintln(manifestFile, checksum.Digest, gf.OriginalPath())
		if err != nil {
			restoreState.PackageSummary.AddError("Error writing checksum for file %s "+
				"to manifest %s: %v", gf.OriginalPath(), manifestPath, err)
			return
		}
	}
}

func (restorer *APTRestorer) fileBelongsInManifest(gf *models.GenericFile, manifestType string) bool {
	// PT #138749039: Payload files go in payload manifest,
	// tag files go in tag manifest.
	isPayloadFile := strings.HasPrefix(gf.OriginalPath(), "data/")
	isPayloadManifest := manifestType == constants.PAYLOAD_MANIFEST
	// A more terse way to calculate the return value is:
	//
	//     return isPayloadFile == isPayloadManifest
	//
	// But I don't like writing clever code that I have
	// to puzzle over later. Better to be clear about intentions.
	return (isPayloadFile && isPayloadManifest) || (!isPayloadFile && !isPayloadManifest)
}

func (restorer *APTRestorer) getManifestPath(manifestType, algorithm string, restoreState *models.RestoreState) string {
	manifestPath := filepath.Join(restoreState.LocalBagDir, fmt.Sprintf("manifest-%s.txt", algorithm))
	if manifestType == constants.TAG_MANIFEST {
		manifestPath = filepath.Join(restoreState.LocalBagDir, fmt.Sprintf("tagmanifest-%s.txt", algorithm))
	}
	return manifestPath
}

func (restorer *APTRestorer) fetchAllFiles(restoreState *models.RestoreState) {
	activeFileCount := 0
	for _, gf := range restoreState.IntellectualObject.GenericFiles {
		if gf.State == "A" {
			activeFileCount++
		}
	}
	if activeFileCount == 0 {
		restoreState.CancelReason = fmt.Sprintf(
			"System cancelled restoration because bag %s has zero active files. "+
				"Check the PREMIS events with event_type 'deletion' for this bag.",
			restoreState.IntellectualObject.Identifier)
		restoreState.PackageSummary.ErrorIsFatal = true
		return
	}

	// Create the local bag directory.
	if err := os.MkdirAll(restoreState.LocalBagDir, 0755); err != nil {
		restoreState.PackageSummary.AddError("Cannot create local bag path %s: %v",
			restoreState.LocalBagDir, err)
		return
	}

	// Set up a downloader to fetch files from S3 long-term storage.
	downloader := network.NewS3Download(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		restorer.Context.Config.PreservationBucket,
		"",   // s3 key to fetch - to be set below
		"",   // local path at which to save the s3 file - set below
		true, // calculate md5 for manifest
		true) // calculate sha256 for manifest and fixity verification

	// Fetch all of the files from S3 to our local bag dir.
	restorer.Context.MessageLog.Info("Starting fetch. Object %s has %d saved (active) files",
		restoreState.IntellectualObject.Identifier, activeFileCount)
	downloaded := 0
	alreadyOnDisk := 0
	for _, gf := range restoreState.IntellectualObject.GenericFiles {
		downloader.Sha256Digest = ""
		downloader.Md5Digest = ""
		downloader.ErrorMessage = ""

		// Except these losers. We don't want them.
		if gf.State == "D" {
			restorer.Context.MessageLog.Info("Skipping deleted file %s", gf.Identifier)
			continue
		}

		// We're going to want to confirm the sha256 digest of the download...
		existingSha256 := gf.GetChecksumByAlgorithm(constants.AlgSha256)
		if existingSha256 == nil {
			restoreState.PackageSummary.AddError("Cannot find sha256 digest for file %s", gf.Identifier)
			break
		}

		// Figure out what the key name is for this file. It's a UUID.
		s3KeyName, err := gf.PreservationStorageFileName()
		if err != nil {
			restoreState.PackageSummary.AddError("File %s: %v", gf.Identifier, err)
			break
		}

		// Tell the downloader what we're downloading, and where to put it.
		downloader.KeyName = s3KeyName
		targetPath := gf.OriginalPath()
		downloader.LocalPath = filepath.Join(restoreState.LocalBagDir, targetPath)

		// See if we already have this file on disk. That may be the case if
		// a recent prior attempt to restore this bag failed with a transient
		// error. We'll assume the file is good if it has the same name and size
		// as the one we're fetching. If the file is bad, we'll catch that in the
		// bag validation step. When bags have tens of thousands of files, or
		// very large files, we want to avoid re-downloading them.
		fileStat, err := os.Stat(downloader.LocalPath)
		if err == nil && fileStat.Size() == gf.Size {
			restorer.Context.MessageLog.Info("File %s is already on disk with size %d, "+
				"so we won't download it again. Will verify checksum in validation step.",
				downloader.LocalPath, fileStat.Size())
			alreadyOnDisk += 1
			continue
		}

		// Fetch is the expensive part, so we don't even want to get to this
		// point if we don't have the info above.
		restorer.Context.MessageLog.Info("Downloading %s (%s) to %s", gf.Identifier,
			s3KeyName, downloader.LocalPath)
		downloader.Fetch()
		if downloader.ErrorMessage != "" {
			msg := fmt.Sprintf("Error fetching %s from S3: %s", gf.Identifier, downloader.ErrorMessage)
			restorer.Context.MessageLog.Error(msg)
			restoreState.PackageSummary.AddError(msg)
			break
		}

		// Validate checksums now, so we don't have to re-calculate
		// them when we create the file manifests.
		if downloader.Sha256Digest != existingSha256.Digest {
			msg := fmt.Sprintf("sha256 digest mismatch for for file %s."+
				"Our digest: %s. Digest of fetched file: %s",
				gf.Identifier, existingSha256, downloader.Sha256Digest)
			restorer.Context.MessageLog.Error(msg)
			restoreState.PackageSummary.AddError(msg)
			break
		}
		downloaded += 1

		// Touch NSQ every now and then, so we don't time out.
		if downloaded%10 == 0 {
			restoreState.TouchNSQ()
		}
	}

	// Final status report for logging and troubleshooting.
	totalFilesPresent := downloaded + alreadyOnDisk
	if totalFilesPresent == activeFileCount {
		restorer.Context.MessageLog.Info("Found all %d files for %s (%d downloaded, %d already on disk)",
			activeFileCount, restoreState.IntellectualObject.Identifier,
			downloaded, alreadyOnDisk)
	} else {
		msg := fmt.Sprintf("Found only %d of %d files for %s (%d downloaded, %d already on disk)",
			totalFilesPresent, activeFileCount,
			restoreState.IntellectualObject.Identifier,
			downloaded, alreadyOnDisk)
		restoreState.PackageSummary.AddError(msg)
		restorer.Context.MessageLog.Error(msg)
	}
}

// LogJson dumps the WorkItemState.State into the JSON log, surrounded by
// markers that make it easy to find.
func (aptRestorer *APTRestorer) logJson(restoreState *models.RestoreState, jsonString string) {
	timestamp := time.Now().UTC().Format(time.RFC3339)
	startMessage := fmt.Sprintf("-------- BEGIN %s | WorkItem: %d | Time: %s --------",
		restoreState.WorkItem.ObjectIdentifier, restoreState.WorkItem.Id, timestamp)
	endMessage := fmt.Sprintf("-------- END %s | WorkItem: %d | Time: %s --------",
		restoreState.WorkItem.ObjectIdentifier, restoreState.WorkItem.Id, timestamp)
	aptRestorer.Context.JsonLog.Println(startMessage, "\n",
		jsonString, "\n",
		endMessage)
}
