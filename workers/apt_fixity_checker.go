package workers

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/nsqio/go-nsq"
)

// APTFixityChecker performs ongoing fixity checks on files stored in S3.
type APTFixityChecker struct {
	Context            *context.Context
	FixityChannel      chan *models.FixityResult
	RecordChannel      chan *models.FixityResult
	PostProcessChannel chan *models.FixityResult
}

func NewAPTFixityChecker(_context *context.Context) *APTFixityChecker {
	checker := &APTFixityChecker{
		Context: _context,
	}
	workerBufferSize := _context.Config.FixityWorker.Workers * 10
	checker.FixityChannel = make(chan *models.FixityResult, workerBufferSize)
	checker.RecordChannel = make(chan *models.FixityResult, workerBufferSize)
	checker.PostProcessChannel = make(chan *models.FixityResult, workerBufferSize)
	for i := 0; i < _context.Config.StoreWorker.Workers; i++ {
		go checker.checkFixity()
		go checker.record()
		go checker.postProcess()
	}
	return checker
}

func (checker *APTFixityChecker) HandleMessage(message *nsq.Message) error {
	fixityResult := checker.buildFixityResult(message)
	if fixityResult.FixityCheckSummary.HasErrors() {
		errMsg := fixityResult.FixityCheckSummary.FirstError()
		checker.Context.MessageLog.Error(errMsg)
		fixityResult.WorkItem.Note = errMsg
		fixityResult.WorkItem.Retry = false
		fixityResult.WorkItem.NeedsAdminReview = true
		checker.saveWorkItem(fixityResult)
		message.Finish()
		return nil
	}
	// If some other process is working on this item, let go of it.
	if fixityResult.WorkItem.Node != "" && fixityResult.WorkItem.Pid != 0 {
		checker.Context.MessageLog.Info("Marking WorkItem %d (%s/%s) as finished "+
			"without doing any work, because this item is currently in process by "+
			"node %s, pid %s. WorkItem was last updated at %s.",
			fixityResult.WorkItem.Id, fixityResult.WorkItem.Bucket,
			fixityResult.WorkItem.Name, fixityResult.WorkItem.Node,
			fixityResult.WorkItem.Pid, fixityResult.WorkItem.UpdatedAt)
		message.Finish()
		return nil
	}
	// We'll ping NSQ manually when we need to.
	message.DisableAutoResponse()
	fixityResult.FixityCheckSummary.ClearErrors()
	checker.Context.MessageLog.Info("Putting %s into fixity channel",
		fixityResult.WorkItem.GenericFileIdentifier)

	checker.FixityChannel <- fixityResult
	return nil
}

func (checker *APTFixityChecker) checkFixity() {
	for fixityResult := range checker.FixityChannel {
		fixityResult.FixityCheckSummary.Start()
		fixityResult.FixityCheckSummary.Attempted = true
		fixityResult.FixityCheckSummary.AttemptNumber += 1

		// Tell Pharos we're on it
		fixityResult.WorkItem.Stage = constants.StageFetch
		fixityResult.WorkItem.Status = constants.StatusStarted
		fixityResult.WorkItem.Note = "Retrieving file for fixity check"
		checker.saveWorkItem(fixityResult)

		// Here's where we do the actual digest calculation.
		checker.getFixityValueOfS3File(fixityResult)
		fixityResult.FixityCheckSummary.Finish()
		if fixityResult.FixityCheckSummary.HasErrors() {
			checker.PostProcessChannel <- fixityResult
		} else {
			checker.RecordChannel <- fixityResult
		}
	}
}

func (checker *APTFixityChecker) record() {
	for fixityResult := range checker.RecordChannel {
		// Create PREMIS event saying whether fixity event
		// succeeded or failed.
		checker.PostProcessChannel <- fixityResult
	}
}

func (checker *APTFixityChecker) postProcess() {
	// for fixityResult := range checker.PostProcessChannel {
	// Update WorkItem
	// No need to save WorkItemState
	// Finish or requeue NSQ
	// }
}

// getFixityValueOfS3File calculates the sha256 digest of an S3 file.
// The downloader streams the file from S3 to /dev/null, because
// we don't need to have the file on disk. We can calculate the
// digest from the stream. We get the file from S3/Virginia, not
// Glacier/Oregon! When this is done, the fixity value will be in
// fixityResult.Sha256.
func (checker *APTFixityChecker) getFixityValueOfS3File(fixityResult *models.FixityResult) {
	bucket, key, err := fixityResult.BucketAndKey()
	if err != nil {
		fixityResult.FixityCheckSummary.AddError(err.Error())
		return
	}
	downloader := network.NewS3Download(
		constants.AWSVirginia,
		bucket,      // should be S3 preservation bucket
		key,         // s3 key to fetch
		"/dev/null", // local path at which to save the s3 file
		false,       // don't calculate md5 digest
		true)        // do calculate sha256 digest
	downloader.Fetch()
	if downloader.ErrorMessage != "" {
		fixityResult.FixityCheckSummary.AddError(
			"Error fetching file %s (%s/%s) from S3: %s",
			fixityResult.GenericFile.Identifier, bucket, key,
			downloader.ErrorMessage)
		return
	}
	fixityResult.S3FileExists = true
	fixityResult.Sha256 = downloader.Sha256Digest
}

func (checker *APTFixityChecker) saveWorkItem(fixityResult *models.FixityResult) {
	resp := checker.Context.PharosClient.WorkItemSave(fixityResult.WorkItem)
	if resp.Error != nil {
		checker.Context.MessageLog.Warning(
			"Error marking WorkItem %d as %s/%s for object %s: %v",
			fixityResult.WorkItem.Id,
			fixityResult.WorkItem.Stage,
			fixityResult.WorkItem.Status,
			fixityResult.WorkItem.ObjectIdentifier,
			resp.Error)
	}
}

func (checker *APTFixityChecker) buildFixityResult(message *nsq.Message) *models.FixityResult {
	fixityResult := models.NewFixityResult(message)
	workItem, err := GetWorkItem(message, checker.Context)
	if err != nil {
		fixityResult.FixityCheckSummary.AddError("Can't get WorkItem: %v", err)
		return fixityResult
	}
	if workItem == nil {
		fixityResult.FixityCheckSummary.AddError("Pharos returned nil WorkItem")
		return fixityResult
	}
	fixityResult.WorkItem = workItem
	if workItem.GenericFileIdentifier == "" {
		fixityResult.FixityCheckSummary.AddError("WorkItem Id %d (%s) has no generic file identifier",
			workItem.Id, workItem.ObjectIdentifier)
		return fixityResult
	}
	// ---------------------------------------------------------------------
	// TODO: Get generic file *** WITH CHECKSUMS ***
	// ---------------------------------------------------------------------
	resp := checker.Context.PharosClient.GenericFileGet(workItem.GenericFileIdentifier)
	if resp.Error != nil {
		fixityResult.FixityCheckSummary.AddError("Can't get generic file %s from Pharos: %v",
			resp.Error.Error())
		return fixityResult
	}
	fixityResult.GenericFile = resp.GenericFile()
	if fixityResult.GenericFile.URI == "" {
		fixityResult.FixityCheckSummary.AddError("GenericFile %s has no S3 URI.",
			fixityResult.GenericFile.Identifier)
		return fixityResult
	}
	return fixityResult
}
