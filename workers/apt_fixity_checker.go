package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/nsqio/go-nsq"
	"strings"
	"time"
)

// APTFixityChecker performs ongoing fixity checks on files stored in S3.
type APTFixityChecker struct {
	Context            *context.Context
	FixityChannel      chan *models.FixityResult
	RecordChannel      chan *models.FixityResult
	PostProcessChannel chan *models.FixityResult
	ItemsInProcess     *models.SynchronizedMap
}

func NewAPTFixityChecker(_context *context.Context) *APTFixityChecker {
	checker := &APTFixityChecker{
		Context:        _context,
		ItemsInProcess: models.NewSynchronizedMap(),
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
	if fixityResult.Error != nil {
		checker.Context.MessageLog.Error("Cannot process %s: %v",
			string(message.Body), fixityResult.Error.Error())
		message.Finish()
		return nil // Should we return an error to NSQ?
	}
	// Check syncmap to see if this item is already in process.
	startedAt := checker.ItemsInProcess.Get(fixityResult.GenericFile.Identifier)
	if startedAt != "" {
		checker.Context.MessageLog.Info("Skipping %s: already in process as of %s.",
			fixityResult.GenericFile.Identifier, startedAt)
		message.Finish()
		return nil
	}

	// Note that we're working on this.
	checker.ItemsInProcess.Add(fixityResult.GenericFile.Identifier, time.Now().UTC().Format(time.RFC3339))
	checker.Context.MessageLog.Info("Added %s to items in process", fixityResult.GenericFile.Identifier)

	// We'll ping NSQ manually when we need to.
	message.DisableAutoResponse()
	checker.Context.MessageLog.Info("Putting %s into fixity channel",
		fixityResult.GenericFile.Identifier)

	checker.FixityChannel <- fixityResult
	return nil
}

func (checker *APTFixityChecker) checkFixity() {
	for fixityResult := range checker.FixityChannel {
		// Here's where we do the actual digest calculation.
		checker.getFixityValueOfS3File(fixityResult)
		if fixityResult.Error != nil {
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
		event, err := models.NewEventGenericFileFixityCheck(
			time.Now().UTC(),
			constants.AlgSha256,
			fixityResult.Sha256,
			fixityResult.Sha256 == fixityResult.PharosSha256())
		if err != nil {
			fixityResult.Error = fmt.Errorf("Could not create Premis Event for %s: %v",
				fixityResult.GenericFile.Identifier, err)
		} else {
			resp := checker.Context.PharosClient.PremisEventSave(event)
			if resp.Error != nil {
				fixityResult.Error = fmt.Errorf("After completing fixity check for %s, "+
					"could not save PremisEvent to Pharos: %v. Event data: %v",
					fixityResult.GenericFile.Identifier, resp.Error, event)
			} else {
				checker.Context.MessageLog.Info("Completing fixity check for %s, "+
					"and saved PremisEvent %s to Pharos",
					fixityResult.GenericFile.Identifier, event.Identifier)
			}
		}
		checker.PostProcessChannel <- fixityResult
	}
}

func (checker *APTFixityChecker) postProcess() {
	for fixityResult := range checker.PostProcessChannel {
		// Finish or requeue NSQ
		if fixityResult.Error != nil {
			if fixityResult.ErrorIsFatal {
				checker.Context.MessageLog.Error("%s (FATAL)", fixityResult.Error.Error())
				fixityResult.NSQMessage.Finish()
			} else {
				checker.Context.MessageLog.Error("%s (transient)", fixityResult.Error.Error())
				fixityResult.NSQMessage.Requeue(1 * time.Minute)
			}
		} else {
			if fixityResult.PharosSha256() == fixityResult.Sha256 {
				checker.Context.MessageLog.Info("Fixity check complete for %s. Fixity %s matches.",
					fixityResult.GenericFile.Identifier, fixityResult.Sha256)
			} else {
				checker.Context.MessageLog.Warning("Fixity check complete for %s. S3 fixity %s "+
					"DOES NOT MATCH PHAROS FIXITY %s",
					fixityResult.GenericFile.Identifier, fixityResult.Sha256,
					fixityResult.PharosSha256())
			}
			fixityResult.NSQMessage.Finish()
		}
		checker.ItemsInProcess.Delete(fixityResult.GenericFile.Identifier)
		checker.Context.MessageLog.Info("Removed %s from items in process", fixityResult.GenericFile.Identifier)
	}
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
		fixityResult.Error = fmt.Errorf("Can't get S3 bucket and key names for %s: %v",
			fixityResult.GenericFile.Identifier, err)
		fixityResult.ErrorIsFatal = true
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
		fixityResult.Error = fmt.Errorf("Error fetching file %s (%s/%s) from S3: %s",
			fixityResult.GenericFile.Identifier, bucket, key,
			downloader.ErrorMessage)
		if strings.Contains(downloader.ErrorMessage, "NoSuchKey") {
			fixityResult.ErrorIsFatal = true
		}
		return
	}
	fixityResult.S3FileExists = true
	fixityResult.Sha256 = downloader.Sha256Digest
	return
}

func (checker *APTFixityChecker) buildFixityResult(message *nsq.Message) *models.FixityResult {
	fixityResult := models.NewFixityResult(message)
	gfIdentifier := strings.TrimSpace(string(message.Body))
	// Get GenericFile with checksums (param includeRelations = true)
	resp := checker.Context.PharosClient.GenericFileGet(gfIdentifier, true)
	if resp.Error != nil {
		fixityResult.Error = fmt.Errorf("Can't get generic file %s from Pharos: %v", resp.Error.Error())
		if resp.Response.StatusCode == 404 {
			fixityResult.ErrorIsFatal = true
		}
		return fixityResult
	}
	fixityResult.GenericFile = resp.GenericFile()
	if fixityResult.GenericFile.URI == "" {
		fixityResult.Error = fmt.Errorf("GenericFile %s has no S3 URI.", fixityResult.GenericFile.Identifier)
		fixityResult.ErrorIsFatal = true
	}
	return fixityResult
}
