package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/nsqio/go-nsq"
	"strings"
	//	"time"
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
	fixityResult, err := checker.buildFixityResult(message)
	if err != nil {
		checker.Context.MessageLog.Error(err.Error())
		return err
	}
	// Check syncmap to see if this item is already in process.

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
		err := checker.getFixityValueOfS3File(fixityResult)
		if err != nil {
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
// fixityResult.Sha256. Returns error if fixity check could not be completed.
func (checker *APTFixityChecker) getFixityValueOfS3File(fixityResult *models.FixityResult) error {
	bucket, key, err := fixityResult.BucketAndKey()
	if err != nil {
		return err
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
		return fmt.Errorf("Error fetching file %s (%s/%s) from S3: %s",
			fixityResult.GenericFile.Identifier, bucket, key,
			downloader.ErrorMessage)
	}
	fixityResult.S3FileExists = true
	fixityResult.Sha256 = downloader.Sha256Digest
	return nil
}

func (checker *APTFixityChecker) buildFixityResult(message *nsq.Message) (*models.FixityResult, error) {
	var err error
	fixityResult := models.NewFixityResult(message)
	gfIdentifier := strings.TrimSpace(string(message.Body))
	// Get GenericFile with checksums (param includeRelations = true)
	resp := checker.Context.PharosClient.GenericFileGet(gfIdentifier, true)
	if resp.Error != nil {
		err = fmt.Errorf("Can't get generic file %s from Pharos: %v", resp.Error.Error())
		return fixityResult, err
	}
	fixityResult.GenericFile = resp.GenericFile()
	if fixityResult.GenericFile.URI == "" {
		err = fmt.Errorf("GenericFile %s has no S3 URI.", fixityResult.GenericFile.Identifier)
	}
	return fixityResult, err
}
