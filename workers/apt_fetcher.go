package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"os"
	"strings"
	"time"
)

// Fetches bags (tar files) from S3 receiving buckets and validates them.
type APTFetcher struct {
	Context             *context.Context
	BagValidationConfig *validation.BagValidationConfig
	FetchChannel        chan *models.IngestState
	ValidationChannel   chan *models.IngestState
	CleanupChannel      chan *models.IngestState
	RecordChannel       chan *models.IngestState
}

func NewAPTFetcher(_context *context.Context) *APTFetcher {
	fetcher := &APTFetcher{
		Context: _context,
	}

	// Load the config settings that describe how to validate
	// APTrust bags. We'll exit here if the config can't be
	// loaded or is invalid.
	fetcher.BagValidationConfig = LoadAPTrustBagValidationConfig(_context)

	// Set up buffered channels
	fetcherBufferSize := _context.Config.FetchWorker.NetworkConnections * 4
	workerBufferSize := _context.Config.FetchWorker.Workers * 10
	fetcher.FetchChannel = make(chan *models.IngestState, fetcherBufferSize)
	fetcher.ValidationChannel = make(chan *models.IngestState, workerBufferSize)
	fetcher.RecordChannel = make(chan *models.IngestState, workerBufferSize)
	fetcher.CleanupChannel = make(chan *models.IngestState, workerBufferSize)
	// Set up a limited number of go routines
	for i := 0; i < _context.Config.FetchWorker.NetworkConnections; i++ {
		go fetcher.fetch()
	}
	for i := 0; i < _context.Config.FetchWorker.Workers; i++ {
		go fetcher.validate()
		go fetcher.cleanup()
		go fetcher.record()
	}
	return fetcher
}

// This is the callback that NSQ workers use to handle messages from NSQ.
func (fetcher *APTFetcher) HandleMessage(message *nsq.Message) error {
	// Set up our IngestState. Most of this comes from Pharos;
	// some of it we have to build fresh.
	ingestState, err := GetIngestState(message, fetcher.Context, true)
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return err
	}
	ingestState.NSQMessage = message

	// If this item was queued more than once, and this process or any
	// other is currently working on it, just finish the message and
	// assume that the in-progress worker will take care of the original.
	if ingestState.WorkItem.Node != "" && ingestState.WorkItem.Pid != 0 {
		fetcher.Context.MessageLog.Info("Marking WorkItem %d (%s/%s) as finished "+
			"without doing any work, because this item is currently in process by "+
			"node %s, pid %d. WorkItem was last updated at %s.",
			ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
			ingestState.WorkItem.Name, ingestState.WorkItem.Node,
			ingestState.WorkItem.Pid, ingestState.WorkItem.UpdatedAt)
		message.Finish()
		return nil
	}

	stage := ingestState.WorkItem.Stage
	if stage != constants.StageReceive && stage != constants.StageFetch &&
		stage != constants.StageUnpack && stage != constants.StageValidate {
		fetcher.Context.MessageLog.Info("Marking WorkItem %d (%s/%s) as finished "+
			"without doing any work, because this item is already past the "+
			"ingest phase.",
			ingestState.WorkItem.Id, ingestState.WorkItem.Bucket,
			ingestState.WorkItem.Name)
		message.Finish()
		return nil
	}

	if fileutil.FileExists(ingestState.IngestManifest.Object.IngestTarFilePath) {
		stat, err := os.Stat(ingestState.IngestManifest.Object.IngestTarFilePath)
		if err == nil && stat != nil && stat.Size() == ingestState.WorkItem.Size {
			fetcher.Context.MessageLog.Info("Bag %s is already on disk and appears "+
				"to be complete.", ingestState.WorkItem.Name)
			if ingestState.IngestManifest.ValidateResult.Attempted == true &&
				ingestState.IngestManifest.ValidateResult.FinishedAt.IsZero() == false &&
				ingestState.IngestManifest.ValidateResult.HasErrors() == false &&
				len(ingestState.IngestManifest.Object.GenericFiles) > 1 {
				fetcher.Context.MessageLog.Info("Bag %s has already been validated. "+
					"Now it's going to the cleanup channel.",
					ingestState.WorkItem.Name)
				fetcher.CleanupChannel <- ingestState
			} else {
				fetcher.Context.MessageLog.Info("Bag %s is going to the validation channel.",
					ingestState.WorkItem.Name)
				fetcher.ValidationChannel <- ingestState
			}
			return nil
		}
		// At this point, it may be on disk, but we can't verify it's correct,
		// so download it again.
	}

	// In case we're loading a previously failed fetch attempt
	ingestState.IngestManifest.FetchResult.ClearErrors()
	ingestState.IngestManifest.UntarResult.ClearErrors()
	ingestState.IngestManifest.ValidateResult.ClearErrors()
	ingestState.IngestManifest.StoreResult.ClearErrors()
	ingestState.IngestManifest.RecordResult.ClearErrors()
	ingestState.IngestManifest.CleanupResult.ClearErrors()

	// Save the state of this item in Pharos.
	RecordWorkItemState(ingestState, fetcher.Context, ingestState.IngestManifest.FetchResult)

	// Tell Pharos that we've started to fetch this item.
	err = MarkWorkItemStarted(ingestState, fetcher.Context, constants.StageFetch,
		"Fetching bag from receiving bucket.")
	if err != nil {
		fetcher.Context.MessageLog.Error(err.Error())
		return err
	}

	// NSQ message autoresponse periodically tells the queue
	// that the message is still being processed. This doesn't
	// work for us in cases where we're fetching a file that's
	// 100GB+ in size. We need to manually Touch() NSQ periodically
	// to let the queue know that we're still actively working on
	// the message. Otherwise, NSQ thinks it timed out and sends
	// the message to a new worker.
	message.DisableAutoResponse()

	if fetcher.canSkipFetchAndValidate(ingestState) {
		fetcher.Context.MessageLog.Info("Sending %s/%s straight to record queue",
			ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key)
		fetcher.RecordChannel <- ingestState
	} else {

		// Reserve disk space to download this item, or requeue it
		// if we can't get the disk space.
		if fetcher.Context.Config.UseVolumeService && !fetcher.reserveSpaceForDownload(ingestState) {

			err = MarkWorkItemRequeued(ingestState, fetcher.Context)
			if err != nil {
				fetcher.Context.MessageLog.Error(
					"Error telling Pharos this item is being requeued: %v",
					err.Error())
			}
			message.Requeue(1 * time.Minute)
			return nil
		}

		// Start at fetch, which is the very beginning.
		// This may be the second or third attempt to ingest this bag.
		// If so, clear out old error message from previous attempts.
		ingestState.IngestManifest.FetchResult.ClearErrors()
		ingestState.IngestManifest.ValidateResult.ClearErrors()

		fetcher.Context.MessageLog.Info("Putting %s/%s straight to fetch queue",
			ingestState.IngestManifest.S3Bucket, ingestState.IngestManifest.S3Key)

		fetcher.FetchChannel <- ingestState
	}

	// Return no error, so NSQ knows we're OK.
	return nil
}

// -------------------------------------------------------------------------
// Step 1 of 4: Fetch
//
// fetch copies the file from S3 to our local staging area.
// If all goes well, the file will wind up in
// ingestState.IngestManifest.Object.IngestTarFilePath
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) fetch() {
	for ingestState := range fetcher.FetchChannel {
		// Tell NSQ we're working on this
		ingestState.TouchNSQ()

		ingestState.IngestManifest.FetchResult.Start()
		ingestState.IngestManifest.FetchResult.Attempted = true
		ingestState.IngestManifest.FetchResult.AttemptNumber += 1

		err := fetcher.downloadFile(ingestState)

		// Download may have taken 1 second or 3 hours.
		// Remind NSQ that we're still on this.
		ingestState.TouchNSQ()

		if err != nil {
			ingestState.IngestManifest.FetchResult.AddError(err.Error())
		}
		ingestState.IngestManifest.FetchResult.Finish()
		fetcher.ValidationChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 2 of 4: Validate
//
// Make sure the tar file is a valid bag.
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) validate() {
	for ingestState := range fetcher.ValidationChannel {
		// Don't time us out, NSQ!
		ingestState.TouchNSQ()

		// Tell Pharos that we've started to validate item.
		// Let's NOT quit if there's an error here. In that case, Pharos
		// might not know that we're validating, but we can still proceed.
		// Restarting the whole fetch process would be expensive.
		MarkWorkItemStarted(ingestState, fetcher.Context, constants.StageValidate,
			"Validating bag.")

		// Set up a new validator to check this bag.
		var validationResult *validation.ValidationResult
		validator, err := validation.NewBagValidator(
			ingestState.IngestManifest.Object.IngestTarFilePath,
			fetcher.BagValidationConfig)
		if err != nil {
			// Could not create a BagValidator. Should this be fatal?
			ingestState.IngestManifest.ValidateResult.AddError(err.Error())
		} else {

			// Here's where bag validation actually happens. There's a lot
			// going on in this call, which can take anywhere from 2 seconds
			// to several hours to complete, depending on the size of the bag.
			// The most time-consuming part of the validation process is
			// calculating md5 and sha256 checksums on every file in the bag.
			// If the bag is 100GB+ in size, that takes a long time.
			validationResult = validator.Validate()

			// The validator creates its own WorkSummary, complete with
			// Start/Finish timestamps, error messages and everything.
			// Just copy that into our IngestManifest.
			ingestState.IngestManifest.ValidateResult = validationResult.ValidationSummary

			// NOTE that we are OVERWRITING the IntellectualObject here
			// with the much more complete version returned by the validator,
			// but we have to reset some basic data that's only available
			// in the current context.
			ingestState.IngestManifest.Object = validationResult.IntellectualObject
			SetBasicObjectInfo(ingestState, fetcher.Context)

			// If the bag is invalid, that's a fatal error. We should not do
			// any further processing on it.
			if validationResult.HasErrors() {
				ingestState.IngestManifest.ValidateResult.ErrorIsFatal = true
				ingestState.IngestManifest.ValidateResult.Retry = false
			}
		}
		ingestState.TouchNSQ()
		fetcher.CleanupChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 3 of 4: Cleanup (conditional)
//
// cleanup deletes the tar file we just downloaded, if we determine that
// something is wrong with it and there should be no further processing.
// If the bag is valid, we leave it in the staging area. The next process
// (store) will pick it up and copy files to S3 and Glacier.
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) cleanup() {
	for ingestState := range fetcher.CleanupChannel {
		tarFile := ingestState.IngestManifest.Object.IngestTarFilePath
		hasErrors := (ingestState.IngestManifest.FetchResult.HasErrors() ||
			ingestState.IngestManifest.ValidateResult.HasErrors())
		if hasErrors && fileutil.FileExists(tarFile) {
			// Most likely bad md5 digest, but perhaps also a partial download.
			fetcher.Context.MessageLog.Info("Deleting due to download error: %s",
				tarFile)
			DeleteBagFromStaging(ingestState, fetcher.Context, ingestState.IngestManifest.FetchResult)
		}
		fetcher.RecordChannel <- ingestState
	}
}

// -------------------------------------------------------------------------
// Step 4 of 4: Record updates the WorkItem and WorkItemState in Pharos.
//
// record tells Pharos what's happened with this WorkItem,
// and it pushes the item into the next queue (validation)
// if necessary.
// -------------------------------------------------------------------------
func (fetcher *APTFetcher) record() {
	for ingestState := range fetcher.RecordChannel {

		// Copy JSON representation of the IngestManifest to Pharos
		// and to the JSON log.
		RecordWorkItemState(ingestState, fetcher.Context, ingestState.IngestManifest.FetchResult)

		// Fatal errors, or too many recurring transient errors
		attemptNumber := ingestState.IngestManifest.FetchResult.AttemptNumber
		maxAttempts := fetcher.Context.Config.FetchWorker.MaxAttempts
		itsTimeToGiveUp := (ingestState.IngestManifest.HasFatalErrors() ||
			(ingestState.IngestManifest.HasErrors() && attemptNumber >= maxAttempts))

		if itsTimeToGiveUp {
			ingestState.FinishNSQ()
			MarkWorkItemFailed(ingestState, fetcher.Context)
		} else if ingestState.IngestManifest.HasErrors() {
			ingestState.RequeueNSQ(30000)
			MarkWorkItemRequeued(ingestState, fetcher.Context)
		} else {
			ingestState.FinishNSQ()
			MarkWorkItemSucceeded(ingestState, fetcher.Context, constants.StageStore)
			PushToQueue(ingestState, fetcher.Context, fetcher.Context.Config.StoreWorker.NsqTopic)
		}
	}
}

// Make sure we have space to download this item.
func (fetcher *APTFetcher) reserveSpaceForDownload(ingestState *models.IngestState) bool {
	okToDownload := false
	err := fetcher.Context.VolumeClient.Ping(500)
	if err == nil {
		path := ingestState.IngestManifest.Object.IngestTarFilePath
		ok, err := fetcher.Context.VolumeClient.Reserve(path, uint64(ingestState.WorkItem.Size))
		if err != nil {
			fetcher.Context.MessageLog.Warning("Volume service returned an error. "+
				"Will requeue bag %s/%s because we may not have enough space to download %d bytes.",
				ingestState.WorkItem.Bucket, ingestState.WorkItem.Name, ingestState.WorkItem.Size)
		} else if ok {
			// VolumeService says we have enough space for this.
			okToDownload = ok
		}
	} else {
		fetcher.Context.MessageLog.Warning("Volume service is not running or returned an error. "+
			"Continuing as if we have enough space to download %d bytes.",
			ingestState.WorkItem.Size)
		okToDownload = true
	}
	return okToDownload
}

// Returns true if we can skip fetch and validate. We can skip those
// steps if on a previous run we validated the bag, and it's still
// there in our working directory. This anticipates the case where
// we did those steps but were not able to update the WorkItem record
// in Pharos at the end of the fetch/validate process.
func (fetcher *APTFetcher) canSkipFetchAndValidate(ingestState *models.IngestState) bool {
	return (ingestState.WorkItem.Stage == constants.StageValidate &&
		ingestState.IngestManifest.ValidateResult.Finished() &&
		!ingestState.IngestManifest.HasFatalErrors() &&
		fileutil.FileExists(ingestState.IngestManifest.Object.IngestTarFilePath))
}

// Download the file, and update the IngestManifest while we're at it.
func (fetcher *APTFetcher) downloadFile(ingestState *models.IngestState) error {
	downloader := network.NewS3Download(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		ingestState.IngestManifest.S3Bucket,
		ingestState.IngestManifest.S3Key,
		ingestState.IngestManifest.Object.IngestTarFilePath,
		true,  // calculate md5 checksum on the entire tar file
		false, // calculate sha256 checksum on the entire tar file
	)

	// It's fairly common for very large bags to fail more than
	// once on transient network errors (e.g. "Connection reset by peer")
	// So we give this several tries.
	for i := 0; i < 10; i++ {
		downloader.ErrorMessage = "" // clear before each attempt
		downloader.Fetch()
		if downloader.ErrorMessage == "" {
			fetcher.Context.MessageLog.Info("Fetched %s/%s after %d attempts",
				ingestState.IngestManifest.S3Bucket,
				ingestState.IngestManifest.S3Key,
				i+1)
			break
		} else {
			retryMessage := "will retry"
			if i >= 9 {
				retryMessage = "will not retry - too many failed attempts"
			}
			fetcher.Context.MessageLog.Warning("Error fetching %s/%s: %s - %s",
				ingestState.IngestManifest.S3Bucket,
				ingestState.IngestManifest.S3Key,
				downloader.ErrorMessage,
				retryMessage)
			if strings.Contains(downloader.ErrorMessage, "NoSuchKey") {
				ingestState.IngestManifest.FetchResult.ErrorIsFatal = true
				break
			}
		}
	}

	// Return now if we failed.
	if downloader.ErrorMessage != "" {
		return fmt.Errorf("Error fetching %s/%s: %v",
			ingestState.IngestManifest.S3Bucket,
			ingestState.IngestManifest.S3Key,
			downloader.ErrorMessage)
	}

	obj := ingestState.IngestManifest.Object
	obj.IngestSize = downloader.BytesCopied
	obj.IngestRemoteMd5 = *downloader.Response.ETag
	obj.IngestLocalMd5 = downloader.Md5Digest

	// The ETag for S3 object uploaded via single-part upload is
	// the file's md5 digest. For objects uploaded via multi-part
	// upload, the ETag is calculated differently and includes a
	// dash near the end, followed by the number of parts in the
	// multipart upload. We can't use that kind of ETag to verify
	// the md5 checksum that we calculated.
	obj.IngestMd5Verifiable = strings.Contains(downloader.Md5Digest, "-")
	if obj.IngestMd5Verifiable {
		obj.IngestMd5Verified = obj.IngestRemoteMd5 == obj.IngestLocalMd5
	}

	// If we got a bad checksum, note the error in the WorkSummary.
	if obj.IngestMd5Verifiable && !obj.IngestMd5Verified {
		ingestState.IngestManifest.FetchResult.AddError("Our md5 '%s' does not match S3 md5 '%s'",
			obj.IngestLocalMd5, obj.IngestRemoteMd5)
		ingestState.IngestManifest.FetchResult.ErrorIsFatal = true
	}

	return nil
}
