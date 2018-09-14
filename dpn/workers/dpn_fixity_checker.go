package workers

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/util/storage"
	"github.com/APTrust/exchange/validation"
	"github.com/nsqio/go-nsq"
	"github.com/satori/go.uuid"
	"time"
)

type DPNFixityChecker struct {
	Context             *context.Context
	LocalDPNRestClient  *network.DPNRestClient
	ValidationChannel   chan *DPNRestoreHelper
	RecordChannel       chan *DPNRestoreHelper
	CleanupChannel      chan *DPNRestoreHelper
	PostTestChannel     chan *DPNRestoreHelper
	BagValidationConfig *validation.BagValidationConfig
}

// NewDPNFixityChecker creates a new DPNFixityChecker.
func NewDPNFixityChecker(_context *context.Context) (*DPNFixityChecker, error) {
	localClient, err := network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.RestClient.LocalAPIRoot,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	if err != nil {
		return nil, fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	checker := &DPNFixityChecker{
		Context:            _context,
		LocalDPNRestClient: localClient,
	}
	// LoadDPNBagValidationConfig is defined in dpn/workers/common.go
	checker.BagValidationConfig = LoadDPNBagValidationConfig(checker.Context)
	workerBufferSize := _context.Config.DPN.DPNFixityWorker.Workers * 4
	checker.ValidationChannel = make(chan *DPNRestoreHelper, workerBufferSize)
	checker.RecordChannel = make(chan *DPNRestoreHelper, workerBufferSize)
	checker.CleanupChannel = make(chan *DPNRestoreHelper, workerBufferSize)
	checker.PostTestChannel = make(chan *DPNRestoreHelper, workerBufferSize)
	for i := 0; i < _context.Config.DPN.DPNPackageWorker.Workers; i++ {
		go checker.validate()
		go checker.record()
		go checker.cleanup()
	}
	return checker, nil
}

// HandleMessage is the NSQ message handler. The NSQ consumer will pass each
// message in the subscribed channel to this function.
func (checker *DPNFixityChecker) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()

	helper, err := NewDPNRestoreHelper(message, checker.Context,
		checker.LocalDPNRestClient, constants.ActionFixityCheck,
		"ValidationSummary")
	if err != nil {
		checker.Context.MessageLog.Error(err.Error())
		return err
	}
	helper.WorkSummary.ClearErrors()
	helper.WorkSummary.Start()
	helper.Manifest.DPNWorkItem.Status = constants.StatusStarted
	helper.Manifest.DPNWorkItem.Stage = constants.StageValidate
	helper.SaveDPNWorkItem()

	if helper.WorkSummary.HasErrors() {
		checker.Context.MessageLog.Error("Error setting up manifest for WorkItem %s: %s",
			string(message.Body), helper.WorkSummary.AllErrorsAsString())
		// No use proceeding...
		checker.CleanupChannel <- helper
		return fmt.Errorf(helper.WorkSummary.AllErrorsAsString())
	}
	if helper.Manifest.DPNWorkItem.IsCompletedOrCancelled() {
		checker.Context.MessageLog.Info("Skipping WorkItem %d because status is %s",
			helper.Manifest.DPNWorkItem.Id, helper.Manifest.DPNWorkItem.Status)
		checker.CleanupChannel <- helper
		return nil
	}

	if helper.Manifest.ExpectedFixityValue == "" {
		helper.WorkSummary.AddError("ExpectedFixityValue for bag %s "+
			"is missing from manifest. Cannot validate.", helper.Manifest.DPNBag.UUID)
		helper.WorkSummary.ErrorIsFatal = true
		checker.CleanupChannel <- helper
		return nil
	}

	checker.ValidationChannel <- helper
	return nil
}

func (checker *DPNFixityChecker) validate() {
	for helper := range checker.ValidationChannel {
		checker.Context.MessageLog.Info("Validating %s", helper.Manifest.LocalPath)
		// Validation can take long time.
		// Ping NSQ immediately before and after,
		// so we don't time out.
		helper.Manifest.NsqMessage.Touch()
		checker.ValidateBag(helper)
		helper.Manifest.NsqMessage.Touch()

		if helper.WorkSummary.HasErrors() {
			checker.CleanupChannel <- helper
		} else {
			checker.RecordChannel <- helper
		}
	}
}

// DPN fixity check requires us to validate the entire bag and
// extract the sha256 checksum of tagmanifest-sha256.txt file.
// If the bag is valid, and the fixity value of the tag manifest
// matches what's in the DPN registry, the fixity check passes.
// DPN currently has no way of recording that a fixity check has
// failed, other than a human looking at the records. We can record
// the DPNWorkItem as failed in Pharos.
func (checker *DPNFixityChecker) ValidateBag(helper *DPNRestoreHelper) {
	validator, err := validation.NewValidator(helper.Manifest.LocalPath,
		checker.BagValidationConfig, false)
	if err != nil {
		helper.WorkSummary.AddError(err.Error())
		helper.WorkSummary.ErrorIsFatal = true
	} else {
		// Validation can take a long time for large bags.
		summary, err := validator.Validate()
		if err != nil {
			helper.WorkSummary.AddError(err.Error())
		} else {
			checker.Context.MessageLog.Info("Finished validating %s",
				helper.Manifest.LocalPath)
			helper.WorkSummary = summary
			checker.getTagManifestChecksum(helper, validator)
		}
	}
}

// The validator records the results of its work in a BoltDB
// file because we often get bags with over 100,000 files.
// We can extract the tagmanifest-sha256.txt checksum from the BoltDB.
func (checker *DPNFixityChecker) getTagManifestChecksum(helper *DPNRestoreHelper, validator *validation.Validator) {
	fileIdentifier := fmt.Sprintf("%s/tagmanifest-sha256.txt", helper.Manifest.DPNBag.UUID)
	checker.Context.MessageLog.Info("Looking up %s in BoltDB %s",
		fileIdentifier, validator.DBName())
	db, err := storage.NewBoltDB(validator.DBName())
	if err != nil {
		helper.WorkSummary.AddError("Error opening BoltDB: %v", err)
		return
	}
	defer db.Close()
	gf, err := db.GetGenericFile(fileIdentifier)
	if err != nil {
		helper.WorkSummary.AddError("Error finding file %s in BoltDB: %v", fileIdentifier, err)
		return
	}
	// Record on the manifest the actual sha256 digest that the validator
	// just calculated for the tagmanifest-sha256.txt file.
	helper.Manifest.ActualFixityValue = gf.IngestSha256
	checker.Context.MessageLog.Info("Validator calculated checksum %s for file %s",
		gf.IngestSha256, fileIdentifier)

	// TODO: Delete BoltDB
}

// Record this fixity check in our local DPN REST server
func (checker *DPNFixityChecker) record() {
	for helper := range checker.RecordChannel {

		checker.CleanupChannel <- helper
	}
}

// Update NSQ and Pharos on the status of this task
func (checker *DPNFixityChecker) cleanup() {
	for helper := range checker.CleanupChannel {
		helper.WorkSummary.Finish()
		if helper.WorkSummary.HasErrors() {
			checker.FinishWithError(helper)
		} else {
			checker.FinishWithSuccess(helper)
		}
		// For testing only. The test code creates the PostTestChannel.
		// When running in demo & production, this channel is nil.
		if checker.PostTestChannel != nil {
			checker.PostTestChannel <- helper
		}
	}
}

// Save the fixity record to the local DPN REST server.
func (checker *DPNFixityChecker) SaveFixityRecord(helper *DPNRestoreHelper) {
	// Create a FixityCheck record and save it with
	// checker.LocalDPNRestClient.FixityCheckCreate()
	if helper.Manifest.ExpectedFixityValue == "" {
		helper.WorkSummary.AddError("Cannot create DPN FixityCheck record because " +
			"because ExpectedFixityValue is missing from manifest.")
		return
	}
	if helper.Manifest.ActualFixityValue == "" {
		helper.WorkSummary.AddError("Cannot create DPN FixityCheck record because " +
			"because ActualFixityValue is missing from manifest.")
		return
	}
	if helper.Manifest.FixityCheck == nil {
		helper.Manifest.FixityCheck = &models.FixityCheck{
			FixityCheckId: uuid.NewV4().String(),
			Bag:           helper.Manifest.DPNBag.UUID,
			Node:          checker.Context.Config.DPN.LocalNode,
			Success:       helper.Manifest.ExpectedFixityValue == helper.Manifest.ActualFixityValue,
			FixityAt:      time.Now().UTC(),
		}
	}
	resp := checker.LocalDPNRestClient.FixityCheckCreate(helper.Manifest.FixityCheck)
	if resp.Error != nil {
		helper.WorkSummary.AddError("Error saving FixityCheck to DPN REST server: %v",
			resp.Error)
		return
	}
	helper.Manifest.FixityCheck.CreatedAt = resp.FixityCheck().CreatedAt
	helper.Manifest.FixityCheckSavedAt = time.Now().UTC()
	if helper.Manifest.CheckCompletedAndFailed() {
		helper.WorkSummary.AddError(
			"Fixity check completed, and fixity record %s was saved to DPN. "+
				"Actual fixity value %s does not match expected fixity %s",
			helper.Manifest.FixityCheck.FixityCheckId,
			helper.Manifest.ActualFixityValue, helper.Manifest.ExpectedFixityValue)
		helper.WorkSummary.ErrorIsFatal = true
	}
}

func (checker *DPNFixityChecker) FinishWithSuccess(helper *DPNRestoreHelper) {
	helper.Manifest.DPNWorkItem.ClearNodeAndPid()
	note := fmt.Sprintf("Fixity check complete. Saved to DPN with FixityCheckId %s",
		helper.Manifest.FixityCheck.FixityCheckId)
	helper.Manifest.DPNWorkItem.Note = &note
	// TODO: To repurpose this code to support restoration,
	// branch here. If DPNWorkItem.Task is fixity check,
	// set Status to Success. If Task is restore, set Stage
	// to StageRestoring, status to StatusPending, and push the
	// bag into the restoration queue, or do whatever else is
	// necessary to complete the restore process.
	helper.Manifest.DPNWorkItem.Stage = constants.StageValidate
	helper.Manifest.DPNWorkItem.Status = constants.StatusSuccess
	helper.SaveDPNWorkItem()
	helper.Manifest.NsqMessage.Finish()
}

func (checker *DPNFixityChecker) FinishWithError(helper *DPNRestoreHelper) {
	helper.Manifest.DPNWorkItem.ClearNodeAndPid()
	// Copy errors into the DPNWorkItem note, so we can see them in
	// the Pharos UI.
	errors := helper.WorkSummary.AllErrorsAsString()
	helper.Manifest.DPNWorkItem.Note = &errors
	checker.Context.MessageLog.Error(errors)
	if helper.WorkSummary.ErrorIsFatal {
		// Mark the DPNWorkItem as failed
		checker.Context.MessageLog.Error("Error for %s is fatal. Not requeueing.",
			helper.Manifest.DPNWorkItem.Identifier)
		helper.Manifest.DPNWorkItem.Status = constants.StatusFailed
		helper.Manifest.DPNWorkItem.Retry = false
		helper.SaveDPNWorkItem()
		helper.Manifest.NsqMessage.Finish()
	} else {
		// Transient errors. Retry DPNWorkItem.
		// MINUTES_BETWEEN_RETRIES is defined in dpn_s3_retriever.go
		helper.Manifest.DPNWorkItem.Retry = true
		helper.SaveDPNWorkItem()
		helper.Manifest.NsqMessage.Requeue(MINUTES_BETWEEN_RETRIES * time.Minute)
	}
}
