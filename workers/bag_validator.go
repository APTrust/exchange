package workers

import (
	"github.com/APTrust/exchange/config"
	"github.com/APTrust/exchange/models"
)

type BagValidator struct {
	PathToUntarredBag    string
	BagValidationConfig  *config.BagValidationConfig
	workSummary          *models.WorkSummary
	intelObj             *models.IntellectualObject
}

// NewBagValidator creates a new BagValidator. Param pathToUntarredBag
// should be an absolute path the untarred bag. Param bagValidationConfig
// defines what we need to validate, in addition to the checksums in the
// manifests.
//
// Param intelObj should be either an IntellectualObject, complete with
// a list of GenericFiles, checksums, and tags, or nil. When tarfile.Reader
// unpacks a tarred bag, it produces an IntellectualObject suitable for
// use here. If the IntellectualObject is nil, this validator will
// calculate the checksums necessary to validate the bag, and will parse
// tag files as necessary.
func NewBagValidator(pathToUntarredBag string, bagValidationConfig *config.BagValidationConfig, intelObj *models.IntellectualObject) (*BagValidator) {
	return &BagValidator{
		PathToUntarredBag: pathToUntarredBag,
		BagValidationConfig: bagValidationConfig,
		intelObj: intelObj,
	}
}

// Validate validates the bag and returns a WorkSummary.
// Param files is an optional
func (validator *BagValidator) Validate() (*models.WorkSummary) {
	validator.workSummary = models.NewWorkSummary()
	validator.workSummary.Start()
	validator.verifyFileSpecs()
	validator.verifyTagSpecs()
	validator.verifyChecksums()
	validator.workSummary.Finish()
	return validator.workSummary
}

func (validator *BagValidator) verifyFileSpecs() {

}

func (validator *BagValidator) verifyTagSpecs() {

}

func (validator *BagValidator) verifyChecksums() {

}
