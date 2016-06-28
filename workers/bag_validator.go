package workers

import (
	"fmt"
	"github.com/APTrust/exchange/config"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
)

type BagValidator struct {
	PathToBag            string
	BagValidationConfig  *config.BagValidationConfig
	workSummary          *models.WorkSummary
	virtualBag           *models.VirtualBag
}

// NewBagValidator creates a new BagValidator. Param pathToBag
// should be an absolute path to either the tarred bag (.tar file)
// or to the untarred bag (a directory). Param bagValidationConfig
// defines what we need to validate, in addition to the checksums in the
// manifests.
func NewBagValidator(pathToBag string, bagValidationConfig *config.BagValidationConfig) (*BagValidator, error) {
	if !fileutil.FileExists(pathToBag) {
		return nil, fmt.Errorf("Bag does not exist at %s", pathToBag)
	}
	if bagValidationConfig == nil {
		return nil, fmt.Errorf("Param bagValidationConfig cannot be nil")
	}
	calculateMd5 := util.StringListContains(bagValidationConfig.FixityAlgorithms, constants.AlgMd5)
	calculateSha256 := util.StringListContains(bagValidationConfig.FixityAlgorithms, constants.AlgSha256)
	tagFilesToParse := make([]string, 0)
	for pathToFile, filespec := range bagValidationConfig.FileSpecs {
		if filespec.ParseAsTagFile {
			tagFilesToParse = append(tagFilesToParse, pathToFile)
		}
	}
	bagValidator := &BagValidator{
		PathToBag: pathToBag,
		BagValidationConfig: bagValidationConfig,
	    virtualBag: models.NewVirtualBag(pathToBag, tagFilesToParse, calculateMd5, calculateSha256),
	}
	return bagValidator, nil
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
