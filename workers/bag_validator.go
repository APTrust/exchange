package workers

import (
	"fmt"
	"github.com/APTrust/exchange/config"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
)

type BagValidator struct {
	Context              *context.Context
	PathToBag            string
	BagValidationConfig  *config.BagValidationConfig
	virtualBag           *models.VirtualBag
	validationSummary    *models.WorkSummary
	intelObj             *models.IntellectualObject
}

// NewBagValidator creates a new BagValidator. Param pathToBag
// should be an absolute path to either the tarred bag (.tar file)
// or to the untarred bag (a directory). Param bagValidationConfig
// defines what we need to validate, in addition to the checksums in the
// manifests.
func NewBagValidator(_context *context.Context, pathToBag string, bagValidationConfig *config.BagValidationConfig) (*BagValidator, error) {
	if _context == nil {
		return nil, fmt.Errorf("Context cannot be nil")
	}
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
		Context: _context,
		PathToBag: pathToBag,
		BagValidationConfig: bagValidationConfig,
	    virtualBag: models.NewVirtualBag(pathToBag, tagFilesToParse, calculateMd5, calculateSha256),
	}
	return bagValidator, nil
}

// Reads the bag, produces a list of generic files, parses tags,
// and calculates checksums, producing an IntellectualObject.
// Call Validator.Validate() after this if you want to validate
// the bag. Returns a WorkSummary describing any issues with the
// read operation, which may fail if the directory can't be read,
// if the tar file is corrupt, etc.
func (validator *BagValidator) ReadBag() (*models.WorkSummary) {
	var vbagSummary *models.WorkSummary
	validator.intelObj, vbagSummary = validator.virtualBag.Read()
	return vbagSummary
}

// Validates the bag and returns a WorkSummary.
// You must call Validator.Read() before calling this, since
// the read operation reads the bag and sets up the IntellectualObject.
// This returns a WorkSummary describing any validation problems.
func (validator *BagValidator) Validate() (*models.WorkSummary) {
	validator.validationSummary = models.NewWorkSummary()
	validator.validationSummary.Start()
	if validator.intelObj == nil {
		validator.validationSummary.AddError("IntellectualObject is nil; cannot validate.")
	} else {
		validator.verifyFileSpecs()
		validator.verifyTagSpecs()
		validator.verifyChecksums()
	}
	validator.validationSummary.Finish()
	return validator.validationSummary
}

func (validator *BagValidator) verifyFileSpecs() {
	for gfPath, fileSpec := range validator.BagValidationConfig.FileSpecs {
		gf := validator.intelObj.FindGenericFile(gfPath)
		if gf == nil && fileSpec.Presence == config.REQUIRED {
			validator.validationSummary.AddError("Required file '%s' is missing.", gfPath)
		} else if gf != nil && fileSpec.Presence == config.FORBIDDEN {
			validator.validationSummary.AddError("Bag contains forbidden file '%s'.", gfPath)
		}
	}
}

func (validator *BagValidator) verifyTagSpecs() {
	for tagName, tagSpec := range validator.BagValidationConfig.TagSpecs {
		tags := obj.FindTag(tagName)
		if tagSpec.Presence == config.FORBIDDEN {
			validator.validationSummary.AddError("Forbidden tag '%s' found in file '%s'.", tagName, tag.FilePath)
			continue
		}
		if tagSpec.Presence == config.REQUIRED {
			validator.checkRequiredTag(tagName, tags, tagSpec)
		}
		if tag != nil && tagSpec.AllowedValues != nil && len(tagSpec.AllowedValues) > 0 {
			validator.checkAllowedValue(tagName, tags, tagSpec)
		}
	}
}

func (validator *BagValidator) checkRequiredTag(tagName string, tags []*models.Tag, tagSpec *models.TagSpec) {
	if tags == nil {
		validator.validationSummary.AddError("Required tag '%s' is missing.", tagName)
		continue
	}
	if !tagSpec.EmptyOK {
		tagHasValue := false
		for _, tag := range tags {
			if tag.Value != "" {
				tagHasValue = true
				break
			}
		}
		if !tagHasValue {
			validator.validationSummary.AddError("Value for tag '%s' is missing.", tagName)
		}
	}
}

func (validator *BagValidator) checkAllowedTagValue(tagName string, tags []*models.Tag, tagSpec *models.TagSpec) {
	for _, value := range tagSpec.AllowedValues {
		for _, tag := range tags {
			lcValue := strings.TrimSpace(strings.ToLower(value))
			tagValue := strings.TrimSpace(strings.ToLower(tag.Value))
			if lcValue != tagValue {
				validator.validationSummary.AddError("Tag '%s' has illegal value '%s'.",
					tagName, tag.Value)
			}
		}
	}
}

func (validator *BagValidator) verifyChecksums() {
	// TODO: START HERE
}
