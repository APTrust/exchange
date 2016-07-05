package validation

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"strings"
	"time"
)

type ValidationResult struct {
	ParseSummary         *models.WorkSummary
	ValidationSummary    *models.WorkSummary
	IntellectualObject   *models.IntellectualObject
}

func (result *ValidationResult) HasErrors() (bool) {
	return result.ParseSummary.HasErrors() ||
		result.ValidationSummary.HasErrors() ||
		result.IntellectualObject.IngestErrorMessage != ""
}

type BagValidator struct {
	PathToBag            string
	BagValidationConfig  *BagValidationConfig
	virtualBag           *models.VirtualBag
}

// NewBagValidator creates a new BagValidator. Param pathToBag
// should be an absolute path to either the tarred bag (.tar file)
// or to the untarred bag (a directory). Param bagValidationConfig
// defines what we need to validate, in addition to the checksums in the
// manifests.
func NewBagValidator(pathToBag string, bagValidationConfig *BagValidationConfig) (*BagValidator, error) {
	if !fileutil.FileExists(pathToBag) {
		return nil, fmt.Errorf("Bag does not exist at %s", pathToBag)
	}
	if bagValidationConfig == nil {
		return nil, fmt.Errorf("Param bagValidationConfig cannot be nil")
	}
	configErrors := bagValidationConfig.ValidateConfig()
	if len(configErrors) > 0 {
		errString := "BagValidationConfig has the following errors:"
		for _, e := range configErrors {
			errString += fmt.Sprintf("\n%s", e.Error())
		}
		return nil, fmt.Errorf(errString)
	}
	err := bagValidationConfig.CompileFileNameRegex()
	if err != nil {
		return nil, fmt.Errorf("Error in BagValidationConfig: %v", err)
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

// Reads and validates the bag.
func (validator *BagValidator) Validate() (*ValidationResult){
	result := &ValidationResult{
		ValidationSummary:  models.NewWorkSummary(),
	}
	result.IntellectualObject, result.ParseSummary = validator.virtualBag.Read()
	if result.IntellectualObject == nil {
		if result.ParseSummary.HasErrors() {
			result.IntellectualObject.IngestErrorMessage = result.ParseSummary.AllErrorsAsString()
		}
		return result
	}
	result.ValidationSummary.Start()
	for _, errMsg := range result.ParseSummary.Errors {
		result.ValidationSummary.AddError(errMsg)
	}
	validator.verifyManifestPresent(result)
	validator.verifyFileSpecs(result)
	validator.verifyTagSpecs(result)
	validator.verifyGenericFiles(result)
	if result.ValidationSummary.HasErrors() {
		result.IntellectualObject.IngestErrorMessage += result.ValidationSummary.AllErrorsAsString()
	}
	result.ValidationSummary.Finish()
	return result
}

func (validator *BagValidator) verifyManifestPresent(result *ValidationResult) {
	for _, gf := range result.IntellectualObject.GenericFiles {
		if gf.IngestFileType == constants.PAYLOAD_MANIFEST {
			// manifest is usually one of the first 5 files in the list.
			return
		}
	}
	result.ValidationSummary.AddError("Bag has no payload manifest (manifest-<alg>.txt)")
}

func (validator *BagValidator) verifyFileSpecs(result *ValidationResult) {
	for gfPath, fileSpec := range validator.BagValidationConfig.FileSpecs {
		gf := result.IntellectualObject.FindGenericFile(gfPath)
		if gf == nil && fileSpec.Presence == REQUIRED {
			result.ValidationSummary.AddError("Required file '%s' is missing.", gfPath)
		} else if gf != nil && fileSpec.Presence == FORBIDDEN {
			result.ValidationSummary.AddError("Bag contains forbidden file '%s'.", gfPath)
		}
	}
}

func (validator *BagValidator) verifyTagSpecs(result *ValidationResult) {
	for tagName, tagSpec := range validator.BagValidationConfig.TagSpecs {
		tags := result.IntellectualObject.FindTag(tagName)
		if tagSpec.Presence == FORBIDDEN {
			result.ValidationSummary.AddError(
				"Forbidden tag '%s' found in file '%s'.", tagName, tags[0].SourceFile)
			continue
		}
		if tagSpec.Presence == REQUIRED {
			validator.checkRequiredTag(result, tagName, tags, tagSpec)
		}
		if tags != nil && tagSpec.AllowedValues != nil && len(tagSpec.AllowedValues) > 0 {
			validator.checkAllowedTagValue(result, tagName, tags, tagSpec)
		}
	}
}

func (validator *BagValidator) verifyGenericFiles(result *ValidationResult) {
	// TODO: Make sure regex is compiled in BagValidationConfig if
	// pattern is present.
	for _, gf := range result.IntellectualObject.GenericFiles {
		// Md5 digests
		if gf.IngestManifestMd5 != "" && gf.IngestManifestMd5 != gf.IngestMd5 {
			result.ValidationSummary.AddError(
				"Bad md5 digest for '%s': manifest says '%s', file digest is '%s'",
				gf.OriginalPath(), gf.IngestManifestMd5, gf.IngestMd5)
		} else {
			gf.IngestMd5VerifiedAt = time.Now().UTC()
		}
		// Sha256 digests
		if gf.IngestManifestSha256 != "" && gf.IngestManifestSha256 != gf.IngestSha256 {
			result.ValidationSummary.AddError(
				"Bad sha256 digest for '%s': manifest says '%s', file digest is '%s'",
				gf.OriginalPath(), gf.IngestManifestSha256, gf.IngestSha256)
		} else {
			gf.IngestSha256VerifiedAt = time.Now().UTC()
		}
		// No manifest entry?
		if gf.IngestFileType == constants.PAYLOAD_FILE &&
			gf.IngestManifestMd5 == "" && gf.IngestManifestSha256 == "" {
			result.ValidationSummary.AddError(
				"File '%s' does not appear in any payload manifest (md5 or sha256)",
				gf.OriginalPath())
		}
		// Make sure name is valid
		// TODO: Verify checksums against pattern in BagValidationConfig
	}
}

func (validator *BagValidator) checkRequiredTag(result *ValidationResult, tagName string, tags []*models.Tag, tagSpec TagSpec) {
	if tags == nil {
		result.ValidationSummary.AddError("Required tag '%s' is missing.", tagName)
		return
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
			result.ValidationSummary.AddError("Value for tag '%s' is missing.", tagName)
		}
	}
}

func (validator *BagValidator) checkAllowedTagValue(result *ValidationResult, tagName string, tags []*models.Tag, tagSpec TagSpec) {
	valueOk := false
	lastValue := ""
	for _, value := range tagSpec.AllowedValues {
		for _, tag := range tags {
			lcValue := strings.TrimSpace(strings.ToLower(value))
			tagValue := strings.TrimSpace(strings.ToLower(tag.Value))
			lastValue = tagValue
			if lcValue == tagValue {
				valueOk = true
			}
		}
	}
	if !valueOk {
		result.ValidationSummary.AddError("Tag '%s' has illegal value '%s'.", tagName, lastValue)
	}
}
