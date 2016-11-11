package validation

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"regexp"
	"strings"
)

const (
	REQUIRED  = "required"
	OPTIONAL  = "optional"
	FORBIDDEN = "forbidden"
)

var presenceValues = []string { REQUIRED, OPTIONAL, FORBIDDEN }

// FileSpec defines whether files at a specified path within
// the bag are required, optional, or forbidden.
type FileSpec struct {
	// Presence can be REQUIRED, OPTIONAL, or FORBIDDEN.
	Presence        string
	// If this is true, the file must be parsed as a BagIt
	// tag file, using the label:value format.
	ParseAsTagFile  bool
}

// Valid tells you whether this FileSpec is valid.
func (filespec *FileSpec) Valid() (bool) {
	return ValidPresenceValue(filespec.Presence)
}

// TagSpec describes rules for tags in colon-delimited BagIt-parsable
// text files.
type TagSpec struct {
	// FilePath is the path of the file within the bag.
	// This will obviously be a relative path.
	FilePath        string
	// Presence can be REQUIRED, OPTIONAL, or FORBIDDEN.
	Presence        string
	// EmptyOK indicates whether its OK for the tag value
	// to be empty.
	EmptyOK         bool
	// Describes which values are allowed (case-insensitive).
	AllowedValues   []string
}

// Valid tells you whether this TagSpec is valid.
func (tagspec *TagSpec) Valid() (bool) {
	return ValidPresenceValue(tagspec.Presence) && tagspec.FilePath != ""
}

// Returns true if value is a valid presence value.
func ValidPresenceValue(value string) (bool) {
	return util.StringListContains(presenceValues, value)
}


// BagValidationConfig lets us specify what constitutes a valid bag.
// While our validator will do standard validations, such as verifying
// checksums against manifests, this config lets us specify whether
// certain files and tags must be present for the specific BagIt spec
// we're validating against.
type BagValidationConfig struct {
	// FileSpecs is a map of FileSpec structures, describing
	// rules for specific files. The key is the relative path
	// to the file within the bag.
	// E.g. bag-info.txt or dpn_tags/dpn-info.txt.
	FileSpecs                   map[string]FileSpec
	// TagSpecs is a map of TagSpec objects. The key is the
	// tag name (e.g. Source-Organization or Internal-Sender-Description)
	// and the value is the TagSpec.
	TagSpecs                    map[string]TagSpec
	// AllowMiscTopLevelFiles describes whether a valid bag can
	// contain files not specifically defined in the config.
	AllowMiscTopLevelFiles      bool
	// AllowMiscDirectories describes whether a valid bag can
	// contain Directories other than the data directory.
	AllowMiscDirectories        bool
	// TopLevelDirMustMatchBagName describes whether a tarred bag
	// must untar to a directory whose name matches the tar file
	// name. E.g. Must my_bag.tar untar to a directory called my_tar?
	TopLevelDirMustMatchBagName bool
	// Which fixity algorithms should we calculate on tag and
	// payload files?
	FixityAlgorithms            []string
	// Regex to describe valid file and directory names.
	// This can also be set to APTRUST to use the standard APTrust
	// filename pattern defined in constants.APTrustFileNamePattern,
	// or POSIX to use POSIX file name rules.
	FileNamePattern             string
	// Regex compiled internally from FileNamePattern.
	FileNameRegex               *regexp.Regexp
}

func NewBagValidationConfig() (*BagValidationConfig) {
	return &BagValidationConfig{
		FileSpecs: make(map[string]FileSpec),
		TagSpecs: make(map[string]TagSpec),
		FixityAlgorithms: make([]string, 0),
		AllowMiscTopLevelFiles: false,
		AllowMiscDirectories: false,
		TopLevelDirMustMatchBagName: false,
	}
}

func (config *BagValidationConfig) ValidateConfig() ([]error) {
	errors := make([]error, 0)
	for _, tagSpec := range config.TagSpecs {
		if !tagSpec.Valid() {
			errors = append(errors, fmt.Errorf(
				"TagSpec for file '%s' requires non-empty FilePath and valid presence value.",
				tagSpec.FilePath))
		}
	}
	return errors
}

// Call this before testing file names in the bag. This compiles the
// filename validation regex, if the config includes a validation pattern.
// Note the two built-in patterns: constants.APTrustFileNamePattern and
// constants.PosixFileNamePattern. If you load your validation config
// from a file, LoadBagValidationConfig calls this for you.
func (config *BagValidationConfig) CompileFileNameRegex() (error) {
	var err error
	if strings.ToUpper(config.FileNamePattern) == "APTRUST" {
		config.FileNameRegex = constants.APTrustFileNamePattern
	} else if strings.ToUpper(config.FileNamePattern) == "POSIX" {
		config.FileNameRegex = constants.PosixFileNamePattern
	} else if config.FileNamePattern != "" {
		config.FileNameRegex, err = regexp.Compile(config.FileNamePattern)
		if err != nil {
			err = fmt.Errorf("Cannot compile regex for FileNamePattern '%s': %v",
				config.FileNamePattern, err)
		}
	}
	return err
}

func LoadBagValidationConfig(pathToConfigFile string) (*BagValidationConfig, []error) {
	errors := make([]error, 0)
	file, err := fileutil.LoadRelativeFile(pathToConfigFile)
	if err != nil {
		detailedError := fmt.Errorf(
			"Error reading bag validation config file '%s': %v\n",
			pathToConfigFile, err)
		errors = append(errors, detailedError)
		return nil, errors
	}
	bagValidationConfig := NewBagValidationConfig()
	err = json.Unmarshal(file, bagValidationConfig)
	if err != nil {
		detailedError := fmt.Errorf(
			"Error parsing JSON from bag validation config file '%s': %v",
			pathToConfigFile, err)
		errors = append(errors, detailedError)
		return nil, errors
	}
	configErrors := bagValidationConfig.ValidateConfig()
	regexErr := bagValidationConfig.CompileFileNameRegex()
	if regexErr != nil {
		configErrors = append(configErrors, regexErr)
	}
	return bagValidationConfig, configErrors
}
