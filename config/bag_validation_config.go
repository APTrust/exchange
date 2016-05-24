package config

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
)

const (
	REQUIRED  = "required"
	OPTIONAL  = "optional"
	FORBIDDEN = "forbidden"
)

// FileSpec defines whether files at a specified path within
// the bag are required, optional, or forbidden.
type FileSpec struct {
	// Presence can be REQUIRED, OPTIONAL, or FORBIDDEN.
	Presence        string
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
}

func NewBagValidationConfig() (*BagValidationConfig) {
	return &BagValidationConfig{
		FileSpecs: make(map[string]FileSpec),
		TagSpecs: make(map[string]TagSpec),
		AllowMiscTopLevelFiles: false,
		AllowMiscDirectories: false,
		TopLevelDirMustMatchBagName: false,
	}
}

func LoadBagValidationConfig(pathToConfigFile string) (*BagValidationConfig, error) {
	file, err := fileutil.LoadRelativeFile(pathToConfigFile)
	if err != nil {
		detailedError := fmt.Errorf(
			"Error reading bag validation config file '%s': %v\n",
			pathToConfigFile, err)
		return nil, detailedError
	}
	bagValidationConfig := NewBagValidationConfig()
	err = json.Unmarshal(file, bagValidationConfig)
	if err != nil {
		detailedError := fmt.Errorf(
			"Error parsing JSON from bag validation config file '%s':",
			pathToConfigFile, err)
		return nil, detailedError
	}
	return bagValidationConfig, nil
}
