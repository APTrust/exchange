package result

import (
	"strings"
)

// BagReadResult contains data describing the result of
// reading a single untarred bag. Reading involves checking
// the manifest, tag files and data files. If there were any
// errors, this structure records exactly what went wrong.
type BagReadResult struct {

	// This is set to true when bag validation starts.
	Started        bool

	// Path is the absolute filepath to the untarred bag.
	// E.g. /mnt/apt_data/ncsu.1840.16-2928
	Path           string

	// Files is a list of the relative paths of all files in the bag.
	// Paths are relative to the root of the untarred bag.
	// E.g. ["aptrust-info.txt", "bag-info.txt", "data/document.pdf",]
	//
	// TODO: Record file stats here?
	Files          []string

	// Errors is a list of strings describing errors that occurred
	// during bag validation.
	Errors         []string

	// Tags is a list of tags (name-value pairs) extracted from the
	// bag's tag files.
	Tags           []Tag

	// ChecksumErrors is a list of error objects describing files
	// the don't match the checksums in the bag manifest. This list
	// is separate from errors because it comes from the bagins
	// bag parsing library.
	ChecksumErrors []error
}

// This Tag struct is essentially the same as the bagins
// TagField struct, but its properties are public and can
// be easily serialized to / deserialized from JSON.
type Tag struct {
	Label string
	Value string
}

// TagValue returns the value of the tag with the specified label.
func (result *BagReadResult) TagValue(tagLabel string) (tagValue string) {
	lcTagLabel := strings.ToLower(tagLabel)
	for _, tag := range result.Tags {
		if strings.ToLower(tag.Label) == lcTagLabel {
			tagValue = tag.Value
			break
		}
	}
	return tagValue
}

// Result Interface functions

func (result *BagReadResult) Succeeded() bool {
	return result.Started && len(result.Errors) == 0
}

func (result *BagReadResult) AddError(errStr string) {
	result.Errors = append(result.Errors, errStr)
}
