package results

import (
	"strings"
)

// BagReadResult contains data describing the result of
// reading a single untarred bag. Reading involves checking
// the manifest, tag files and data files. If there were any
// errors, this structure records exactly what went wrong.
type BagReadResult struct {

	// Path is the absolute filepath to the untarred bag.
	// E.g. /mnt/apt_data/ncsu.1840.16-2928
	Path           string

	// Files is a list of the relative paths of all files in the bag.
	// Paths are relative to the root of the untarred bag.
	// E.g. ["aptrust-info.txt", "bag-info.txt", "data/document.pdf",]
	//
	// TODO: Record file stats here?
	Files          []string

	// Tags is a list of tags (name-value pairs) extracted from the
	// bag's tag files.
	Tags           []Tag

	// Summary contains general result information about this process.
	Summary         Summary
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

// Returns the bag's access rights, which should be one of
// "consortia", "institution" or "restricted" (all lower-case,
// to comply with the needs of our Fluctus REST service).
func (result *BagReadResult) Access() string {
	accessRights := result.TagValue("Access")
	if accessRights == "" {
		// Some older bags may have the deprecated Rights tag
		accessRights = result.TagValue("Rights")
	}
	return strings.ToLower(accessRights)
}

// Returns the title of the bag, which is extracted from
// the tag files.
func (result *BagReadResult) Title() string {
	return result.TagValue("Title")
}

// Returns the description of the bag, which is extracted from
// the tag files.
func (result *BagReadResult) Description() string {
	return result.TagValue("Internal-Sender-Description")
}

// Returns the alternate ID of the bag, which is extracted from
// the tag files. Note that this is a slice containing a single
// string, because Fedora expects an array of one or more
// alternate identifiers.
func (result *BagReadResult) AltId() []string {
	altId := make([]string, 1)
	altId[0] = result.TagValue("Internal-Sender-Identifier")
	return altId
}
