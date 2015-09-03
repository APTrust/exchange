package models

// MetadataRecord describes the result of an attempt to record metadata
// in Fluctus/Fedora.
type MetadataRecord struct {
	// Type describes what we're trying to record in Fedora. It can
	// be "IntellectualObject", "GenericFile", or "PremisEvent"
	Type string
	// Action contains information about what was in Fedora.
	// For Type IntellectualObject, this will be "object_registered".
	// For Type GenericFile, this will be "file_registered".
	// For Type PremisEvent, this will be the name of the event:
	// "ingest", "identifier_assignment", or "fixity_generation".
	Action string
	// For actions or events pertaining to a GenericFile this will be the path
	// of the file the action pertains to. For example, for fixity_generation
	// on the file "data/images/aerial.jpg", the EventObject would be
	// "data/images/aerial.jpg". For actions or events pertaining to the
	// IntellectualObject, this will be the IntellectualObject identifier.
	EventObject string
	// ErrorMessage contains a description of the error that occurred
	// when we tried to save this bit of metadata in Fluctus/Fedora.
	// It will be empty if there was no error, or if we have not yet
	// attempted to save the item.
	ErrorMessage string
}

// Returns true if this bit of metadata was successfully saved to Fluctus/Fedora.
func (record *MetadataRecord) Succeeded() bool {
	return record.ErrorMessage == ""
}
