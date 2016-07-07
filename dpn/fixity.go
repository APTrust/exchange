package dpn

// Fixity represents a checksum for a bag in the DPN REST
// service.
type Fixity struct {

	// The algorithm used to check the fixity. Usually 'sha256',
	// but others may be valid in the future.
	Sha256               string       `json:"sha256"`

}
