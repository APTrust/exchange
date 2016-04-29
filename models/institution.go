package models

// Institution represents an institution in Fuctus.

type Institution struct {
	// Id is the Pharos id for this object.
	Id         int    `json:"id"`

	// Name is the institution's full name.
	Name       string `json:"name"`

	// BriefName is a shortened name.
	// E.g. "uva" for University of Virginia.
	BriefName  string `json:"brief_name"`

	// Identifier is the institution's domain name.
	Identifier string `json:"identifier"`
}
