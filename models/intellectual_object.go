package models

import (
	"encoding/json"
	"github.com/APTrust/exchange/constants"
	"strings"
)

/*
IntellectualObject is Fluctus' version of an IntellectualObject.
It belongs to an Institution and consists of one or more
GenericFiles and a number of events.
Institution is the owner of the intellectual object.
Title is the title.
Description is a free-text description of the object.
Identifier is the object's unique identifier. (Whose assigned
this id? APTrust or the owner?)
Access indicate who can access the object. Valid values are
consortial, institution and restricted.
*/
type IntellectualObject struct {
	// If Id is non-zero, this has been recorded in Pharos.
	Id            string         `json:"id"`
	Identifier    string         `json:"identifier"`
	BagName       string         `json:"bag_name"`
	Institution   string         `json:"institution"`
	InstitutionId int            `json:"institution_id"`
	Title         string         `json:"title"`
	Description   string         `json:"description"`
	Access        string         `json:"access"`
	AltIdentifier string         `json:"alt_identifier"`
	GenericFiles  []*GenericFile `json:"generic_files"`
	Events        []*PremisEvent `json:"events"`
}

// Returns the total number of bytes of all of the generic
// files in this object. The object's bag size will be slightly
// larger than this, because it will include a manifest, tag
// files and tar header.
func (obj *IntellectualObject) TotalFileSize() (int64) {
	total := int64(0)
	for _, genericFile := range obj.GenericFiles {
		total += genericFile.Size
	}
	return total
}

// AccessValid returns true or false to indicate whether the
// structure's Access property contains a valid value.
func (obj *IntellectualObject) AccessValid() bool {
	lcAccess := strings.ToLower(obj.Access)
	for _, value := range constants.AccessRights {
		if value == lcAccess {
			return true
		}
	}
	return false
}

// Serialize the subset of IntellectualObject data that Pharos
// will accept. This is for post/put, where essential info, such
// as institution id and/or object id will be in the URL.
func (obj *IntellectualObject) SerializeForPharos() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"identifier":     obj.Identifier,
		"bag_name":       obj.BagName,
		"institution":    obj.Institution,
		"title":          obj.Title,
		"description":    obj.Description,
		"alt_identifier": obj.AltIdentifier,
		"access":         obj.Access,
	})
}
