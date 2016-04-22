package models

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/nu7hatch/gouuid"
	"strings"
	"time"
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
	Id            string         `json:"id"`
	Identifier    string         `json:"identifier"`
	Institution   string         `json:"institution"`
	InstitutionId int            `json:"institution_id"`
	Title         string         `json:"title"`
	Description   string         `json:"description"`
	Access        string         `json:"access"`
	AltIdentifier []string       `json:"alt_identifier"`
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


func (obj *IntellectualObject) CreateIngestEvent() (*PremisEvent, error) {
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for ingest event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "ingest",
		DateTime:           time.Now(),
		Detail:             "Copied all files to perservation bucket",
		Outcome:            "Success",
		OutcomeDetail:      fmt.Sprintf("%d files copied", len(obj.GenericFiles)),
		Object:             "goamz S3 client",
		Agent:              "https://github.com/crowdmob/goamz",
		OutcomeInformation: "Multipart put using md5 checksum",
	}, nil
}

func (obj *IntellectualObject) CreateIdEvent() (*PremisEvent, error) {
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for ingest event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "identifier_assignment",
		DateTime:           time.Now(),
		Detail:             "Assigned bag identifier",
		Outcome:            "Success",
		OutcomeDetail:      obj.Identifier,
		Object:             "APTrust bagman",
		Agent:              "https://github.com/APTrust/bagman",
		OutcomeInformation: "Institution domain + tar file name",
	}, nil
}

func (obj *IntellectualObject) CreateRightsEvent() (*PremisEvent, error) {
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for ingest access/rights event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          "access_assignment",
		DateTime:           time.Now(),
		Detail:             "Assigned bag access rights",
		Outcome:            "Success",
		OutcomeDetail:      obj.Access,
		Object:             "APTrust bagman",
		Agent:              "https://github.com/APTrust/bagman",
		OutcomeInformation: "Set access to " + obj.Access,
	}, nil
}

// Serialize the subset of IntellectualObject data that fluctus
// will accept. This is for post/put, where essential info, such
// as institution id and/or object id will be in the URL.
func (obj *IntellectualObject) SerializeForFluctus() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"identifier":     obj.Identifier,
		"title":          obj.Title,
		"description":    obj.Description,
		"alt_identifier": obj.AltIdentifier,
		"access":         obj.Access,
	})
}
