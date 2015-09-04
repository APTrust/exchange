package results

import (
	"github.com/APTrust/exchange/models"
	"fmt"
)

// FedoraResult is a collection of MetadataRecords, each indicating
// whether or not some bit of metadata has been recorded in Fluctus/Fedora.
// The bag processor needs to keep track of this information to ensure
// it successfully records all metadata in Fedora.
type FedoraResult struct {
	ObjectIdentifier string
	GenericFilePaths []string
	MetadataRecords  []*models.MetadataRecord
	IsNewObject      bool
	Summary          Summary
}

// Creates a new FedoraResult object with the specified IntellectualObject
// identifier and list of GenericFile paths.
func NewFedoraResult(objectIdentifier string, genericFilePaths []string) *FedoraResult {
	return &FedoraResult{
		ObjectIdentifier: objectIdentifier,
		GenericFilePaths: genericFilePaths,
		IsNewObject:      true,
		Summary:          NewSummary(),
	}
}

// AddRecord adds a new MetadataRecord to the Fedora result.
func (result *FedoraResult) AddRecord(recordType, action, eventObject, errorMessage string) error {
	if recordType != "IntellectualObject" && recordType != "GenericFile" && recordType != "PremisEvent" {
		return fmt.Errorf("Param recordType must be one of 'IntellectualObject', 'GenericFile', or 'PremisEvent'")
	}
	if recordType == "PremisEvent" && action != "ingest" && action != "fixity_check" &&
		action != "identifier_assignment" && action != "fixity_generation" {
		return fmt.Errorf("'%s' is not a valid action for PremisEvent", action)
	} else if recordType == "IntellectualObject" && action != "object_registered" {
		return fmt.Errorf("'%s' is not a valid action for IntellectualObject", action)
	} else if recordType == "GenericFile" && action != "file_registered" {
		return fmt.Errorf("'%s' is not a valid action for GenericFile", action)
	}
	if eventObject == "" {
		return fmt.Errorf("Param eventObject cannot be empty")
	}
	record := &models.MetadataRecord{
		Type:         recordType,
		Action:       action,
		EventObject:  eventObject,
		ErrorMessage: errorMessage,
	}
	result.MetadataRecords = append(result.MetadataRecords, record)
	return nil
}

// FindRecord returns the MetadataRecord with the specified type,
// action and event object.
func (result *FedoraResult) FindRecord(recordType, action, eventObject string) *models.MetadataRecord {
	for _, record := range result.MetadataRecords {
		if record.Type == recordType && record.Action == action && record.EventObject == eventObject {
			return record
		}
	}
	return nil
}

// Returns true/false to indicate whether the specified bit of
// metadata was recorded successfully in Fluctus/Fedora.
func (result *FedoraResult) RecordSucceeded(recordType, action, eventObject string) bool {
	record := result.FindRecord(recordType, action, eventObject)
	return record != nil && record.Succeeded()
}

// Returns true if all metadata was recorded successfully in Fluctus/Fedora.
// A true result means that all of the following were successfully recorded:
//
// 1) Registration of the IntellectualObject. This may mean creating a new
// IntellectualObject or updating an existing one.
//
// 2) Recording the ingest PremisEvent for the IntellectualObject.
//
// 3) Registration of EACH of the object's GenericFiles. This may mean
// creating a new GenericFile or updating an existing one.
//
// 4) Recording the intentifier_assignment for EACH GenericFile. The
// identifier is typically a UUID.
//
// 5) Recording the fixity_generation for EACH GenericFile. Although most
// files already come with md5 checksums from S3, we always generate a
// sha256 as well.
//
// A successful FedoraResult will have (2 + (3 * len(GenericFilePaths)))
// successful MetadataRecords.
func (result *FedoraResult) AllRecordsSucceeded() bool {
	for _, record := range result.MetadataRecords {
		if false == record.Succeeded() {
			return false
		}
	}
	return true
}
