package result_test

import (
	"github.com/APTrust/exchange/result"
	"github.com/APTrust/exchange/util/testutil"
	"testing"
)

func getFedoraResult() *result.FedoraResult {
	genericFilePaths := []string {
		"data/metadata.xml",
		"data/object.properties",
		"data/ORIGINAL/1",
		"data/ORIGINAL/1-metadata.xml",
	}
	return result.NewFedoraResult("ncsu.edu/ncsu.1840.16-2928", genericFilePaths)
}

func TestFedoraResultAddRecord(t *testing.T) {

	fedoraResult := getFedoraResult()

	// Add some invalid MetadataRecords, and make sure we get errors
	// Bad type
	err := fedoraResult.AddRecord("BadType", "some action", "some object", "")
	if err == nil {
		t.Errorf("FedoraResult.AddRecord did not reject record with bad type")
	}
	if len(fedoraResult.MetadataRecords) > 0 {
		t.Errorf("FedoraResult.AddRecord added record with bad type to its collection")
	}

	// Good type, bad action
	err = fedoraResult.AddRecord("PremisEvent", "some action", "some object", "")
	if err == nil {
		t.Errorf("FedoraResult.AddRecord did not reject record with bad action")
	}
	if len(fedoraResult.MetadataRecords) > 0 {
		t.Errorf("FedoraResult.AddRecord added record with bad action to its collection")
	}

	// Good type, good action, missing eventObject
	err = fedoraResult.AddRecord("PremisEvent", "some action", "", "")
	if err == nil {
		t.Errorf("FedoraResult.AddRecord did not reject record with missing event object")
	}
	if len(fedoraResult.MetadataRecords) > 0 {
		t.Errorf("FedoraResult.AddRecord added record with missing event object to its collection")
	}

	// Good records
	err = fedoraResult.AddRecord("IntellectualObject", "object_registered", fedoraResult.ObjectIdentifier, "")
	if err != nil {
		t.Errorf("FedoraResult.AddRecord rejected a valid IntellectualObject record: %v", err)
	}
	err = fedoraResult.AddRecord("GenericFile", "file_registered", "data/ORIGINAL/1", "")
	if err != nil {
		t.Errorf("FedoraResult.AddRecord rejected a valid GenericFile record: %v", err)
	}
	err = fedoraResult.AddRecord("PremisEvent", "fixity_generation", "data/ORIGINAL/1", "")
	if err != nil {
		t.Errorf("FedoraResult.AddRecord rejected a valid PremisEvent record for fixity_generation: %v", err)
	}
	err = fedoraResult.AddRecord("PremisEvent", "identifier_assignment", "data/ORIGINAL/1", "")
	if err != nil {
		t.Errorf("FedoraResult.AddRecord rejected a valid PremisEvent record for identifier_assignment: %v", err)
	}
	if len(fedoraResult.MetadataRecords) != 4 {
		t.Errorf("FedoraResult should have 4 MetadataRecords, but it has %d", len(fedoraResult.MetadataRecords))
	}
}

func TestFedoraResultFindRecord(t *testing.T) {

	fedoraResult := getFedoraResult()

	_ = fedoraResult.AddRecord("IntellectualObject", "object_registered", fedoraResult.ObjectIdentifier, "")
	_ = fedoraResult.AddRecord("GenericFile", "file_registered", "data/ORIGINAL/1", "")
	_ = fedoraResult.AddRecord("PremisEvent", "fixity_generation", "data/ORIGINAL/1", "")

	record := fedoraResult.FindRecord("IntellectualObject", "object_registered", fedoraResult.ObjectIdentifier)
	if record == nil {
		t.Error("FedoraResult.FindRecord did not return expected record")
	}
	record = fedoraResult.FindRecord("GenericFile", "file_registered", "data/ORIGINAL/1")
	if record == nil {
		t.Error("FedoraResult.FindRecord did not return expected record")
	}
	record = fedoraResult.FindRecord("PremisEvent", "fixity_generation", "data/ORIGINAL/1")
	if record == nil {
		t.Error("FedoraResult.FindRecord did not return expected record")
	}
	record = fedoraResult.FindRecord("No such record", "", "")
	if record != nil {
		t.Error("FedoraResult.FindRecord returned a record when it shouldn't have")
	}

}

func TestFedoraResultRecordSucceeded(t *testing.T) {

	fedoraResult := getFedoraResult()

	_ = fedoraResult.AddRecord("IntellectualObject", "object_registered", fedoraResult.ObjectIdentifier, "")
	_ = fedoraResult.AddRecord("GenericFile", "file_registered", "data/ORIGINAL/1", "Internet blew up")

	succeeded := fedoraResult.RecordSucceeded("IntellectualObject", "object_registered",
		fedoraResult.ObjectIdentifier)
	if false == succeeded {
		t.Error("FedoraResult.RecordSucceeded returned false when it should have returned true")
	}
	succeeded = fedoraResult.RecordSucceeded("GenericFile", "file_registered", "data/ORIGINAL/1")
	if true == succeeded {
		t.Error("FedoraResult.RecordSucceeded returned true when it should have returned false")
	}
}

func TestAllRecordsSucceeded(t *testing.T) {

	fedoraResult := getFedoraResult()

	// Add successful events for the intellectual object
	_ = fedoraResult.AddRecord("IntellectualObject", "object_registered", fedoraResult.ObjectIdentifier, "")
	_ = fedoraResult.AddRecord("PremisEvent", "ingest", fedoraResult.ObjectIdentifier, "")
	// Add successful events for each generic file
	for _, path := range testutil.ExpectedPaths {
		_ = fedoraResult.AddRecord("GenericFile", "file_registered", path, "")
		_ = fedoraResult.AddRecord("PremisEvent", "identifier_assignment", path, "")
		_ = fedoraResult.AddRecord("PremisEvent", "fixity_generation", path, "")
	}

	if fedoraResult.AllRecordsSucceeded() == false {
		t.Error("FedoraResult.AllRecordsSucceeded() returned false when it should have returned true")
	}

	// Alter one record so it fails...
	record := fedoraResult.FindRecord("PremisEvent", "fixity_generation", "data/ORIGINAL/1")
	record.ErrorMessage = "Fluctus got drunk and dropped all punch cards in the toilet"

	if fedoraResult.AllRecordsSucceeded() == true {
		t.Error("FedoraResult.AllRecordsSucceeded() returned true when it should have returned false")
	}
}
