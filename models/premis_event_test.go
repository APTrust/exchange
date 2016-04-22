package models_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"testing"
)

func TestEventTypeValid(t *testing.T) {
	for _, eventType := range constants.EventTypes {
		premisEvent := &models.PremisEvent{
			EventType: eventType,
		}
		if premisEvent.EventTypeValid() == false {
			t.Errorf("EventType '%s' should be valid", eventType)
		}
	}
	premisEvent := &models.PremisEvent{
		EventType: "pub_crawl",
	}
	if premisEvent.EventTypeValid() == true {
		t.Errorf("EventType 'pub_crawl' should not be valid")
	}
}

func TestNewEventObjectIngest(t *testing.T) {
	event, err := models.NewEventObjectIngest(300)
	if err != nil {
		t.Errorf("Error creating PremisEvent: %v", err)
		return
	}
	if len(event.Identifier) != 36 {
		t.Errorf("Event identifier '%s' doesn't look like a UUID", event.Identifier)
	}
	if event.EventType != "ingest" {
		t.Errorf("EventType: expected 'ingest', got '%s'", event.EventType)
	}
}

func TestNewEventObjectIdentifierAssignment(t *testing.T) {
	event, err := models.NewEventObjectIdentifierAssignment("test.edu/object001")
	if err != nil {
		t.Errorf("Error creating PremisEvent: %v", err)
		return
	}
	if len(event.Identifier) != 36 {
		t.Errorf("Event identifier '%s' doesn't look like a UUID", event.Identifier)
	}
	if event.EventType != "identifier_assignment" {
		t.Errorf("EventType: expected 'identifier_assignment', got '%s'", event.EventType)
	}

}

func TestNewEventObjectRights(t *testing.T) {
	event, err := models.NewEventObjectRights("institution")
	if err != nil {
		t.Errorf("Error creating PremisEvent: %v", err)
		return
	}
	if len(event.Identifier) != 36 {
		t.Errorf("Event identifier '%s' doesn't look like a UUID", event.Identifier)
	}
	if event.EventType != "access_assignment" {
		t.Errorf("EventType: expected 'access_assignment', got '%s'", event.EventType)
	}
}

func TestNewEventGenericFileIngest(t *testing.T) {
	event, err := models.NewEventGenericFileIngest(TEST_TIMESTAMP, "123456789")
	if err != nil {
		t.Errorf("Error creating PremisEvent: %v", err)
		return
	}
	if len(event.Identifier) != 36 {
		t.Errorf("Event identifier '%s' doesn't look like a UUID", event.Identifier)
	}
	if event.EventType != "ingest" {
		t.Errorf("EventType: expected 'ingest', got '%s'", event.EventType)
	}
}

func TestNewEventGenericFileFixityCheck(t *testing.T) {
	event, err := models.NewEventGenericFileFixityCheck(TEST_TIMESTAMP, constants.AlgMd5, "123456789", true)
	if err != nil {
		t.Errorf("Error creating PremisEvent: %v", err)
		return
	}
	if len(event.Identifier) != 36 {
		t.Errorf("Event identifier '%s' doesn't look like a UUID", event.Identifier)
	}
	if event.EventType != "fixity_check" {
		t.Errorf("EventType: expected 'fixity_check', got '%s'", event.EventType)
	}
}

func TestNewEventGenericFileFixityGeneration(t *testing.T) {
	event, err := models.NewEventGenericFileFixityGeneration(TEST_TIMESTAMP, constants.AlgMd5, "123456789")
	if err != nil {
		t.Errorf("Error creating PremisEvent: %v", err)
		return
	}
	if len(event.Identifier) != 36 {
		t.Errorf("Event identifier '%s' doesn't look like a UUID", event.Identifier)
	}
}

func TestNewEventGenericFileIdentifierAssignment(t *testing.T) {
	event, err := models.NewEventGenericFileIdentifierAssignment(TEST_TIMESTAMP, constants.IdTypeURL, "blah/blah/blah")
	if err != nil {
		t.Errorf("Error creating PremisEvent: %v", err)
		return
	}
	if len(event.Identifier) != 36 {
		t.Errorf("Event identifier '%s' doesn't look like a UUID", event.Identifier)
	}
	if event.EventType != "identifier_assignment" {
		t.Errorf("EventType: expected 'identifier_assignment', got '%s'", event.EventType)
	}
}

func TestNewEventGenericFileReplication(t *testing.T) {
	event, err := models.NewEventGenericFileReplication(TEST_TIMESTAMP, "http://example.com/123456789")
	if err != nil {
		t.Errorf("Error creating PremisEvent: %v", err)
		return
	}
	if len(event.Identifier) != 36 {
		t.Errorf("Event identifier '%s' doesn't look like a UUID", event.Identifier)
	}
	if event.EventType != "replication" {
		t.Errorf("EventType: expected 'replication', got '%s'", event.EventType)
	}
}
