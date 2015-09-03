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
