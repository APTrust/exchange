package models

import (
	"github.com/APTrust/exchange/constants"
	"strings"
	"time"
)

/*
PremisEvent contains information about events that occur during
the processing of a file or intellectual object, such as the
verfication of checksums, generation of unique identifiers, etc.
We use this struct to exchange data in JSON format with the
fluctus API. Fluctus, in turn, is responsible for managing all of
this data in Fedora.
*/
type PremisEvent struct {
	// Identifier is a UUID string assigned by Fedora.
	Identifier         string    `json:"identifier"`

	// EventType is the type of Premis event we want to register: ingest,
	// validation, fixity_generation, fixity_check or identifier_assignment.
	EventType          string    `json:"type"`

	// DateTime is when this event occurred in our system.
	DateTime           time.Time `json:"date_time"`

	// Detail is a brief description of the event.
	Detail             string    `json:"detail"`

	// Outcome is either success or failure
	Outcome            string    `json:"outcome"`

	// Outcome detail is the checksum for checksum generation,
	// the id for id generation.
	OutcomeDetail      string    `json:"outcome_detail"`

	// Object is a description of the object that generated
	// the checksum or id.
	Object             string    `json:"object"`

	// Agent is a URL describing where to find more info about Object.
	Agent              string    `json:"agent"`

	// OutcomeInformation contains the text of an error message, if
	// Outcome was failure.
	OutcomeInformation string    `json:"outcome_information"`
}

// EventTypeValid returns true/false, indicating whether the
// structure's EventType property contains the name of a
// valid premis event.
func (premisEvent *PremisEvent) EventTypeValid() bool {
	lcEventType := strings.ToLower(premisEvent.EventType)
	for _, value := range constants.EventTypes {
		if value == lcEventType {
			return true
		}
	}
	return false
}
