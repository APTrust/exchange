package models

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/util"
	"github.com/nu7hatch/gouuid"
	"strings"
	"time"
)

/*
PremisEvent contains information about events that occur during
the processing of a file or intellectual object, such as the
verfication of checksums, generation of unique identifiers, etc.
We use this struct to exchange data in JSON format with the
Pharos API.
*/
type PremisEvent struct {
	// The Pharos id for this event. Will be zero if the event
	// is not yet in Pharos. If non-zero, it's been recorded
	// in Pharos. Do not serialize zero values to JSON, or
	// Pharos complains.
	Id                 int       `json:"id,omitempty"`

	// Identifier is a UUID string assigned by Pharos.
	Identifier         string    `json:"identifier"`

	// EventType is the type of Premis event we want to register.
	// See constants.EventTypes.
	EventType          string    `json:"event_type"`

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

	// Id of the IntellectualObject with which this event is
	// associated.
	IntellectualObjectId int     `json:"intellectual_object_id"`

	// Identifier of the IntellectualObject with which this
	// event is associated.
	IntellectualObjectIdentifier string `json:"intellectual_object_identifier"`

	// Id of the GenericFile with which this event is
	// associated. This will be zero for object-level events.
	GenericFileId int            `json:"generic_file_id"`

	// Identifier of the GenericFile with which this
	// event is associated. This will be an empty string
	// for object-level events.
	GenericFileIdentifier string `json:"generic_file_identifier"`

	// Rails timestamp for when this object was created.
	CreatedAt     time.Time `json:"created_at,omitempty"`

	// Rails timestamp for when this object was last updated.
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
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

func NewEventObjectCreation() (*PremisEvent, error) {
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for creation event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventCreation,
		DateTime:           time.Now(),
		Detail:             "Object created.",
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      "Intellectual object created.",
		Object:             "APTrust Exchange ingest services",
		Agent:              "https://github.com/APTrust/exchange",
		OutcomeInformation: "Object created, files stored and replicated, awaiting recording of all files and events in Pharos.",
	}, nil
}

func NewEventObjectIngest(numberOfFilesIngested int) (*PremisEvent, error) {
	if numberOfFilesIngested <= 0 {
		return nil, fmt.Errorf("Param numberOfFilesIngested must be greater than zero.")
	}
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for ingest event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventIngestion,
		DateTime:           time.Now(),
		Detail:             "Copied all files to perservation bucket",
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      fmt.Sprintf("%d files copied", numberOfFilesIngested),
		Object:             "goamz S3 client",
		Agent:              "https://github.com/crowdmob/goamz",
		OutcomeInformation: "Multipart put using md5 checksum",
	}, nil
}

func NewEventObjectIdentifierAssignment(objectIdentifier string) (*PremisEvent, error) {
	if objectIdentifier == "" {
		return nil, fmt.Errorf("Param objectIdentifier cannot be empty.")
	}
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for ingest event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventIdentifierAssignment,
		DateTime:           time.Now(),
		Detail:             "Assigned bag identifier",
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      objectIdentifier,
		Object:             "APTrust exchange",
		Agent:              "https://github.com/APTrust/exchange",
		OutcomeInformation: "Institution domain + tar file name",
	}, nil
}

func NewEventObjectRights(accessSetting string) (*PremisEvent, error) {
	if !util.StringListContains(constants.AccessRights, strings.ToLower(accessSetting)) {
		return nil, fmt.Errorf("Param accessSetting '%s' is not valid.", accessSetting)
	}
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for ingest access/rights event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventAccessAssignment,
		DateTime:           time.Now(),
		Detail:             "Assigned bag access rights",
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      accessSetting,
		Object:             "APTrust exchange",
		Agent:              "https://github.com/APTrust/exchange",
		OutcomeInformation: "Set access to " + accessSetting,
	}, nil
}

// We ingested a generic file into primary long-term storage.
func NewEventGenericFileIngest(storedAt time.Time, md5Digest string) (*PremisEvent, error) {
	if storedAt.IsZero() {
		return nil, fmt.Errorf("Param storedAt cannot be empty.")
	}
	if len(md5Digest) != 32 {
		return nil, fmt.Errorf("Param md5Digest must have 32 characters. '%s' doesn't.",
			md5Digest)
	}
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for generic file ingest event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventIngestion,
		DateTime:           storedAt,
		Detail:             "Completed copy to S3",
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      fmt.Sprintf("md5:%s", md5Digest),
		Object:             "exchange + goamz S3 client",
		Agent:              "https://github.com/APTrust/exchange",
		OutcomeInformation: "Put using md5 checksum",
	}, nil
}

// We checked fixity against the manifest.
// If fixity didn't match, we wouldn't be ingesting this.
func NewEventGenericFileFixityCheck(checksumVerifiedAt time.Time, fixityAlg, digest string, fixityMatched bool) (*PremisEvent, error) {
	if checksumVerifiedAt.IsZero() {
		return nil, fmt.Errorf("Param checksumVerifiedAt cannot be empty.")
	}
	if !util.StringListContains(constants.ChecksumAlgorithms, fixityAlg) {
		return nil, fmt.Errorf("Param fixityAlg '%s' is not valid.", fixityAlg)
	}
	if len(digest) != 32 && len(digest) != 64 {
		return nil, fmt.Errorf("Param digest must have 32 or 64 characters. '%s' doesn't.",
			digest)
	}
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for generic file fixity check: %v", err)
	}
	object := "Go language crypto/md5"
	agent := "http://golang.org/pkg/crypto/md5/"
	outcomeInformation := "Fixity matches"
	outcome := string(constants.StatusSuccess)
	if fixityAlg == constants.AlgSha256 {
		object = "Go language crypto/sha256"
		agent = "http://golang.org/pkg/crypto/sha256/"
	}
	if fixityMatched == false {
		outcome = string(constants.StatusFailed)
		outcomeInformation = "Fixity did not match"
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventFixityCheck,
		DateTime:           checksumVerifiedAt,
		Detail:             "Fixity check against registered hash",
		Outcome:            outcome,
		OutcomeDetail:      fmt.Sprintf("%s:%s", fixityAlg, digest),
		Object:             object,
		Agent:              agent,
		OutcomeInformation: outcomeInformation,
	}, nil
}

// We generated a sha256 checksum.
func NewEventGenericFileDigestCalculation(checksumGeneratedAt time.Time, fixityAlg, digest string) (*PremisEvent, error) {
	if checksumGeneratedAt.IsZero() {
		return nil, fmt.Errorf("Param checksumVerifiedAt cannot be empty.")
	}
	if !util.StringListContains(constants.ChecksumAlgorithms, fixityAlg) {
		return nil, fmt.Errorf("Param fixityAlg '%s' is not valid.", fixityAlg)
	}
	if len(digest) != 32 && len(digest) != 64 {
		return nil, fmt.Errorf("Param digest must have 32 or 64 characters. '%s' doesn't.",
			digest)
	}
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for generic file ingest event: %v", err)
	}
	object := "Go language crypto/md5"
	agent := "http://golang.org/pkg/crypto/md5/"
	if fixityAlg == constants.AlgSha256 {
		object = "Go language crypto/sha256"
		agent = "http://golang.org/pkg/crypto/sha256/"
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventDigestCalculation,
		DateTime:           checksumGeneratedAt,
		Detail:             "Calculated fixity value",
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      fmt.Sprintf("%s:%s", fixityAlg, digest),
		Object:             object,
		Agent:              agent,
		OutcomeInformation: "Calculated fixity value",
	}, nil
}

// We assigned an identifier: either a generic file identifier
// or a new storage URL. Note that when identifierType is
// constants.IdTypeStorageURL, identifierGeneratedAt is the
// timestamp at which the file was stored in S3.
func NewEventGenericFileIdentifierAssignment(identifierGeneratedAt time.Time, identifierType, identifier string) (*PremisEvent, error) {
	if identifierGeneratedAt.IsZero() {
		return nil, fmt.Errorf("Param identifierGeneratedAt cannot be empty.")
	}
	if identifierType != constants.IdTypeStorageURL && identifierType != constants.IdTypeBagAndPath {
		return nil, fmt.Errorf("Param identifierType '%s' is not valid.", identifierType)
	}
	if identifier == "" {
		return nil, fmt.Errorf("Param identifier cannot be empty.")
	}
	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for generic file ingest event: %v", err)
	}
	object := "APTrust exchange/ingest processor"
	agent := "https://github.com/APTrust/exchange"
	detail := "Assigned new institution.bag/path identifier"
	if identifierType == constants.IdTypeStorageURL {
		object = "Go uuid library + goamz S3 library"
		agent = "http://github.com/nu7hatch/gouuid"
		// Don't change these words. They're used in IsUrlAssignment below.
		detail = fmt.Sprintf("Assigned new storage URL identifier, and item was stored at %s",
			identifierGeneratedAt.Format(time.RFC3339))
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventIdentifierAssignment,
		DateTime:           identifierGeneratedAt,
		Detail:             detail,
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      identifier,
		Object:             object,
		Agent:              agent,
		OutcomeInformation: fmt.Sprintf("Assigned %s identifier", identifierType),
	}, nil
}

// We saved the file to replication storage.
func NewEventGenericFileReplication(replicatedAt time.Time, replicationUrl string) (*PremisEvent, error) {
	if replicatedAt.IsZero() {
		return nil, fmt.Errorf("Param replicatedAt cannot be empty.")
	}
	if replicationUrl == "" {
		return nil, fmt.Errorf("Param identifier cannot be empty.")
	}

	eventId, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("Error generating UUID for generic file replication event: %v", err)
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventReplication,
		DateTime:           replicatedAt,
		Detail:             "Copied to replication storage and assigned replication URL identifier",
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      replicationUrl,
		Object:             "Go uuid library + goamz S3 library",
		Agent:              "http://github.com/nu7hatch/gouuid",
		OutcomeInformation: "Replicated to secondary storage",
	}, nil
}


// Sets the Id, CreatedAt and UpdatedAt properties of this event to
// match those os savedEvent. We call this after saving a record to
// Pharos, which sets all of those properties. Generally, savedEvent
// is a temporary event record returned from Pharos, while this event
// is one we want to keep.
func (event *PremisEvent) MergeAttributes(savedEvent *PremisEvent) (error) {
	if savedEvent == nil {
		return fmt.Errorf("Param savedEvent cannot be nil.")
	}
	event.Id = savedEvent.Id
	event.CreatedAt = savedEvent.CreatedAt
	event.UpdatedAt = savedEvent.UpdatedAt
	return nil
}

// Returns true if this event is a URL assignment event.
func (event *PremisEvent) IsUrlAssignment() (bool) {
	return (event.EventType == constants.EventIdentifierAssignment &&
		strings.HasPrefix(event.Detail, "Assigned new storage URL"))
}
