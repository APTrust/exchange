package dpn

import (
	"time"
)

// Member describes an institution or depositor that owns
// a bag.
type Member struct {

	// UUID is the unique identifier for a member
	UUID               string               `json:"uuid"`

	// Name is the member's name
	Name               string               `json:"name"`

	// Email is the member's email address
	Email              string               `json:"email"`

	// CreatedAt is when this record was created.
	CreatedAt          time.Time            `json:"created_at"`

	// UpdatedAt is when this record was last updated.
	UpdatedAt          time.Time            `json:"updated_at"`

}
