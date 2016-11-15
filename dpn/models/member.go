package models

// Member describes an institution or depositor that owns
// a bag.
type Member struct {

	// MemberId is the unique identifier for a member.
	// This is a UUID in string format.
	MemberId string `json:"member_id"`

	// Name is the member's name
	Name string `json:"name"`

	// Email is the member's email address
	Email string `json:"email"`
}
