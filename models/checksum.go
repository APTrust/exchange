package models

import (
	"time"
)

/*
Checksum contains information about a checksum that
can be used to validate the integrity of a GenericFile.
DateTime should be in ISO8601 format for local time or UTC.
For example:
1994-11-05T08:15:30-05:00     (Local Time)
1994-11-05T08:15:30Z          (UTC)
*/
type Checksum struct {
	Id            int       `json:"id"`
	GenericFileId int   `json:"generic_file_id"`
	Algorithm     string    `json:"algorithm"`
	DateTime      time.Time `json:"datetime"`
	Digest        string    `json:"digest"`
}
