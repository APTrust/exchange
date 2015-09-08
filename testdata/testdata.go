package testdata

// This package contains test data used by various unit tests
// in other packages.

import (
	"time"
)

var TimeZero, Apr_25_2014, June_9_2014, July_3_2014 time.Time

// Our test fixture describes a bag that includes the following file paths
var ExpectedPaths []string = []string{
	"data/metadata.xml",
	"data/object.properties",
	"data/ORIGINAL/1",
	"data/ORIGINAL/1-metadata.xml",
}

func InitDateTimes() {
	// Initialize only if necessary
	if July_3_2014.Year() != 2014 {
		TimeZero = time.Time{}
		Apr_25_2014, _ = time.Parse(time.RFC3339, "2014-04-25T18:05:51Z")
		June_9_2014, _ = time.Parse(time.RFC3339, "2014-06-09T14:12:45Z")
		July_3_2014, _ = time.Parse(time.RFC3339, "2014-07-03T16:05:51Z")
	}
}
