package models_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)


func source() string {
	_, fileName, fileLine, ok := runtime.Caller(2)
	var s string
	if ok {
		fileNameParts := strings.Split(fileName, "/exchange/")
		s = fmt.Sprintf("%s:%d", fileNameParts[1], fileLine)
	} else {
		s = ""
	}
	return s
}

// Bloomsday
var TEST_TIMESTAMP time.Time = time.Date(2016, 6, 16, 10, 24, 16, 0, time.UTC)

func assertMinStringLength(t *testing.T, fieldName, fieldValue string, minLength int) {
	if len(fieldValue) < minLength {
		t.Errorf("[%s] Value in field '%s', should be at least %d chars. Got '%s'",
			source(), fieldName, minLength, fieldValue)
	}
}
