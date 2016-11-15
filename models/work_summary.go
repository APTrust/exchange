package models

import (
	"fmt"
	"strings"
	"time"
)

type WorkSummary struct {
	// This is set to true when the process that produces
	// this result starts.
	Attempted bool

	// AttemptNumber is the number of the read attempt.
	// This starts at one. This is uint16 to match the datatype
	// of NsqMessage.Attempt.
	AttemptNumber uint16

	// This will be set to true if an error is fatal. In that
	// case, we should not try to reprocess the item.
	ErrorIsFatal bool

	// Errors is a list of strings describing errors that occurred
	// during bag validation.
	Errors []string

	// StartedAt describes when the attempt to read the bag started.
	// If StartedAt.IsZero(), we have not yet attempted to read the
	// bag.
	StartedAt time.Time

	// FinishedAt describes when the attempt to read the bag completed.
	// If FinishedAt.IsZero(), we have not yet attempted to read the
	// bag. Note that the attempt may have completed without succeeding.
	// Check the Succeeded() method to see if the process actually
	// completed successfully.
	FinishedAt time.Time

	// Retry indicates whether we should retry a failed process.
	// After non-fatal errors, such as network timeout, this will
	// generally be set to true. For fatal errors, such as invalid
	// data, this will generally be set to false. This defaults to
	// true, because fatal errors are rare, and we don't want to
	// give up on transient errors. Just requeue and try again.
	Retry bool
}

func NewWorkSummary() *WorkSummary {
	return &WorkSummary{
		Attempted:     false,
		AttemptNumber: 0,
		ErrorIsFatal:  false,
		Errors:        make([]string, 0),
		StartedAt:     time.Time{},
		FinishedAt:    time.Time{},
		Retry:         true,
	}
}

func (summary *WorkSummary) Start() {
	summary.StartedAt = time.Now().UTC()
}

func (summary *WorkSummary) Started() bool {
	return !summary.StartedAt.IsZero()
}

func (summary *WorkSummary) Finish() {
	summary.FinishedAt = time.Now().UTC()
}

func (summary *WorkSummary) Finished() bool {
	return !summary.FinishedAt.IsZero()
}

func (summary *WorkSummary) RunTime() time.Duration {
	startTime := summary.StartedAt
	if startTime.IsZero() {
		return time.Duration(0)
	}
	endTime := summary.FinishedAt
	if endTime.IsZero() {
		endTime = time.Now()
	}
	return endTime.Sub(startTime)
}

func (summary *WorkSummary) Succeeded() bool {
	return summary.Finished() && len(summary.Errors) == 0
}

func (summary *WorkSummary) AddError(format string, a ...interface{}) {
	summary.Errors = append(summary.Errors, fmt.Sprintf(format, a...))
}

func (summary *WorkSummary) ClearErrors() {
	summary.Errors = nil
	summary.ErrorIsFatal = false
	summary.Errors = make([]string, 0)
}

func (summary *WorkSummary) HasErrors() bool {
	return len(summary.Errors) > 0
}

func (summary *WorkSummary) FirstError() string {
	firstError := ""
	if len(summary.Errors) > 0 {
		firstError = summary.Errors[0]
	}
	return firstError
}

func (summary *WorkSummary) AllErrorsAsString() string {
	if len(summary.Errors) > 0 {
		return strings.Join(summary.Errors, "\n")
	}
	return ""
}
