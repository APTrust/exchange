package models

import (
	"fmt"
	"strings"
	"sync"
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
	// during bag validation. Don't write to this. It's public so
	// we can serialize it to/from JSON, but access is locked internally
	// with a mutex. Hmm...
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

	mutex *sync.RWMutex
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
		mutex:         &sync.RWMutex{},
	}
}

func (summary *WorkSummary) Start() {
	summary.StartedAt = time.Now().UTC()
	summary.Attempted = true
	summary.AttemptNumber += 1
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
	summary.getMutex().RLock()
	succeeded := summary.Finished() && len(summary.Errors) == 0
	summary.getMutex().RUnlock()
	return succeeded
}

// A.D. 2019-09-16: Cap total errors at 30.
// In rare cases, ingest server can encounter thousands of read
// errors. If WorkSummary captures them all, the data becomes
// too large to post to Pharos.
func (summary *WorkSummary) AddError(format string, a ...interface{}) {
	if len(summary.Errors) > 29 {
		return
	}
	summary.getMutex().Lock()
	if len(summary.Errors) == 29 {
		summary.Errors = append(summary.Errors, "Too many errors")
	} else {
		summary.Errors = append(summary.Errors, fmt.Sprintf(format, a...))
	}
	summary.getMutex().Unlock()
}

func (summary *WorkSummary) ClearErrors() {
	summary.getMutex().Lock()
	summary.Errors = nil
	summary.ErrorIsFatal = false
	summary.Errors = make([]string, 0)
	summary.getMutex().Unlock()
}

func (summary *WorkSummary) HasErrors() bool {
	summary.getMutex().RLock()
	hasErrors := len(summary.Errors) > 0
	summary.getMutex().RUnlock()
	return hasErrors
}

func (summary *WorkSummary) FirstError() string {
	summary.getMutex().RLock()
	firstError := ""
	if len(summary.Errors) > 0 {
		firstError = summary.Errors[0]
	}
	summary.getMutex().RUnlock()
	return firstError
}

func (summary *WorkSummary) AllErrorsAsString() string {
	summary.getMutex().RLock()
	defer summary.getMutex().RUnlock()
	if len(summary.Errors) > 0 {
		return strings.Join(summary.Errors, "\n")
	}
	return ""
}

// getMutex returns the mutex that guards the Errors list.
// When we're restoring a WorkSummary from JSON, we have
// no guarantee the constructor is called, so this function
// ensures the mutex is present before anything tries to
// access it.
func (summary *WorkSummary) getMutex() *sync.RWMutex {
	if summary.mutex == nil {
		summary.mutex = &sync.RWMutex{}
	}
	return summary.mutex
}
