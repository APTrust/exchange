package results_test

import (
	"github.com/APTrust/exchange/results"
	"testing"
	"time"
)

func TestNewResult(t *testing.T) {
	s := results.NewSummary()
	if s.Attempted {
		t.Errorf("result.Attempted should be false")
	}
	if s.AttemptNumber != 1 {
		t.Errorf("result.AttemptNumber: expected 1, got %d", s.AttemptNumber)
	}
	if s.Errors == nil {
		t.Errorf("result.Errors should not be nil")
	}
 	if !s.StartedAt.IsZero() {
		t.Errorf("result.StartedAt should be zero, but it's %v", s.StartedAt)
	}
 	if !s.FinishedAt.IsZero() {
		t.Errorf("result.FinishedAt should be zero, but it's %v", s.FinishedAt)
	}
	if s.Retry == false {
		t.Errorf("result.Retry should be true")
	}
}

func TestResultStart(t *testing.T) {
	s := results.NewSummary()
	s.Start()
	if s.StartedAt.IsZero() {
		t.Errorf("result.StartedAt should not be zero")
	}
}

func TestResultStarted(t *testing.T) {
	s := results.NewSummary()
	s.Start()
	if s.Started() == false {
		t.Errorf("result.Started() should have returned true")
	}
}

func TestResultFinish(t *testing.T) {
	s := results.NewSummary()
	s.Finish()
	if s.FinishedAt.IsZero() {
		t.Errorf("result.FinishedAt should not be zero")
	}
}

func TestResultFinished(t *testing.T) {
	s := results.NewSummary()
	s.Finish()
	if s.Finished() == false {
		t.Errorf("result.Finished() should have returned true")
	}
}

func TestResultRuntime(t *testing.T) {
	s := results.NewSummary()
	now := time.Now()
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	s.StartedAt = fiveMinutesAgo
	s.FinishedAt = now
	if s.RunTime() != 5 * time.Minute {
		t.Errorf("result.RunTime() returned %v; expected 5 minutes",
			s.RunTime())
	}
}

func TestResultSucceeded(t *testing.T) {
	s := results.NewSummary()

	// Not finished.
	if s.Succeeded() == true {
		t.Errorf("s.Succeeded() should have returned false")
	}

	// Finished with no errors
	s.Finish()
	if s.Succeeded() == false {
		t.Errorf("s.Succeeded() should have returned true")
	}

	// Finished with errors
	s.AddError("Oopsie!")
	if s.Succeeded() == true {
		t.Errorf("s.Succeeded() should have returned false")
	}
}

func TestAddError(t *testing.T) {
	s := results.NewSummary()
	s.AddError("First error is number %d", 1)
	if len(s.Errors) != 1 {
		t.Errorf("Expected 1 error, found %d", len(s.Errors))
	}
	if s.Errors[0] != "First error is number 1" {
		t.Errorf("Incorrect text if Error 1: %s", s.Errors[0])
	}
	s.AddError("%s error is number %d", "Second", 2)
	if len(s.Errors) != 2 {
		t.Errorf("Expected 2 errors, found %d", len(s.Errors))
	}
	if s.Errors[1] != "Second error is number 2" {
		t.Errorf("Incorrect text if Error 2: %s", s.Errors[0])
	}
}
