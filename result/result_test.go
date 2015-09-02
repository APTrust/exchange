package result_test

import (
	"github.com/APTrust/exchange/result"
	"testing"
	"time"
)

func TestNewResult(t *testing.T) {
	r := result.NewResult()
	if r.Attempted {
		t.Errorf("result.Attempted should be false")
	}
	if r.AttemptNumber != 1 {
		t.Errorf("result.AttemptNumber: expected 1, got %d", r.AttemptNumber)
	}
	if r.Errors == nil {
		t.Errorf("result.Errors should not be nil")
	}
 	if !r.StartedAt.IsZero() {
		t.Errorf("result.StartedAt should be zero, but it's %v", r.StartedAt)
	}
 	if !r.FinishedAt.IsZero() {
		t.Errorf("result.FinishedAt should be zero, but it's %v", r.FinishedAt)
	}
	if r.Retry == false {
		t.Errorf("result.Retry should be true")
	}
}

func TestResultStart(t *testing.T) {
	r := result.NewResult()
	r.Start()
	if r.StartedAt.IsZero() {
		t.Errorf("result.StartedAt should not be zero")
	}
}

func TestResultStarted(t *testing.T) {
	r := result.NewResult()
	r.Start()
	if r.Started() == false {
		t.Errorf("result.Started() should have returned true")
	}
}

func TestResultFinish(t *testing.T) {
	r := result.NewResult()
	r.Finish()
	if r.FinishedAt.IsZero() {
		t.Errorf("result.FinishedAt should not be zero")
	}
}

func TestResultFinished(t *testing.T) {
	r := result.NewResult()
	r.Finish()
	if r.Finished() == false {
		t.Errorf("result.Finished() should have returned true")
	}
}

func TestResultRuntime(t *testing.T) {
	r := result.NewResult()
	now := time.Now()
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	r.StartedAt = fiveMinutesAgo
	r.FinishedAt = now
	if r.RunTime() != 5 * time.Minute {
		t.Errorf("result.RunTime() returned %v; expected 5 minutes",
			r.RunTime())
	}
}

func TestResultSucceeded(t *testing.T) {
	r := result.NewResult()

	// Not finished.
	if r.Succeeded() == true {
		t.Errorf("r.Succeeded() should have returned false")
	}

	// Finished with no errors
	r.Finish()
	if r.Succeeded() == false {
		t.Errorf("r.Succeeded() should have returned true")
	}

	// Finished with errors
	r.AddError("Oopsie!")
	if r.Succeeded() == true {
		t.Errorf("r.Succeeded() should have returned false")
	}
}

func TestAddError(t *testing.T) {
	r := result.NewResult()
	r.AddError("First error is number %d", 1)
	if len(r.Errors) != 1 {
		t.Errorf("Expected 1 error, found %d", len(r.Errors))
	}
	if r.Errors[0] != "First error is number 1" {
		t.Errorf("Incorrect text if Error 1: %s", r.Errors[0])
	}
	r.AddError("%s error is number %d", "Second", 2)
	if len(r.Errors) != 2 {
		t.Errorf("Expected 2 errors, found %d", len(r.Errors))
	}
	if r.Errors[1] != "Second error is number 2" {
		t.Errorf("Incorrect text if Error 2: %s", r.Errors[0])
	}
}
