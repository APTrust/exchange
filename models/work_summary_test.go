package models_test

import (
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewResult(t *testing.T) {
	s := models.NewWorkSummary()
	assert.False(t, s.Attempted)
	assert.Equal(t, 0, s.AttemptNumber)
	assert.NotNil(t, s.Errors)
	assert.Equal(t, 0, len(s.Errors))
	assert.True(t, s.StartedAt.IsZero())
	assert.True(t, s.FinishedAt.IsZero())
	assert.True(t, s.Retry)
}

func TestResultStart(t *testing.T) {
	s := models.NewWorkSummary()
	assert.True(t, s.StartedAt.IsZero())
	s.Start()
	assert.False(t, s.StartedAt.IsZero())
}

func TestResultStarted(t *testing.T) {
	s := models.NewWorkSummary()
	assert.False(t, s.Started())
	s.Start()
	assert.True(t, s.Started())
}

func TestResultFinish(t *testing.T) {
	s := models.NewWorkSummary()
	assert.True(t, s.FinishedAt.IsZero())
	s.Finish()
	assert.False(t, s.FinishedAt.IsZero())
}

func TestResultFinished(t *testing.T) {
	s := models.NewWorkSummary()
	s.Finish()
	if s.Finished() == false {
		t.Errorf("result.Finished() should have returned true")
	}
}

func TestResultRuntime(t *testing.T) {
	s := models.NewWorkSummary()
	now := time.Now()
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	s.StartedAt = fiveMinutesAgo
	s.FinishedAt = now
	assert.EqualValues(t, 5 * time.Minute, s.RunTime())
}

func TestResultSucceeded(t *testing.T) {
	s := models.NewWorkSummary()

	// Not finished.
	assert.False(t, s.Succeeded())

	// Finished with no errors
	s.Finish()
	assert.True(t, s.Succeeded())

	// Finished with errors
	s.AddError("Oopsie!")
	assert.False(t, s.Succeeded())
}

func TestAddError(t *testing.T) {
	s := models.NewWorkSummary()
	s.AddError("First error is number %d", 1)
	assert.Equal(t, 1, len(s.Errors))
	assert.Equal(t, "First error is number 1", s.Errors[0])

	s.AddError("%s error is number %d", "Second", 2)
	assert.Equal(t, 2, len(s.Errors))
	assert.Equal(t, "Second error is number 2", s.Errors[1])
}

func TestHasErrors(t *testing.T) {
	s := models.NewWorkSummary()
	assert.False(t, s.HasErrors())
	s.AddError("First error is number %d", 1)
	assert.True(t, s.HasErrors())
}

func TestClearErrors(t *testing.T) {
	s := models.NewWorkSummary()
	s.AddError("First error is number %d", 1)
	assert.NotEmpty(t, s.Errors)
	s.ClearErrors()
	assert.Empty(t, s.Errors)
}

func TestFirstError(t *testing.T) {
	s := models.NewWorkSummary()
	assert.Equal(t, "", s.FirstError())
	s.AddError("First error is number %d", 1)
	assert.Equal(t, "First error is number 1", s.FirstError())
	s.AddError("Second error is number %d", 2)
	assert.Equal(t, "First error is number 1", s.FirstError())
}

func TestAllErrorsAsString(t *testing.T) {
	s := models.NewWorkSummary()
	s.AddError("First error is number %d", 1)
	s.AddError("Second error is number %d", 2)
	assert.Equal(t, "First error is number 1\nSecond error is number 2", s.AllErrorsAsString())
}
