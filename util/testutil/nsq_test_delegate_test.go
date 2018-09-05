package testutil_test

import (
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestOnFinish(t *testing.T) {
	delegate := testutil.NewNSQTestDelegate()
	message := testutil.MakeNsqMessage("hello")
	delegate.OnFinish(message)
	assert.Equal(t, message.Body, delegate.Message.Body)
	assert.Equal(t, "finish", delegate.Operation)
}

func TestOnRequeue(t *testing.T) {
	delegate := testutil.NewNSQTestDelegate()
	message := testutil.MakeNsqMessage("hello")
	delegate.OnRequeue(message, time.Minute*3, true)
	assert.Equal(t, message.Body, delegate.Message.Body)
	assert.Equal(t, "requeue", delegate.Operation)
	assert.Equal(t, time.Minute*3, delegate.Delay)
	assert.True(t, delegate.Backoff)
}

func TestOnTouch(t *testing.T) {
	delegate := testutil.NewNSQTestDelegate()
	message := testutil.MakeNsqMessage("hello")
	delegate.OnTouch(message)
	assert.Equal(t, message.Body, delegate.Message.Body)
	assert.Equal(t, "touch", delegate.Operation)
}
