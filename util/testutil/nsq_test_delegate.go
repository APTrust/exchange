package testutil

import (
	"github.com/nsqio/go-nsq"
	"time"
)

// NSQTestDelegate is a struct used in unit tests to capture
// NSQ messages and actions. The interface we're mocking is
// the MessageDelegate interface defined here:
// https://github.com/nsqio/go-nsq/blob/master/delegates.go#L35
type NSQTestDelegate struct {
	Message   *nsq.Message
	Delay     time.Duration
	Backoff   bool
	Operation string
}

// NewNSQTestDelegate returns a pointer to a new NSQTestDelegate.
func NewNSQTestDelegate() *NSQTestDelegate {
	return &NSQTestDelegate{}
}

// OnFinish receives the Finish() call from an NSQ message.
func (delegate *NSQTestDelegate) OnFinish(message *nsq.Message) {
	delegate.Message = message
	delegate.Operation = "finish"
}

// OnRequeue receives the Requeue() call from an NSQ message.
func (delegate *NSQTestDelegate) OnRequeue(message *nsq.Message, delay time.Duration, backoff bool) {
	delegate.Message = message
	delegate.Delay = delay
	delegate.Backoff = backoff
	delegate.Operation = "requeue"
}

// OnTouch receives the Touch() call from an NSQ message.
func (delegate *NSQTestDelegate) OnTouch(message *nsq.Message) {
	delegate.Message = message
	delegate.Operation = "touch"
}
