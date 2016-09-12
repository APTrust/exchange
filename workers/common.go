package workers

import (
	"github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
)

// Creates and returns an NSQ consumer for a worker process.
func CreateNsqConsumer(config *models.Config, workerConfig *models.WorkerConfig) (*nsq.Consumer, error) {
	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", workerConfig.MaxInFlight)
	nsqConfig.Set("heartbeat_interval", workerConfig.HeartbeatInterval)
	nsqConfig.Set("max_attempts", workerConfig.MaxAttempts)
	nsqConfig.Set("read_timeout", workerConfig.ReadTimeout)
	nsqConfig.Set("write_timeout", workerConfig.WriteTimeout)
	nsqConfig.Set("msg_timeout", workerConfig.MessageTimeout)
	return nsq.NewConsumer(workerConfig.NsqTopic, workerConfig.NsqChannel, nsqConfig)
}
