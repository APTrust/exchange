package models

import (
	"encoding/json"
	apt_models "github.com/APTrust/exchange/models"
	"github.com/nsqio/go-nsq"
	"time"
)

// DPNS3FetchState holds information about the state of the Glacier
// restore process.
type DPNS3FetchState struct {
	// NSQMessage is the NSQ message being processed in this restore
	// request. Not serialized because it will change each time we
	// try to process a request.
	NSQMessage *nsq.Message `json:"-"`
	// DPNWorkItem is the DPNWorkItem being processed. This object
	// comes from Pharos.
	DPNWorkItem *apt_models.DPNWorkItem `json:"-"`
	// DPNBag is the bag we're restoring. This object comes from
	// the DPN REST server.
	DPNBag *DPNBag
	// S3Bucket is the bucket that contains the item we want to download.
	S3Bucket string
	// GlacierKey is the key we want to restore (the DPN Bag UUID).
	S3Key string
	// StartedAt is when the worker started trying to download the file.
	StartedAt time.Time
	// CompletedAt is when the worker finished downloading the file.
	CompletedAt time.Time
	// AttemptNumber is the number of times we've made this particular
	// restoration request.
	AttemptNumber int
	// ErrorMessage is the text of the error sent by the S3 or
	// as written by the DPN S3 Fetch Worker.
	ErrorMessage string
	// ErrorIsFatal indicates whether the error encountered during the
	// restoration was fatal or transient. If the key does not exist,
	// the error is fatal. This may be set to true in some other circumstances
	// as well, such as after repeated network errors, simply to prevent
	// infinite retries without administrative review.
	ErrorIsFatal bool
	// LocalPath is the path to the downloaded file on the local file system.
	LocalPath string
}

func DPNS3FetchStateFromJson(jsonString string) (*DPNS3FetchState, error) {
	state := &DPNS3FetchState{}
	err := json.Unmarshal([]byte(jsonString), state)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (state *DPNS3FetchState) ToJson() (string, error) {
	jsonStr, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	return string(jsonStr), nil
}
