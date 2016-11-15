package network

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/nsqio/nsq/nsqd"
	"io/ioutil"
	"net/http"
	"strconv"
)

// NSQStats contains info about the status of NSQ and its topics
// and queues. This info comes from a GET call to the /stats endpoint.
type NSQStats struct {
	StatusCode int          `json:"status_code"`
	StatusText string       `json:"status_txt"`
	Data       NSQStatsData `json:"data"`
}

// NSQStats data contains the important info returned by a call
// to NSQ's /stats endpoint, including the number of items in each
// topic and queue.
type NSQStatsData struct {
	Version string            `json:"version"`
	Health  string            `json:"status_code"`
	Topics  []nsqd.TopicStats `json:"topics"`
}

type NSQClient struct {
	URL string
}

// Returns a new NSQ client that will connect to the NSQ server
// and the specified url. The URL is typically available through
// Config.NsqdHttpAddress, and usually ends with :4151. This is
// the URL to which we post items we want to queue, and from
// which our workers read.
//
// Note that this client provides write access to queue, so we can
// add things. It does not provide read access. The workers do the
// reading.
func NewNSQClient(url string) *NSQClient {
	return &NSQClient{URL: url}
}

// Posts data to NSQ, which essentially means putting it into a work topic.
// Param topic is the topic under which you want to queue something.
// For example, prepare_topic, fixity_topic, etc.
// Param workItemId is the id of the WorkItem record in Pharos we want to queue.
func (client *NSQClient) Enqueue(topic string, workItemId int) error {
	url := fmt.Sprintf("%s/put?topic=%s", client.URL, topic)
	idAsString := strconv.Itoa(workItemId)
	resp, err := http.Post(url, "text/html", bytes.NewBuffer([]byte(idAsString)))
	if err != nil {
		return fmt.Errorf("Nsqd returned an error when queuing data: %v", err)
	}
	if resp == nil {
		return fmt.Errorf("No response from nsqd at '%s'. Is it running?", url)
	}

	// nsqd sends a simple OK. We have to read the response body,
	// or the connection will hang open forever.
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyText := "[no response body]"
		if len(body) > 0 {
			bodyText = string(body)
		}
		return fmt.Errorf("nsqd returned status code %d when attempting to queue data. "+
			"Response body: %s", resp.StatusCode, bodyText)
	}
	return nil
}

// GetStats allows us to get some basic stats from NSQ. The NSQ /stats endpoint
// returns a richer set of stats than what this fuction returns, but we only
// need some basic data for integration tests, so that's all we're parsing.
// The return value is a map whose key is the topic name and whose value is
// an NSQTopicStats object. NSQ is supposed to support topic_name as a query
// param, but this doesn't seem to be working in NSQ 0.3.0, so we're just
// returning stats for all topics right now. Also note that requests to
// /stats/ (with trailing slash) produce a 404.
func (client *NSQClient) GetStats() (*NSQStats, error) {
	url := fmt.Sprintf("%s/stats?format=json", client.URL)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("NSQ returned status code %d, body: %s",
			resp.StatusCode, body)
	}
	stats := &NSQStats{}
	err = json.Unmarshal(body, stats)
	if err != nil {
		return nil, err
	}
	return stats, nil
}
