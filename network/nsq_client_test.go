package network_test

import (
	"fmt"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

var nsqTopic string
var nsqId int
var nsqTester *testing.T

func TestEnqueue(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(nsqEnqueueHandler))
	defer testServer.Close()

	client := network.NewNSQClient(testServer.URL)
	nsqTester = t
	nsqTopic = "test_topic1"
	nsqId = 5891
	err := client.Enqueue(nsqTopic, nsqId)
	assert.Nil(t, err)

	nsqTopic = "test_topic2"
	nsqId = 9353
	err = client.Enqueue(nsqTopic, nsqId)
	assert.Nil(t, err)
}

func TestNSQStats(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(nsqStatsHandler))
	defer testServer.Close()

	client := network.NewNSQClient(testServer.URL)
	stats, err := client.GetStats()
	require.Nil(t, err)
	require.NotNil(t, stats)

	require.Equal(t, 3, len(stats.Data.Topics))
	assert.Equal(t, "record_channel", stats.Data.Topics[1].Channels[0].ChannelName)
}

func nsqEnqueueHandler(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	require.Nil(nsqTester, err)
	id, err := strconv.Atoi(string(data))
	require.Nil(nsqTester, err)
	topic := r.URL.Query().Get("topic")
	assert.Equal(nsqTester, nsqTopic, topic)
	assert.Equal(nsqTester, nsqId, id)
	fmt.Fprintln(w, "OK")
}

func nsqStatsHandler(w http.ResponseWriter, r *http.Request) {
	data := `{"status_code":200,"status_txt":"OK","data":{"version":"0.3.8","health":"OK","start_time":1478555743,"topics":[{"topic_name":"fetch_topic","channels":[{"channel_name":"fetch_channel","depth":0,"backend_depth":0,"in_flight_count":0,"deferred_count":0,"message_count":16,"requeue_count":0,"timeout_count":0,"clients":[{"name":"d-128-143-197-221","client_id":"d-128-143-197-221","hostname":"d-128-143-197-221.dhcp.virginia.edu","version":"V2","remote_address":"128.143.197.221:56819","state":3,"ready_count":20,"in_flight_count":0,"message_count":16,"finish_count":16,"requeue_count":0,"connect_ts":1478555758,"sample_rate":0,"deflate":false,"snappy":false,"user_agent":"go-nsq/1.0.6","tls":false,"tls_cipher_suite":"","tls_version":"","tls_negotiated_protocol":"","tls_negotiated_protocol_is_mutual":false}],"paused":false,"e2e_processing_latency":{"count":0,"percentiles":null}}],"depth":0,"backend_depth":0,"message_count":16,"paused":false,"e2e_processing_latency":{"count":0,"percentiles":null}},{"topic_name":"record_topic","channels":[{"channel_name":"record_channel","depth":0,"backend_depth":0,"in_flight_count":0,"deferred_count":0,"message_count":11,"requeue_count":0,"timeout_count":0,"clients":[{"name":"d-128-143-197-221","client_id":"d-128-143-197-221","hostname":"d-128-143-197-221.dhcp.virginia.edu","version":"V2","remote_address":"128.143.197.221:56903","state":3,"ready_count":20,"in_flight_count":0,"message_count":11,"finish_count":11,"requeue_count":0,"connect_ts":1478555778,"sample_rate":0,"deflate":false,"snappy":false,"user_agent":"go-nsq/1.0.6","tls":false,"tls_cipher_suite":"","tls_version":"","tls_negotiated_protocol":"","tls_negotiated_protocol_is_mutual":false}],"paused":false,"e2e_processing_latency":{"count":0,"percentiles":null}}],"depth":0,"backend_depth":0,"message_count":11,"paused":false,"e2e_processing_latency":{"count":0,"percentiles":null}},{"topic_name":"store_topic","channels":[{"channel_name":"store_channel","depth":0,"backend_depth":0,"in_flight_count":0,"deferred_count":0,"message_count":11,"requeue_count":0,"timeout_count":0,"clients":[{"name":"d-128-143-197-221","client_id":"d-128-143-197-221","hostname":"d-128-143-197-221.dhcp.virginia.edu","version":"V2","remote_address":"128.143.197.221:56862","state":3,"ready_count":20,"in_flight_count":0,"message_count":11,"finish_count":11,"requeue_count":0,"connect_ts":1478555768,"sample_rate":0,"deflate":false,"snappy":false,"user_agent":"go-nsq/1.0.6","tls":false,"tls_cipher_suite":"","tls_version":"","tls_negotiated_protocol":"","tls_negotiated_protocol_is_mutual":false}],"paused":false,"e2e_processing_latency":{"count":0,"percentiles":null}}],"depth":0,"backend_depth":0,"message_count":11,"paused":false,"e2e_processing_latency":{"count":0,"percentiles":null}}]}}`
	fmt.Fprintln(w, data)
}
