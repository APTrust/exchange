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
	testServer := httptest.NewServer(http.HandlerFunc(nsqHandler))
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


func nsqHandler(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	require.Nil(nsqTester, err)
	id, err := strconv.Atoi(string(data))
	require.Nil(nsqTester, err)
	topic := r.URL.Query().Get("topic")
	assert.Equal(nsqTester, nsqTopic, topic)
	assert.Equal(nsqTester, nsqId, id)
	fmt.Fprintln(w, "OK")
}
