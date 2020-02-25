package workers_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/APTrust/exchange/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Test server to handle Pharos requests
var spotPharosTestServer = httptest.NewServer(http.HandlerFunc(spotPharosHandler))

func getSpotRestoreWorker(t *testing.T) *workers.APTSpotTestRestore {
	_context, err := testutil.GetContext("integration.json")
	require.Nil(t, err)
	if !testutil.ShouldRunIntegrationTests() {
		_context.PharosClient = getPharosClientForTest(pharosTestServer.URL)
	}
	worker := workers.NewAPTSpotTestRestore(_context, 1000000,
		testutil.TEST_TIMESTAMP, testutil.TEST_TIMESTAMP)
	require.NotNil(t, worker)
	client, _ := network.NewPharosClient(spotPharosTestServer.URL, "v2", "frankzappa", "abcxyz")
	worker.Context.PharosClient = client
	require.Equal(t, _context, worker.Context)
	require.EqualValues(t, 1000000, worker.MaxSize)
	require.Equal(t, testutil.TEST_TIMESTAMP, worker.CreatedBefore)
	require.Equal(t, testutil.TEST_TIMESTAMP, worker.NotRestoredSince)
	return worker
}

func TestSpotGetInsitutions(t *testing.T) {
	worker := getSpotRestoreWorker(t)
	institutions, err := worker.GetInstitutions()
	require.Nil(t, err)
	assert.Equal(t, 4, len(institutions))
}

func TestSpotGetObjectFor(t *testing.T) {
	worker := getSpotRestoreWorker(t)
	obj, err := worker.GetObjectFor("example.edu")
	require.Nil(t, err)
	assert.NotNil(t, obj)
}

func TestSpotHasCompletedRestore(t *testing.T) {
	worker := getSpotRestoreWorker(t)
	hasRestore, err := worker.HasCompletedRestore("example.edu/bag")
	require.Nil(t, err)
	assert.False(t, hasRestore)
}

func TestGetLastIngestWorkItem(t *testing.T) {
	worker := getSpotRestoreWorker(t)
	item, err := worker.GetLastIngestWorkItem("example.edu")
	require.Nil(t, err)
	assert.NotNil(t, item)
}

func TestSpotCreateWorkItem(t *testing.T) {
	worker := getSpotRestoreWorker(t)
	obj, err := worker.GetObjectFor("example.edu")
	assert.Nil(t, err)
	require.NotNil(t, obj)
	item, err := worker.CreateWorkItem(obj)
	assert.Nil(t, err)
	require.NotNil(t, item)
	assert.Equal(t, "Automated object restoration spot test created by system", item.Note)
}

func TestSpotRun(t *testing.T) {
	worker := getSpotRestoreWorker(t)
	items, err := worker.Run()
	require.Nil(t, err)
	require.NotNil(t, items)
	assert.Equal(t, 4, len(items))
}

func spotInstitutionGetHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.Institution, 4)
	for i := 0; i < len(list); i++ {
		list[i] = testutil.MakeInstitution()
	}
	data := make(map[string]interface{})
	data["count"] = 4
	data["next"] = nil
	data["previous"] = nil
	data["results"] = list
	dataJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(dataJson))
}

func spotIntellectualObjectListHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeIntellectualObject(1, 0, 0, 0)
	obj.Access = "consortia"
	obj.CreatedAt, _ = time.Parse(time.RFC3339, "2016-08-06T15:33:00+00:00")
	obj.FileSize = int64(56)
	list := make([]*models.IntellectualObject, 1)
	list[0] = obj
	data := make(map[string]interface{})
	data["count"] = 4
	data["next"] = nil
	data["previous"] = nil
	data["results"] = list
	dataJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(dataJson))
}

func spotWorkItemGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeWorkItem()
	obj.Action = constants.ActionIngest
	obj.Stage = constants.StageCleanup
	obj.Status = constants.StatusSuccess
	list := make([]*models.WorkItem, 1)
	list[0] = obj
	data := make(map[string]interface{})
	data["count"] = 4
	data["next"] = nil
	data["previous"] = nil
	data["results"] = list
	dataJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(dataJson))
}

// Returns an empty WorkItems list
func spotWorkItemEmptyGetHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.WorkItem, 0)
	data := make(map[string]interface{})
	data["count"] = 4
	data["next"] = nil
	data["previous"] = nil
	data["results"] = list
	dataJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(dataJson))
}

func spotWorkItemPostHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintln(w, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(body))
}

// pharosHandler handles all requests that the GlacierRestoreInit
// worker would send to Pharos.
func spotPharosHandler(w http.ResponseWriter, r *http.Request) {
	url := r.URL.String()
	if strings.Contains(url, "/objects/") {
		spotIntellectualObjectListHandler(w, r)
	} else if strings.Contains(url, "action=Restore") {
		spotWorkItemEmptyGetHandler(w, r)
	} else if strings.Contains(url, "/items/") {
		if r.Method == http.MethodGet {
			spotWorkItemGetHandler(w, r)
		} else {
			spotWorkItemPostHandler(w, r)
		}
	} else if strings.Contains(url, "/institutions/") {
		spotInstitutionGetHandler(w, r)
	} else {
		panic(fmt.Sprintf("Don't know how to handle request for %s", url))
	}
}
