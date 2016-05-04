package network_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/testdata"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNewPharosClient(t *testing.T) {
	_, err := network.NewPharosClient("http://example.com", "v1", "user", "key")
	if err != nil {
		t.Error(err)
	}
}

func TestInstitutionGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(institutionGetHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.InstitutionGet("college.edu")

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v1/institutions/college.edu/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)
	assert.EqualValues(t, "Institution", response.ObjectType())
	if response.Institution() == nil {
		t.Errorf("Institution should not be nil")
	}
	assert.NotEqual(t, "", response.Institution().Identifier)
}

func TestInstitutionList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(institutionListHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.InstitutionList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v1/institutions/?", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)
	assert.EqualValues(t, "Institution", response.ObjectType())

	list := response.Institutions()
	if list == nil {
		t.Errorf("Institution list should not be nil")
		return
	}
	if len(list) != 4 {
		t.Errorf("Institutions list should have four items. Found %d.", len(list))
		return
	}
	for _, obj := range list {
		assert.NotEqual(t, "", obj.Identifier)
	}

	// Make sure params are added to URL
	params := sampleParams()
	response = client.InstitutionList(params)
	expectedUrl := fmt.Sprintf("/api/v1/institutions/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestIntellectualObjectGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(intellectualObjectGetHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.IntellectualObjectGet("college.edu/object")

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v1/objects/college.edu%2Fobject", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj := response.IntellectualObject()
	assert.EqualValues(t, "IntellectualObject", response.ObjectType())
	if obj == nil {
		t.Errorf("IntellectualObject should not be nil")
	}
	assert.NotEqual(t, "", obj.Identifier)

	// Check that child objects were parsed correctly
	assert.Equal(t, 2, len(obj.GenericFiles))
	assert.Equal(t, 3, len(obj.PremisEvents))
	assert.Equal(t, 4, len(obj.GenericFiles[0].Checksums))
	assert.Equal(t, 5, len(obj.IngestTags))
}

func TestIntellectualObjectList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(intellectualObjectListHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.IntellectualObjectList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v1/objects/?", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)
	assert.EqualValues(t, "IntellectualObject", response.ObjectType())

	list := response.IntellectualObjects()
	if list == nil {
		t.Errorf("IntellectualObject list should not be nil")
		return
	}
	if len(list) != 4 {
		t.Errorf("IntellectualObjects list should have four items. Found %d.", len(list))
		return
	}
	for _, obj := range list {
		assert.NotEqual(t, "", obj.Identifier)
	}

	// Make sure params are added to URL
	params := sampleParams()
	response = client.IntellectualObjectList(params)
	expectedUrl := fmt.Sprintf("/api/v1/objects/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestIntellectualObjectSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(intellectualObjectSaveHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// First, test create...
	// ---------------------------------------------
	obj := testdata.MakeIntellectualObject(0,0,0,0)
	obj.Id = 0
	response := client.IntellectualObjectSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v1/objects/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.IntellectualObject()
	assert.EqualValues(t, "IntellectualObject", response.ObjectType())
	if obj == nil {
		t.Errorf("IntellectualObject should not be nil")
	}
	assert.NotEqual(t, "", obj.Identifier)

	// Make sure the client returns the SAVED object,
	// not the unsaved one we sent.
	assert.NotEqual(t, 0, obj.Id)


	// ---------------------------------------------
	// Now test with an update...
	// ---------------------------------------------
	obj = testdata.MakeIntellectualObject(0,0,0,0)
	origModTime := obj.UpdatedAt
	response = client.IntellectualObjectSave(obj)

	// Check the request URL and method
	expectedUrl := fmt.Sprintf("/api/v1/objects/%s", strings.Replace(obj.Identifier, "/", "%2F", -1))
	assert.Equal(t, "PUT", response.Request.Method)
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.IntellectualObject()
	assert.EqualValues(t, "IntellectualObject", response.ObjectType())
	if obj == nil {
		t.Errorf("IntellectualObject should not be nil")
	}
	assert.NotEqual(t, "", obj.Identifier)
	assert.Equal(t, 1000, obj.Id)
	assert.NotEqual(t, origModTime, obj.UpdatedAt)
}

func TestGenericFileGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(genericFileGetHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.GenericFileGet("college.edu/object/file.xml")

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v1/files/college.edu%2Fobject%2Ffile.xml", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj := response.GenericFile()
	assert.EqualValues(t, "GenericFile", response.ObjectType())
	if obj == nil {
		t.Errorf("GenericFile should not be nil")
	}
	assert.True(t, strings.HasPrefix(obj.Identifier, "kollege.kom/objekt"))

	// Check that child objects were parsed correctly
	assert.Equal(t, 2, len(obj.PremisEvents))
	assert.Equal(t, 3, len(obj.Checksums))
}

func TestGenericFileList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(genericFileListHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.GenericFileList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v1/files/?", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)
	assert.EqualValues(t, "GenericFile", response.ObjectType())

	list := response.GenericFiles()
	if list == nil {
		t.Errorf("GenericFile list should not be nil")
		return
	}
	if len(list) != 4 {
		t.Errorf("GenericFiles list should have four items. Found %d.", len(list))
		return
	}
	for _, obj := range list {
		assert.NotEqual(t, "", obj.Identifier)
	}

	// Make sure params are added to URL
	params := sampleParams()
	response = client.GenericFileList(params)
	expectedUrl := fmt.Sprintf("/api/v1/files/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestGenericFileSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(genericFileSaveHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// First, test create...
	// ---------------------------------------------
	obj := testdata.MakeGenericFile(0,0,"kollege.kom/objekt/file.xml")
	obj.Id = 0
	response := client.GenericFileSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v1/files/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.GenericFile()
	assert.EqualValues(t, "GenericFile", response.ObjectType())
	if obj == nil {
		t.Errorf("GenericFile should not be nil")
	}
	assert.NotEqual(t, "", obj.Identifier)

	// Make sure the client returns the SAVED object,
	// not the unsaved one we sent.
	assert.NotEqual(t, 0, obj.Id)


	// ---------------------------------------------
	// Now test with an update...
	// ---------------------------------------------
	obj = testdata.MakeGenericFile(0,0,"kollege.kom/objekt/file.xml")
	origModTime := obj.UpdatedAt
	response = client.GenericFileSave(obj)

	// Check the request URL and method
	objIdEncoded := strings.Replace(obj.Identifier, " ", "%20", -1)
	expectedUrl := fmt.Sprintf("/api/v1/files/%s", strings.Replace(objIdEncoded, "/", "%2F", -1))
	assert.Equal(t, "PUT", response.Request.Method)
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.GenericFile()
	assert.EqualValues(t, "GenericFile", response.ObjectType())
	if obj == nil {
		t.Errorf("GenericFile should not be nil")
	}
	assert.NotEqual(t, "", obj.Identifier)
	assert.Equal(t, 1000, obj.Id)
	assert.NotEqual(t, origModTime, obj.UpdatedAt)
}

func TestPremisEventGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(premisEventGetHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.PremisEventGet("000000000000-0000-0000-0000-00000000")

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v1/events/000000000000-0000-0000-0000-00000000/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj := response.PremisEvent()
	assert.EqualValues(t, "PremisEvent", response.ObjectType())
	if obj == nil {
		t.Errorf("PremisEvent should not be nil")
	}
	assert.Equal(t, "000000000000-0000-0000-0000-00000000", obj.Identifier)
	assert.NotEqual(t, "", obj.EventType)
}

func TestPremisEventList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(premisEventListHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.PremisEventList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v1/events/?", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)
	assert.EqualValues(t, "PremisEvent", response.ObjectType())

	list := response.PremisEvents()
	if list == nil {
		t.Errorf("PremisEvent list should not be nil")
		return
	}
	if len(list) != 4 {
		t.Errorf("PremisEvents list should have four items. Found %d.", len(list))
		return
	}
	for _, obj := range list {
		assert.NotEqual(t, "", obj.Identifier)
	}

	// Make sure params are added to URL
	params := sampleParams()
	response = client.PremisEventList(params)
	expectedUrl := fmt.Sprintf("/api/v1/events/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestPremisEventSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(premisEventSaveHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// Test create only. PremisEvents cannot be updaed
	// ---------------------------------------------
	obj := testdata.MakePremisEvent()
	obj.Id = 0
	response := client.PremisEventSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v1/events/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.PremisEvent()
	assert.EqualValues(t, "PremisEvent", response.ObjectType())
	if obj == nil {
		t.Errorf("PremisEvent should not be nil")
	}
	assert.NotEqual(t, "", obj.Identifier)

	// Make sure the client returns the SAVED object,
	// not the unsaved one we sent.
	assert.NotEqual(t, 0, obj.Id)
}

func TestWorkItemGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(workItemGetHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.WorkItemGet(999)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v1/work_items/999/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj := response.WorkItem()
	assert.EqualValues(t, "WorkItem", response.ObjectType())
	if obj == nil {
		t.Errorf("WorkItem should not be nil")
	}
	assert.NotEqual(t, "", obj.Action)
	assert.NotEqual(t, "", obj.Status)
	assert.NotEqual(t, "", obj.State)
	assert.True(t, obj.Retry)
}

func TestWorkItemList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(workItemListHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.WorkItemList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v1/work_items/?", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)
	assert.EqualValues(t, "WorkItem", response.ObjectType())

	list := response.WorkItems()
	if list == nil {
		t.Errorf("WorkItem list should not be nil")
		return
	}
	if len(list) != 4 {
		t.Errorf("WorkItems list should have four items. Found %d.", len(list))
		return
	}
	for _, obj := range list {
		assert.NotEqual(t, "", obj.Action)
		assert.NotEqual(t, "", obj.Status)
		assert.NotEqual(t, "", obj.State)
	}

	// Make sure params are added to URL
	params := sampleParams()
	response = client.WorkItemList(params)
	expectedUrl := fmt.Sprintf("/api/v1/work_items/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestWorkItemSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(workItemSaveHander))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// First, test create...
	// ---------------------------------------------
	obj := testdata.MakeWorkItem()
	obj.Id = 0
	response := client.WorkItemSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v1/work_items/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.WorkItem()
	assert.EqualValues(t, "WorkItem", response.ObjectType())
	if obj == nil {
		t.Errorf("WorkItem should not be nil")
	}
	assert.NotEqual(t, "", obj.Name)
	assert.NotEqual(t, "", obj.Bucket)
	assert.NotEqual(t, "", obj.ETag)
	assert.NotEqual(t, "", obj.Action)
	assert.NotEqual(t, "", obj.Stage)

	// Make sure the client returns the SAVED object,
	// not the unsaved one we sent.
	assert.NotEqual(t, 0, obj.Id)


	// ---------------------------------------------
	// Now test with an update...
	// ---------------------------------------------
	obj = testdata.MakeWorkItem()
	origModTime := obj.UpdatedAt
	response = client.WorkItemSave(obj)

	// Check the request URL and method
	expectedUrl := fmt.Sprintf("/api/v1/work_items/%d/", obj.Id)
	assert.Equal(t, "PUT", response.Request.Method)
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.WorkItem()
	assert.EqualValues(t, "WorkItem", response.ObjectType())
	if obj == nil {
		t.Errorf("WorkItem should not be nil")
	}
	assert.Equal(t, 1000, obj.Id)
	assert.NotEqual(t, origModTime, obj.UpdatedAt)
}


// -------------------------------------------------------------------------
// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

// Build a simple struct that mimics the structure of a Pharos
// JSON list response. That includes keys count, next, previous,
// and results. The caller will add ["results"] with a list of
// objects of the appropriate type.
func listResponseData() (map[string]interface{}) {
	data := make(map[string]interface{})
	data["count"] = 100
	data["next"] = "http://example.com/?page=11"
	data["previous"] = "http://example.com/?page=9"
	return data
}

// Returns some sample URL parameters.
func sampleParams() (url.Values) {
	v := url.Values{}
	v.Add("institution", "aptrust.org")
	v.Add("page", "1")
	v.Add("per_page", "20")
	v.Add("action", "ingest")
	return v
}

// -------------------------------------------------------------------------
// Institution handlers
// -------------------------------------------------------------------------

func institutionGetHander(w http.ResponseWriter, r *http.Request) {
	obj := testdata.MakeInstitution()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func institutionListHander(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.Institution, 4)
	for i := 0; i < 4; i++ {
		list[i] = testdata.MakeInstitution()
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}


// -------------------------------------------------------------------------
// IntellectualObject handlers
// -------------------------------------------------------------------------

func intellectualObjectGetHander(w http.ResponseWriter, r *http.Request) {
	obj := testdata.MakeIntellectualObject(2,3,4,5)
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func intellectualObjectListHander(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.IntellectualObject, 4)
	for i := 0; i < 4; i++ {
		list[i] = testdata.MakeIntellectualObject(2,3,4,5)
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func intellectualObjectSaveHander(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
    data := make(map[string]interface{})
    err := decoder.Decode(&data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON data: %v", err)
		fmt.Fprintln(w, "")
		return
	}
	// Assign ID and timestamps, as if the object has been saved.
	data["id"] = 1000
	data["created_at"] = time.Now().UTC()
	data["updated_at"] = time.Now().UTC()
	objJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

// -------------------------------------------------------------------------
// GenericFile handlers
// -------------------------------------------------------------------------

func genericFileGetHander(w http.ResponseWriter, r *http.Request) {
	obj := testdata.MakeGenericFile(2,3, "kollege.kom/objekt")
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func genericFileListHander(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.GenericFile, 4)
	for i := 0; i < 4; i++ {
		list[i] = testdata.MakeGenericFile(2,3,"kollege.kom/objekt")
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func genericFileSaveHander(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
    data := make(map[string]interface{})
    err := decoder.Decode(&data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON data: %v", err)
		fmt.Fprintln(w, "")
		return
	}

	// Assign ID and timestamps, as if the object has been saved.
	data["id"] = 1000
	data["created_at"] = time.Now().UTC()
	data["updated_at"] = time.Now().UTC()
	objJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

// -------------------------------------------------------------------------
// PremisEvent handlers
// -------------------------------------------------------------------------

func premisEventGetHander(w http.ResponseWriter, r *http.Request) {
	obj := testdata.MakePremisEvent()
	obj.Identifier = "000000000000-0000-0000-0000-00000000"
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func premisEventListHander(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.PremisEvent, 4)
	for i := 0; i < 4; i++ {
		list[i] = testdata.MakePremisEvent()
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func premisEventSaveHander(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
    data := make(map[string]interface{})
    err := decoder.Decode(&data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON data: %v", err)
		fmt.Fprintln(w, "")
		return
	}

	// Assign ID and timestamps, as if the object has been saved.
	data["id"] = 1000
	data["created_at"] = time.Now().UTC()
	data["updated_at"] = time.Now().UTC()
	objJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

// -------------------------------------------------------------------------
// WorkItem handlers
// -------------------------------------------------------------------------

func workItemGetHander(w http.ResponseWriter, r *http.Request) {
	obj := testdata.MakeWorkItem()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func workItemListHander(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.WorkItem, 4)
	for i := 0; i < 4; i++ {
		list[i] = testdata.MakeWorkItem()
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func workItemSaveHander(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
    data := make(map[string]interface{})
    err := decoder.Decode(&data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON data: %v", err)
		fmt.Fprintln(w, "")
		return
	}

	// Assign ID and timestamps, as if the object has been saved.
	data["id"] = 1000
	data["created_at"] = time.Now().UTC()
	data["updated_at"] = time.Now().UTC()
	objJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}
