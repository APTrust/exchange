package network_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNewPharosClient(t *testing.T) {
	_, err := network.NewPharosClient("http://example.com", "v2", "user", "key")
	if err != nil {
		t.Error(err)
	}
}

func TestInstitutionGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(institutionGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.InstitutionGet("college.edu")

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/institutions/college.edu/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)
	assert.EqualValues(t, "Institution", response.ObjectType())
	if response.Institution() == nil {
		t.Errorf("Institution should not be nil")
	}
	assert.NotEqual(t, "", response.Institution().Identifier)
}

func TestInstitutionList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(institutionListHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.InstitutionList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/institutions/?", response.Request.URL.Opaque)

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
	expectedUrl := fmt.Sprintf("/api/v2/institutions/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestIntellectualObjectGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(intellectualObjectGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.IntellectualObjectGet("college.edu/object", false, false)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/objects/college.edu%2Fobject", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj := response.IntellectualObject()
	assert.EqualValues(t, "IntellectualObject", response.ObjectType())
	if obj == nil {
		t.Errorf("IntellectualObject should not be nil")
	}
	assert.NotEqual(t, "", obj.Identifier)
	assert.NotEmpty(t, obj.DPNUUID)
	assert.NotEmpty(t, obj.ETag)

	// Check that child objects were parsed correctly
	assert.Equal(t, 2, len(obj.GenericFiles))
	assert.Equal(t, 3, len(obj.PremisEvents))
	assert.Equal(t, 4, len(obj.GenericFiles[0].Checksums))
	assert.Equal(t, 5, len(obj.IngestTags))

	// Check with includeFiles option
	response = client.IntellectualObjectGet("college.edu/object", true, false)
	assert.Equal(t,
		"/api/v2/objects/college.edu%2Fobject?include_files=true",
		response.Request.URL.Opaque)
	// Check with includeEvents option
	response = client.IntellectualObjectGet("college.edu/object", false, true)
	assert.Equal(t,
		"/api/v2/objects/college.edu%2Fobject?include_events=true",
		response.Request.URL.Opaque)
	// Check with includeFiles and includeEvents options
	response = client.IntellectualObjectGet("college.edu/object", true, true)
	assert.Equal(t,
		"/api/v2/objects/college.edu%2Fobject?include_all_relations=true",
		response.Request.URL.Opaque)
}

func TestIntellectualObjectList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(intellectualObjectListHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.IntellectualObjectList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/objects/?", response.Request.URL.Opaque)

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
		assert.NotEmpty(t, obj.DPNUUID)
		assert.NotEmpty(t, obj.ETag)
	}

	// Make sure params are added to URL
	params := sampleParams()
	response = client.IntellectualObjectList(params)
	expectedUrl := fmt.Sprintf("/api/v2/objects/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestIntellectualObjectSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(intellectualObjectSaveHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// First, test create...
	// ---------------------------------------------
	obj := testutil.MakeIntellectualObject(0, 0, 0, 0)
	obj.Id = 0
	response := client.IntellectualObjectSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	expectedUrl := fmt.Sprintf("/api/v2/objects/%s", obj.Institution)
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.IntellectualObject()
	assert.EqualValues(t, "IntellectualObject", response.ObjectType())
	if obj == nil {
		t.Errorf("IntellectualObject should not be nil")
	}
	assert.NotEqual(t, "", obj.Identifier)
	assert.NotEmpty(t, obj.DPNUUID)
	assert.NotEmpty(t, obj.ETag)

	// Make sure the client returns the SAVED object,
	// not the unsaved one we sent.
	assert.NotEqual(t, 0, obj.Id)

	// ---------------------------------------------
	// Now test with an update...
	// ---------------------------------------------
	obj = testutil.MakeIntellectualObject(0, 0, 0, 0)
	origModTime := obj.UpdatedAt
	response = client.IntellectualObjectSave(obj)

	// Check the request URL and method
	expectedUrl = fmt.Sprintf("/api/v2/objects/%s", strings.Replace(obj.Identifier, "/", "%2F", -1))
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

func TestIntellectualObjectPushToDPN(t *testing.T) {
	// Note the handler. That's not an error, because this call returns
	// a WorkItem object.
	testServer := httptest.NewServer(http.HandlerFunc(workItemGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// This method is used only in integration tests.
	response := client.IntellectualObjectPushToDPN("college.edu/object")

	// Check the request URL and method
	assert.Equal(t, "PUT", response.Request.Method)
	assert.Equal(t, "/api/v2/objects/college.edu%2Fobject/dpn", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	assert.EqualValues(t, "WorkItem", response.ObjectType())
	item := response.WorkItem()
	require.NotNil(t, item)
	assert.NotEqual(t, 0, item.Id)
	assert.NotEqual(t, "", item.ObjectIdentifier)
}

func TestIntellectualObjectRequestRestore(t *testing.T) {
	// We just need a handler that returns a WorkItem object.
	testServer := httptest.NewServer(http.HandlerFunc(workItemGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// This method is used only in integration tests.
	response := client.IntellectualObjectRequestRestore("college.edu/object")

	// Check the request URL and method.
	assert.Equal(t, "PUT", response.Request.Method)
	assert.Equal(t, "/api/v2/objects/college.edu%2Fobject/restore", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	assert.EqualValues(t, "WorkItem", response.ObjectType())
	item := response.WorkItem()
	require.NotNil(t, item)
	assert.NotEqual(t, 0, item.Id)
	assert.NotEqual(t, "", item.ObjectIdentifier)
}

func TestGenericFileGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(genericFileGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.GenericFileGet("college.edu/object/file.xml", false)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/files/college.edu%2Fobject%2Ffile.xml", response.Request.URL.Opaque)

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
	testServer := httptest.NewServer(http.HandlerFunc(genericFileListHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.GenericFileList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/files/?", response.Request.URL.Opaque)

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
	expectedUrl := fmt.Sprintf("/api/v2/files/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestGenericFileSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(genericFileSaveHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// First, test create...
	// ---------------------------------------------
	obj := testutil.MakeGenericFile(0, 0, "kollege.kom/objekt/file.xml")
	obj.Id = 0
	response := client.GenericFileSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v2/files/", response.Request.URL.Opaque)

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
	obj = testutil.MakeGenericFile(0, 0, "kollege.kom/objekt/file.xml")
	origModTime := obj.UpdatedAt
	response = client.GenericFileSave(obj)

	expectedUrl := fmt.Sprintf("/api/v2/files/%s", url.QueryEscape(obj.Identifier))
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

func TestGenericFileSaveBatch(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(genericFileSaveBatchHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// Make an IntellectualObject with 5 GenericFiles,
	// each with 2 checksums and 2 events.
	// ---------------------------------------------
	obj := testutil.MakeIntellectualObject(5, 2, 2, 0)
	for _, gf := range obj.GenericFiles {
		gf.Id = 0
	}
	response := client.GenericFileSaveBatch(obj.GenericFiles)

	// Check the request URL and method
	expectedUrl := fmt.Sprintf("/api/v2/files/%d/create_batch", obj.GenericFiles[0].IntellectualObjectId)
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)

	// Basic sanity check on response values
	require.Nil(t, response.Error)

	savedFiles := response.GenericFiles()
	assert.EqualValues(t, "GenericFile", response.ObjectType())
	assert.Equal(t, 5, len(savedFiles))

	// Make sure the client returns the SAVED object,
	// not the unsaved one we sent.
	for _, gf := range savedFiles {
		assert.NotEqual(t, 0, gf.Id)
		assert.Equal(t, 2, len(gf.Checksums))
		assert.Equal(t, 2, len(gf.PremisEvents))
	}
}

func TestGenericFileRequestRestore(t *testing.T) {
	// We just need a handler that returns a WorkItem object.
	testServer := httptest.NewServer(http.HandlerFunc(workItemGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// This method is used only in integration tests.
	response := client.GenericFileRequestRestore("college.edu/object/data/file.txt")

	// Check the request URL and method.
	assert.Equal(t, "PUT", response.Request.Method)
	assert.Equal(t, "/api/v2/files/restore/college.edu%2Fobject%2Fdata%2Ffile.txt",
		response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	assert.EqualValues(t, "WorkItem", response.ObjectType())
	item := response.WorkItem()
	require.NotNil(t, item)
	assert.NotEqual(t, 0, item.Id)
	assert.NotEqual(t, "", item.ObjectIdentifier)
}

func TestCheckumGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(checksumGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.ChecksumGet(123)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/checksums/123/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	assert.EqualValues(t, "Checksum", response.ObjectType())
	obj := response.Checksum()
	if obj == nil {
		t.Errorf("Checksum should not be nil")
	}
	assert.NotEqual(t, "", obj.Digest)
}

func TestCheckumList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(checksumListHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	params := url.Values{}
	params.Add("generic_file_identifier", "test.edu/obj1/file.txt")
	response := client.ChecksumList(params)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/checksums/?generic_file_identifier=test.edu%2Fobj1%2Ffile.txt",
		response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	assert.EqualValues(t, "Checksum", response.ObjectType())
	list := response.Checksums()
	if list == nil {
		t.Errorf("Checksums should not be nil")
	}
	assert.Equal(t, 4, len(list))
}

func TestCheckumSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(checksumSaveHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	checksum := testutil.MakeChecksum()
	response := client.ChecksumSave(checksum, "test.edu/obj1/file.txt")

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v2/checksums/test.edu%2Fobj1%2Ffile.txt", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	assert.EqualValues(t, "Checksum", response.ObjectType())
	obj := response.Checksum()
	if obj == nil {
		t.Errorf("Checksum should not be nil")
	}
	assert.NotEqual(t, checksum.CreatedAt, obj.CreatedAt)
	assert.Equal(t, checksum.Digest, obj.Digest)
}

func TestPremisEventGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(premisEventGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.PremisEventGet("000000000000-0000-0000-0000-00000000")

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/events/000000000000-0000-0000-0000-00000000/", response.Request.URL.Opaque)

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
	testServer := httptest.NewServer(http.HandlerFunc(premisEventListHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.PremisEventList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/events/?", response.Request.URL.Opaque)

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
	expectedUrl := fmt.Sprintf("/api/v2/events/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestPremisEventSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(premisEventSaveHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// Test create only. PremisEvents cannot be updaed
	// ---------------------------------------------
	obj := testutil.MakePremisEvent()
	obj.Id = 0
	response := client.PremisEventSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v2/events/", response.Request.URL.Opaque)

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
	testServer := httptest.NewServer(http.HandlerFunc(workItemGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.WorkItemGet(999)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/items/999/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj := response.WorkItem()
	assert.EqualValues(t, "WorkItem", response.ObjectType())
	if obj == nil {
		t.Errorf("WorkItem should not be nil")
	}
	assert.NotEqual(t, "", obj.Action)
	assert.NotEqual(t, "", obj.Status)
	assert.True(t, obj.Retry)
}

func TestWorkItemList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(workItemListHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.WorkItemList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/items/?", response.Request.URL.Opaque)

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
	}

	// Make sure params are added to URL
	params := sampleParams()
	response = client.WorkItemList(params)
	expectedUrl := fmt.Sprintf("/api/v2/items/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestWorkItemSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(workItemSaveHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// First, test create...
	// ---------------------------------------------
	obj := testutil.MakeWorkItem()
	obj.Id = 0
	response := client.WorkItemSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v2/items/", response.Request.URL.Opaque)

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
	obj = testutil.MakeWorkItem()
	origModTime := obj.UpdatedAt
	response = client.WorkItemSave(obj)

	// Check the request URL and method
	expectedUrl := fmt.Sprintf("/api/v2/items/%d/", obj.Id)
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

func TestWorkStateItemGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(workItemStateGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.WorkItemStateGet(999)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/item_state/999/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj := response.WorkItemState()
	assert.EqualValues(t, "WorkItemState", response.ObjectType())
	if obj == nil {
		t.Errorf("WorkItemState should not be nil")
	}
	assert.NotEqual(t, "", obj.Action)
	assert.NotEqual(t, "", obj.State)
	assert.False(t, obj.CreatedAt.IsZero())
	assert.False(t, obj.UpdatedAt.IsZero())
}

func TestWorkItemStateSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(workItemStateSaveHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// First, test create...
	// ---------------------------------------------
	obj := testutil.MakeWorkItemState()
	obj.Id = 0
	response := client.WorkItemStateSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v2/item_state/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.WorkItemState()
	assert.EqualValues(t, "WorkItemState", response.ObjectType())
	if obj == nil {
		t.Errorf("WorkItemState should not be nil")
	}
	assert.NotEqual(t, 0, obj.Id)
	assert.NotEqual(t, 0, obj.WorkItemId)
	assert.Equal(t, "Ingest", obj.Action)
	assert.Equal(t, `{"key1":"value1","key2":"value2"}`, obj.State)
	assert.False(t, obj.CreatedAt.IsZero())
	assert.False(t, obj.UpdatedAt.IsZero())

	// Make sure the client returns the SAVED object,
	// not the unsaved one we sent.
	assert.NotEqual(t, 0, obj.Id)

	// ---------------------------------------------
	// Now test with an update...
	// ---------------------------------------------
	obj = testutil.MakeWorkItemState()
	origModTime := obj.UpdatedAt
	origId := obj.Id
	origWorkItemId := obj.WorkItemId

	// Change this
	obj.State = `{"key3":"value3"}`
	response = client.WorkItemStateSave(obj)

	// Check the request URL and method
	expectedUrl := fmt.Sprintf("/api/v2/item_state/%d/", obj.Id)
	assert.Equal(t, "PUT", response.Request.Method)
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.WorkItemState()
	assert.EqualValues(t, "WorkItemState", response.ObjectType())
	if obj == nil {
		t.Errorf("WorkItemState should not be nil")
	}
	assert.Equal(t, origId, obj.Id)
	assert.Equal(t, origWorkItemId, obj.WorkItemId)
	assert.Equal(t, `{"key3":"value3"}`, obj.State)
	assert.NotEqual(t, origModTime, obj.UpdatedAt)
}

func TestDPNWorkItemGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(dpnWorkItemGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.DPNWorkItemGet(999)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/dpn_items/999/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj := response.DPNWorkItem()
	assert.EqualValues(t, "DPNWorkItem", response.ObjectType())
	if obj == nil {
		t.Errorf("DPNWorkItem should not be nil")
	}
	assert.NotEqual(t, "", obj.Task)
	assert.NotEqual(t, "", obj.Identifier)
}

func TestDPNWorkItemList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(dpnWorkItemListHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.DPNWorkItemList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/dpn_items/?", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)
	assert.EqualValues(t, "DPNWorkItem", response.ObjectType())

	list := response.DPNWorkItems()
	if list == nil {
		t.Errorf("DPNWorkItem list should not be nil")
		return
	}
	if len(list) != 4 {
		t.Errorf("DPNWorkItems list should have four items. Found %d.", len(list))
		return
	}
	for _, obj := range list {
		assert.NotEqual(t, "", obj.Task)
		assert.NotEqual(t, "", obj.Identifier)
	}

	// Make sure params are added to URL
	params := sampleParams()
	response = client.DPNWorkItemList(params)
	expectedUrl := fmt.Sprintf("/api/v2/dpn_items/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestDPNWorkItemSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(dpnWorkItemSaveHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// First, test create...
	// ---------------------------------------------
	obj := testutil.MakeDPNWorkItem()
	obj.Id = 0
	response := client.DPNWorkItemSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v2/dpn_items/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.DPNWorkItem()
	assert.EqualValues(t, "DPNWorkItem", response.ObjectType())
	if obj == nil {
		t.Errorf("DPNWorkItem should not be nil")
	}
	assert.NotEqual(t, "", obj.Task)
	assert.NotEqual(t, "", obj.Identifier)

	// Make sure the client returns the SAVED object,
	// not the unsaved one we sent.
	assert.NotEqual(t, 0, obj.Id)

	// ---------------------------------------------
	// Now test with an update...
	// ---------------------------------------------
	obj = testutil.MakeDPNWorkItem()
	origModTime := obj.UpdatedAt
	response = client.DPNWorkItemSave(obj)

	// Check the request URL and method
	expectedUrl := fmt.Sprintf("/api/v2/dpn_items/%d/", obj.Id)
	assert.Equal(t, "PUT", response.Request.Method)
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.DPNWorkItem()
	assert.EqualValues(t, "DPNWorkItem", response.ObjectType())
	if obj == nil {
		t.Errorf("DPNWorkItem should not be nil")
	}
	assert.Equal(t, 1000, obj.Id)
	assert.NotEqual(t, origModTime, obj.UpdatedAt)
}

func TestDPNBagGet(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(pharosDPNBagGetHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.DPNBagGet(999)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/dpn_bags/999/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj := response.DPNBag()
	assert.EqualValues(t, "PharosDPNBag", response.ObjectType())
	if obj == nil {
		t.Errorf("PharosDPNBag should not be nil")
	}
	assert.NotEqual(t, 0, obj.Id)
	assert.NotEqual(t, "", obj.ObjectIdentifier)
	assert.NotEqual(t, "", obj.DPNIdentifier)
}

func TestDPNBagList(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(pharosDPNBagListHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	response := client.DPNBagList(nil)

	// Check the request URL and method
	assert.Equal(t, "GET", response.Request.Method)
	assert.Equal(t, "/api/v2/dpn_bags/?", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)
	assert.EqualValues(t, "PharosDPNBag", response.ObjectType())

	list := response.DPNBags()
	if list == nil {
		t.Errorf("PharosDPNBag list should not be nil")
		return
	}
	if len(list) != 4 {
		t.Errorf("PharosDPNBag list should have four items. Found %d.", len(list))
		return
	}
	for _, obj := range list {
		assert.NotEqual(t, "", obj.ObjectIdentifier)
		assert.NotEqual(t, "", obj.DPNIdentifier)
	}

	// Make sure params are added to URL
	params := sampleParams()
	response = client.DPNBagList(params)
	expectedUrl := fmt.Sprintf("/api/v2/dpn_bags/?%s", params.Encode())
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)
}

func TestPharosDPNBagSave(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(pharosDPNBagSaveHandler))
	defer testServer.Close()

	client, err := network.NewPharosClient(testServer.URL, "v2", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}

	// ---------------------------------------------
	// First, test create...
	// ---------------------------------------------
	obj := testutil.MakePharosDPNBag()
	obj.Id = 0
	response := client.DPNBagSave(obj)

	// Check the request URL and method
	assert.Equal(t, "POST", response.Request.Method)
	assert.Equal(t, "/api/v2/dpn_bags/", response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.DPNBag()
	assert.EqualValues(t, "PharosDPNBag", response.ObjectType())
	if obj == nil {
		t.Errorf("DPNBag should not be nil")
	}
	assert.NotEqual(t, "", obj.ObjectIdentifier)
	assert.NotEqual(t, "", obj.DPNIdentifier)

	// Make sure the client returns the SAVED object,
	// not the unsaved one we sent.
	assert.NotEqual(t, 0, obj.Id)

	// ---------------------------------------------
	// Now test with an update...
	// ---------------------------------------------
	obj = testutil.MakePharosDPNBag()
	origModTime := obj.UpdatedAt
	response = client.DPNBagSave(obj)

	// Check the request URL and method
	expectedUrl := fmt.Sprintf("/api/v2/dpn_bags/%d/", obj.Id)
	assert.Equal(t, "PUT", response.Request.Method)
	assert.Equal(t, expectedUrl, response.Request.URL.Opaque)

	// Basic sanity check on response values
	assert.Nil(t, response.Error)

	obj = response.DPNBag()
	assert.EqualValues(t, "PharosDPNBag", response.ObjectType())
	if obj == nil {
		t.Errorf("DPNBag should not be nil")
	}
	assert.Equal(t, 1000, obj.Id)
	assert.Equal(t, "popeye", obj.ObjectIdentifier)
	assert.Equal(t, "olive oyl", obj.DPNIdentifier)
	assert.NotEqual(t, origModTime, obj.UpdatedAt)
}

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

// Build a simple struct that mimics the structure of a Pharos
// JSON list response. That includes keys count, next, previous,
// and results. The caller will add ["results"] with a list of
// objects of the appropriate type.
func listResponseData() map[string]interface{} {
	data := make(map[string]interface{})
	data["count"] = 100
	data["next"] = "http://example.com/?page=11"
	data["previous"] = "http://example.com/?page=9"
	return data
}

// Returns some sample URL parameters.
func sampleParams() url.Values {
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

func institutionGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeInstitution()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func institutionListHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.Institution, 4)
	for i := 0; i < 4; i++ {
		list[i] = testutil.MakeInstitution()
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

func intellectualObjectGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeIntellectualObject(2, 3, 4, 5)
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func intellectualObjectListHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.IntellectualObject, 4)
	for i := 0; i < 4; i++ {
		list[i] = testutil.MakeIntellectualObject(2, 3, 4, 5)
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func intellectualObjectSaveHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	topLevelData := make(map[string]interface{})
	err := decoder.Decode(&topLevelData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON data: %v", err)
		fmt.Fprintln(w, "")
		return
	}
	data := topLevelData["intellectual_object"].(map[string]interface{})
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

func genericFileGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeGenericFile(2, 3, "kollege.kom/objekt")
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func genericFileListHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.GenericFile, 4)
	for i := 0; i < 4; i++ {
		list[i] = testutil.MakeGenericFile(2, 3, "kollege.kom/objekt")
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func genericFileSaveHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	topLevelData := make(map[string]interface{})
	err := decoder.Decode(&topLevelData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON data: %v", err)
		fmt.Fprintln(w, "")
		return
	}

	data := topLevelData["generic_file"].(map[string]interface{})

	// Assign ID and timestamps, as if the object has been saved.
	data["id"] = 1000
	data["created_at"] = time.Now().UTC()
	data["updated_at"] = time.Now().UTC()
	objJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func genericFileSaveBatchHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	batch := make([]*models.GenericFileForPharos, 0)
	err := decoder.Decode(&batch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON data: %v", err)
		fmt.Fprintln(w, "")
		return
	}

	// Assign ID and timestamps, as if the object has been saved.
	files := make([]*models.GenericFile, 0)
	for i, file := range batch {
		checksums := make([]*models.Checksum, len(file.Checksums))
		for i, cs := range file.Checksums {
			checksums[i] = &models.Checksum{
				Digest: cs.Digest,
			}
		}
		events := make([]*models.PremisEvent, len(file.PremisEvents))
		for i, event := range file.PremisEvents {
			events[i] = &models.PremisEvent{
				Identifier: event.Identifier,
			}
		}
		gf := &models.GenericFile{
			Id:                   1000 + i,
			Identifier:           file.Identifier,
			IntellectualObjectId: file.IntellectualObjectId,
			FileFormat:           file.FileFormat,
			URI:                  file.URI,
			Size:                 file.Size,
			// TODO: Restore these when they are part of the Pharos model.
			//FileCreated: file.FileCreated,
			//FileModified: file.FileModified,
			Checksums:    checksums,
			PremisEvents: events,
			CreatedAt:    time.Now().UTC(),
			UpdatedAt:    time.Now().UTC(),
		}
		files = append(files, gf)
	}
	temp := struct {
		Count    int                   `json:"count"`
		Next     *string               `json:"next"`
		Previous *string               `json:"previous"`
		Results  []*models.GenericFile `json:"results"`
	}{
		Count:    0,
		Next:     nil,
		Previous: nil,
		Results:  files,
	}
	jsonData, err := json.Marshal(temp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error encoding JSON data: %v", err)
		fmt.Fprintln(w, "")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(jsonData))
}

// -------------------------------------------------------------------------
// Checksum handlers
// -------------------------------------------------------------------------

func checksumGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeChecksum()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func checksumListHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.Checksum, 4)
	for i := 0; i < 4; i++ {
		list[i] = testutil.MakeChecksum()
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func checksumSaveHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	topLevelData := make(map[string]interface{})
	err := decoder.Decode(&topLevelData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON data: %v", err)
		fmt.Fprintln(w, "")
		return
	}
	data := topLevelData["checksum"].(map[string]interface{})
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

func premisEventGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakePremisEvent()
	obj.Identifier = "000000000000-0000-0000-0000-00000000"
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func premisEventListHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.PremisEvent, 4)
	for i := 0; i < 4; i++ {
		list[i] = testutil.MakePremisEvent()
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func premisEventSaveHandler(w http.ResponseWriter, r *http.Request) {
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

func workItemGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeWorkItem()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func workItemListHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.WorkItem, 4)
	for i := 0; i < 4; i++ {
		list[i] = testutil.MakeWorkItem()
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func workItemSaveHandler(w http.ResponseWriter, r *http.Request) {
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
// WorkItemState handlers
// -------------------------------------------------------------------------

func workItemStateGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeWorkItemState()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func workItemStateSaveHandler(w http.ResponseWriter, r *http.Request) {
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
	if r.Method == "POST" {
		data["id"] = 1000
		data["created_at"] = time.Now().UTC()
	}
	data["updated_at"] = time.Now().UTC()
	objJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

// -------------------------------------------------------------------------
// DPNWorkItem handlers
// -------------------------------------------------------------------------

func dpnWorkItemGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakeDPNWorkItem()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func dpnWorkItemListHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.DPNWorkItem, 4)
	for i := 0; i < 4; i++ {
		list[i] = testutil.MakeDPNWorkItem()
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func dpnWorkItemSaveHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	topLevelData := make(map[string]interface{})
	err := decoder.Decode(&topLevelData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON data: %v", err)
		fmt.Fprintln(w, "")
		return
	}
	data := topLevelData["dpn_work_item"].(map[string]interface{})

	// Assign ID and timestamps, as if the object has been saved.
	data["id"] = 1000
	data["created_at"] = time.Now().UTC()
	data["updated_at"] = time.Now().UTC()
	objJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

// -------------------------------------------------------------------------
// PharosDPNBag handlers
// -------------------------------------------------------------------------

func pharosDPNBagGetHandler(w http.ResponseWriter, r *http.Request) {
	obj := testutil.MakePharosDPNBag()
	objJson, _ := json.Marshal(obj)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}

func pharosDPNBagListHandler(w http.ResponseWriter, r *http.Request) {
	list := make([]*models.PharosDPNBag, 4)
	for i := 0; i < 4; i++ {
		list[i] = testutil.MakePharosDPNBag()
	}
	data := listResponseData()
	data["results"] = list
	listJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(listJson))
}

func pharosDPNBagSaveHandler(w http.ResponseWriter, r *http.Request) {
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
	data["object_identifier"] = "popeye"
	data["dpn_identifier"] = "olive oyl"
	data["created_at"] = time.Now().UTC()
	data["updated_at"] = time.Now().UTC()
	objJson, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(objJson))
}
