package network_test

import (
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

var objectTypes = []network.PharosObjectType{
	network.PharosIntellectualObject,
	network.PharosInstitution,
	network.PharosGenericFile,
	network.PharosPremisEvent,
	network.PharosWorkItem,
}

func TestNewPharosResponse(t *testing.T) {
	for _, objType := range objectTypes {
		resp := network.NewPharosResponse(objType)
		assert.NotNil(t, resp)
		assert.Equal(t, objType, resp.ObjectType())
		assert.Equal(t, 0, resp.Count)
		assert.Nil(t, resp.Next)
		assert.Nil(t, resp.Previous)
	}
}

func TestRawResponseData(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(institutionGetHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.InstitutionGet("college.edu")

	// Should be able to call repeatedly without error.
	// Incorrect implementation would try to read from
	// closed network socket.
	for i := 0; i < 3; i++ {
		bytes, err := resp.RawResponseData()
		assert.NotNil(t, bytes)
		assert.NotEmpty(t, bytes)
		assert.Nil(t, err)
	}
}

func TestObjectType(t *testing.T) {
	for _, objType := range objectTypes {
		resp := network.NewPharosResponse(objType)
		assert.Equal(t, objType, resp.ObjectType())
	}
}

func TestHasNextPage(t *testing.T) {
	resp := network.NewPharosResponse(network.PharosInstitution)
	assert.False(t, resp.HasNextPage())
	link := "http://example.com"
	resp.Next = &link
	assert.True(t, resp.HasNextPage())
}

func TestHasPreviousPage(t *testing.T) {
	resp := network.NewPharosResponse(network.PharosInstitution)
	assert.False(t, resp.HasPreviousPage())
	link := "http://example.com"
	resp.Previous = &link
	assert.True(t, resp.HasPreviousPage())
}

func TestParamsForNextPage(t *testing.T) {
	resp := network.NewPharosResponse(network.PharosInstitution)
	link := "http://example.com?name=college.edu&page=6&per_page=20"
	resp.Next = &link
	params := resp.ParamsForNextPage()
	assert.Equal(t, 3, len(params))
	assert.Equal(t, "college.edu", params.Get("name"))
	assert.Equal(t, "6", params.Get("page"))
	assert.Equal(t, "20", params.Get("per_page"))
}

func TestParamsForPreviousPage(t *testing.T) {
	resp := network.NewPharosResponse(network.PharosInstitution)
	link := "http://example.com?name=college.edu&page=6&per_page=20"
	resp.Previous = &link
	params := resp.ParamsForPreviousPage()
	assert.Equal(t, 3, len(params))
	assert.Equal(t, "college.edu", params.Get("name"))
	assert.Equal(t, "6", params.Get("page"))
	assert.Equal(t, "20", params.Get("per_page"))
}

func TestInstitution(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(institutionGetHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.InstitutionGet("college.edu")
	assert.NotNil(t, resp.Institution())
}

func TestInstitutions(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(institutionListHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.InstitutionList(nil)
	assert.NotEmpty(t, resp.Institutions())
}

func TestIntellectualObject(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(intellectualObjectGetHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.IntellectualObjectGet("college.edu/object", true, false)
	assert.NotNil(t, resp.IntellectualObject())
}

func TestIntellectualObjects(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(intellectualObjectListHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.IntellectualObjectList(nil)
	assert.NotEmpty(t, resp.IntellectualObjects())
}

func TestGenericFile(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(genericFileGetHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.GenericFileGet("college.edu/object/file.xml", false)
	assert.NotNil(t, resp.GenericFile())
}

func TestGenericFiles(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(genericFileListHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.GenericFileList(nil)
	assert.NotEmpty(t, resp.GenericFiles())
}

func TestChecksum(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(checksumGetHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.ChecksumGet(999)
	assert.Nil(t, resp.Error)
	assert.NotNil(t, resp.Checksum())
}

func TestChecksums(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(checksumListHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.ChecksumList(nil)
	assert.NotEmpty(t, resp.Checksums())
}

func TestPremisEvent(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(premisEventGetHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.PremisEventGet("000000000000-0000-0000-0000-00000000")
	assert.NotNil(t, resp.PremisEvent())
}

func TestPremisEvents(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(premisEventListHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.PremisEventList(nil)
	assert.NotEmpty(t, resp.PremisEvents())
}

func TestWorkItem(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(workItemGetHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.WorkItemGet(1000)
	assert.NotNil(t, resp.WorkItem())
}

func TestWorkItems(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(workItemListHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.WorkItemList(nil)
	assert.NotEmpty(t, resp.WorkItems())
}

func TestWorkItemState(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(workItemStateGetHandler))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.WorkItemStateGet(1000)
	assert.NotNil(t, resp.WorkItemState())
}
