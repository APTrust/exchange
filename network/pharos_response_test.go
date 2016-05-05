package network_test

import (
//	"encoding/json"
//	"fmt"
//	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
//	"github.com/APTrust/exchange/testdata"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
//	"net/url"
//	"os"
//	"strings"
	"testing"
//	"time"
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
	testServer := httptest.NewServer(http.HandlerFunc(institutionGetHander))
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
	testServer := httptest.NewServer(http.HandlerFunc(institutionGetHander))
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
	testServer := httptest.NewServer(http.HandlerFunc(institutionListHander))
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
	testServer := httptest.NewServer(http.HandlerFunc(intellectualObjectGetHander))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.IntellectualObjectGet("college.edu/object")
	assert.NotNil(t, resp.IntellectualObject())
}

func TestIntellectualObjects(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(intellectualObjectListHander))
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
	testServer := httptest.NewServer(http.HandlerFunc(genericFileGetHander))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.GenericFileGet("college.edu/object/file.xml")
	assert.NotNil(t, resp.GenericFile())
}

func TestGenericFiles(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(genericFileListHander))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.GenericFileList(nil)
	assert.NotEmpty(t, resp.GenericFiles())
}

func TestPremisEvent(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(premisEventGetHander))
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
	testServer := httptest.NewServer(http.HandlerFunc(premisEventListHander))
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
	testServer := httptest.NewServer(http.HandlerFunc(workItemGetHander))
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
	testServer := httptest.NewServer(http.HandlerFunc(workItemListHander))
	defer testServer.Close()
	client, err := network.NewPharosClient(testServer.URL, "v1", "user", "key")
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.WorkItemList(nil)
	assert.NotEmpty(t, resp.WorkItems())
}
