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

}

func TestInstitutions(t *testing.T) {

}

func TestIntellectualObject(t *testing.T) {

}

func TestIntellectualObjects(t *testing.T) {

}

func TestGenericFile(t *testing.T) {

}

func TestGenericFiles(t *testing.T) {

}

func TestPremisEvent(t *testing.T) {

}

func TestPremisEvents(t *testing.T) {

}

func TestWorkItem(t *testing.T) {

}

func TestWorkItems(t *testing.T) {

}

func TestUnmarshalJsonList(t *testing.T) {

}
