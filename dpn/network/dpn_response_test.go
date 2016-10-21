package network_test

import (
	"github.com/APTrust/exchange/dpn"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

var objectTypes = []dpn.DPNObjectType{
	dpn.DPNTypeBag,
	dpn.DPNTypeDigest,
	dpn.DPNTypeFixityCheck,
	dpn.DPNTypeIngest,
	dpn.DPNTypeMember,
	dpn.DPNTypeNode,
	dpn.DPNTypeReplication,
	dpn.DPNTypeRestore,
}

func TestNewDPNResponse(t *testing.T) {
	for _, objType := range objectTypes {
		resp := dpn.NewDPNResponse(objType)
		assert.NotNil(t, resp)
		assert.Equal(t, objType, resp.ObjectType())
		assert.Equal(t, 0, resp.Count)
		assert.Nil(t, resp.Next)
		assert.Nil(t, resp.Previous)
	}
}

func TestRawResponseData(t *testing.T) {
	// nodeGetHandler is defined in dpn_rest_client_test.go
	testServer := httptest.NewServer(http.HandlerFunc(nodeGetHandler))
	defer testServer.Close()

	// configFile is defined in dpn_rest_client_test.go
	config, err := models.LoadConfigFile(configFile)
	require.Nil(t, err)
	client, err := dpn.NewDPNRestClient(
		testServer.URL,
		"",
		"",
		config.DPN.LocalNode,
		config.DPN)
	if err != nil {
		t.Error(err)
		return
	}
	resp := client.NodeGet("luna")

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
		resp := dpn.NewDPNResponse(objType)
		assert.Equal(t, objType, resp.ObjectType())
	}
}

func TestHasNextPage(t *testing.T) {
	resp := dpn.NewDPNResponse(dpn.DPNTypeNode)
	assert.False(t, resp.HasNextPage())
	link := "http://example.com"
	resp.Next = &link
	assert.True(t, resp.HasNextPage())
}

func TestHasPreviousPage(t *testing.T) {
	resp := dpn.NewDPNResponse(dpn.DPNTypeNode)
	assert.False(t, resp.HasPreviousPage())
	link := "http://example.com"
	resp.Previous = &link
	assert.True(t, resp.HasPreviousPage())
}

func TestParamsForNextPage(t *testing.T) {
	resp := dpn.NewDPNResponse(dpn.DPNTypeNode)
	link := "http://example.com?name=college.edu&page=6&per_page=20"
	resp.Next = &link
	params := resp.ParamsForNextPage()
	assert.Equal(t, 3, len(params))
	assert.Equal(t, "college.edu", params.Get("name"))
	assert.Equal(t, "6", params.Get("page"))
	assert.Equal(t, "20", params.Get("per_page"))
}

func TestParamsForPreviousPage(t *testing.T) {
	resp := dpn.NewDPNResponse(dpn.DPNTypeNode)
	link := "http://example.com?name=college.edu&page=6&per_page=20"
	resp.Previous = &link
	params := resp.ParamsForPreviousPage()
	assert.Equal(t, 3, len(params))
	assert.Equal(t, "college.edu", params.Get("name"))
	assert.Equal(t, "6", params.Get("page"))
	assert.Equal(t, "20", params.Get("per_page"))
}
