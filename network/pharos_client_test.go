package network_test

import (
	"encoding/json"
	"fmt"
//	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/testdata"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
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
	assert.Nil(t, response.Error)
	assert.EqualValues(t, "Institution", response.ObjectType())
	if response.Institution() == nil {
		t.Errorf("Institution should not be nil")
	}
	assert.NotEqual(t, "", len(response.Institution().Identifier))
}


// HTTP test server handler functions

func institutionGetHander(w http.ResponseWriter, r *http.Request) {
	inst := testdata.MakeInstitution()
	instJson, _ := json.Marshal(inst)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(instJson))
}

// func institutionListHander(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")
// 	fmt.Fprintln(w, `{"fake twitter json string"}`)
// }
