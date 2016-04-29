package network_test

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewPharosClient(t *testing.T) {

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

	respData, _ := response.RawResponseData()
	fmt.Println(string(respData))
	assert.Nil(t, response.Error)
}


// HTTP test server handler functions

func institutionGetHander(w http.ResponseWriter, r *http.Request) {

	// ---------------------------------------------------
	// TODO: Look into https://github.com/icrowley/fake
	// for generating test data.
	// ---------------------------------------------------

	inst := models.Institution{}
	instJson, _ := json.Marshal(inst)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, string(instJson))
}

// func institutionListHander(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")
// 	fmt.Fprintln(w, `{"fake twitter json string"}`)
// }
