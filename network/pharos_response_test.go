package network_test

import (
//	"encoding/json"
//	"fmt"
//	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
//	"github.com/APTrust/exchange/testdata"
	"github.com/stretchr/testify/assert"
//	"net/http"
//	"net/http/httptest"
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

}

func TestObjectType(t *testing.T) {

}

func TestHasNextPage(t *testing.T) {

}

func TestHasPreviousPage(t *testing.T) {

}

func TestParamsForNextPage(t *testing.T) {

}

func TestParamsForPreviousPage(t *testing.T) {

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
