package dpn_test

import (
	"github.com/APTrust/exchange/dpn"
	"github.com/stretchr/testify/assert"
//	"net/http"
//	"net/http/httptest"
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
