package network_test

import (
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetS3Session(t *testing.T) {
	if !canTestS3() {
		return
	}
	session, err := network.GetS3Session(constants.AWSVirginia)
	assert.NotNil(t, session)
	assert.Nil(t, err)
}
