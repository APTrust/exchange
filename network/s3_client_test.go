package network_test

import (
	"fmt"
	"github.com/APTrust/exchange/network"
	"github.com/crowdmob/goamz/aws"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewS3Client(t *testing.T) {
	client, err := network.NewS3Client(aws.USEast)
	assert.NotNil(t, client)
	if err != nil {
		t.Errorf("Error creating S3 client: %v.\n", err)
		if os.Getenv("") == "" || os.Getenv("") == "" {
			fmt.Println("Note that NewS3Client() expects ENV vars " +
				"AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
		}
	}
}

func TestNewS3ClientExplicitAuth(t *testing.T) {

}

func TestListBucket(t *testing.T) {

}
