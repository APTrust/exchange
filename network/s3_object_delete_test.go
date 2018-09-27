package network_test

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestNewS3ObjectDelete(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	s3ObjectDelete := network.NewS3ObjectDelete(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket,
		[]string{"test_obj_1.tar", "test_obj_2.tar"},
	)
	assert.Equal(t, constants.AWSVirginia, s3ObjectDelete.AWSRegion)
	assert.Equal(t, testBucket, *s3ObjectDelete.DeleteObjectsInput.Bucket)
	assert.Equal(t, "test_obj_1.tar", *s3ObjectDelete.DeleteObjectsInput.Delete.Objects[0].Key)
	assert.Equal(t, "test_obj_2.tar", *s3ObjectDelete.DeleteObjectsInput.Delete.Objects[1].Key)
}

func TestS3ObjectDelete(t *testing.T) {
	if !testutil.CanTestS3() {
		return
	}
	// Hmmm... don't like having to upload objects first.
	// But how else to test delete?
	err := upload(t, "test_obj_1.tar")
	if err != nil {
		assert.FailNow(t, "Could not upload file 1 for delete test")
	}
	err = upload(t, "test_obj_2.tar")
	if err != nil {
		assert.FailNow(t, "Could not upload file 2 for delete test")
	}

	// Now delete those objects
	s3ObjectDelete := network.NewS3ObjectDelete(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket,
		[]string{"test_obj_1.tar", "test_obj_2.tar"},
	)
	s3ObjectDelete.DeleteList()
	assert.Equal(t, "", s3ObjectDelete.ErrorMessage)
	assert.Equal(t, 2, len(s3ObjectDelete.Response.Deleted))
	assert.Empty(t, s3ObjectDelete.Response.Errors)
}

func upload(t *testing.T, key string) error {
	upload := network.NewS3Upload(
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		constants.AWSVirginia,
		testBucket,
		key,
		"application/tar",
	)
	upload.AddMetadata("testdata", "THIS IS DELETABLE TEST DATA")
	file, err := os.Open("../testdata/unit_test_bags/virginia.edu.uva-lib_2278801.tar")
	require.Nil(t, err)
	upload.Send(file)
	if upload.ErrorMessage != "" {
		return fmt.Errorf(upload.ErrorMessage)
	}
	return nil
}
