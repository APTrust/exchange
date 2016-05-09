package network_test

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/network"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewS3ObjectDelete(t *testing.T) {
	if !canTestS3() {
		return
	}
	s3ObjectDelete := network.NewS3ObjectDelete(
		constants.AWSVirginia,
		testBucket,
		[]string{ "test_obj_1.tar", "test_obj_2.tar"},
	)
	assert.Equal(t, constants.AWSVirginia, s3ObjectDelete.AWSRegion)
	assert.Equal(t, testBucket, *s3ObjectDelete.DeleteObjectsInput.Bucket)
	assert.Equal(t, "test_obj_1.tar", *s3ObjectDelete.DeleteObjectsInput.Delete.Objects[0].Key)
	assert.Equal(t, "test_obj_2.tar", *s3ObjectDelete.DeleteObjectsInput.Delete.Objects[1].Key)
}

func TestS3ObjectDelete(t *testing.T) {
	if !canTestS3() {
		return
	}
	// Hmmm... don't like having to upload objects first.
	// But how else to test delete?
	err := upload("test_obj_1.tar")
	if err != nil {
		assert.FailNow(t, "Could not upload file 1 for delete test")
	}
	err = upload("test_obj_2.tar")
	if err != nil {
		assert.FailNow(t, "Could not upload file 2 for delete test")
	}

	// Now delete those objects
	s3ObjectDelete := network.NewS3ObjectDelete(
		constants.AWSVirginia,
		testBucket,
		[]string{ "test_obj_1.tar", "test_obj_2.tar"},
	)
	s3ObjectDelete.DeleteList()
	assert.Equal(t, "", s3ObjectDelete.ErrorMessage)
	assert.Equal(t, 2, len(s3ObjectDelete.Response.Deleted))
	assert.Empty(t, s3ObjectDelete.Response.Errors)
}

func upload(key string) (error) {
	upload := network.NewS3Upload(
		constants.AWSVirginia,
		testBucket,
		key,
		"../testdata/virginia.edu.uva-lib_2278801.tar",
		"application/tar",
	)
	upload.AddMetadata("testdata", "THIS IS DELETABLE TEST DATA")
	upload.Send()
	if upload.ErrorMessage != "" {
		return fmt.Errorf(upload.ErrorMessage)
	}
	return nil
}