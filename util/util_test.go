package util_test

import (
	"github.com/APTrust/exchange/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOwnerOf(t *testing.T) {
	if util.OwnerOf("aptrust.receiving.unc.edu") != "unc.edu" {
		t.Error("OwnerOf misidentified receiving bucket owner")
	}
	if util.OwnerOf("aptrust.receiving.test.unc.edu") != "unc.edu" {
		t.Error("OwnerOf misidentified receiving bucket owner")
	}
	if util.OwnerOf("aptrust.restore.unc.edu") != "unc.edu" {
		t.Error("OwnerOf misidentified restoration bucket owner")
	}
}

func TestRestorationBucketFor(t *testing.T) {
	if util.RestorationBucketFor("unc.edu") != "aptrust.restore.unc.edu" {
		t.Error("RestorationBucketFor returned incorrect restoration bucket name")
	}
}

func TestBagNameFromTarFileName(t *testing.T) {
	name, err := util.BagNameFromTarFileName("/mnt/apt/data/uc.edu/photos.bag22.tar")
	assert.Nil(t, err)
	assert.Equal(t, "photos.bag22", name)

	name, err = util.BagNameFromTarFileName("/mnt/apt/data/uc.edu/photos.bag22.b001.of200.tar")
	assert.Nil(t, err)
	assert.Equal(t, "photos.bag22", name)

	name, err = util.BagNameFromTarFileName("/mnt/apt/data/uc.edu/photos.bag22.b1.of12.tar")
	assert.Nil(t, err)
	assert.Equal(t, "photos.bag22", name)
}

func TestCleanBagName(t *testing.T) {
	expected := "some.file"
	actual, _ := util.CleanBagName("some.file.b001.of200.tar")
	if actual != expected {
		t.Error("CleanBagName should have returned '%s', but returned '%s'",
			expected, actual)
	}
	actual, _ = util.CleanBagName("some.file.b1.of2.tar")
	if actual != expected {
		t.Error("CleanBagName should have returned '%s', but returned '%s'",
			expected, actual)
	}
}

func TestMin(t *testing.T) {
	if util.Min(10, 12) != 10 {
		t.Error("Min() thinks 12 is less than 10")
	}
}

func TestBase64EncodeMd5(t *testing.T) {
	digest := "4d66f1ec9491addded54d17b96df8c96"
	expectedResult := "TWbx7JSRrd3tVNF7lt+Mlg=="
	encodedDigest, err := util.Base64EncodeMd5(digest)
	if err != nil {
		t.Error(err)
	}
	if encodedDigest != expectedResult {
		t.Errorf("Base64EncodeMd5() returned '%s'. Expected '%s'",
			encodedDigest, expectedResult)
	}
}

func TestLooksLikeURL(t *testing.T) {
	if util.LooksLikeURL("http://s3.amazonaws.com/bucket/key") == false {
		t.Error("That was a valid URL!")
	}
	if util.LooksLikeURL("https://s3.amazonaws.com/bucket/key") == false {
		t.Error("That was a valid URL!")
	}
	if util.LooksLikeURL("tpph\\backslash\\slackbash\\iaintnourl!") == true {
		t.Error("That was not a valid URL!")
	}
	if util.LooksLikeURL("") == true {
		t.Error("That was not a valid URL! That was an empty string!")
	}
}

func TestLooksLikeUUID(t *testing.T) {
	if util.LooksLikeUUID("1552abf5-28f3-46a5-ba63-95302d08e209") == false {
		t.Error("That was a valid UUID!")
	}
	if util.LooksLikeUUID("88198c5a-ec91-4ce1-bfcc-0f607ebdcca3") == false {
		t.Error("That was a valid UUID!")
	}
	if util.LooksLikeUUID("88198C5A-EC91-4CE1-BFCC-0F607EBDCCA3") == false {
		t.Error("That was a valid UUID!")
	}
	if util.LooksLikeUUID("88198c5a-ec91-4ce1-bfcc-0f607ebdccx3") == true {
		t.Error("That was not a valid UUID!")
	}
	if util.LooksLikeUUID("88198c5a-ec91-4ce1-bfcc-0f6c") == true {
		t.Error("That was not a valid UUID!")
	}
	if util.LooksLikeUUID("") == true {
		t.Error("That was not a valid UUID! That was an empty string!")
	}
}

func TestCleanString(t *testing.T) {
	clean := util.CleanString("  spaces \t\n ")
	if clean != "spaces" {
		t.Error("Expected to receive string 'spaces'")
	}
	clean = util.CleanString("  ' embedded spaces 1 '   ")
	if clean != " embedded spaces 1 " {
		t.Error("Expected to receive string ' embedded spaces 1 '")
	}
	clean = util.CleanString("  \" embedded spaces 2 \"   ")
	if clean != " embedded spaces 2 " {
		t.Error("Expected to receive string ' embedded spaces '")
	}
}

func TestBucketNameAndKey(t *testing.T) {
	url := "https://s3.amazonaws.com/aptrust.test.restore/ncsu.1840.16-1004.tar"
	expectedBucket := "aptrust.test.restore"
	expectedKey := "ncsu.1840.16-1004.tar"
	bucketName, key := util.BucketNameAndKey(url)
	if bucketName != expectedBucket {
		t.Errorf("Expected bucket name %s, got %s", expectedBucket, bucketName)
	}
	if key != expectedKey {
		t.Errorf("Expected key %s, got %s", expectedKey, key)
	}
}

func TestGetInstitutionFromBagName(t *testing.T) {
	inst, err := util.GetInstitutionFromBagName("chc0390_metadata")
	if err == nil {
		t.Error("GetInstitutionFromBagName accepted invalid bag name 'chc0390_metadata'")
	}
	inst, err = util.GetInstitutionFromBagName("chc0390_metadata.tar")
	if err == nil {
		t.Error("GetInstitutionFromBagName accepted invalid bag name 'chc0390_metadata.tar'")
	}
	inst, err = util.GetInstitutionFromBagName("miami.chc0390_metadata.tar")
	if err != nil {
		t.Error(err)
	}
	if inst != "miami" {
		t.Error("GetInstitutionFromBagName return institution name '%s', expected 'miami'", inst)
	}
	inst, err = util.GetInstitutionFromBagName("miami.edu.chc0390_metadata.tar")
	if err != nil {
		t.Error("GetInstitutionFromBagName should have accepted bag name 'miami.edu.chc0390_metadata.tar'")
	}
}

func TestSavableName(t *testing.T) {
	assert.False(t, util.HasSavableName("."))
	assert.False(t, util.HasSavableName(".."))
	assert.False(t, util.HasSavableName("._junk.txt"))
	assert.False(t, util.HasSavableName("data/subdir/._junk.txt"))
	assert.False(t, util.HasSavableName("bagit.txt"))
	assert.False(t, util.HasSavableName("manifest-md5.txt"))
	assert.False(t, util.HasSavableName("manifest-sha256.txt"))
	assert.False(t, util.HasSavableName("tagmanifest-md5.txt"))
	assert.False(t, util.HasSavableName("tagmanifest-sha256.txt"))

	assert.True(t, util.HasSavableName("data/stuff/bagit.txt"))
	assert.True(t, util.HasSavableName("custom_tags/manifest-md5.txt"))
	assert.True(t, util.HasSavableName("custom_tags/manifest-sha256.txt"))
	assert.True(t, util.HasSavableName("useless_tags/tagmanifest-md5.txt"))
	assert.True(t, util.HasSavableName("my_tags/tagmanifest-sha256.txt"))
	assert.True(t, util.HasSavableName("polly/wolly/doodle/all/day"))
}

func TestStringListContains(t *testing.T) {
	list := []string{ "apple", "orange", "banana" }
	assert.True(t, util.StringListContains(list, "orange"))
	assert.False(t, util.StringListContains(list, "wedgie"))
}
