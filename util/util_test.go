package util_test

import (
	"github.com/APTrust/exchange/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.Equal(t, "test.edu", util.OwnerOf("aptrust.receiving.test.edu"))
}

func TestRestorationBucketFor(t *testing.T) {
	assert.Equal(t, "aptrust.restore.unc.edu", util.RestorationBucketFor("unc.edu", false))
	assert.Equal(t, "aptrust.restore.test.unc.edu", util.RestorationBucketFor("unc.edu", true))
}

func TestBagNameFromTarFileName(t *testing.T) {
	name := util.BagNameFromTarFileName("/mnt/apt/data/uc.edu/photos.bag22.tar")
	assert.Equal(t, "photos.bag22", name)

	name = util.BagNameFromTarFileName("/mnt/apt/data/uc.edu/photos.bag22.b001.of200.tar")
	assert.Equal(t, "photos.bag22", name)

	name = util.BagNameFromTarFileName("/mnt/apt/data/uc.edu/photos.bag22.b1.of12.tar")
	assert.Equal(t, "photos.bag22", name)
}

func TestCleanBagName(t *testing.T) {
	expected := "some.file"
	actual := util.CleanBagName("some.file.b001.of200.tar")
	if actual != expected {
		t.Errorf("CleanBagName should have returned '%s', but returned '%s'",
			expected, actual)
	}
	actual = util.CleanBagName("some.file.b1.of2.tar")
	if actual != expected {
		t.Errorf("CleanBagName should have returned '%s', but returned '%s'",
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
		t.Errorf("GetInstitutionFromBagName return institution name '%s', expected 'miami'", inst)
	}
	_, err = util.GetInstitutionFromBagName("miami.edu.chc0390_metadata.tar")
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

func TestLooksLikeJunkFile(t *testing.T) {
	assert.False(t, util.LooksLikeJunkFile("."))
	assert.False(t, util.LooksLikeJunkFile(".."))
	assert.True(t, util.LooksLikeJunkFile("._junk.txt"))
	assert.True(t, util.LooksLikeJunkFile("data/subdir/._junk.txt"))
	assert.False(t, util.LooksLikeJunkFile("bagit.txt"))
	assert.False(t, util.LooksLikeJunkFile("manifest-md5.txt"))
}

func TestStringListContains(t *testing.T) {
	list := []string{"apple", "orange", "banana"}
	assert.True(t, util.StringListContains(list, "orange"))
	assert.False(t, util.StringListContains(list, "wedgie"))
	// Don't crash on nil list
	assert.False(t, util.StringListContains(nil, "mars"))
}

func TestIntListContains(t *testing.T) {
	list := []int{101, 102, 103}
	assert.True(t, util.IntListContains(list, 102))
	assert.False(t, util.IntListContains(list, 87))
	// Don't crash on nil list
	assert.False(t, util.IntListContains(nil, 599))
}

func TestPointerToString(t *testing.T) {
	str := "Hello"
	strPointer := &str
	var nilPointer *string
	assert.Equal(t, str, util.PointerToString(strPointer))
	assert.Equal(t, "", util.PointerToString(nilPointer))
}

func TestDeleteFromStringList(t *testing.T) {
	list := []string{"apple", "orange", "banana"}
	newList := util.DeleteFromStringList(list, "orange")
	require.Equal(t, 2, len(newList))
	assert.Equal(t, "apple", newList[0])
	assert.Equal(t, "banana", newList[1])

	anotherList := util.DeleteFromStringList(list, "item_does_not_exist")
	assert.Equal(t, 3, len(anotherList))
}

func TestContainsControlCharacter(t *testing.T) {
	assert.True(t, util.ContainsControlCharacter("\u0000 -- NULL"))
	assert.True(t, util.ContainsControlCharacter("\u0001 -- START OF HEADING"))
	assert.True(t, util.ContainsControlCharacter("\u0002 -- START OF TEXT"))
	assert.True(t, util.ContainsControlCharacter("\u0003 -- END OF TEXT"))
	assert.True(t, util.ContainsControlCharacter("\u0004 -- END OF TRANSMISSION"))
	assert.True(t, util.ContainsControlCharacter("\u0005 -- ENQUIRY"))
	assert.True(t, util.ContainsControlCharacter("\u0006 -- ACKNOWLEDGE"))
	assert.True(t, util.ContainsControlCharacter("\u0007 -- BELL"))
	assert.True(t, util.ContainsControlCharacter("\u0008 -- BACKSPACE"))
	assert.True(t, util.ContainsControlCharacter("\u0009 -- CHARACTER TABULATION"))
	assert.True(t, util.ContainsControlCharacter("\u000A -- LINE FEED (LF)"))
	assert.True(t, util.ContainsControlCharacter("\u000B -- LINE TABULATION"))
	assert.True(t, util.ContainsControlCharacter("\u000C -- FORM FEED (FF)"))
	assert.True(t, util.ContainsControlCharacter("\u000D -- CARRIAGE RETURN (CR)"))
	assert.True(t, util.ContainsControlCharacter("\u000E -- SHIFT OUT"))
	assert.True(t, util.ContainsControlCharacter("\u000F -- SHIFT IN"))
	assert.True(t, util.ContainsControlCharacter("\u0010 -- DATA LINK ESCAPE"))
	assert.True(t, util.ContainsControlCharacter("\u0011 -- DEVICE CONTROL ONE"))
	assert.True(t, util.ContainsControlCharacter("\u0012 -- DEVICE CONTROL TWO"))
	assert.True(t, util.ContainsControlCharacter("\u0013 -- DEVICE CONTROL THREE"))
	assert.True(t, util.ContainsControlCharacter("\u0014 -- DEVICE CONTROL FOUR"))
	assert.True(t, util.ContainsControlCharacter("\u0015 -- NEGATIVE ACKNOWLEDGE"))
	assert.True(t, util.ContainsControlCharacter("\u0016 -- SYNCHRONOUS IDLE"))
	assert.True(t, util.ContainsControlCharacter("\u0017 -- END OF TRANSMISSION BLOCK"))
	assert.True(t, util.ContainsControlCharacter("\u0018 -- CANCEL"))
	assert.True(t, util.ContainsControlCharacter("\u0019 -- END OF MEDIUM"))
	assert.True(t, util.ContainsControlCharacter("\u001A -- SUBSTITUTE"))
	assert.True(t, util.ContainsControlCharacter("\u001B -- ESCAPE"))
	assert.True(t, util.ContainsControlCharacter("\u001C -- INFORMATION SEPARATOR FOUR"))
	assert.True(t, util.ContainsControlCharacter("\u001D -- INFORMATION SEPARATOR THREE"))
	assert.True(t, util.ContainsControlCharacter("\u001E -- INFORMATION SEPARATOR TWO"))
	assert.True(t, util.ContainsControlCharacter("\u001F -- INFORMATION SEPARATOR ONE"))
	assert.True(t, util.ContainsControlCharacter("\u007F -- DELETE"))
	assert.True(t, util.ContainsControlCharacter("\u0080 -- <control>"))
	assert.True(t, util.ContainsControlCharacter("\u0081 -- <control>"))
	assert.True(t, util.ContainsControlCharacter("\u0082 -- BREAK PERMITTED HERE"))
	assert.True(t, util.ContainsControlCharacter("\u0083 -- NO BREAK HERE"))
	assert.True(t, util.ContainsControlCharacter("\u0084 -- <control>"))
	assert.True(t, util.ContainsControlCharacter("\u0085 -- NEXT LINE (NEL)"))
	assert.True(t, util.ContainsControlCharacter("\u0086 -- START OF SELECTED AREA"))
	assert.True(t, util.ContainsControlCharacter("\u0087 -- END OF SELECTED AREA"))
	assert.True(t, util.ContainsControlCharacter("\u0088 -- CHARACTER TABULATION SET"))
	assert.True(t, util.ContainsControlCharacter("\u0089 -- CHARACTER TABULATION WITH JUSTIFICATION"))
	assert.True(t, util.ContainsControlCharacter("\u008A -- LINE TABULATION SET"))
	assert.True(t, util.ContainsControlCharacter("\u008B -- PARTIAL LINE FORWARD"))
	assert.True(t, util.ContainsControlCharacter("\u008C -- PARTIAL LINE BACKWARD"))
	assert.True(t, util.ContainsControlCharacter("\u008D -- REVERSE LINE FEED"))
	assert.True(t, util.ContainsControlCharacter("\u008E -- SINGLE SHIFT TWO"))
	assert.True(t, util.ContainsControlCharacter("\u008F -- SINGLE SHIFT THREE"))
	assert.True(t, util.ContainsControlCharacter("\u0090 -- DEVICE CONTROL STRING"))
	assert.True(t, util.ContainsControlCharacter("\u0091 -- PRIVATE USE ONE"))
	assert.True(t, util.ContainsControlCharacter("\u0092 -- PRIVATE USE TWO"))
	assert.True(t, util.ContainsControlCharacter("\u0093 -- SET TRANSMIT STATE"))
	assert.True(t, util.ContainsControlCharacter("\u0094 -- CANCEL CHARACTER"))
	assert.True(t, util.ContainsControlCharacter("\u0095 -- MESSAGE WAITING"))
	assert.True(t, util.ContainsControlCharacter("\u0096 -- START OF GUARDED AREA"))
	assert.True(t, util.ContainsControlCharacter("\u0097 -- END OF GUARDED AREA"))
	assert.True(t, util.ContainsControlCharacter("\u0098 -- START OF STRING"))
	assert.True(t, util.ContainsControlCharacter("\u0099 -- <control>"))
	assert.True(t, util.ContainsControlCharacter("\u009A -- SINGLE CHARACTER INTRODUCER"))
	assert.True(t, util.ContainsControlCharacter("\u009B -- CONTROL SEQUENCE INTRODUCER"))
	assert.True(t, util.ContainsControlCharacter("\u009C -- STRING TERMINATOR"))
	assert.True(t, util.ContainsControlCharacter("\u009D -- OPERATING SYSTEM COMMAND"))
	assert.True(t, util.ContainsControlCharacter("\u009E -- PRIVACY MESSAGE"))
	assert.True(t, util.ContainsControlCharacter("\u009F -- APPLICATION PROGRAM COMMAND"))

	assert.False(t, util.ContainsControlCharacter("./this/is/a/valid/file/name.txt"))
}
