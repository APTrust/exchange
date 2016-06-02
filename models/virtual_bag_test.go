package models_test

import (
	"encoding/json"
	"fmt"
	// "github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/stretchr/testify/assert"
	// "os"
	// "path"
	"path/filepath"
	"runtime"
	"testing"
	// "time"
)

func TestNewVirtualBag(t *testing.T) {
	tarFilePath := vbagGetPath("example.edu.tagsample_good.tar")
	vbag := models.NewVirtualBag(tarFilePath, nil, false, false)
	assert.NotNil(t, vbag)
}

func TestVirtualBagRead_FromDirectory(t *testing.T) {

}

func TestVirtualBagRead_FromTarFile(t *testing.T) {
	tarFilePath := vbagGetPath("example.edu.tagsample_good.tar")
	files := []string {"bagit.txt", "bag-info.txt", "aptrust-info.txt"}
	vbag := models.NewVirtualBag(tarFilePath, files, true, true)
	assert.NotNil(t, vbag)
	obj, summary := vbag.Read()
	objJson, _ := json.Marshal(obj)
	summaryJson, _ := json.Marshal(summary)
	fmt.Println(string(objJson))
	fmt.Println(string(summaryJson))
}

func TestVirtualBagRead_ChecksumOptions(t *testing.T) {

}

// With md5 manifest only, sha256 only, and both
func TestVirtualBagRead_ManifestOptions(t *testing.T) {

}

func vbagGetPath(fileName string) (string) {
	_, filename, _, _ := runtime.Caller(0)
	dir, _ := filepath.Abs(filepath.Dir(filename))
	testDataPath := filepath.Join(dir, "..", "testdata", fileName)
	return testDataPath
}
