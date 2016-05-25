package models

import (
	"github.com/APTrust/exchange/util/fileutil"
	//"os"
	"strings"
)

type VirtualBag struct {
	obj         *IntellectualObject
	summary     *WorkSummary
	pathToBag   string
}

func NewVirtualBag(pathToBag string) (*VirtualBag) {
	obj := NewIntellectualObject()
	if strings.HasSuffix(pathToBag, ".tar") {
		obj.IngestTarFilePath = pathToBag
	} else {
		obj.IngestUntarredPath = pathToBag
	}
	return &VirtualBag{
		obj: obj,
		summary: NewWorkSummary(),
		pathToBag: pathToBag,
	}
}

func (vbag *VirtualBag) Read() () {
	vbag.summary.Start()
	if !fileutil.FileExists(vbag.pathToBag) {
		vbag.summary.AddError("Bag file or directory does not exist at '%s'", vbag.pathToBag)
	} else {
		vbag.read()
	}
	vbag.summary.Finish()
}

func (vbag *VirtualBag) read() {

}

func (vbag *VirtualBag) readFromFileSystem() {

}
