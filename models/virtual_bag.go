package models

import (
	"fmt"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nu7hatch/gouuid"
	"io"
	//"os"
	"strings"
	"time"
)

type VirtualBag struct {
	pathToBag    string
	checksumAlgs []string
	obj          *IntellectualObject
	summary      *WorkSummary
	readIterator fileutil.ReadIterator
}

func NewVirtualBag(pathToBag string, checksumAlgs []string) (*VirtualBag) {
	return &VirtualBag{
		checksumAlgs: checksumAlgs,
		pathToBag: pathToBag,
	}
}

func (vbag *VirtualBag) Read() (*IntellectualObject, *WorkSummary) {
	vbag.summary = NewWorkSummary()
	vbag.summary.Start()
	vbag.obj = NewIntellectualObject()
	vbag.obj.Identifier, _ = util.CleanBagName(vbag.pathToBag)
	if strings.HasSuffix(vbag.pathToBag, ".tar") {
		vbag.obj.IngestTarFilePath = vbag.pathToBag
	} else {
		vbag.obj.IngestUntarredPath = vbag.pathToBag
	}
	var err error
	if vbag.obj.IngestTarFilePath != "" {
		vbag.readIterator, err = fileutil.NewTarFileIterator(vbag.obj.IngestTarFilePath)
	} else {
		vbag.readIterator, err = fileutil.NewFileSystemIterator(vbag.obj.IngestUntarredPath)
	}
	if err != nil {
		vbag.summary.AddError("Could not read bag: %v", err)
	} else {
		vbag.addGenericFiles()
	}
	vbag.summary.Finish()
	return vbag.obj, vbag.summary
}

func (vbag *VirtualBag) addGenericFiles() () {
	for {
		err := vbag.addGenericFile()
		if err == io.EOF {
			break
		} else {
			vbag.summary.AddError(err.Error())
		}
	}
}

func (vbag *VirtualBag) addGenericFile() (error) {
	reader, fileSummary, err := vbag.readIterator.Next()
	if err != nil {
		return err
	}
	if !fileSummary.IsRegularFile {
		return nil
	}
	uuid, err := uuid.NewV4()
	if err != nil {
		panic("Can't read from /dev/urandom!")
	}
	gf := NewGenericFile()
	gf.Identifier = fmt.Sprintf("%s/%s", vbag.obj.Identifier, fileSummary.Name)
	gf.IntellectualObjectIdentifier = vbag.obj.Identifier
	gf.Size = fileSummary.Size
	gf.FileModified = fileSummary.ModTime
	gf.IngestLocalPath = fileSummary.AbsPath // will be empty if bag is tarred
	gf.IngestUUID = uuid.String()
	gf.IngestUUIDGeneratedAt = time.Now().UTC()
	gf.IngestFileUid = fileSummary.Uid
	gf.IngestFileGid = fileSummary.Gid
	return vbag.calculateChecksums(reader, gf)
}

func (vbag *VirtualBag) calculateChecksums(reader io.Reader, gf *GenericFile) (error) {

	return nil
}
