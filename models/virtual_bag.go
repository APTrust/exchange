package models

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/exchange/platform"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nu7hatch/gouuid"
	"hash"
	"io"
	//"os"
	"strings"
	"time"
)

type VirtualBag struct {
	pathToBag        string
	calculateMd5     bool
	calculateSha256  bool
	obj              *IntellectualObject
	summary          *WorkSummary
	readIterator     fileutil.ReadIterator
}

func NewVirtualBag(pathToBag string, calculateMd5, calculateSha256 bool) (*VirtualBag) {
	return &VirtualBag{
		calculateMd5: calculateMd5,
		calculateSha256: calculateSha256,
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
	gf.Identifier = fmt.Sprintf("%s/%s", vbag.obj.Identifier, fileSummary.RelPath)
	gf.IntellectualObjectIdentifier = vbag.obj.Identifier
	gf.Size = fileSummary.Size
	gf.FileModified = fileSummary.ModTime
	gf.IngestLocalPath = fileSummary.AbsPath // will be empty if bag is tarred
	gf.IngestUUID = uuid.String()
	gf.IngestUUIDGeneratedAt = time.Now().UTC()
	gf.IngestFileUid = fileSummary.Uid
	gf.IngestFileGid = fileSummary.Gid
	vbag.setIngestFileType(gf, fileSummary)
	return vbag.calculateChecksums(reader, gf)
}

func (vbag *VirtualBag) setIngestFileType(gf *GenericFile, fileSummary *fileutil.FileSummary) {
	// manifest-
	// tagmanifest-
	// data/
	// else is tag file
}

func (vbag *VirtualBag) calculateChecksums(reader io.Reader, gf *GenericFile) (error) {
	hashes := make([]io.Writer, 0)
	var md5Hash hash.Hash
	var sha256Hash hash.Hash
	if vbag.calculateMd5 {
		md5Hash = md5.New()
		hashes = append(hashes, md5Hash)
	}
	if vbag.calculateSha256 {
		sha256Hash = sha256.New()
		hashes = append(hashes, sha256Hash)
	}
	if len(hashes) > 0 {
		multiWriter := io.MultiWriter(hashes...)
		io.Copy(multiWriter, reader)
		utcNow := time.Now().UTC()
		if md5Hash != nil {
			gf.IngestMd5 = fmt.Sprintf("%x", md5Hash.Sum(nil))
			gf.IngestMd5GeneratedAt = utcNow
		}
		if sha256Hash != nil {
			gf.IngestSha256 = fmt.Sprintf("%x", sha256Hash.Sum(nil))
			gf.IngestSha256GeneratedAt = utcNow
		}
	}
	// on err, defaults to application/binary
	buf := make([]byte, 256)
	_, _ = reader.Read(buf)
	gf.FileFormat, _ = platform.GuessMimeTypeByBuffer(buf)
	return nil
}
