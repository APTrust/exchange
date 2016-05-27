package models

import (
	"bufio"
//	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/platform"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nu7hatch/gouuid"
	"hash"
	"io"
	"regexp"
	"strings"
	"time"
)

// VirtualBag creates an IntellectualObject from a bag on disk.
// The IntellectualObject can then be validated by workers.BagValidator.
type VirtualBag struct {
	pathToBag        string
	calculateMd5     bool
	calculateSha256  bool
	tagFilesToParse  []string
	obj              *IntellectualObject
	summary          *WorkSummary
	readIterator     fileutil.ReadIterator
}

// NewVirtualBag creates a new virtual bag. Param pathToBag should
// be an absolute path to either a tar file or a directory containing
// an untarred bag. It pathToBag points to a tar file, the Read()
// function will read the bag without untarring it. Param tagFilesToParse
// should be a list of relative paths, pointing to tag files within the
// bag that should be parsed. For example, "aptrust-info.txt" or
// "dpn_tags/dpn-info.txt" Params calculateMd5 and calculateSha256
// indicate whether we should calculate md5 and/or sha256 checksums
// on the files in the bag.
func NewVirtualBag(pathToBag string, tagFilesToParse []string, calculateMd5, calculateSha256 bool) (*VirtualBag) {
	return &VirtualBag{
		calculateMd5: calculateMd5,
		calculateSha256: calculateSha256,
		pathToBag: pathToBag,
	}
}

// Read() reads the bag and returns an IntellectualObject and a WorkSummary.
// The WorkSummary will include a list of errors, if there were any.
// The list of files contained in IntellectualObject.GenericFiles will include
// ALL files found in the bag, even some we may not want to save, such as
// those beginning with dots and dashes. If you don't want to preserve those
// files you can delete them from the IntellectualObject manually later.
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

// Add every file in the bag to the list of generic files.
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

// Adds a single generic file to the bag.
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

	// START HERE
	// If file should be parsed as tag file, or if file
	// is manifest, copy to buffer, so it can be read
	// multiple times. Do this ONLY for parsable tag files
	// and manifests, which tend to be only a few KB. Other
	// files may be many GB. There's no need to parse them,
	// and they'll eat up all our RAM.
	// ------------------------------------------------------
	//buffer := bytes.NewBuffer(make([]byte, 0))
	//_, err = io.Copy(buffer, reader)

	if vbag.tagFilesToParse != nil && util.StringListContains(vbag.tagFilesToParse, fileSummary.RelPath) {
		vbag.parseTags(reader, fileSummary.RelPath)
	}
	return vbag.calculateChecksums(reader, gf)
}

// Figure out what type of file this is.
func (vbag *VirtualBag) setIngestFileType(gf *GenericFile, fileSummary *fileutil.FileSummary) {
	if strings.HasPrefix(fileSummary.RelPath, "tagmanifest-") {
		gf.IngestFileType = constants.TAG_MANIFEST
	} else if strings.HasPrefix(fileSummary.RelPath, "manifest-") {
		gf.IngestFileType = constants.PAYLOAD_MANIFEST
	} else if strings.HasPrefix(fileSummary.RelPath, "data/") {
		gf.IngestFileType = constants.PAYLOAD_FILE
	} else {
		gf.IngestFileType = constants.TAG_FILE
	}
}

// Calculate the md5 and/or sha256 checksums on a file.
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

// Parse the tag fields in a file.
func (vbag *VirtualBag) parseTags(reader io.Reader, relFilePath string) () {
	re := regexp.MustCompile(`^(\S*\:)?(\s.*)?$`)
	scanner := bufio.NewScanner(reader)
	var tag *Tag
	for scanner.Scan() {
		line := scanner.Text()
		if re.MatchString(line) {
			data := re.FindStringSubmatch(line)
			data[1] = strings.Replace(data[1], ":", "", 1)
			if data[1] != "" {
				if tag.Label != "" {
					vbag.obj.IngestTags = append(vbag.obj.IngestTags, tag)
				}
				tag = NewTag(relFilePath, data[1], strings.Trim(data[2], " "))
				continue
			}
			value := strings.Trim(data[2], " ")
			tag.Value = strings.Join([]string{tag.Value, value}, " ")

		} else {
			vbag.summary.AddError("Unable to parse tag data from line: %s", line)
		}
	}
	if tag.Label != "" {
		vbag.obj.IngestTags = append(vbag.obj.IngestTags, tag)
	}
	if scanner.Err() != nil {
		vbag.summary.AddError("Error reading tag file '%s': %v",
			relFilePath, scanner.Err().Error())
	}
}
