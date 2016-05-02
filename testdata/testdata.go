package testdata

import (
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/crowdmob/goamz/s3"
	"github.com/icrowley/fake"
	"github.com/nu7hatch/gouuid"
	"math"
	"math/rand"
	"strings"
	"time"
)

func MakeChecksum() (*models.Checksum) {
	return &models.Checksum{
		Id: rand.Intn(50000) + 1,
		GenericFileId: rand.Intn(50000) + 1,
		Algorithm: RandomAlgorithm(),
		DateTime: RandomDateTime(),
		Digest: fake.Sentence(),
	}
}

func MakeGenericFile(eventCount, checksumCount int, objIdentifier string) (*models.GenericFile) {
	if objIdentifier == "" {
		objIdentifier = RandomObjectIdentifier()
	}
	objIdParts := strings.Split(objIdentifier, "/")
	inst := objIdParts[0]
	objName := objIdParts[1]
	fileIdentifier := RandomFileIdentifier(objIdentifier)
	_uuid, _ := uuid.NewV4()
	checksums := make([]*models.Checksum, checksumCount)
	events := make([]*models.PremisEvent, eventCount)
	for i := 0; i < checksumCount; i++ {
		checksums[i] = MakeChecksum()
	}
	for i := 0; i < eventCount; i++ {
		events[i] = MakePremisEvent()
	}
	return &models.GenericFile {
		Id: rand.Intn(50000) + 1,
		Identifier: fileIdentifier,
		IntellectualObjectId: rand.Intn(50000) + 1,
		IntellectualObjectIdentifier: objIdentifier,
		FileFormat: RandomFileFormat(),
		URI: fmt.Sprintf("%s/%s.%s/%s", constants.S3UriPrefix, constants.ReceiveTestBucketPrefix, inst, objName),
		Size: int64(rand.Intn(5000000)),
		Created: RandomDateTime(),
		Modified: RandomDateTime(),
		Checksums: checksums,
		PremisEvents: events,
		IngestLocalPath: fmt.Sprintf("/mnt/aptrust/data/%s", fileIdentifier),
		IngestMd5: fake.Word(),
		IngestMd5VerifiedAt: RandomDateTime(),
		IngestSha256: fake.Word(),
		IngestSha256GeneratedAt: RandomDateTime(),
		IngestUUID: _uuid.String(),
		IngestUUIDGeneratedAt: RandomDateTime(),
		IngestStorageURL: fmt.Sprintf("https://storage.aws/%s", _uuid.String()),
		IngestStoredAt: RandomDateTime(),
		IngestPreviousVersionExists: false,
		IngestNeedsSave: true,
		IngestErrorMessage: "",
	}
}

func MakeInstitution() (*models.Institution) {
	return &models.Institution{
		Id: rand.Intn(50000) + 1,
		Name: fake.Product(),
		BriefName: fake.Word(),
		Identifier: fake.DomainName(),
	}
}

func MakeIntellectualObject(fileCount, eventCount, checksumCount, tagCount int) (*models.IntellectualObject) {
	objIdentifier := RandomObjectIdentifier()
	objIdParts := strings.Split(objIdentifier, "/")
	inst := objIdParts[0]
	objName := objIdParts[1]
	files := make([]*models.GenericFile, fileCount)
	events := make([]*models.PremisEvent, eventCount)
	tags := make([]*models.Tag, tagCount)
	for i := 0; i < fileCount; i++ {
		files[i] = MakeGenericFile(eventCount, checksumCount, objIdentifier)
	}
	for i := 0; i < eventCount; i++ {
		events[i] = MakePremisEvent()
	}
	for i := 0; i < tagCount; i++ {
		tags[i] = MakeTag()
	}
	return &models.IntellectualObject{
		Id: rand.Intn(50000) + 1,
		Identifier: objIdentifier,
		BagName: objName,
		Institution: inst,
		InstitutionId: rand.Intn(50000) + 1,
		Title: fake.Words(),
		Description: fake.Sentence(),
		Access: RandomAccess(),
		AltIdentifier: fake.Word(),
		GenericFiles: files,
		Events: events,
		IngestS3Bucket: fmt.Sprintf("%s.%s", constants.ReceiveTestBucketPrefix, inst),
		IngestS3Key: fmt.Sprintf("%s.tar", objName),
		IngestTarFilePath: fmt.Sprintf("/mnt/aptrust/data/%s/%s.tar", inst, objName),
		IngestUntarredPath: fmt.Sprintf("/mnt/aptrust/data/%s/%s/", inst, objName),
		IngestRemoteMd5: fake.Word(),
		IngestLocalMd5: fake.Word(),
		IngestMd5Verified: false,
		IngestMd5Verifiable: false,
		IngestFilesIgnored: make([]string, 0),
		IngestTags: tags,
		IngestSummary: MakeWorkSummary(),
		IngestErrorMessage: "",
	}
}

func MakePremisEvent() (*models.PremisEvent) {
	_uuid, _ := uuid.NewV4()
	return &models.PremisEvent{
		Id: rand.Intn(50000) + 1,
		Identifier: _uuid.String(),
		EventType: RandomEventType(),
		DateTime: RandomDateTime(),
		Detail: fake.Words(),
		Outcome: fake.Word(),
		OutcomeDetail: fake.Sentence(),
		Object: fake.Words(),
		Agent: fake.MaleFullName(),
		OutcomeInformation: fake.Sentence(),
	}
}

func MakeS3File() (*models.S3File) {
	key := s3.Key{
		Key: fake.Word(),
		LastModified: RandomDateTime().Format(constants.S3DateFormat),
		Size: int64(rand.Intn(2000000)),
		ETag: fake.Word(),
		StorageClass: fake.JobTitle(),
		Owner: s3.Owner{},
	}
	return &models.S3File{
		BucketName: fake.Word(),
		Key: key,
		ErrorMessage : "",
		DeletedAt: time.Time{},
		DeleteSkippedPerConfig: false,
	}
}

func MakeTag() (*models.Tag) {
	return &models.Tag{
		Label: fake.Word(),
		Value: fake.Sentence(),
	}
}

func MakeWorkSummary() (*models.WorkSummary) {
	return &models.WorkSummary{
		Attempted: true,
		AttemptNumber: 1,
		Errors: make([]string, 0),
		StartedAt: RandomDateTime(),
		FinishedAt: time.Now().UTC(),
		Retry: true,
	}
}

func RandomDateTime() (time.Time) {
	t := time.Now().UTC()
	minutes := rand.Intn(500000) * -1
	return t.Add(time.Duration(minutes) * time.Minute)
}

func RandomAccess() (string) {
	access := []string{"consortia", "institution", "restricted"}
	return RandomFromList(access)
}

func RandomAlgorithm() (string) {
	algs := []string{"md5", "sha256", "sha512"}
	return RandomFromList(algs)
}

func RandomObjectIdentifier() (string) {
	return fmt.Sprintf("%s/%s", fake.DomainName(), fake.FemalePatronymic())
}

func RandomFileIdentifier(objectIdentifier string) (string) {
	if objectIdentifier == "" {
		objectIdentifier = RandomObjectIdentifier()
	}
	extensions := []string{"jpg", "tiff", "ogg", "txt", "xml", "pdf", "mp3", "mp4"}
	ext := RandomFromList(extensions)
	return fmt.Sprintf("%s/%s/%s.%s", objectIdentifier, fake.FemaleFirstName(), fake.ProductName(), ext)
}

func RandomFileFormat() (string) {
	formats := []string{"text/plain", "application/pdf", "audio/x-aac",
		"application/x-doom", "application/vnd.ms-excel"}
	return RandomFromList(formats)
}

func RandomEventType() (string) {
	return RandomFromList(constants.EventTypes)
}


func RandomFromList(items []string) (string) {
	i := int(math.Mod(float64(rand.Intn(200)), float64(len(items))))
	return items[i]
}
