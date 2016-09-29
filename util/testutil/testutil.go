package testutil

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/icrowley/fake"
	"github.com/nu7hatch/gouuid"
	"math"
	"math/rand"
	"strings"
	"time"
)

// Bloomsday
var TEST_TIMESTAMP time.Time = time.Date(2016, 6, 16, 10, 24, 16, 0, time.UTC)

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
		Size: int64(rand.Intn(5000000) + 1),
		FileCreated: RandomDateTime(),
		FileModified: RandomDateTime(),
		CreatedAt: RandomDateTime(),
		UpdatedAt: RandomDateTime(),
		Checksums: checksums,
		PremisEvents: events,
		IngestLocalPath: fmt.Sprintf("/mnt/aptrust/data/%s", fileIdentifier),
		IngestMd5: fake.CharactersN(32),
		IngestMd5GeneratedAt: RandomDateTime(),
		IngestMd5VerifiedAt: RandomDateTime(),
		IngestSha256: fake.CharactersN(64),
		IngestSha256GeneratedAt: RandomDateTime(),
		IngestUUID: _uuid.String(),
		IngestUUIDGeneratedAt: RandomDateTime(),
		IngestStorageURL: fmt.Sprintf("https://storage.aws/%s", _uuid.String()),
		IngestStoredAt: RandomDateTime(),
		IngestReplicationURL: fmt.Sprintf("https://replication.aws/%s", _uuid.String()),
		IngestReplicatedAt: RandomDateTime(),
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
		PremisEvents: events,
		CreatedAt: RandomDateTime(),
		UpdatedAt: RandomDateTime(),
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

func MakeTag() (*models.Tag) {
	return &models.Tag{
		Label: fake.Word(),
		Value: fake.Sentence(),
	}
}

func MakeWorkItem() (*models.WorkItem) {
	return &models.WorkItem{
		Id: rand.Intn(50000) + 1,
		ObjectIdentifier: RandomObjectIdentifier(),
		GenericFileIdentifier: "",
		Name: fake.Word(),
		Bucket: "aptrust.receiving.virginia.edu",
		ETag: fake.Word(),
		BagDate: RandomDateTime(),
		InstitutionId: rand.Intn(50000) + 1,
		User: fake.EmailAddress(),
		Date: RandomDateTime(),
		Note: fake.Sentence(),
		Action: RandomAction(),
		Stage: RandomStage(),
		Status: RandomStatus(),
		Outcome: fake.Sentence(),
		Retry: true,
		Node: fake.Word(),
		Pid: rand.Intn(50000) + 1,
		NeedsAdminReview: false,
	}
}

func MakeWorkItemState() (*models.WorkItemState) {
	return &models.WorkItemState{
		Id: rand.Intn(50000) + 1,
		WorkItemId: rand.Intn(50000) + 1,
		Action: constants.ActionIngest,
		State: `{"key1":"value1","key2":"value2"}`,
		CreatedAt: RandomDateTime(),
		UpdatedAt: time.Now().UTC(),
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

func RandomAction() (string) {
	return RandomFromList(constants.ActionTypes)
}

func RandomStage() (string) {
	return RandomFromList(constants.StageTypes)
}

func RandomStatus() (string) {
	return RandomFromList(constants.StatusTypes)
}

func RandomFromList(items []string) (string) {
	i := int(math.Mod(float64(rand.Intn(200)), float64(len(items))))
	return items[i]
}

// Loads an IntellectualObject fixture (a JSON file) from
// the testdata directory for testing.
func LoadIntelObjFixture(filename string) (*models.IntellectualObject, error) {
	data, err := fileutil.LoadRelativeFile(filename)
	if err != nil {
		return nil, err
	}
	intelObj := &models.IntellectualObject{}
	err = json.Unmarshal(data, intelObj)
	if err != nil {
		return nil, err
	}
	return intelObj, nil
}
