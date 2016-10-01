package models

import (
	"encoding/json"
	"time"
)

// This struct is a special subset of GenericFile, with special JSON
// serialization rules that conform to Rails 4 nested strong paramaters
// naming conventions. When we create GenericFiles in batches, we need
// to send them in this format.
type GenericFileForPharos struct {
	Identifier                   string         `json:"identifier"`
	IntellectualObjectId         int            `json:"intellectual_object_id"`
	IntellectualObjectIdentifier string         `json:"intellectual_object_identifier"`
	FileFormat                   string         `json:"file_format"`
	URI                          string         `json:"uri"`
	Size                         int64          `json:"size"`
	FileCreated                  time.Time      `json:"file_created"`
	FileModified                 time.Time      `json:"file_modified"`
	CreatedAt                    time.Time      `json:"created_at"`
	UpdatedAt                    time.Time      `json:"updated_at"`
	Checksums                    []*Checksum    `json:"checksums_attributes"`
	PremisEvents                 []*PremisEvent `json:"premis_events_attributes"`
}

func NewGenericFileForPharos(gf *GenericFile) (*GenericFileForPharos) {
	return &GenericFileForPharos{
		Identifier:                     gf.Identifier,
		IntellectualObjectId:           gf.IntellectualObjectId,
		IntellectualObjectIdentifier:   gf.IntellectualObjectIdentifier,
		FileFormat:                     gf.FileFormat,
		URI:                            gf.URI,
		Size:                           gf.Size,
		FileCreated:                    gf.FileCreated,
		FileModified:                   gf.FileModified,
		Checksums:                      gf.Checksums,
		PremisEvents:                   gf.PremisEvents,
	}
}

// This struct allows us to serialize a batch of GenericFile objects to JSON
// in the format that Rails 4 strong nested parameters expects. The format
// looks like this:
// {
//    "generic_files": {
//      "files": [
//        {
//          "identifier": "obj/file.txt",
//          ... <more generic file attributes> ...
//          "premis_events_attributes": [ <premis event>, <premis event>, ... ],
//          "checksum_attributes": [ <checksum>, <checksum>, ... ]
//        },
//        { ... another generic file ... },
//        { ... another generic file ... },
//      ]
//    }
// }
type GenericFileBatchForPharos struct {
	Files  []*GenericFileForPharos
}

func NewGenericFileBatchForPharos(genericFiles []*GenericFile) (*GenericFileBatchForPharos) {
	filesForPharos := make([]*GenericFileForPharos, len(genericFiles))
	for i, gf := range genericFiles {
		filesForPharos[i] = NewGenericFileForPharos(gf)
	}
	return &GenericFileBatchForPharos{
		Files: filesForPharos,
	}
}

// This serializes a batch of GenericFiles to JSON in a format that works
// with our Rails app.
func (batch *GenericFileBatchForPharos) ToJson() ([]byte, error) {
	genericFiles := make(map[string][]*GenericFileForPharos)
	genericFiles["files"] = batch.Files
	temp := struct{
		GenericFiles map[string][]*GenericFileForPharos `json:"generic_files"`
	} {
		GenericFiles: genericFiles,
	}
	return json.Marshal(temp)
}
