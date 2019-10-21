package testutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	apt_models "github.com/APTrust/exchange/models"
	"io"
	"os"
	"strings"
)

// FindIngestManifestInLog returns the IngestManifest for the specified
// bag in the specified JSON log file. Param bucketAndKey is the S3 bucket
// name and key name, separated by a slash. Key name ends with ".tar". For example,
// "aptrust.receiving.virginia.edu/bag_o_goodies.tar".
func FindIngestManifestInLog(pathToLogFile, bagName string) (manifest *apt_models.IngestManifest, err error) {
	file, err := os.Open(pathToLogFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	jsonString := findJsonString(file, bagName)
	if len(jsonString) == 0 {
		err = fmt.Errorf("Bag %s not found in %s", bagName, pathToLogFile)
	} else {
		manifest = &apt_models.IngestManifest{}
		err = json.Unmarshal([]byte(jsonString), manifest)
	}
	return manifest, err
}

// FindRestoreStateInLog returns the RestoreState for the specified
// IntellectualObject identifier in the specified JSON log file.
func FindRestoreStateInLog(pathToLogFile, objIdentifier string) (restoreState *apt_models.RestoreState, err error) {
	file, err := os.Open(pathToLogFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	jsonString := findJsonString(file, objIdentifier)
	if len(jsonString) == 0 {
		err = fmt.Errorf("Bag %s not found in %s", objIdentifier, pathToLogFile)
	} else {
		restoreState = &apt_models.RestoreState{}
		err = json.Unmarshal([]byte(jsonString), restoreState)
	}
	return restoreState, err
}

// FindFileRestoreStateInLog returns the FileRestoreState for the specified
// GenericFile identifier in the specified JSON log file.
func FindFileRestoreStateInLog(pathToLogFile, gfIdentifier string) (restoreState *apt_models.FileRestoreState, err error) {
	file, err := os.Open(pathToLogFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	jsonString := findJsonString(file, gfIdentifier)
	if len(jsonString) == 0 {
		err = fmt.Errorf("File %s not found in %s", gfIdentifier, pathToLogFile)
	} else {
		restoreState = &apt_models.FileRestoreState{}
		err = json.Unmarshal([]byte(jsonString), restoreState)
	}
	return restoreState, err
}

// ExtractJson extracts the last JSON record for the specified
// bag from the specified JSON log file. JSON records may appear more
// than once in a JSON log, if the system attempted to process the item
// multiple times. The last JSON record represents the item in its most
// current state.
//
// Param pathToLogFile should be an absolute path to a JSON log file,
// such as /mnt/efs/apt/logs/apt_recorder.json.
//
// Param identifier is the identifier that can locate the record. This
// varies according to the log you're searching. Log files and their
// corresponding identifiers are as follows:
//
// apt_restore.json -> IntellectualObject.Identifier
// e.g. virginia.edu/bag_o_goodies
//
// apt_fetch.json, apt_store.json, apt_record.json -> S3 bucket and key
// e.g. aptrust.receiving.virginia.edu/bag_o_goodies.tar
//
// e.g. bag_o_goodies.tar
//
func ExtractJson(pathToLogFile, identifier string) (string, error) {
	file, err := os.Open(pathToLogFile)
	if err != nil {
		return "", err
	}
	defer file.Close()
	jsonString := findJsonString(file, identifier)
	if jsonString == "" {
		return "", fmt.Errorf("Identifier '%s' not found in %s", identifier, pathToLogFile)
	}
	return jsonString, nil
}

// findJsonString returns the string of JSON found between the beginning
// and end markers for the specified bag in the file reader.
// If there is more than one JSON record for the specified marker,
// this returns only the LAST record, which is the most up-to-date.
func findJsonString(file io.Reader, marker string) string {
	startPrefix := fmt.Sprintf("-------- BEGIN %s", marker)
	endPrefix := fmt.Sprintf(" -------- END %s", marker)
	inJson := false
	foundOne := false
	jsonLines := make([]string, 0)
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.HasPrefix(line, startPrefix) {
			inJson = true
			if foundOne {
				// Replace the old with the new
				// because we only want the last
				// known state of this object.
				jsonLines = nil
				jsonLines = make([]string, 0)
			}
			continue
		} else if strings.HasPrefix(line, endPrefix) {
			inJson = false
			foundOne = true
		}
		if inJson {
			jsonLines = append(jsonLines, line)
		}
	}
	return strings.Join(jsonLines, "")
}
