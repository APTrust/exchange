package testutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	dpn_models "github.com/APTrust/exchange/dpn/models"
	apt_models "github.com/APTrust/exchange/models"
	"io"
	"os"
	"strings"
)

// FindIngestManifestInLog returns the IngestManifest for the specified
// bag in the specified JSON log file. Param bagName should
// include the .tar extension. So "mybag.tar", not "mybag" or "test.edu/mybag".
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

// FindDPNIngestManifestInLog returns the DPNIngestManifest for the
// specified bag in the specified log file. Param bagName should
// include the .tar extension. So "mybag.tar", not "mybag" or "test.edu/mybag".
func FindDPNIngestManifestInLog(pathToLogFile, bagName string) (manifest *dpn_models.DPNIngestManifest, err error) {
	file, err := os.Open(pathToLogFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	jsonString := findJsonString(file, bagName)
	if len(jsonString) == 0 {
		err = fmt.Errorf("Bag %s not found in %s", bagName, pathToLogFile)
	} else {
		manifest = &dpn_models.DPNIngestManifest{}
		err = json.Unmarshal([]byte(jsonString), manifest)
	}
	return manifest, err
}

func FindReplicationManifestInLog(pathToLogFile, replicationId string) (manifest *dpn_models.ReplicationManifest, err error) {
	file, err := os.Open(pathToLogFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	jsonString := findJsonString(file, replicationId)
	if len(jsonString) == 0 {
		err = fmt.Errorf("Replication %s not found in %s", replicationId, pathToLogFile)
	} else {
		manifest = &dpn_models.ReplicationManifest{}
		err = json.Unmarshal([]byte(jsonString), manifest)
	}
	return manifest, err
}

// findJsonString returns the string of JSON found between the beginning
// and end markers for the specified bag in the file reader.
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
