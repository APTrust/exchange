package testutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/APTrust/exchange/models"
	"io"
	"os"
	"strings"
)

func FindResultInLog(pathToLogFile, bagName string) (ingestManifest *models.IngestManifest, err error) {
	file, err := os.Open(pathToLogFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	jsonString := findJsonString(file, bagName)
	if len(jsonString) == 0 {
		err = fmt.Errorf("Bag %s not found in %s", bagName, pathToLogFile)
	} else {
		ingestManifest = &models.IngestManifest{}
		err = json.Unmarshal([]byte(jsonString), ingestManifest)
	}
	return ingestManifest, err
}

func findJsonString(file io.Reader, bagName string) (string) {
	startPrefix := fmt.Sprintf("-------- BEGIN %s", bagName)
	endPrefix := fmt.Sprintf(" -------- END %s", bagName)
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
