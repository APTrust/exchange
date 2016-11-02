package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/testutil"
	"os"
	"strings"
)

// test_push_to_dpn is for integration testing only.
// This app creates a few WorkItems in Pharos asking that
// a handful of bags be pushed to DPN.
func main() {
	pathToConfigFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	for _, s3Key := range testutil.INTEGRATION_GOOD_BAGS[0:7] {
		identifier := strings.Replace(s3Key, "aptrust.receiving.test.", "", 1)
		identifier = strings.Replace(identifier, ".tar", "", 1)
		resp := _context.PharosClient.IntellectualObjectPushToDPN(identifier)
		workItem := resp.WorkItem()
		if resp.Error != nil {
			_context.MessageLog.Error(resp.Error.Error())
		} else if workItem == nil {
			_context.MessageLog.Error("Attempt to create DPN work item for %s returned nil",
				workItem.Id, workItem.ObjectIdentifier)
		} else {
			_context.MessageLog.Info("Created DPN work item #%d for %s",
				workItem.Id, workItem.ObjectIdentifier)
		}
	}
}

func parseCommandLine() (configFile string) {
	var pathToConfigFile string
	flag.StringVar(&pathToConfigFile, "config", "", "Path to APTrust config file")
	flag.Parse()
	if pathToConfigFile == "" {
		printUsage()
		os.Exit(1)
	}
	return pathToConfigFile
}

// Tell the user about the program.
func printUsage() {
	message := `
test_push_to_dpn is for integration testing only.
This app is run after the ingest tests in scripts/ingest_test.sh.
It creates a few WorkItems in Pharos asking that a handful of
APTrust bags be pushed to DPN.

Usage: apt_fetch_and_validate -config=<path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
