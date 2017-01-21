package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/util/testutil"
	"os"
)

func main() {
	pathToLogFile, identifier := parseCommandLine()
	jsonString, err := testutil.ExtractJson(pathToLogFile, identifier)
	if err == nil {
		fmt.Println(jsonString)
		os.Exit(0)
	} else {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func parseCommandLine() (string, string) {
	var pathToLogFile string
	var identifier string
	flag.StringVar(&pathToLogFile, "log", "", "Path to JSON log file")
	flag.StringVar(&identifier, "identifier", "", "Identifier of item to find")
	flag.Parse()
	if pathToLogFile == "" || identifier == "" {
		printUsage()
		os.Exit(1)
	}
	return pathToLogFile, identifier
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_json_extractor: Extracts the latest JSON record for the item
identified by param identifier from the JSON log and prints it to
stdout. You'll normally want to pipe the output to a file, since
it can be large. JSON log data is always at least current with, and
often ahead of data in WorkItemState.State. For some items where
processing has stalled or failed, you may want to overwrite
WorkItemState.state with the JSON data from the logs, and then
restart processing.

Usage: apt_json_extractor -log=<path to JSON log file> -identifier=<identifier>

Param pathToLogFile should be an absolute path to a JSON log file,
such as /mnt/efs/apt/logs/apt_recorder.json.

Param identifier is the identifier that can locate the record. This
varies according to the log you're searching. Log files and their
corresponding identifiers are as follows:

apt_restore.json -> IntellectualObject.Identifier
e.g. virginia.edu/bag_o_goodies

apt_fetch.json, apt_store.json, apt_record.json -> S3 bucket and key
e.g. aptrust.receiving.virginia.edu/bag_o_goodies.tar

dpn_ingest.json -> bag name, with .tar extension, without institution prefix
e.g. bag_o_goodies.tar

dpn_replicate.json -> ReplicationId, which is a uuid
e.g. 45498989-3465-416d-9b49-767f1db22cd6

`
	fmt.Println(message)
}
