package main

// go build -o create_events
// create_events -config=config/audit_test.json > __events_saved.txt 2>__events_errors.txt

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"os"
	"strings"
	"time"
)

const inputFile = "create_replication_events.txt"
const glacierUrl = "https://s3-us-west-2.amazonaws.com/aptrust.preservation.oregon"

var _context *context.Context

func main() {
	pathToConfigFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context = context.NewContext(config)
	createEvents()
}

func createEvents() {
	file, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "\t")
		uuid := strings.TrimSpace(parts[0])
		// Skip first line, which has headers
		// and all lines that begin with #
		if uuid == "uuid" || strings.HasPrefix(uuid, "#") {
			continue
		}
		identifier := strings.TrimSpace(parts[1])
		event, err := createEvent(uuid, identifier)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"Error creating event for uuid %s, file %s: %v",
				uuid, identifier, err)
			continue
		}
		resp := _context.PharosClient.PremisEventSave(event)
		if resp.Error != nil {
			fmt.Fprintf(os.Stderr,
				"Error creating event for uuid %s, file %s: %v",
				uuid, identifier, err)
			continue
		}
		fmt.Printf("Saved event with id %s for uuid %s, file %s.",
			event.Identifier, uuid, identifier)
		fmt.Println(event)
	}
}

func createEvent(uuid, identifier string) (*models.PremisEvent, error) {
	now := time.Now()
	replicationUrl := fmt.Sprintf("%s/%s", glacierUrl, uuid)
	gf, err := getGenericFile(identifier)
	if err != nil {
		return nil, err
	}
	event, err := models.NewEventGenericFileReplication(now, replicationUrl)
	if err != nil {
		return nil, fmt.Errorf(
			"Error building replication event for %s: %v",
			identifier, err)
	}
	event.IntellectualObjectId = gf.IntellectualObjectId
	event.IntellectualObjectIdentifier = gf.IntellectualObjectIdentifier
	event.GenericFileId = gf.Id
	event.GenericFileIdentifier = gf.Identifier
	event.Detail = fmt.Sprintf("%s. Part of Fall 2019 audit cleanup.", event.Detail)
	return event, nil
}

func getGenericFile(identifier string) (*models.GenericFile, error) {
	resp := _context.PharosClient.GenericFileGet(identifier, false)
	if resp.Error != nil {
		return nil, resp.Error
	}
	gf := resp.GenericFile()
	if gf == nil {
		return nil, fmt.Errorf("Pharos returned nothing for gfid %s", identifier)
	}
	return gf, nil
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

func printUsage() {
	message := `
create_replication_events created replication PREMIS events in Pharos for
files that were copied to aptrust.preservation.oregon as part of the Oct/Nov
2019 audit.

Usage: create_replication_events -config=<path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
