package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/workers"
	"os"
	"time"
)

// 20 GB
const MAX_FILE_SIZE = int64(21474836480)
const DAYS_SINCE_INGEST = 180
const DAYS_SINCE_LAST_RESTORE = 180

func main() {
	pathToConfigFile, dryRun := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	createdBefore := time.Now().UTC().AddDate(0, 0, DAYS_SINCE_INGEST)
	notRestoredSince := time.Now().UTC().AddDate(0, 0, DAYS_SINCE_LAST_RESTORE)
	_context := context.NewContext(config)
	worker := workers.NewAPTSpotTestRestore(_context, MAX_FILE_SIZE, createdBefore, notRestoredSince)
	worker.DryRun = dryRun
	items, err := worker.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error: ", err.Error())
	} else {
		printResults(items)
	}
}

func printResults(items []*models.WorkItem) {
	fmt.Println("Created Restore WorkItems for the following intellectual objects:")
	for _, item := range items {
		fmt.Println(item.ObjectIdentifier)
	}
}

// See if you can figure out from the function name what this does.
func parseCommandLine() (string, bool) {
	var pathToConfigFile string
	dryRun := flag.Bool("dryrun", false, "List which bags would be chosen, but don't queue any WorkItems")
	flag.StringVar(&pathToConfigFile, "config", "", "Path to APTrust config file")
	flag.Parse()
	if pathToConfigFile == "" {
		printUsage()
		os.Exit(1)
	}
	return pathToConfigFile, *dryRun
}

// Tell the user about the program.
func printUsage() {
	message := `

apt_spot_test_restore selects one bag from each institution to restore, so that
APTrust and its depositors can see that bag restoration is working properly.
After selecting a bag, this app creates a Restore WorkItem in Pharos for the
bag. From there, it follows the normal restoration process.

The restorer chooses the first bag from each institution that meets the
following criteria:

* Bag is less than 20 GB in size.
* Bag was ingested at least 180 days ago.
* Bag has not been restored in the past 180 days.

Usage:

    apt_spot_test_restore -config=<absolute path to APTrust config file>
    apt_spot_test_restore -config=<absolute path to APTrust config file> -dryrun

Param -config is required.

If param -dryrun is present, the program will return a list of bags that would be
queued for restoration, but it won't actually queue them.

`
	fmt.Println(message)
}
