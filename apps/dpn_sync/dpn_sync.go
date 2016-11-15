package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/workers"
	"github.com/APTrust/exchange/models"
	"os"
)

// dpn_sync syncs data in our local DPN registry by pulling data about
// bags, replication requests, etc. from other nodes. See printUsage().

func main() {
	pathToConfigFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	dpnSync, err := workers.NewDPNSync(_context)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	dpnSync.Run()
}

// See if you can figure out from the function name what this does.
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
dpn_sync syncs data in our local DPN registry by pulling data about
bags, replication requests, etc. from other nodes. The other nodes
have authoritative data for all bags for which they are the admin
node. We pull data about TDR bags, requests, etc. from TDR. We pull
data about SDR bags, requests, etc. from SDR. And so on. This typically
runs as a cron job. There are separate config files for demo, production,
etc.

Usage: dpn_sync -config=<absolute path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
