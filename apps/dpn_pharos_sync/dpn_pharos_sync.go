package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	//"github.com/APTrust/exchange/dpn/network"
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
	err = syncToPharos(_context)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// getLatestTimestamp returns the latest UpdatedAt timestamp
// from the DPN bags table in Pharos.
func getLatestTimestamp(ctx *context.Context) {
	//ctx.PharosClient.
}

func syncToPharos(ctx *context.Context) error {
	// get latest timestamp
	// get all DPN bags updated since that timestamp
	// for each bag:
	//    convert to Pharos DPNBag record
	//    save to Pharos

	// localClient, err := network.NewDPNRestClient(
	// 	ctx.Config.DPN.RestClient.LocalServiceURL,
	// 	ctx.Config.DPN.DPNAPIVersion,
	// 	ctx.Config.DPN.RestClient.LocalAuthToken,
	// 	ctx.Config.DPN.LocalNode,
	// 	ctx.Config.DPN)
	// if err != nil {
	// 	return fmt.Errorf("Error creating local DPN REST client: %v", err)
	// }

	return nil
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
dpn_pharos_sync syncs data from our local DPN registry to Pharos.

Usage: dpn_sync -config=<absolute path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
