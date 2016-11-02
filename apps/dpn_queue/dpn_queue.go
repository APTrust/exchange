package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/workers"
	"github.com/APTrust/exchange/models"
	"os"
)


func main() {
	pathToConfigFile, hours := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	dpnQueue, err := workers.NewDPNQueue(_context, hours)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context.MessageLog.Info("dpn_queue started with config %s", pathToConfigFile)
	_context.MessageLog.Info("Checking requests over the past %d hours", hours)
	dpnQueue.Run()
}


// See if you can figure out from the function name what this does.
func parseCommandLine() (configFile string, hours int) {
	flag.StringVar(&configFile, "config", "", "Path to APTrust config file")
	flag.IntVar(&hours, "hours", 8, "Check for requests over the past N hours")
	flag.Parse()
	if configFile == "" {
		printUsage()
		os.Exit(1)
	}
	return configFile, hours
}

// Tell the user about the program.
func printUsage() {
	message := `
dpn_queue checks the local DPN REST server for pending replication and restore
requests, and checks WorkItems in the local Pharos server for pending DPN
ingest requests. This program creates new DPNWorkItems in Pharos for replication
and restore requests, and it creates entries in NSQ for new replication,
restore, and ingest requests. It then sets the queued_at field for all queued
WorkItems and DPNWorkItems.

Usage: apt_bucket_reader -config=<path to config file> -hours=<hours>

Param -config is required. It can be an absolute path, or a path in the format
config/env.json, where env is dev, test, demo, integration or production.

Param -hours tells the application to check for requests updated over the
past N hours. If not specified, hours defaults to 8.
`
	fmt.Println(message)
}
