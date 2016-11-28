package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/workers"
	"github.com/APTrust/exchange/models"
	apt_workers "github.com/APTrust/exchange/workers"
	"os"
)

// dpn_store copies valid replicated bags from our staging area
// to long-term storage.

func main() {
	pathToConfigFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	_context.MessageLog.Info("Connecting to NSQLookupd at %s", _context.Config.NsqLookupd)
	_context.MessageLog.Info("NSQDHttpAddress is %s", _context.Config.NsqdHttpAddress)
	consumer, err := apt_workers.CreateNsqConsumer(_context.Config, &_context.Config.DPN.DPNReplicationStoreWorker)
	if err != nil {
		_context.MessageLog.Fatalf(err.Error())
	}
	storer, err := workers.NewDPNReplicationStorer(_context)
	if err != nil {
		_context.MessageLog.Error(err.Error())
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context.MessageLog.Info("dpn_replication_store started")
	consumer.AddHandler(storer)
	consumer.ConnectToNSQLookupd(_context.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
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
dpn_store copies valid replicated bags from our staging area
to long-term storage

Usage: dpn_store -config=<path to APTrust config file>

Param -config is required and can be an absolute path or config/env.json,
where env is dev, test, production, etc.

`
	fmt.Println(message)
}
