package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/workers"
	"os"
)

// apt_fetch receives messages from nsqd describing
// items in the S3 receiving buckets. It fetches and and validates
// tar files, then queues them for storage, if they validate successfully.
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
	consumer, err := workers.CreateNsqConsumer(_context.Config, &_context.Config.FetchWorker)
	if err != nil {
		_context.MessageLog.Fatalf(err.Error())
	}
	_context.MessageLog.Info("apt_fetch started")

	fetcher := workers.NewAPTFetcher(_context)
	consumer.AddHandler(fetcher)
	consumer.ConnectToNSQLookupd(_context.Config.NsqLookupd)

	// This reader blocks until we get an interrupt, so our program does not exit.
	<-consumer.StopChan
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
apt_fetch_and_validate: Reads from NSQ to find which tar files are waiting
in the receiving buckets to be ingested. It fetches those files into the
local staging area, validates them, and pushes them into the record queue
if they are valid.

Usage: apt_fetch_and_validate -config=<absolute path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
