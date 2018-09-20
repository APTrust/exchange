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

// dpn_s3_retriever retrieves items from the DPN restoration
// bucket to local storage. We do this primarily for fixity
// checking, when we have to revalidate the entire bag and
// calculate the fixity on the tag manifest.
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
	consumer, err := apt_workers.CreateNsqConsumer(_context.Config, &_context.Config.DPN.DPNS3DownloadWorker)
	if err != nil {
		_context.MessageLog.Fatalf(err.Error())
	}
	_context.MessageLog.Info("dpn_s3_retriever started")

	restorer, err := workers.NewDPNS3Retriever(_context)
	if err != nil {
		_context.MessageLog.Fatalf(err.Error())
	}
	consumer.AddHandler(restorer)
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
dpn_s3_retriever retrieves items from the DPN restoration
bucket to local storage. We do this primarily for fixity
checking, when we have to revalidate the entire bag and
calculate the fixity on the tag manifest.

Usage: dpn_s3_retriever -config=<path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
