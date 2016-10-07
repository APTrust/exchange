package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/workers"
	"os"
)

// apt_record records IntellectualObjects, GenericFiles, PremisEvents
// and Checksums in Pharos. Those items will have already been stored
// in S3/Glacier by apt_store. This is the third and last step in the
// ingest process.
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
	consumer, err := workers.CreateNsqConsumer(_context.Config, &_context.Config.RecordWorker)
	if err != nil {
		_context.MessageLog.Fatalf(err.Error())
	}
	_context.MessageLog.Info("apt_record started with config %s", _context.Config.ActiveConfig)
	_context.MessageLog.Info("DeleteOnSuccess is set to %t", _context.Config.DeleteOnSuccess)

	recorder := workers.NewAPTRecorder(_context)
	consumer.AddHandler(recorder)
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
apt_record: Records objects, files, events and checksums in Pharos.

Usage: apt_record -config=<absolute path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
