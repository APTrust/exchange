package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/workers"
	"os"
)

// apt_file_delete deletes individual files from long-term storage.
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
	consumer, err := workers.CreateNsqConsumer(_context.Config, &_context.Config.FileDeleteWorker)
	if err != nil {
		_context.MessageLog.Fatalf(err.Error())
	}
	_context.MessageLog.Info("apt_file_delete started")

	deleter := workers.NewAPTFileDeleter(_context)
	consumer.AddHandler(deleter)
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
apt_file_delete deletes individual GenericFiles from long-term
storage in S3 and Glacier.

Usage: apt_file_delete -config=<path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
