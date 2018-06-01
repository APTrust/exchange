package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/workers"
	"os"
)

// apt_glacier_restore_init sends requests to AWS asking
// that Glacier files be moved into S3 (temporarily) so
// we can retrieve the. It typically takes several hours
// for a Glacier file to move to S3.
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
	consumer, err := workers.CreateNsqConsumer(_context.Config, &_context.Config.GlacierRestoreWorker)
	if err != nil {
		_context.MessageLog.Fatalf(err.Error())
	}
	_context.MessageLog.Info("apt_glacier_restore_init started")

	restorer := workers.NewGlacierRestore(_context)
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
apt_glacier_restore_init requests retrieval of Glacier files
into S3. This is the first step in the process of restoring
files and bags from Glacier. After files have been moved from
Glacier into S3, they are accessible for retrieval/restoration.
It usually takes several hours for a Glacier file to move to S3.

Usage: apt_glacier_restore_init -config=<path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
