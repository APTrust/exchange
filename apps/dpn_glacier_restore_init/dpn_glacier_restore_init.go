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

// dpn_glacier_restore_init sends requests to AWS asking
// that Glacier files be moved into S3 (temporarily) so
// we can retrieve them. We will initially be retrieving
// Glacier files for fixity checking only. We may later
// retrieve them for restoration as well, though restoration
// is not implemented as of September 2018.
//
// It typically takes several hours for a Glacier file to
// move to S3, and for DPN, we're using the Bulk restore option,
// which is slower and cheaper.
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
	consumer, err := apt_workers.CreateNsqConsumer(_context.Config,
		&_context.Config.DPN.DPNGlacierRestoreWorker)
	if err != nil {
		_context.MessageLog.Fatalf(err.Error())
	}
	_context.MessageLog.Info("dpn_glacier_restore_init started")

	restorer, err := workers.DPNNewGlacierRestoreInit(_context)
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
dpn_glacier_restore_init requests retrieval of Glacier files
into S3. This is the first step in the process of restoring
files and bags from Glacier. After files have been moved from
Glacier into S3, they are accessible for retrieval/restoration.
It usually takes several hours for a Glacier file to move to S3.

Usage: dpn_glacier_restore_init -config=<path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
