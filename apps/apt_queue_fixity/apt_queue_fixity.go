package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/workers"
	"os"
)

func main() {
	pathToConfigFile, identifierLike, maxFiles := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	aptQueue := workers.NewAPTQueueFixity(_context, identifierLike, maxFiles)
	aptQueue.Run()
}

func parseCommandLine() (configFile string, identifierLike string, maxFiles int) {
	maxFiles = 100
	flag.StringVar(&configFile, "config", "", "Path to APTrust config file")
	flag.StringVar(&identifierLike, "like", "", "Queue only files that have this string in identifier")
	flag.IntVar(&maxFiles, "maxfiles", 100, "Maximum number of files to queue")
	flag.Parse()
	if configFile == "" {
		printUsage()
		os.Exit(1)
	}
	return configFile, identifierLike, maxFiles
}

func printUsage() {
	message := `
apt_queue_fixity: Adds files in need of fixity check to NSQ's
fixity check topic.

Usage: apt_queue_fixity -config=<path to APTrust config file> -like=<string> -maxfiles=<integer>

Param -config is required.

Param -like is optional. If specified, this will only queue files
whose identifier contains the specified string. E.g. -like=virginia.edu/photos
will only queue files with virginia.edu/photos in the GenericFile.Identifier.

Param -maxfiles is optional. If specified, no more than maxfiles will be
added to NSQ for fixity checking. If not specified, defaults to 100.
`
	fmt.Println(message)
}
