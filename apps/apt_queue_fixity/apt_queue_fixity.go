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
	pathToConfigFile, maxFiles := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	aptQueue := workers.NewAPTQueueFixity(_context, maxFiles)
	aptQueue.Run()
}

func parseCommandLine() (configFile string, maxFiles int) {
	var pathToConfigFile string
	maxFiles = 100
	flag.StringVar(&pathToConfigFile, "config", "", "Path to APTrust config file")
	flag.IntVar(&maxFiles, "maxFiles", 100, "Maximum number of files to queue")
	flag.Parse()
	if pathToConfigFile == "" {
		printUsage()
		os.Exit(1)
	}
	return pathToConfigFile, maxFiles
}

func printUsage() {
	message := `
apt_queue_fixity: Adds files in need of fixity check to NSQ's
fixity check topic.

Usage: apt_queue_fixity -config=<path to APTrust config file> -maxfiles=<integer>

Param -config is required.

Param -maxfiles is optional. If specified, no more than maxfiles will be
added to NSQ for fixity checking. If not specified, defaults to 100.
`
	fmt.Println(message)
}
