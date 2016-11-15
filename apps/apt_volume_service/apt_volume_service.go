package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/service"
	"os"
)

// See printUsage for a description.
func main() {
	pathToConfigFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	_context.MessageLog.Info("apt_volume_service started")

	volumeService := service.NewVolumeService(
		_context.Config.VolumeServicePort,
		_context.MessageLog)
	volumeService.Serve()
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
apt_volume_service keeps track of how much disk space we have in our staging
area. Workers reserve space with the volume service before trying to download
large files for processing. Without this service, we regularly run into errors
because the disk fills up. The service listens for HTTP requests on localhost,
on the port specified in the VolumeServicePort setting of the JSON config file.
Use Control-C, SIGINT, or SIGKILL to shut down the service.

Usage: apt_volume_service -config=<absolute path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
