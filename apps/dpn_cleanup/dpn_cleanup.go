package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	pathToConfigFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	dpnClient, err := network.NewDPNRestClient(
		_context.Config.DPN.RestClient.LocalServiceURL,
		_context.Config.DPN.DPNAPIVersion,
		_context.Config.DPN.RestClient.LocalAuthToken,
		_context.Config.DPN.LocalNode,
		_context.Config.DPN)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	deleteReplicatedBags(_context, dpnClient)
}

func deleteReplicatedBags(_context *context.Context, dpnClient *network.DPNRestClient) {
	_context.MessageLog.Info("Deleting replicated bags in %s",
		_context.Config.DPN.StagingDirectory)
	files, err := ioutil.ReadDir(_context.Config.DPN.StagingDirectory)
	if err != nil {
		_context.MessageLog.Error(err.Error())
		return
	}
	for _, finfo := range files {
		bagUUID := strings.Replace(finfo.Name(), ".tar", "", 1)
		if !util.LooksLikeUUID(bagUUID) {
			continue // Don't delete it if it's not a DPN tar file
		}
		params := url.Values{}
		params.Set("bag", bagUUID)
		params.Set("stored", "true")
		params.Set("from_node", _context.Config.DPN.LocalNode)
		resp := dpnClient.ReplicationTransferList(params)
		if resp.Error != nil {
			_context.MessageLog.Error("Error getting replication info for bag '%s': %v",
				bagUUID, resp.Error.Error())
			continue
		}
		tarfile := filepath.Join(_context.Config.DPN.StagingDirectory, finfo.Name())
		successfulReplications := resp.ReplicationTransfers()
		if len(successfulReplications) >= _context.Config.DPN.ReplicateToNumNodes {
			_context.MessageLog.Info("Deleting %s: %d successful replications",
				tarfile, len(successfulReplications))
			removeFile(_context, tarfile)
			// ------------------------------------------------------------------------
			// Skip this for now. We're linking the entire /home/dpn.*/outbound dirs
			// to the DPN staging dir. If there are no problems with that, delete
			// this whole block of commented code.
			// ------------------------------------------------------------------------
			// for _, xfer := range successfulReplications {
			// 	symlink := fmt.Sprintf("%s/dpn.%s/outbound/%s.tar",
			// 		_context.Config.DPN.RemoteNodeHomeDirectory, xfer.ToNode, bagUUID)
			// 	removeFile(_context, symlink)
			// }
		} else {
			_context.MessageLog.Info("Leaving %s: only %d successful replications so far",
				tarfile, len(successfulReplications))
		}
	}
}

func removeFile(_context *context.Context, pathToFile string) {
	err := os.Remove(pathToFile)
	if err != nil {
		_context.MessageLog.Info("Error deleting %s: %v", pathToFile, err)
	} else {
		_context.MessageLog.Info("Deleted %s", pathToFile)
	}
}

func parseCommandLine() (configFile string) {
	flag.StringVar(&configFile, "config", "", "Path to APTrust config file")
	flag.Parse()
	if configFile == "" {
		printUsage()
		os.Exit(1)
	}
	return configFile
}

// Tell the user about the program.
func printUsage() {
	message := `
dpn_cleanup deletes tar files from the DPN staging directory and the
symlinks to those tar files from the /home/dpn.<user>/outbound directories
once the items have been replicated to the minimum number of required
nodes. The number of required nodes is defined in the config file,
under DPN.ReplicateToNumNodes.

Usage: apt_cleanup -config=<path to config file>

Param -config is required. It can be an absolute path, or a path in the format
config/env.json, where env is dev, test, demo, integration or production.

`
	fmt.Println(message)
}