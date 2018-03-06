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

// deleteReplicated bags deletes DPN bags (tar files) after they've
// been replicated to the required number of nodes. (That's 2 nodes
// as of February, 2017.) When we ingest bags and ask that other
// nodes replicate them, the bags go into a subdirectory of the DPN
// staging directory. That subdirectory has the domain name of the
// institution that owns the bag. For example, <dpn_staging>/ncsu.edu,
// <dpn_staging>/virginia.edu, etc.
func deleteReplicatedBags(_context *context.Context, dpnClient *network.DPNRestClient) {
	files, err := ioutil.ReadDir(_context.Config.DPN.StagingDirectory)
	if err != nil {
		_context.MessageLog.Error(err.Error())
		return
	}
	for _, f := range files {
		if f.IsDir() {
			pathToDir := filepath.Join(_context.Config.DPN.StagingDirectory, f.Name())
			cleanDirectory(_context, dpnClient, pathToDir)
		}
	}
}

func cleanDirectory(_context *context.Context, dpnClient *network.DPNRestClient, directory string) {
	_context.MessageLog.Info("Deleting replicated bags in %s", directory)
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		_context.MessageLog.Error(err.Error())
		return
	}
	for _, finfo := range files {
		bagUUID := strings.Replace(finfo.Name(), ".tar", "", 1)
		if !util.LooksLikeUUID(bagUUID) {
			continue // Don't delete it if it's not a DPN tar file
		}

		// Make sure bag exists before checking for replications.
		// The DPN server will tell us that non-existent bags have
		// been replicated, and that causes us to delete some tar
		// files from our staging area before we've had a chance
		// to store them in DPN.
		// PT #153636373
		// https://github.com/dpn-admin/dpn-server/issues/158
		bagResponse := dpnClient.DPNBagGet(bagUUID)
		if bagResponse.Error != nil {
			_context.MessageLog.Error("Error getting bag record '%s': %v",
				bagUUID, bagResponse.Error.Error())
			continue
		}
		if bagResponse.Bag() == nil {
			_context.MessageLog.Info("DPN has no record of bag %s yet. Skipping deletion for now.",
				bagUUID)
			continue
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
		tarfile := filepath.Join(directory, finfo.Name())
		successfulReplications := resp.ReplicationTransfers()

		// PT #155739755: Make sure completed replications are from two
		// different nodes before deleting the bag from staging.
		replicatingNodes := make(map[string]bool)
		for _, repl := range successfulReplications {
			replicatingNodes[repl.ToNode] = true
		}

		if len(replicatingNodes) >= _context.Config.DPN.ReplicateToNumNodes {
			_context.MessageLog.Info("Deleting %s: item is replicated at %d nodes",
				tarfile, len(successfulReplications))
			removeFile(_context, tarfile)
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

Usage: dpn_cleanup -config=<path to config file>

Param -config is required. It can be an absolute path, or a path in the format
config/env.json, where env is dev, test, demo, integration or production.

`
	fmt.Println(message)
}
