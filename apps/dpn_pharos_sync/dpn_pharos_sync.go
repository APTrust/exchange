package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	dpn_models "github.com/APTrust/exchange/dpn/models"
	"github.com/APTrust/exchange/dpn/network"
	"github.com/APTrust/exchange/models"
	"net/url"
	"os"
	"time"
)

// dpn_sync syncs data in our local DPN registry by pulling data about
// bags, replication requests, etc. from other nodes. See printUsage().

func main() {
	pathToConfigFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	err = syncToPharos(_context)
	if err != nil {
		_context.MessageLog.Error(err.Error())
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
}

// getLatestTimestamp returns the latest UpdatedAt timestamp
// from the DPN bags table in Pharos.
func getLatestTimestamp(ctx *context.Context) (time.Time, error) {
	params := url.Values{}
	params.Add("sort", "dpn_upated_at DESC")
	resp := ctx.PharosClient.DPNBagList(params)
	if resp.Error != nil {
		return time.Time{}, resp.Error
	}
	return resp.DPNBag().DPNUpdatedAt, nil
}

func syncToPharos(ctx *context.Context) error {
	timestamp, err := getLatestTimestamp(ctx)
	if err != nil {
		return err
	}
	dpnClient, err := network.NewDPNRestClient(
		ctx.Config.DPN.RestClient.LocalServiceURL,
		ctx.Config.DPN.DPNAPIVersion,
		ctx.Config.DPN.RestClient.LocalAuthToken,
		ctx.Config.DPN.LocalNode,
		ctx.Config.DPN)
	if err != nil {
		return fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	params := url.Values{}
	params.Add("after", timestamp.Format(time.RFC3339))
	params.Add("page", "1")
	params.Add("page_size", "100")

	for {
		resp := dpnClient.DPNBagList(params)
		if resp.Error != nil {
			return resp.Error
		}
		for _, bag := range resp.Bags() {
			pharosDPNBag := convertToPharos(bag)
			saveResponse := ctx.PharosClient.DPNBagSave(pharosDPNBag)
			if saveResponse.Error != nil {
				ctx.MessageLog.Error("Error saving DPN Bag %s to Pharos: %v",
					bag.UUID, saveResponse.Error)
			} else {
				ctx.MessageLog.Info("Saved DPN Bag %s with id %d",
					bag.UUID, saveResponse.DPNBag().Id)
			}
		}
		if !resp.HasNextPage() {
			break
		} else {
			params = resp.ParamsForNextPage()
		}
	}

	return nil
}

func convertToPharos(dpnBag *dpn_models.DPNBag) *models.PharosDPNBag {
	return &models.PharosDPNBag{}
}

// See if you can figure out from the function name what this does.
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
dpn_pharos_sync syncs data from our local DPN registry to Pharos.

Usage: dpn_sync -config=<absolute path to APTrust config file>

Param -config is required.
`
	fmt.Println(message)
}
