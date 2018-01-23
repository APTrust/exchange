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
	params.Add("ingest_node", ctx.Config.DPN.LocalNode) // only bags we ingested
	params.Add("page", "1")
	params.Add("page_size", "100")

	for {
		resp := dpnClient.DPNBagList(params)
		if resp.Error != nil {
			return resp.Error
		}
		for _, dpnBag := range resp.Bags() {
			existingBag := getExistingPharosDPNBag(ctx, dpnBag.UUID)
			existingBagId := 0
			if existingBag != nil {
				existingBagId = existingBag.Id
			}
			updatedBag := convertToPharos(dpnBag, existingBagId)
			saveResponse := ctx.PharosClient.DPNBagSave(updatedBag)
			if saveResponse.Error != nil {
				ctx.MessageLog.Error("Error saving DPN Bag %s to Pharos: %v",
					dpnBag.UUID, saveResponse.Error)
			} else {
				ctx.MessageLog.Info("Saved DPN Bag %s with id %d",
					dpnBag.UUID, saveResponse.DPNBag().Id)
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

func convertToPharos(dpnBag *dpn_models.DPNBag, existingBagId int) *models.PharosDPNBag {
	pharosDPNBag := &models.PharosDPNBag{
		Id: existingBagId,
	}
	pharosDPNBag.InstitutionId = pharosInstIdFor(dpnBag.Member)
	pharosDPNBag.ObjectIdentifier = dpnBag.LocalId
	pharosDPNBag.DPNIdentifier = dpnBag.UUID
	pharosDPNBag.DPNSize = dpnBag.Size
	if len(dpnBag.ReplicatingNodes) > 2 {
		pharosDPNBag.Node3 = dpnBag.ReplicatingNodes[2]
	}
	if len(dpnBag.ReplicatingNodes) > 1 {
		pharosDPNBag.Node2 = dpnBag.ReplicatingNodes[1]
	}
	if len(dpnBag.ReplicatingNodes) > 0 {
		pharosDPNBag.Node1 = dpnBag.ReplicatingNodes[0]
	}
	pharosDPNBag.DPNCreatedAt = dpnBag.CreatedAt
	pharosDPNBag.DPNUpdatedAt = dpnBag.UpdatedAt
	return pharosDPNBag
}

func getExistingPharosDPNBag(ctx *context.Context, dpnUUID string) *models.PharosDPNBag {
	params := url.Values{}
	params.Add("dpn_identifier", dpnUUID)
	params.Add("page", "1")
	params.Add("page_size", "10")
	resp := ctx.PharosClient.DPNBagList(params)
	if resp.Error != nil {
		// Quit here, so we don't corrupt the DPNBags table.
		// We don't want to insert a bag with a DPN UUID that's already
		// in the table.
		ctx.MessageLog.Error(resp.Error.Error())
		fmt.Fprintf(os.Stderr, resp.Error.Error())
		os.Exit(1)
	}
	count := len(resp.DPNBags())
	if count > 1 {
		// Again: quit. Duplicate records need to be fixed
		// before we proceed.
		ctx.MessageLog.Error("Fatal: Found %d records for DPN Bag %s", count, dpnUUID)
		os.Exit(1)
	}
	if count == 0 {
		return nil
	}
	return resp.DPNBag()
}

func pharosInstIdFor(dpnMemberUUID string) int {
	return 0
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
