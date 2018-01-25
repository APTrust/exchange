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

var InstitutionIdMap map[string]int

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
	err = initInstitutionIdMap(_context)
	if err != nil {
		// Use "%s" so percent signs in the error message are not
		// interprested as formatting directives.
		_context.MessageLog.Error("%s", err.Error())
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	err = syncToPharos(_context)
	if err != nil {
		_context.MessageLog.Error("%s", err.Error())
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func initInstitutionIdMap(ctx *context.Context) error {
	ctx.MessageLog.Info("Caching institutions")
	InstitutionIdMap = make(map[string]int)
	params := url.Values{}
	params.Add("page", "1")
	params.Add("per_page", "100")
	resp := ctx.PharosClient.InstitutionList(params)
	ctx.MessageLog.Info(resp.Request.URL.Opaque)
	if resp.Error != nil {
		return resp.Error
	}
	for _, inst := range resp.Institutions() {
		if inst.DPNUUID != "" {
			InstitutionIdMap[inst.DPNUUID] = inst.Id
			ctx.MessageLog.Info("(%d) %s: %s", inst.Id, inst.Name, inst.DPNUUID)
		}
	}
	return nil
}

// getLatestTimestamp returns the latest UpdatedAt timestamp
// from the DPN bags table in Pharos.
func getLatestTimestamp(ctx *context.Context) (time.Time, error) {
	ctx.MessageLog.Info("Getting latest timestamp from Pharos")
	params := url.Values{}
	params.Add("sort", "dpn_updated_at DESC")
	resp := ctx.PharosClient.DPNBagList(params)
	ctx.MessageLog.Info(resp.Request.URL.Opaque)
	if resp.Error != nil {
		return time.Time{}, resp.Error
	}
	if resp.DPNBag() == nil {
		return time.Time{}, nil
	}
	return resp.DPNBag().DPNUpdatedAt, nil
}

func syncToPharos(ctx *context.Context) error {
	timestamp, err := getLatestTimestamp(ctx)
	if err != nil {
		return err
	}
	ctx.MessageLog.Info("Most recent DPN bag has update timestamp of %s", timestamp.Format(time.RFC3339))
	ctx.MessageLog.Info("Using DPN API key that starts with %s", ctx.Config.DPN.RestClient.LocalAuthToken[0:4])

	dpnClient, err := network.NewDPNRestClient(
		ctx.Config.DPN.RestClient.LocalServiceURL,
		ctx.Config.DPN.DPNAPIVersion,
		ctx.Config.DPN.RestClient.LocalAuthToken,
		ctx.Config.DPN.LocalNode,
		ctx.Config.DPN)
	if err != nil {
		return fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	ctx.MessageLog.Info("Set up DPN client for %s", ctx.Config.DPN.RestClient.LocalServiceURL)

	params := url.Values{}
	params.Add("after", timestamp.Format(time.RFC3339))
	params.Add("ingest_node", ctx.Config.DPN.LocalNode) // only bags we ingested
	params.Add("page", "1")
	params.Add("page_size", "100")

	ctx.MessageLog.Info("Checking for bags updated since %s", timestamp.Format(time.RFC3339))
	for {
		resp := dpnClient.DPNBagList(params)
		ctx.MessageLog.Info("%s", resp.Request.URL.String())

		if resp.Response != nil {
			ctx.MessageLog.Info("Server responded: %s", resp.Response.Status)
		} else {
			ctx.MessageLog.Warning("Server did not return a response")
		}

		// VERBOSE LOGGING
		// ctx.MessageLog.Info("%v", resp.Request)
		// ctx.MessageLog.Info("%v", resp.Response)
		// data, _ := resp.RawResponseData()
		// ctx.MessageLog.Info(string(data))
		// END VERBOSE LOGGING

		if resp.Error != nil {
			return resp.Error
		}
		ctx.MessageLog.Info("Request returned %d bags", len(resp.Bags()))
		for i, dpnBag := range resp.Bags() {
			if dpnBag == nil {
				ctx.MessageLog.Info("Item %d in bag list is nil", i)
				continue
			}
			// Quit early if this happens. It shouldn't.
			if InstitutionIdMap[dpnBag.Member] == 0 {
				return fmt.Errorf("Pharos has no institution record for DPN member %s",
					dpnBag.Member)
			}
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
	pharosDPNBag.InstitutionId = InstitutionIdMap[dpnBag.Member]
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
	ctx.MessageLog.Info(resp.Request.URL.Opaque)
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
