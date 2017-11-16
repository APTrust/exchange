package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/constants"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/partner_apps/common"
	"net/url"
	"os"
	"time"
)

const APIVersion = "v2"

type OutputObject struct {
	WorkItem           *models.WorkItem
	IntellectualObject *models.IntellectualObject
}

// apt_check_ingest returns information about whether an ingest
// has been completed.
func main() {
	fileToCheck := ""
	opts := getUserOptions()
	if opts.HasErrors() {
		fmt.Fprintln(os.Stderr, opts.AllErrorsAsString())
		os.Exit(1)
	}
	args := flag.Args() // non-flag args
	if len(args) > 0 {
		fileToCheck = args[0]
	}
	if fileToCheck == "" {
		fmt.Fprintln(os.Stderr, "Missing required argument filename")
		fmt.Fprintln(os.Stderr, "Try: apt_check_ingest --help")
		os.Exit(1)
	}
	client, err := network.NewPharosClient(opts.PharosURL, APIVersion,
		opts.APTrustAPIUser, opts.APTrustAPIKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	params := url.Values{}
	params.Set("name", fileToCheck)
	params.Set("item_action", constants.ActionIngest)
	params.Set("sort", "date")

	resp := client.WorkItemList(params)
	if resp.Error != nil {
		fmt.Fprintln(os.Stderr, resp.Error.Error())
		os.Exit(1)
	}
	items := resp.WorkItems()
	outputObjects := make([]OutputObject, len(items))
	for i, item := range items {
		outputObjects[i] = OutputObject{
			WorkItem: item,
		}
		if item.ObjectIdentifier != "" {
			resp = client.IntellectualObjectGet(item.ObjectIdentifier, false, false)
			if resp.Error != nil {
				fmt.Fprintln(os.Stderr, resp.Error.Error())
			}
			outputObjects[i].IntellectualObject = resp.IntellectualObject()
		}
	}
	if opts.OutputFormat == "text" {
		printText(outputObjects)
	} else {
		printJson(outputObjects)
	}
}

func printText(objects []OutputObject) {
	for i, obj := range objects {
		ingested := (obj.WorkItem.Stage == constants.StageCleanup &&
			obj.WorkItem.Status == constants.StatusSuccess)
		fmt.Printf("%d) %s\n", i+1, obj.WorkItem.Name)
		fmt.Printf("    Updated: %s, Stage: %s, Status: %s, Ingested: %t\n",
			obj.WorkItem.UpdatedAt.Format(time.RFC3339),
			obj.WorkItem.Stage, obj.WorkItem.Status, ingested)
	}
}

func printJson(objects []OutputObject) {
	jsonBytes, err := json.Marshal(objects)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	fmt.Println(string(jsonBytes))
}

// Get user-specified options from the command line,
// environment, and/or config file.
func getUserOptions() *common.Options {
	opts := parseCommandLine()
	opts.MergeConfigFileOptions()
	opts.VerifyOutputFormat()
	opts.VerifyRequiredAPICredentials()
	return opts
}

func parseCommandLine() *common.Options {
	var pathToConfigFile string
	var pharosEnv string
	var outputFormat string
	var help bool
	flag.StringVar(&pathToConfigFile, "config", "", "Path to partner config file")
	flag.StringVar(&pharosEnv, "env", "production", "Which environment to query: production [default] or demo.")
	flag.StringVar(&outputFormat, "format", "text", "Output format ('text' or 'json')")
	flag.BoolVar(&help, "help", false, "Show help")
	flag.Parse()

	if help {
		printUsage()
		os.Exit(0)
	}

	if pharosEnv != "production" && pharosEnv != "demo" {
		fmt.Fprintln(os.Stderr, "Invalid value for -env:", pharosEnv)
		printUsage()
		os.Exit(0)
	}

	pharosUrl := "https://repo.aptrust.org"
	if pharosEnv == "demo" {
		pharosUrl = "https://demo.aptrust.org"
	}
	return &common.Options{
		PathToConfigFile: pathToConfigFile,
		OutputFormat:     outputFormat,
		PharosURL:        pharosUrl,
	}
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_check_ingest: Query APTrust REST API to discover whether a bag
has completed ingest. You'll need to set the variables
AptrustApiUser and AptrustApiKey in your APTrust config file.

APTrust issues API keys to users by request. The APTrust API user
is the email address of the user to whom the key was issued. If
you're using a config file, the required entries for user and API
key might look like this:

AptrustApiUser = "archivist@example.edu"
AptrustApiKey = "f887afc5e1624eda92ae1a5aecdf210c"

See https://wiki.aptrust.org/Partner_Tools for more info on the
APTrust config file.

Usage: apt_check_ingest [-config=<path to config file>] [-env=<production|demo>] \
                        [-format=<json|text>] <filename.tar>

Option -config is should point the APTrust partner config file that
contains your user email and API key. If you don't want to specify the
user and key in a config file, the program will try to read them from
the environment keys APTRUST_API_USER and APTRUST_API_KEY.

Option -env specifies whether the tool should query the APTrust production
system at https://repo.aptrust.org or the demo system at
https://demo.aptrust.org. If unspecified, this defaults to production.

Option -format specifies whether the result of the query should be printed
to STDOUT in json or plain text format. Default is json.

Param filename.tar is the name of the tar file you uploaded for
ingest. For example, virginia.edu.bag_of_images.tar

You will get multiple results for bags that have been ingested more than
once. For example, if you uploaded version 1 of a bag last year, and then
a newer version today, the output will include results for both bags,
with the most recent version listed first.
`
	fmt.Println(message)
}
