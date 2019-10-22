package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util"
	"net/url"
	"os"
	"strconv"
	"time"
)

// -------------------------------------------------------------------------
// Pending Pharos fix. https://trello.com/c/Yzm3hdlM
// -------------------------------------------------------------------------

var client network.PharosClient
var err error
var writer *csv.Writer

func main() {
	pathToConfigFile, identifierLike, maxFiles := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	writer = csv.NewWriter(os.Stdout)
	writeCSVHeaders()
	run(_context, identifierLike, maxFiles)
}

func parseCommandLine() (configFile string, identifierLike string, maxFiles int) {
	maxFiles = 100
	flag.StringVar(&configFile, "config", "", "Path to APTrust config file")
	flag.StringVar(&identifierLike, "like", "", "Queue only files that have this string in identifier")
	flag.IntVar(&maxFiles, "maxfiles", 100, "Maximum number of files to queue")
	flag.Parse()
	if configFile == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	return configFile, identifierLike, maxFiles
}

func run(_context *context.Context, identifierLike string, maxFiles int) {
	perPage := util.Min(100, maxFiles)
	params := url.Values{}
	itemsAdded := 0
	params.Set("include_relations", "true")
	params.Set("institution_identifier", "aptrust.org")
	params.Set("per_page", strconv.Itoa(perPage))
	params.Set("sort", "created_at")
	params.Set("page", "1")
	if identifierLike != "" {
		params.Set("identifier_like", identifierLike)
	}
	for {
		resp := _context.PharosClient.GenericFileList(params)
		if resp.Error != nil {
			fmt.Fprintln(os.Stderr,
				"Error getting GenericFile list from Pharos: ",
				resp.Error)
			fmt.Fprintln(os.Stderr, resp.Request.URL)
		}
		for _, gf := range resp.GenericFiles() {
			writeCSV(gf)
			itemsAdded += 1
		}
		writer.Flush()
		if resp.HasNextPage() == false || itemsAdded >= maxFiles {
			break
		}
		params = resp.ParamsForNextPage()
	}
}

func writeCSV(gf *models.GenericFile) {
	md5 := gf.GetChecksumByAlgorithm("md5")
	md5Digest := "undefined"
	if md5 != nil {
		md5Digest = md5.Digest
	}
	sha256 := gf.GetChecksumByAlgorithm("sha256")
	sha256Digest := "undefined"
	if sha256 != nil {
		sha256Digest = sha256.Digest
	}
	values := []string{
		strconv.Itoa(gf.Id),
		gf.Identifier,
		strconv.Itoa(gf.IntellectualObjectId),
		gf.IntellectualObjectIdentifier,
		gf.FileFormat,
		gf.URI,
		strconv.FormatInt(gf.Size, 10),
		gf.FileCreated.Format(time.RFC3339),
		gf.FileModified.Format(time.RFC3339),
		gf.CreatedAt.Format(time.RFC3339),
		gf.UpdatedAt.Format(time.RFC3339),
		md5Digest,
		sha256Digest,
	}
	writer.Write(values)
}

func writeCSVHeaders() {
	headers := []string{
		"id", "identifier", "intellectual_object_id", "intellectual_object_identifier",
		"file_format", "uri", "size", "file_created", "file_modified", "created_at",
		"updated_at", "md5", "sha256"}
	writer.Write(headers)
	writer.Flush()
}
