package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/context"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/workers"
	"os"
)


func main() {
	pathToConfigFile, pathToStatsFile := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	enableStats := pathToStatsFile != ""
	bucketReader := workers.NewAPTBucketReader(_context, enableStats)
	bucketReader.Run()
	if enableStats {
		bucketReader.GetStats().DumpToFile(pathToStatsFile)
		fmt.Println("Wrote stats to", pathToStatsFile)
		_context.MessageLog.Info("Wrote stats to %s", pathToStatsFile)
	}
}


// See if you can figure out from the function name what this does.
func parseCommandLine() (configFile string, statsFile string) {
	var pathToConfigFile string
	var pathToStatsFile string
	flag.StringVar(&pathToConfigFile, "config", "", "Path to APTrust config file")
	flag.StringVar(&pathToStatsFile, "stats", "", "Path to file where we should dump JSON stats")
	flag.Parse()
	if pathToConfigFile == "" {
		printUsage()
		os.Exit(1)
	}
	pathToStatsFile, err := fileutil.ExpandTilde(pathToStatsFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	return pathToConfigFile, pathToStatsFile
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_bucket_reader: Reads the contents of S3 receiving buckets, and creates
WorkItem entries and NSQ entries for bags awaiting ingest in those buckets.

Usage: apt_bucket_reader -config=<absolute path to APTrust config file> -stats=<path_to_stats_file>

Param -config is required.
Param -stats tells us where to dump the stats file. This is mainly for testing
and diagnostics. Do not use -stats in production, because it will use a lot of
memory when there is a lot of data in the receiving buckets and/or in the
WorkItems list.
`
	fmt.Println(message)
}
