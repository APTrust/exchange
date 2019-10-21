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
	pathToConfigFile, pathToStatsFile, topic, dryRun := parseCommandLine()
	config, err := models.LoadConfigFile(pathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	_context := context.NewContext(config)
	enableStats := pathToStatsFile != ""
	aptQueue := workers.NewAPTQueue(_context, topic, enableStats, dryRun)
	aptQueue.Run()
	if enableStats {
		aptQueue.GetStats().DumpToFile(pathToStatsFile)
		fmt.Println("Wrote stats to", pathToStatsFile)
		_context.MessageLog.Info("Wrote stats to %s", pathToStatsFile)
	}
}

// See if you can figure out from the function name what this does.
func parseCommandLine() (configFile string, statsFile string, topic string, dryRun bool) {
	var pathToConfigFile string
	var pathToStatsFile string
	var topicName string
	flag.StringVar(&pathToConfigFile, "config", "", "Path to APTrust config file")
	flag.StringVar(&pathToStatsFile, "stats", "", "Path to file where we should dump JSON stats")
	flag.StringVar(&topicName, "topic", "", "Queue only those items bound for this topic")
	flag.BoolVar(&dryRun, "dryrun", false, "If true, do a dry run, logging what would be queued without actually sending anything to NSQ")
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
	return pathToConfigFile, pathToStatsFile, topicName, dryRun
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_queue: Queues WorkItems in NSQ. Any WorkItem that does not have
a queued_at timestamp will have its ID copied into the appropriate
NSQ topic. Topics are specified in the NSQTopic property of each Worker
entry in the config file. For example, the topic for fixity checking
would be in config/<environment>.json, in the NsqTopic property of the
FixityWorker section.

Usage: apt_queue -config=<path to APTrust config file> -topic=<topic_name> -stats=<path_to_stats_file> -dryrun=<true>

Param -config is required.

Param -topic is optional. If specified, this will queue only those items
bound for that topic. If unspecified, this will queue all items that need
to be queued. Queue names are specified in the config files and include
apt_fetch_topic, apt_store_topic, apt_record_topic, apt_restore_topic,
apt_file_delete_topic, and apt_fixity_topic.

Param -stats tells us where to dump the stats file. This is mainly for testing
and diagnostics. Do not use -stats in production, because it will use a lot of
memory when there is a lot of data in the receiving buckets and/or in the
WorkItems list.

If optional param dryrun is true, apt_queue will print messages to the log
describing everything it would queue, and which queue each item would go
into, but it will not actually queue anything.
`
	fmt.Println(message)
}
