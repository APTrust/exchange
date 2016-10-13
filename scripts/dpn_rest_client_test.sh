#!/bin/bash

#
# This script runs integration tests for the DPN REST client
# against a locally-running cluster of DPN REST servers.
# Use Control-C to kill the local DPN cluster after the
# tests finish.
#

# TODO: Set this dir name in a config file?
echo "Starting DPN cluster. This takes a minute or so..."
cd ~/dpn/dpn-server
bundle exec ./script/run_cluster.rb -f &
sleep 60

echo "Starting DPN REST client tests"
cd ~/go/src/github.com/APTrust/exchange/dpn
go test

echo "\n\nShutting down DPN cluster"
pkill -TERM -P $$
echo "Done."
