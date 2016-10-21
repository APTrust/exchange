#!/bin/bash

#
# This script runs integration tests for the dpn_sync app
# against a locally-running cluster of DPN REST servers.
# Use Control-C to kill the local DPN cluster after the
# tests finish.
#

[ -z "$DPN_SERVER_ROOT" ] && echo "Set env var DPN_SERVER_ROOT" && exit 1;
[ -z "$EXCHANGE_ROOT" ] && echo "Set env var EXCHANGE_ROOT" && exit 1;

rm ~/tmp/bin/*
mkdir -p ~/tmp/bin

echo "Building dpn_sync"
cd $EXCHANGE_ROOT/apps/dpn_sync
go build -o ~/tmp/bin/dpn_sync dpn_sync.go

# Quit here if the build failed.
if [ $? != 0 ]; then
    exit $?
fi

echo "Starting DPN cluster. This takes a minute or so..."
cd $DPN_SERVER_ROOT
rm log/impersonate*
bundle exec ./script/run_cluster.rb -f &
sleep 30

echo "Starting dpn_sync"
cd ~/tmp/bin
./dpn_sync -config=config/integration.json

echo "Running sync post tests"
cd $EXCHANGE_ROOT/dpn/workers
echo -e "\n\n***********************************************"
RUN_DPN_SYNC_POST_TEST=true go test dpn_sync_test.go
echo -e "***********************************************\n\n"

echo -e "\n\nShutting down DPN cluster"
pkill -TERM -P $$
echo "Done."
