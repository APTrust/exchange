#!/bin/bash

#
# This script runs integration tests for the DPN REST client
# against a locally-running cluster of DPN REST servers.
# Use Control-C to kill the local DPN cluster after the
# tests finish.
#

[ -z "$DPN_SERVER_ROOT" ] && echo "Set env var DPN_SERVER_ROOT" && exit 1;
[ -z "$EXCHANGE_ROOT" ] && echo "Set env var EXCHANGE_ROOT" && exit 1;

echo "Starting DPN cluster. This takes a minute or so..."
cd $DPN_SERVER_ROOT
rm log/impersonate*
bundle exec ./script/run_cluster.rb -f &
sleep 30

echo "Starting DPN REST client tests"
cd $EXCHANGE_ROOT/dpn
echo -e "\n\n***********************************************"
# Go has to compile helper_test.go because the REST client
# tests uses some fuctions defined in helper_test.go.
go test dpn_rest_client_test.go helper_test.go
echo -e "***********************************************\n\n"

echo -e "\n\nShutting down DPN cluster"
pkill -TERM -P $$
echo "Done."
