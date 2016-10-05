#!/bin/bash

#
# This script provides end-to-end testing for ingest functions.
#

echo "You can turn verbose output on and off by setting LogToStderr"
echo "in the config file at config/integration.json"
echo ""

echo "Getting rid of old logs and data files"
rm -r ~/tmp/*
mkdir -p ~/tmp/test_logs

echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/exchange/apps/nsq_service
go run nsq_service.go -config ~/go/src/github.com/APTrust/exchange/config/nsq/integration.config &>/dev/null &
NSQ_PID=$!

echo "Loading Rails fixtures"
cd ~/aptrust/pharos
RAILS_ENV=integration bundle exec rake db:fixtures:load

echo "Starting Pharos server"
RAILS_ENV=integration rails server &>~/tmp/test_logs/pharos.log &
RAILS_PID=$!
sleep 3

echo "Starting Volume Service"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_volume_service
go run apt_volume_service.go -config=config/integration.json &
VOLUME_SERVICE_PID=$!

# Wait for this one to finish
# Note that ~/tmp/logs is set in config/integration.json
# Our integration test in integration/apt_bucket_reader_test.go expects
# to find bucket_reader_stats.json in that directory.
echo "Starting bucket reader"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_bucket_reader
go run apt_bucket_reader.go -config=config/integration.json -stats=~/tmp/test_logs/bucket_reader_stats.json

echo "Testing bucket reader output"
cd ~/go/src/github.com/APTrust/exchange/integration
go test apt_bucket_reader_test.go
RUN_EXCHANGE_INTEGRATION=true go test -v apt_bucket_reader_test.go

echo "Starting apt_fetch"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_fetch
go run apt_fetch.go -config=config/integration.json &
FETCH_PID=$!

echo "Starting apt_store"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_store
go run apt_store.go -config=config/integration.json &
STORE_PID=$!

echo "Starting apt_record"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_record
go run apt_record.go -config=config/integration.json &
RECORD_PID=$!

echo "Go ingest processes are running. Control-C to quit."

kill_all()
{
    echo "Shutting down NSQ"
    kill -s SIGKILL $NSQ_PID

    echo "Shutting down Volume Service"
    kill -s SIGKILL $VOLUME_SERVICE_PID

    echo "Shutting down Pharos Rails app"
    kill -s SIGKILL $RAILS_PID

    echo "Shutting down apt_fetch"
    kill -s SIGKILL $FETCH_PID

    echo "Shutting down apt_store"
    kill -s SIGKILL $STORE_PID

    echo "Shutting down apt_record"
    kill -s SIGKILL $RECORD_PID

    echo "We're all done. Logs are in ~/tmp/logs."
}

trap kill_all SIGINT

wait $NSQ_PID
