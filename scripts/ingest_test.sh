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
mkdir -p ~/tmp/bin

echo "Building nsq_service"
cd ~/go/src/github.com/APTrust/exchange/apps/nsq_service
go build -o ~/tmp/bin/nsq_service nsq_service.go

echo "Building apt_volume_service"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_volume_service
go build -o ~/tmp/bin/apt_volume_service apt_volume_service.go

echo "Building apt_bucket_reader"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_bucket_reader
go build -o ~/tmp/bin/apt_bucket_reader apt_bucket_reader.go

echo "Building apt_fetch"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_fetch
go build -o ~/tmp/bin/apt_fetch apt_fetch.go

echo "Building apt_store"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_store
go build -o ~/tmp/bin/apt_store apt_store.go

echo "Building apt_record"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_record
go build -o ~/tmp/bin/apt_record apt_record.go


echo "Starting NSQ"
cd ~/tmp/bin
./nsq_service -config ~/go/src/github.com/APTrust/exchange/config/nsq/integration.config &>/dev/null &
NSQ_PID=$!

echo "Deleting old Rails data"
cd ~/aptrust/pharos
RAILS_ENV=integration bundle exec rake pharos:empty_db

echo "Loading Rails fixtures"
RAILS_ENV=integration bundle exec rake db:fixtures:load

echo "Starting Pharos server"
RAILS_ENV=integration rails server &>~/tmp/test_logs/pharos.log &
sleep 5

echo "Starting Volume Service"
cd ~/tmp/bin
./apt_volume_service -config=config/integration.json &
sleep 3

# Wait for this one to finish
# Note that ~/tmp/logs is set in config/integration.json
# Our integration test in integration/apt_bucket_reader_test.go expects
# to find bucket_reader_stats.json in that directory.
echo "Starting bucket reader"
./apt_bucket_reader -config=config/integration.json -stats=~/tmp/test_logs/bucket_reader_stats.json

echo "Starting apt_fetch"
./apt_fetch -config=config/integration.json &
sleep 20

echo "Starting apt_store"
./apt_store -config=config/integration.json &
sleep 20

echo "Starting apt_record"
./apt_record -config=config/integration.json &

echo "Go ingest processes are running. Control-C to quit."

kill_all()
{
    pkill -TERM -P $$
    echo "We're all done. Logs are in ~/tmp/logs."
}

trap kill_all SIGINT

wait $NSQ_PID
