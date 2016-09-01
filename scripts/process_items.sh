#!/bin/bash

#
# This script provides end-to-end testing for ingest functions.
#

echo "Getting rid of old logs and data files"
rm -r ~/tmp/*
mkdir -p ~/tmp/logs

echo "Starting NSQ"
cd ~/go/src/github.com/APTrust/exchange/apps/nsq_service
go run nsq_service.go -config ~/go/src/github.com/APTrust/exchange/config/nsq/integration.config &>/dev/null &
NSQ_PID=$!

echo "Loading Rails fixtures"
cd ~/aptrust/pharos
RAILS_ENV=integration bundle exec rake db:fixtures:load

echo "Starting Pharos server"
RAILS_ENV=integration rails server &>~/tmp/logs/pharos.log &
RAILS_PID=$!
sleep 3

# Wait for this one to finish
echo "Starting bucket reader"
cd ~/go/src/github.com/APTrust/exchange/apps/apt_bucket_reader
go run apt_bucket_reader.go -config=config/integration.json

echo "We're all done. Logs are in ~/tmp/logs. Control-C to quit."

kill_all()
{
    echo "Shutting down NSQ"
    kill -s SIGINT $NSQ_PID

    echo "Shutting down Pharos Rails app"
    kill -s SIGINT $RAILS_PID
}

trap kill_all SIGINT

wait $NSQ_PID
