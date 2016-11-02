#!/bin/bash

#
# This script runs DPN replication tests against a locally-running
# cluster of DPN REST servers. Use Control-C to kill the local DPN
# cluster after the tests finish.

[ -z "$DPN_SERVER_ROOT" ] && echo "Set env var DPN_SERVER_ROOT" && exit 1;
[ -z "$EXCHANGE_ROOT" ] && echo "Set env var EXCHANGE_ROOT" && exit 1;
[ -z "$PHAROS_ROOT" ] && echo "Set env var PHAROS_ROOT" && exit 1;

# echo "Getting rid of old logs and data files"
# rm -r ~/tmp/*
# mkdir -p ~/tmp/test_logs
# mkdir -p ~/tmp/bin

quit_on_build_error()
{
    if [ $? != 0 ]; then
        exit $?
    fi
}

echo "Building nsq_service"
cd $EXCHANGE_ROOT/apps/nsq_service
go build -o ~/tmp/bin/nsq_service nsq_service.go
quit_on_build_error

echo "Building apt_volume_service"
cd $EXCHANGE_ROOT/apps/apt_volume_service
go build -o ~/tmp/bin/apt_volume_service apt_volume_service.go
quit_on_build_error

echo "Building dpn_sync"
cd $EXCHANGE_ROOT/apps/dpn_sync
go build -o ~/tmp/bin/dpn_sync dpn_sync.go
quit_on_build_error

echo "Building dpn_queue"
cd $EXCHANGE_ROOT/apps/dpn_queue
go build -o ~/tmp/bin/dpn_queue dpn_queue.go
quit_on_build_error

echo "Building dpn_copy"
cd $EXCHANGE_ROOT/apps/dpn_copy
go build -o ~/tmp/bin/dpn_copy dpn_copy.go
quit_on_build_error

echo "Building test_push_to_dpn"
cd $EXCHANGE_ROOT/apps/test_push_to_dpn
go build -o ~/tmp/bin/test_push_to_dpn test_push_to_dpn.go
quit_on_build_error

echo "Starting DPN cluster. This takes a minute or so..."
cd $DPN_SERVER_ROOT
rm log/impersonate*
bundle exec ./script/run_cluster.rb -f &
sleep 30

echo "Starting NSQ"
cd ~/tmp/bin
./nsq_service -config $EXCHANGE_ROOT/config/nsq/integration.config &>/dev/null &
NSQ_PID=$!

# echo "Deleting old Rails data"
# cd $PHAROS_ROOT
# RAILS_ENV=integration bundle exec rake pharos:empty_db

# echo "Loading Rails fixtures"
# RAILS_ENV=integration bundle exec rake db:fixtures:load

echo "Starting Pharos server"
cd $PHAROS_ROOT
RAILS_ENV=integration rails server &>~/tmp/test_logs/pharos.log &

echo "Starting Volume Service"
cd ~/tmp/bin
./apt_volume_service -config=config/integration.json &
sleep 10

echo "Starting dpn_sync"
./dpn_sync -config=config/integration.json

echo "Starting test_push_to_dpn"
./test_push_to_dpn -config=config/integration.json

echo "Starting dpn_queue"
./dpn_queue -config=config/integration.json -hours=240000

# echo "Starting dpn_copy"
# ./dpn_copy -config=config/integration.json &

echo "Running sync post tests"
cd $EXCHANGE_ROOT/dpn/workers
echo -e "\n\n***********************************************"
RUN_DPN_SYNC_POST_TEST=true go test dpn_sync_test.go
echo -e "***********************************************\n\n"


run_post_tests()
{
    kill_all
}

kill_all()
{
	pkill -TERM -P $$
	echo "We're all done. Logs are in ~/tmp/logs."
}

trap run_post_tests EXIT HUP INT QUIT TERM

wait $NSQ_PID
