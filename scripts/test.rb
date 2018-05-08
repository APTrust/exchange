#!/usr/bin/env ruby

require 'optparse'
require_relative 'context'
require_relative 'integration_test'

def run
  context = Context.new
  parse_options(context)
  test_name = ARGV[0]
  if tests[test_name].nil?
    puts "Unknown test: #{test_name}"
    puts "Try --help to see options."
    return
  end
  # Note that `go clean -testcache` forces Golang to run all the unit
  # tests anew. Without this option, Golang caches test results and then
  # does not re-run tests if the file being tested hasn't changed since
  # the last run. This is particularly problematic for integration tests.
  puts "Clearing the Go test cache..."
  `go clean -testcache`
  integration_test = IntegrationTest.new(context)
  integration_test.send(test_name, false)
end

def parse_options(context)
  OptionParser.new do |opts|
    opts.on("-v", "--verbose", "Log to stderr") do |v|
      context.verbose = v
    end
    opts.on("-h", "--help", helpdoc) do
      puts opts
      exit!
    end
  end.parse!
end

def tests
  {
    'apt_bucket_reader' => 'Test the APTrust bucket reader',
    'apt_queue' => 'Test queueing of WorkItems',
    'apt_ingest' => 'Test the APTrust ingest process (runs apt_queue)',
    'apt_restore' => 'Test the APTrust restore and delete processes (runs apt_ingest)',
    'apt_fixity' => 'Test the APTrust fixity checking process (runs apt_restore)',
    'dpn_pharos_sync' => 'Sync DPN bag records to Pharos dpn_bags table',
    'dpn_rest_client' => 'Test the DPN REST client against a local cluster',
    'dpn_sync' => 'Test DPN sync against a local cluster',
    'dpn_replicate' => 'Test DPN replication. Runs dpn_sync and dpn_rest_client',
    'dpn_ingest' => 'Test DPN ingest (runs apt_ingest)',
    'units' => 'Run all unit tests. Starts no external services, but does talk to S3.',
  }
end

def tests_string
  str = ""
  tests.sort_by do |name, description|
    str += sprintf("%-18s  %s\n", name, description)
  end
  str
end

def helpdoc
  <<-eoh
Usage: ruby test.rb [-vi] test_to_run

test.rb runs unit and integration tests for APTrust and DPN services.
This script will start, seed, and stop all of the external services
required for testing, including NSQ, Pharos and a cluster of DPN REST
servers (one to impersonate each DPN node).

All of these tests run in a local "integration" environment. No tests
actually touch other DPN nodes. The only service these tests rely on
that isn't running locally is AWS S3. You must have S3 keys in your
environment that can read from the integration test bucket at
aptrust.integration.test and can write to the test preservation
buckets for APTrust and DPN.

Some tests do copy bags into the APTrust and DPN preservation buckets,
and those buckets should be cleaned out periodically.

Valid options for test_to_run include:

#{tests_string}

You may see sporadic failures in these tests when you have a slow
connection to S3. One test in apt_volume_service occasionally fails,
if other processes are writing or deleting files while the test runs.

Generally, you'll want to run one of these three tests, which
together cover everything.

    - apt_fixity: runs all APTrust operations except send-to-DPN
    - dpn_replicate: runs dpn_sync, exercises the dpn_rest_client,
      and tests all inter-node operations.
    - dpn_ingest: runs the APTrust send-to-DPN operations.

Each of those tests may take 10 minutes or more to run. The tests
for apt_bucket_reader, apt_queue, dpn_rest_client and dpn_sync
run much more quickly. Use those if your changes only affect one
of those operations.

The units test usually runs in a few seconds, and only tests basic
code units. It does not do any integration testing, except for some
exchanges with S3.

eoh
end

if __FILE__ == $0
  run()
end
