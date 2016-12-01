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
  integration_test = IntegrationTest.new(context)
  integration_test.send(test_name, false)
end

def parse_options(context)
  OptionParser.new do |opts|
    opts.on("-v", "--verbose", "Log to stderr") do |v|
      context.verbose = v
    end
    opts.on("-i", "--init-dpn-cluster", "Initialize DPN cluster before starting it") do |i|
      context.run_dpn_cluster_init = i
    end
    opts.on("-h", "--help", helpdoc) do
      puts opts
      exit!
    end
  end.parse!
end

def tests
  {
    'apt_ingest' => 'Test the APTrust ingest process.',
    'apt_restore' => 'Test the APTrust restore process',
    'apt_delete' => 'Test APTrust file deletion',
    'bucket_reader' => 'Test the APTrust bucket reader',
    'dpn_rest_client' => 'Test the DPN REST client',
    'dpn_sync' => 'Test synching data from other nodes',
    'dpn_queue' => 'Test queueing ingest and transfer requests',
    'dpn_copy' => 'Test copying bags from remote nodes',
    'dpn_validate' => 'Test validation of bags from remote nodes',
    'dpn_replication_store' => 'Test storing bags from remote nodes',
    'dpn_package' => 'Test packaging APTrust bags for DPN ingest',
    'dpn_ingest_store' => 'Test storing locally-ingested DPN bags',
    'dpn_ingest_record' => 'Test recording of locally-ingested DPN bags',
    #'dpn_replicate' => 'Test replicating bags from other DPN nodes',
    'units' => 'Run all unit tests. Starts no external services.',
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
aptrust.receiving.test.test.edu and can write to the test preservation
buckets for APTrust and DPN.

Some tests do copy bags into the APTrust and DPN preservation buckets,
and those buckets should be cleaned out periodically.

Valid options for test_to_run include:

#{tests_string}
eoh
end

if __FILE__ == $0
  run()
end
