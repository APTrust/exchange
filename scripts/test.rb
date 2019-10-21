#!/usr/bin/env ruby

require 'optparse'
require_relative 'context'
require_relative 'integration_test'

def run
  start = Time.now
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
  finish = Time.now
  diff = finish - start
  running_time = "0:#{diff}"
  if (diff > 60)
	mins = (diff / 60).to_i
	secs = (diff % 60).to_i
	secs = "0" + secs.to_s if secs < 10
	running_time = "#{mins}:#{secs}"
  end
  puts "Tests finished in #{running_time}"
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
	'apt_delete' => 'Test the APTrust delete process (runs apt_ingest)',
	'apt_restore' => 'Test the APTrust restore process (runs apt_ingest)',
	'apt_fixity' => 'Test the APTrust fixity checking process (runs apt_restore)',
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

test.rb runs unit and integration tests for APTrust services.
This script will start, seed, and stop all of the external services
required for testing, including NSQ and Pharos.

All of these tests run in a local "integration" environment.
The only service these tests rely on
that isn't running locally is AWS S3. You must have S3 keys in your
environment that can read from the integration test bucket at
aptrust.integration.test and can write to the test preservation
buckets for APTrust.

Some tests do copy bags into the APTrust preservation buckets,
and those buckets should be cleaned out periodically.

Valid options for test_to_run include:

#{tests_string}

You may see sporadic failures in these tests when you have a slow
connection to S3. One test in apt_volume_service occasionally fails,
if other processes are writing or deleting files while the test runs.

Generally, you'll want to run one of these three tests, which
together cover everything.

	- apt_fixity: runs all APTrust operations

Each of those tests may take 10 minutes or more to run. The tests
for apt_bucket_reader and apt_queue
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
