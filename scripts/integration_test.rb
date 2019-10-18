require_relative 'build'
require_relative 'service'

# --------------------------------------------------------------------------
# IntegrationTest runs integration tests for APTrust code,
# as well as the APTrust Go services unit test suite. Integration
# tests run against a local Pharos server,
# which is emptied and then re-seeded with essential fixture
# data before each test run.
#
# Most integration tests depend on the outcome of prior integration tests.
# Integration tests that require other tests to run first will
# run those test automatically. The general chain of events here, which
# mirrors the chaing of events in production, looks like this for APTrust
# Ingest:
#
# 1. The bucket reader scans receiving buckets and creates new WorkItems
#    in Pharos and then adds the ids of those WorkItems to the NSQ
#    fetch_topic.
# 2. apt_fetch reads the WorkItem ids from NSQ fetch_channel, copies tar
#    files from the receiving buckets, and validates them. If a bag is
#    valid, the WorkItem id is pushed into store_topic in NSQ. Whether
#    the bag is valid or not, apt_fetch records information about the
#    status of its work in the WorkItem record, and it stores a JSON
#    representation of the state of its work in WorkItemState.
# 3. apt_store reads WorkItem ids from the NSQ store_channel. It stores
#    GenericFiles in the APTrust preservation storage bucket (S3 Virginia)
#    and in Glacier preservation storage in Oregon. Then it pushes the
#    WorkItem id into the record_topic.
# 4. apt_record reads WorkItem ids from the NSQ record_channel. From there
#    it gets the WorkItemState (a JSON representation of the state of the
#    entire IntellectualObject and its files and events), and begins
#    recording that state in Pharos (creating an IntellectualObject record,
#    GenericFile records, and PREMIS event records).
#
# After Ingest, we can do any of the following:
##
# * Mark bags to be restored by creating a WorkItem for the bag where
#   action='Restore'.
#
# * Mark IntellectualObjects and/or GenericFiles to be deleted by creating
#   a WorkItem where action='Delete'. Note that one delete WorkItem will be
#   created for the bag, and one for EACH GenericFile in the bag.
#
# These integration tests [will soon] perform all of these operations
# against locally-running services. The only outside services these integration
# tests touch are S3 and Glacier. Integration test bags are in the S3 bucket
# aptrust.integration.test, and if those are ever deleted, they can
# be restored from testdata/s3_bags/TestBags.zip. These tests store ingested
# and replicated bags in the APTrust preservation test buckets, which
# should be emptied periodically.
#
# --------------------------------------------------------------------------
class IntegrationTest

  def initialize(context)
	@context = context
	@build = Build.new(context)
	@service = Service.new(context)
	@results = {}
	@context.make_test_dirs
	@context.clear_logs
	@context.clear_staging
	@context.clear_restore
	@context.clear_binaries
	@context.clear_nsq_data
  end


  # apt_bucket_reader scans depositor receiving buckets on S3 for
  # new files that need to be ingested. It creates WorkItem entries
  # and NSQ tasks for each new tar file in the receiving buckets.
  # It will not create a new WorkItem + NSQ entry if a WorkItem already
  # exists for the tar file.
  def apt_bucket_reader(more_tests_follow)
	run_suite(more_tests_follow) do
	  # Build everything anew
	  @build.build(@context.apps['nsq_service'])
	  @build.build(@context.apps['apt_bucket_reader'])

	  # Start services with a little extra time for startup and shutdown
	  @service.pharos_reset_db
	  @service.pharos_load_fixtures
	  @service.pharos_start
	  @service.nsq_start
	  sleep 10
	  @service.app_start(@context.apps['apt_bucket_reader'])
	  @service.stop_everything unless more_tests_follow
	  sleep 5

	  # Run the post tests.
	  @results['apt_bucket_reader_test'] = run('apt_bucket_reader_post_test.go')
	end
  end

  # apt_ingest runs the entire APTrust ingest process, from end to end,
  # using fixtures, local services, and AWS S3/Glacier.
  def apt_ingest(more_tests_follow)

	# apt_ingest can be called from more than one method below.
	# If it has already run, it will have recorded the results
	# of apt_record_test, and we don't want to run it again.
	if !@results['apt_record_test'].nil?
	  return true
	end

	run_suite(more_tests_follow) do
	  # Rebuild binaries
	  @build.build(@context.apps['apt_volume_service'])
	  @build.build(@context.apps['apt_fetch'])
	  @build.build(@context.apps['apt_store'])
	  @build.build(@context.apps['apt_record'])

	  # Run the prerequisite process (with tests)
	  # Note that the prereq starts most of the required services.
	  apt_bucket_reader_ok = apt_bucket_reader(true)
	  if !apt_bucket_reader_ok
		puts "Skipping apt_ingest test because of prior failures."
		return false
	  end

	  # Start services required for this specific set of tests.
	  @service.app_start(@context.apps['apt_volume_service'])
	  sleep 5
	  @service.app_start(@context.apps['apt_fetch'])
	  sleep 30  # let nsq store topic fill before client connects
	  @service.app_start(@context.apps['apt_store'])
	  sleep 30  # let nsq record topic fill before client connects
	  @service.app_start(@context.apps['apt_record'])
	  sleep 30  # allow fetch/store/record time to finish

	  # Run the post tests. This is where we check to see if the
	  # ingest services (fetch, store, record) correctly performed
	  # all of the expected work.
	  @results['apt_fetch_test'] = run('apt_fetch_post_test.go')
	  @results['apt_store_test'] = run('apt_store_post_test.go')
	  @results['apt_record_test'] = run('apt_record_post_test.go')
	  @results['apt_ingest_test'] = run('apt_ingest_post_test.go')

	  # Now get an updated bag from the special bucket and
	  # ingest that so we can run our update integration tests.
	  @service.run_bucket_reader_for_update()
	  puts 'Done with bucket reader. Allowing time to process updated files.'

	  # This is a problem: tests pass or fail depending on how
	  # long we sleep here and above. Tests that pass on one
	  # system may fail on a system with a slower internet
	  # connection.
	  #sleep 75

      # Wail for the following two etags to appear in the log file.
      # These are defined in util/testutil/testutil.go as
      # UPDATED_BAG_ETAG and UPDATED_GLACIER_BAG_ETAG.
      log_file = File.join(@context.log_dir, 'apt_record.json')
      wait_for_match(log_file, 'ec520876f7c87e24f926a8efea390b26', 120)
      wait_for_match(log_file, 'bf01126663915a4f5d135a37443b8349', 120)
	  @results['apt_update_test'] = run('apt_update_post_test.go')

	  @service.stop_everything unless more_tests_follow
	  sleep 5
	  print_results unless more_tests_follow

	  # Return value should say whether any tests failed
	  return (@results['apt_fetch_test'] &&
			  @results['apt_store_test'] &&
			  @results['apt_record_test'] &&
			  @results['apt_ingest_test'] &&
			  @results['apt_update_test'])
	end
  end

  # apt_queue copies WorkItems into NSQ. For example, any oustanding
  # requests to delete files, restore files, etc.,
  # that have no queued_at timestamp will be put into the appropriate
  # NSQ topic.
  def apt_queue(more_tests_follow)
	# Don't run this if it's already been run.
	if !@results['apt_queue_test'].nil?
	  return true
	end
	run_suite(more_tests_follow) do
	  @build.build(@context.apps['apt_queue'])

	  # Run the prerequisite process (with tests)
	  # Note that the prereq starts most of the required services.
	  apt_ingest_ok = apt_ingest(true)
	  if !apt_ingest_ok
		puts "Skipping apt_queue test because of prior failures."
		return false
	  end

	  # Mark some IntellectualObjects for restoration in Pharos,
	  # so that apt_restore and apt_file_delete will have something to work on.
	  # Marking an item for deletion causes Pharos to initiate a
	  # multi-step DB transaction. It should finish in 1/2 second or less,
	  # but give it 5 seconds to be save. Without any sleep, we'll often
	  # get a 'database is locked' exception in the Pharos logs, and then
	  # subsequent test operations fail.
	  @results['apt_mark_for_restore'] = run('apt_mark_for_restore_test.go')
	  @results['apt_mark_for_delete'] = run('apt_mark_for_delete_test.go')
	  sleep 5

	  # apt_queue is not a service. It runs to completion, then exits.
	  # For integration tests, it should take just a second or two.
	  @service.app_start(@context.apps['apt_queue'])
	  @service.stop_everything unless more_tests_follow
	  sleep 5

	  # Run the post tests.
	  @results['apt_queue_test'] = run('apt_queue_post_test.go')
	end
  end

  # apt_restore runs the APTrust bag restoration service to restore
  # a number of bags. It also runs file deletions.
  def apt_restore(more_tests_follow)
	run_suite(more_tests_follow) do
	  @build.build(@context.apps['apt_restore'])
      @build.build(@context.apps['apt_file_restore'])
	  @build.build(@context.apps['apt_file_delete'])

	  # Run the prerequisite process (with tests)
	  # Note that the prereq starts most of the required services,
	  # and apt_queue marks items for restore and pushes them into
	  # NSQ.
	  apt_queue_ok = apt_queue(true)
	  if !apt_queue_ok
		puts "Skipping apt_restore test because of prior failures."
		return false
	  end

	  # Start services required for this specific set of tests.
	  @service.app_start(@context.apps['apt_restore'])
	  @service.app_start(@context.apps['apt_file_restore'])
	  @service.app_start(@context.apps['apt_file_delete'])
	  sleep 90

	  # Run the post tests.
	  @results['apt_restore_test'] = run('apt_restore_post_test.go')
	  puts "apt_delete_post_test is currently disabled per PT #156321235"
	  # @results['apt_delete_test'] = run('apt_delete_post_test.go')
	end
  end

  # apt_delete runs the APTrust file deletion service to delete a
  # number of GenericFiles from the archive. This test cannot be run
  # with apt_restore, because it deletes some of the times that
  # apt_restore is trying to restore.
  def apt_delete(more_tests_follow)
	run_suite(more_tests_follow) do
	  @build.build(@context.apps['apt_file_delete'])

	  # Run the prerequisite process (with tests)
	  # Note that the prereq starts most of the required services,
	  # and apt_queue marks items for restore and pushes them into
	  # NSQ.
	  apt_queue_ok = apt_queue(true)
	  if !apt_queue_ok
		puts "Skipping apt_restore test because of prior failures."
		return false
	  end

	  # Start services required for this specific set of tests.
	  @service.app_start(@context.apps['apt_file_delete'])
	  sleep 60

	  # Run the post tests.
	  @results['apt_delete_test'] = run('apt_delete_post_test.go')
	end
  end

  # apt_fixity runs the fixity checking service.
  def apt_fixity(more_tests_follow)
	run_suite(more_tests_follow) do
	  @build.build(@context.apps['apt_queue_fixity'])
	  @build.build(@context.apps['apt_fixity_check'])

	  # Run the prerequisite process (with tests)
	  # Note that the prereq starts most of the required services,
	  # and apt_queue marks items for restore and pushes them into
	  # NSQ.
	  apt_restore_ok = apt_restore(true)
	  if !apt_restore_ok
		puts "Skipping apt_fixity test because of prior failures."
		return false
	  end

	  # Queue up some files for fixity checking. Note that service.rb
	  # sets -maxfiles=10 for apt_queue_fixity.
	  @service.app_start(@context.apps['apt_queue_fixity'])
	  @service.app_start(@context.apps['apt_fixity_check'])
	  sleep 45

	  # Run the post test.
	  @results['apt_fixity_test'] = run('apt_fixity_check_post_test.go')
	end
  end

  # Runs all the APTrust unit tests. Does not run any tests that
  # rely on external services. Returns true/false to indicate whether all
  # tests passed.
  def units(more_tests_follow)
	@results['unit_tests'] = run_all_unit_tests
	print_results
  end

  private

  def print_exception(ex)
	puts ex
	puts ex.backtrace
  end

  # print_results prints the results of each test that was run
  # and returns true if all tests passed, false if any test failed.
  def print_results
	all_tests_passed = true
	puts "\n---Results---"
	@results.each do |test_name, passed|
	  if passed
		message = 'PASS'
	  else
		message = 'FAIL'
		all_tests_passed = false
	  end
	  printf("%-30s: %s\n", test_name, message)
	end
	puts "\n"
	return all_tests_passed
  end

  def all_tests_passed?
	@results.each do |test_name, passed|
	  return false unless passed
	end
	return true
  end

  # wait_for_match keeps checking file for the presence of
  # string until timeout seconds have passed. If it finds
  # the string within timeout seconds, it returns true.
  def wait_for_match(file, string, max_timeout)
    interval = 5
    max_retries = max_timeout / interval || 1
    found = false
    max_retries.times do |i|
      if i % 2 == 0 || i == max_retries
        puts "[#{i * interval}s]Checking #{file} for #{string}"
      end
      if File.readlines(file).grep(Regexp.new(string)).size > 0
        puts "Found #{string}"
        found = true
        break
      end
      sleep interval
    end
    return found
  end

  # run_all_unit_tests runs all of the APTrust unit tests.
  # These tests do not require any outside services to run, and
  # they omit a handful of tests that do require outside services.
  #
  def run_all_unit_tests
	env = @context.env_hash
	cmd = "go test ./..."
	pid = Process.spawn(env, cmd, chdir: @context.exchange_root)
	Process.wait pid
	return $?.exitstatus == 0
  end

  # Runs the specified integration test, setting up the necessary
  # environment first.
  def run(test_file)
	env = @context.env_hash
	env['RUN_EXCHANGE_INTEGRATION'] = 'true'
	dir = "#{@context.exchange_root}/integration"
	cmd = "go test #{test_file}"
	pid = Process.spawn(env, cmd, chdir: dir)
	puts "Running #{test_file} with pid #{pid}... "
	Process.wait pid
	return $?.exitstatus == 0
  end

  # run_suite runs the suite of commands in the given block.
  # That usually includes building binaries, starting services,
  # and running tests.
  def run_suite(more_tests_follow, &block)
	begin
	  yield
	rescue Exception => ex
	  print_exception(ex)
	  return false
	ensure
	  @service.stop_everything unless more_tests_follow
	end
	if more_tests_follow
	  return all_tests_passed?
	else
	  return print_results
	end
  end

end
