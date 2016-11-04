require_relative 'build'
require_relative 'service'
require_relative 'test_runner'

# IntegrationTest runs integration tests for APTrust and DPN code.
class IntegrationTest

  def initialize(context)
    @context = context
    @build = Build.new(context)
    @service = Service.new(context)
    @test_runner = TestRunner.new(context)
    @results = {}
    @context.make_test_dirs
    @context.clear_logs
    @context.clear_nsq_data
    @context.clear_binaries
  end

  def test_apt_ingest(more_tests_follow)
    begin
      # Build everything anew
      @build.build(@context.apps['nsq_service'])
      @build.build(@context.apps['apt_volume_service'])
      @build.build(@context.apps['apt_bucket_reader'])
      @build.build(@context.apps['apt_fetch'])
      @build.build(@context.apps['apt_store'])
      @build.build(@context.apps['apt_record'])

      # Start all required services.
      # Some sleep statements below all NSQ topics to fill before
      # a worker connects. This speeds up testing because when a
      # worker queries an empty or non-existent NSQ channel, it will
      # doze off and not re-check that channel for a minute or so.
      # We don't really want that minute lag time for each worker.
      # If the workers query an existing, populated channel, they'll
      # get right to work. That shaves 2-3 minutes off our integration
      # test time.
      @service.pharos_reset_db
      @service.pharos_load_fixtures
      @service.pharos_start
      @service.app_start(@context.apps['apt_volume_service'])
      @service.nsq_start
      sleep 10  # give all services time to start
      @service.app_start(@context.apps['apt_bucket_reader'])
      @service.app_start(@context.apps['apt_fetch'])
      sleep 10  # let nsq store topic fill before client connects
      @service.app_start(@context.apps['apt_store'])
      sleep 10  # let nsq record topic fill before client connects
      @service.app_start(@context.apps['apt_record'])
      sleep 40  # allow fetch/store/record time to finish
      @service.stop_everything unless more_tests_follow
      sleep 5

      # Run the post tests.
      @results['apt_bucket_reader_test'] = @test_runner.run_bucket_reader_post_test
      @results['apt_fetch_test'] = @test_runner.run_apt_fetch_post_test
      @results['apt_store_test'] = @test_runner.run_apt_store_post_test
      @results['apt_record_test'] = @test_runner.run_apt_record_post_test
    rescue Exception => ex
      print_exception(ex)
    ensure
      @service.stop_everything unless more_tests_follow
    end
    print_results unless more_tests_follow
  end

  def test_apt_send_to_dpn

  end

  def test_apt_restore

  end

  def test_apt_delete

  end

  def test_bucket_reader

  end

  # test_dpn_rest_client tests the DPN REST client against a
  # locally-running DPN cluster. Returns true if all tests passed,
  # false otherwise.
  def test_dpn_rest_client
    begin
      @service.dpn_cluster_start
      @results['dpn_rest_client_test'] = @test_runner.run_dpn_rest_client_test
    rescue Exception => ex
      print_exception(ex)
    ensure
      @service.stop_everything
    end
    print_results
  end

  # test_dpn_sync tests the dpn_sync app against a locally-running
  # DPN cluster. dpn_sync runs as a cron job in our staging and
  # production environments, and exits on its own when it's done.
  # The DPN sync post test checks to ensure that all remote records
  # were synched as expected to the local node. Returns true/false
  # to indicate whether all tests passed.
  def test_dpn_sync(more_tests_follow)
    begin
      dpn_sync = @context.apps['dpn_sync']
      @build.build(dpn_sync)
      @service.dpn_cluster_start
      @service.app_start(dpn_sync)
      @results['dpn_sync_test'] = @test_runner.run_dpn_sync_post_test
    rescue Exception => ex
      print_exception(ex)
    ensure
      @service.stop_everything unless more_tests_follow
    end
    print_results unless more_tests_follow
  end

  # test_dpn_queue tests the dpn_queue application, which is responsible
  # for finding and queueing 1) replication requests recently synched
  # to our local DPN node that APTrust is responsible for fulfilling
  # (i.e. APTrust is the to_node in those requests), and 2) WorkItems
  # in Pharos that request an APTrust bag be pushed to DPN. Those are
  # DPN ingests performed by APTrust. This test just checks to see that
  # dpn_queue actually finds and queues all the right items.
  #
  # This runs apt_fetch, apt_store, and apt_record before dpn_queue,
  # because we need to ingest the APTrust bags that we're going to
  # mark for DPN.
  def test_dpn_queue(more_tests_follow)
    begin
      @build.build(@context.apps['dpn_sync'])
      @build.build(@context.apps['dpn_queue'])
      #@build.build(@context.apps['dpn_copy']) # TODO: Test this when we can
      @build.build(@context.apps['test_push_to_dpn'])

      # Run all the ingest code first, so Pharos has
      # bags that can be pushed to DPN. If ingest is OK,
      # test_push_bags_to_dpn will mark a few APTrust
      # bags for DPN ingest.
      ingest_ok = test_apt_ingest(true)
      if ingest_ok
        @service.app_start(@context.apps['test_push_to_dpn'])
        push_to_dpn_ok = test_dpn_sync(true)
      else
        puts "Skipping push_to_dpn test because of prior failures."
      end
      if push_to_dpn_ok
        @service.app_start(@context.apps['dpn_queue'])
        # TODO: Need dpn_queue post test here.
      else
        puts "Skipping dpn_queue test because of prior failures."
      end
    rescue Exception => ex
      print_exception(ex)
    ensure
      @service.stop_everything unless more_tests_follow
    end
    print_results unless more_tests_follow
  end

  def test_dpn_ingest

  end

  def test_dpn_replicate

  end

  # Runs all the APTrust and DPN unit tests. Does not run any tests that
  # rely on external services. Returns true/false to indicate whether all
  # tests passed.
  def test_units
    @results['unit_tests'] = @test_runner.run_all_unit_tests
    print_results
  end

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

end

if __FILE__ == $0
  context = Context.new
  test = IntegrationTest.new(context)
  #test.test_dpn_rest_client
  #test.test_dpn_sync
  #test.test_units
  test.test_dpn_queue(false)
end
