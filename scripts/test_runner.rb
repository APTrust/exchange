# TestRunner runs specific tests for APTrust and DPN
class TestRunner

  def initialize(context)
    @context = context
  end

  # run_all_unit_tests runs all of the APTrust and DPN unit tests.
  # These tests do not require any outside services to run, and
  # they omit a handful of tests that do require outside services.
  def run_all_unit_tests
    env = @context.env_hash
    cmd = "go test $(go list ./... | grep -v /vendor/)"
    pid = Process.spawn(env, cmd, chdir: @context.exchange_root)
    Process.wait pid
    return $?.exitstatus == 0
  end

  # dpn_rest_client test runs our Go DPN REST client against a locally-running
  # DPN cluster. The DPN REST client is in exchange/dpn/network.
  # Returns true if tests passed.
  def run_dpn_rest_client_test
    env = @context.env_hash
    cmd = "go test dpn_rest_client_test.go"
    dir = "#{@context.exchange_root}/dpn/network"
    pid = Process.spawn(env, cmd, chdir: dir)
    Process.wait pid
    return $?.exitstatus == 0
  end

  # dpn_sync_test runs the dpn_sync worker against a locally-running
  # DPN cluster. Note that the test requires the environment var
  # RUN_DPN_SYNC_POST_TEST=true. Also note that the dpn_sync app
  # must run before this test, since this is a post-test that just
  # checks for expected post conditions (i.e. verifies that certain
  # items were synched). Returns true if tests passed.
  def run_dpn_sync_post_test
    return run_integration_post_test('go test dpn_sync_post_test.go')
    # env = @context.env_hash
    # env['RUN_DPN_SYNC_POST_TEST'] = 'true'
    # cmd = "go test dpn_sync_post_test.go"
    # dir = "#{@context.exchange_root}/integration"
    # pid = Process.spawn(env, cmd, chdir: dir)
    # Process.wait pid
    # return $?.exitstatus == 0
  end

  # This runs the the dpn_queue post test, to ensure that items
  # marked for DPN by apps/test_push_to_dpn have WorkItems created
  # and have been pushed into NSQ.
  def run_dpn_queue_post_test
    return run_integration_post_test('go test dpn_queue_post_test.go')
    # env = @context.env_hash
    # env['RUN_EXCHANGE_INTEGRATION'] = 'true'
    # cmd = "go test dpn_queue_post_test.go"
    # dir = "#{@context.exchange_root}/integration"
    # pid = Process.spawn(env, cmd, chdir: dir)
    # Process.wait pid
    # return $?.exitstatus == 0
  end

  # This runs the the dpn_copy post test, to ensure that DPN
  # replication items were copied, DPNWorkItems updated, and
  # dpn_validation_topic entries created in NSQ.
  def run_dpn_copy_post_test
    return run_integration_post_test('go test dpn_copy_post_test.go')
    # env = @context.env_hash
    # env['RUN_EXCHANGE_INTEGRATION'] = 'true'
    # cmd = "go test dpn_copy_post_test.go"
    # dir = "#{@context.exchange_root}/integration"
    # pid = Process.spawn(env, cmd, chdir: dir)
    # Process.wait pid
    # return $?.exitstatus == 0
  end

  # Run the dpn_validate post test to ensure Pharos and NSQ
  # were updated with the expected information.
  def run_dpn_validate_post_test
    return run_integration_post_test('go test dpn_validate_post_test.go')
    # env = @context.env_hash
    # env['RUN_EXCHANGE_INTEGRATION'] = 'true'
    # cmd = "go test dpn_validate_post_test.go"
    # dir = "#{@context.exchange_root}/integration"
    # pid = Process.spawn(env, cmd, chdir: dir)
    # Process.wait pid
    # return $?.exitstatus == 0
  end

  def run_bucket_reader_post_test
    return run_integration_post_test('go test apt_bucket_reader_post_test.go')
  end

  def run_apt_fetch_post_test
    return run_integration_post_test('go test apt_fetch_post_test.go')
  end

  def run_apt_store_post_test
    return run_integration_post_test('go test apt_store_post_test.go')
  end

  def run_apt_record_post_test
    return run_integration_post_test('go test apt_record_post_test.go')
  end

  private

  def run_integration_post_test(cmd)
    env = @context.env_hash
    env['RUN_EXCHANGE_INTEGRATION'] = 'true'
    dir = "#{@context.exchange_root}/integration"
    pid = Process.spawn(env, cmd, chdir: dir)
    Process.wait pid
    return $?.exitstatus == 0
  end

end
