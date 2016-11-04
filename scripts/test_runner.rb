# TestRunner runs specific tests for APTrust and DPN
class TestRunner

  def initialize(context)
    @context = context
  end

  # dpn_rest_client test runs our Go DPN REST client against a locally-running
  # DPN cluster. The DPN REST client is in exchange/dpn/network.
  def run_dpn_rest_client_test
    env = @context.env_hash
    cmd = "go test dpn_rest_client_test.go"
    dir = "#{@context.exchange_root}/dpn/network"
    pid = Process.spawn(env, cmd, chdir: dir)
    Process.wait pid
  end

  # dpn_sync_test runs the dpn_sync worker against a locally-running
  # DPN cluster. Note that the test requires the environment var
  # RUN_DPN_SYNC_POST_TEST=true. Also note that the dpn_sync app
  # must run before this test, since this is a post-test that just
  # checks for expected post conditions (i.e. verifies that certain
  # items were synched).
  def run_dpn_sync_post_test
    env = @context.env_hash
    env['RUN_DPN_SYNC_POST_TEST'] = 'true'
    cmd = "go test dpn_sync_test.go"
    dir = "#{@context.exchange_root}/dpn/workers"
    pid = Process.spawn(env, cmd, chdir: dir)
    Process.wait pid
  end


end
