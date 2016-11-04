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
    @context.make_test_dirs
    @context.clear_logs
    @context.clear_binaries
  end

  def test_bucket_reader

  end

  def test_apt_ingest

  end

  def test_apt_send_to_dpn

  end

  def test_apt_restore

  end

  def test_apt_delete

  end

  # test_dpn_rest_client tests the DPN REST client against a
  # locally-running DPN cluster.
  def test_dpn_rest_client
    begin
      @service.dpn_cluster_start
      @test_runner.run_dpn_rest_client_test
    rescue Exception => ex
      print_exception(ex)
    ensure
      @service.stop_everything
    end
  end

  # test_dpn_sync tests the dpn_sync app against a locally-running
  # DPN cluster. dpn_sync runs as a cron job in our staging and
  # production environments, and exits on its own when it's done.
  # The DPN sync post test checks to ensure that all remote records
  # were synched as expected to the local node.
  def test_dpn_sync
    begin
      dpn_sync = @context.apps['dpn_sync']
      @build.build(dpn_sync)
      @service.dpn_cluster_start
      @service.app_start(dpn_sync)
      @test_runner.run_dpn_sync_post_test
    rescue Exception => ex
      print_exception(ex)
    ensure
      @service.stop_everything
    end
  end

  def test_dpn_queue

  end

  def test_dpn_ingest

  end

  def test_dpn_replicate

  end

  def print_exception(ex)
    puts ex
    puts ex.backtrace
  end

end

if __FILE__ == $0
  context = Context.new
  test = IntegrationTest.new(context)
  #test.test_dpn_rest_client
  test.test_dpn_sync
end
