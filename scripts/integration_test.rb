require_relative 'build'
require_relative 'service'

# IntegrationTest runs integration tests for APTrust and DPN code.
class IntegrationTest

  def initialize(context)
    @context = context
    @build = Build.new(context)
    @service = Service.new(context)
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

  # Test the DPN REST client against a locally-running DPN cluster.
  def test_dpn_rest_client
    begin
      @service.dpn_cluster_start
      puts "Waiting 30 seconds for cluster to start..."
      sleep 30
      cmd = "go test dpn_rest_client_test.go"
      dir = "#{@context.exchange_root}/dpn/network"
      pid = Process.spawn(cmd, chdir: dir)
      Process.wait pid
    ensure
      @service.dpn_cluster_stop
    end
  end

  def test_dpn_sync

  end

  def test_dpn_queue

  end

  def test_dpn_ingest

  end

  def test_dpn_replicate

  end
end

if __FILE__ == $0
  context = Context.new
  test = IntegrationTest.new(context)
  test.test_dpn_rest_client
end
