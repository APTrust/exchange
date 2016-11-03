require 'fileutils'

# Context provides contextual information for the IntegrationTest module
# that runs APTrust and DPN integration tests.
class Context

  attr_reader :dpn_server_root, :exchange_root, :pharos_root

  def initialize
    @nsq_pid = 0
    @pharos_pid = 0
    @volume_service_pid = 0
    @dpn_cluster_pid = 0

    @dpn_server_root = ENV['DPN_SERVER_ROOT'] || abort("Set env var DPN_SERVER_ROOT")
    @exchange_root = ENV['EXCHANGE_ROOT'] || abort("Set env var EXCHANGE_ROOT")
    @pharos_root = ENV['PHAROS_ROOT'] || abort("Set env var PHAROS_ROOT")
    @log_dir = "#{ENV['HOME']}/tmp/test_logs"
    FileUtils.mkdir_p @log_dir
  end

  def env_hash
    env = {}
    ENV.each{ |k,v| env[k] = v }
    env['RAILS_ENV'] = 'integration'
    env['RBENV_VERSION'] = `cat #{@pharos_root}/.ruby-version`.chomp
    env
  end

  def reset_pharos_db

  end

  def load_pharos_fixtures

  end

  def start_pharos
    if @pharos_pid == 0
      env = env_hash
      puts env.inspect
      cmd = 'rbenv exec rails server'
      log_file = "#{@log_dir}/pharos.log"
      @pharos_pid = Process.spawn(env,
                                  cmd,
                                  chdir: @pharos_root,
                                  out: [log_file, 'w'],
                                  err: [log_file, 'w'])
      Process.detach @pharos_pid
      puts "Started Pharos with command '#{cmd}' and pid #{@pharos_pid}"
    end
  end

  def stop_pharos
    if @pharos_pid != 0
      puts "Stopping Pharos (pid #{@pharos_pid})"
      Process.kill('TERM', @pharos_pid)
      @pharos_pid = 0
    end
  end

  def start_nsq
    if @nsq_pid == 0

    end
  end

  def start_volume_service
    if @volume_service_pid == 0

    end
  end

  def start_dpn_cluster
    if @dpn_cluster_pid == 0

    end
  end

end

if __FILE__ == $0
  c = Context.new
  c.start_pharos
  sleep 20
  c.stop_pharos
end
