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
    @log_dir = "#{ENV['HOME']}/tmp/logs"
    @go_bin_dir = "#{ENV['HOME']}/tmp/bin"
    FileUtils.mkdir_p @log_dir
    FileUtils.mkdir_p @go_bin_dir
  end

  def env_hash
    env = {}
    ENV.each{ |k,v| env[k] = v }
    # Are APTrust and DPN on the same ruby verson?
    env['RBENV_VERSION'] = `cat #{@pharos_root}/.ruby-version`.chomp
    env['RAILS_ENV'] = 'integration'
    env
  end

  def reset_pharos_db
    puts "Resetting Pharos DB"
    env = env_hash
    cmd = 'rbenv exec rake pharos:empty_db'
    log_file = "#{@log_dir}/pharos.log"
    pid = Process.spawn(env, cmd, chdir: @pharos_root)
    Process.wait pid
    puts "Finished resetting Pharos DB"
  end

  def load_pharos_fixtures
    puts "Loading Pharos fixtures"
    env = env_hash
    cmd = 'rbenv exec rake db:fixtures:load'
    log_file = "#{@log_dir}/pharos.log"
    pid = Process.spawn(env, cmd, chdir: @pharos_root)
    Process.wait pid
    puts "Finished loading Pharos fixtures"
  end

  def start_pharos
    if @pharos_pid == 0
      env = env_hash
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

  def stop_nsq
    if @nsq_pid != 0
      puts "Stopping NSQ service (pid #{@nsq_pid})"
      Process.kill('TERM', @nsq_pid)
      @nsq_pid = 0
    end
  end

  def start_volume_service
    if @volume_service_pid == 0

    end
  end

  def stop_volume_service
    if @volume_service_pid != 0
      puts "Stopping volume service service (pid #{@volume_service_pid})"
      Process.kill('TERM', @volume_service_pid)
      @volume_service_pid = 0
    end
  end

  def start_dpn_cluster
    # If this fails, you may have to call the DPN script setup_cluster.rb first.
    if @dpn_cluster_pid == 0
      puts "Deleting old DPN cluster log files"
      FileUtils.rm Dir.glob("#{@dpn_server_root}/impersonate*")
      env = env_hash
      cmd = "bundle exec ./script/run_cluster.rb"
      log_file = "#{@log_dir}/dpn_cluster.log"
      @dpn_cluster_pid = Process.spawn(env,
                                  cmd,
                                  chdir: @dpn_server_root,
                                  out: [log_file, 'w'],
                                  err: [log_file, 'w'])
      Process.detach @dpn_cluster_pid
      puts "Started DPN cluster with command '#{cmd}' and pid #{@dpn_cluster_pid}"
    end
  end

  def stop_dpn_cluster
    if @dpn_cluster_pid != 0
      puts "Stopping DPN cluster (pid #{@dpn_cluster_pid})"
      Process.kill('TERM', @dpn_cluster_pid)
      @dpn_cluster_pid = 0
    end
  end


end

if __FILE__ == $0
  c = Context.new
  #c.reset_pharos_db
  #c.load_pharos_fixtures

  c.start_dpn_cluster
  sleep 30
  c.stop_dpn_cluster
end
