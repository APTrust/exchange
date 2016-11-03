require 'fileutils'

# The Serice class provides a means to start and stop services
# for APTrust and DPN integration tests. It also provides access
# to rake tasks that load fixtures and perform other fuctions
# required for integration testing.
class Service

  attr_reader :dpn_server_root, :exchange_root, :pharos_root

  def initialize
    @apt_fetch_pid = 0
    @apt_store_pid = 0
    @apt_record_pid = 0
    @apt_volume_service_pid = 0
    @dpn_cluster_pid = 0
    @dpn_copy_pid = 0
    @nsq_pid = 0
    @pharos_pid = 0

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

  def apt_fetch_start
    if @apt_fetch_pid == 0
      @apt_fetch_pid = start_go_service('apt_fetch')
    end
  end

  def apt_fetch_stop
    if @apt_fetch_pid != 0
      stop_go_service('apt_fetch', @apt_fetch_pid)
      @apt_fetch_pid = 0
    end
  end

  def apt_record_start
    if @apt_record_pid == 0
      @apt_record_pid = start_go_service('apt_record')
    end
  end

  def apt_record_stop
    if @apt_record_pid != 0
      stop_go_service('apt_record', @apt_record_pid)
      @apt_record_pid = 0
    end
  end

  def apt_store_start
    if @apt_store_pid == 0
      @apt_store_pid = start_go_service('apt_store')
    end
  end

  def apt_store_stop
    if @apt_store_pid != 0
      stop_go_service('apt_store', @apt_store_pid)
      @apt_store_pid = 0
    end
  end

  def apt_volume_service_start
    if @apt_volume_service_pid == 0
      @apt_volume_service_pid = start_go_service('apt_volume_service')
    end
  end

  def apt_volume_service_stop
    if @apt_volume_service_pid != 0
      stop_go_service('apt_volume_service', @apt_volume_service_pid)
      @apt_volume_service_pid = 0
    end
  end

  def dpn_cluster_init
    if @dpn_cluster_pid == 0
      env = env_hash
      puts "Setting up DPN cluster"
      cmd = "bundle exec ./script/setup_cluster.rb"
      log_file = "#{@log_dir}/dpn_cluster_setup.log"
      pid = Process.spawn(env,
                          cmd,
                          chdir: @dpn_server_root,
                          out: [log_file, 'w'],
                          err: [log_file, 'w'])
      Process.wait pid
      puts "Migrating DPN cluster"
      cmd = "bundle exec ./script/migrate_cluster.rb"
      log_file = "#{@log_dir}/dpn_cluster_migrate.log"
      pid = Process.spawn(env,
                          cmd,
                          chdir: @dpn_server_root,
                          out: [log_file, 'w'],
                          err: [log_file, 'w'])
      Process.wait pid
    end
  end

  def dpn_cluster_start
    if @dpn_cluster_pid == 0
      self.init_dpn_cluster
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

  def dpn_cluster_stop
    if @dpn_cluster_pid != 0
      puts "Stopping DPN cluster (pid #{@dpn_cluster_pid})"
      Process.kill('TERM', @dpn_cluster_pid)
      @dpn_cluster_pid = 0
    end
  end

  def dpn_copy_start
    if @dpn_copy_pid == 0
      @dpn_copy_pid = start_go_service('dpn_copy')
    end
  end

  def dpn_copy_stop
    if @dpn_copy_pid != 0
      stop_go_service('dpn_copy', @dpn_copy)
      @dpn_copy = 0
    end
  end

  def nsq_start
    if @nsq_pid == 0
      env = env_hash
      cmd = "./nsq_service -config #{@exchange_root}/config/nsq/integration.config"
      log_file = '/dev/null'
      @nsq_pid = Process.spawn(env,
                                  cmd,
                                  chdir: @go_bin_dir,
                                  out: [log_file, 'w'],
                                  err: [log_file, 'w'])
      Process.detach @nsq_pid
      puts "Started NSQ service with command '#{cmd}' and pid #{@nsq_pid}"
    end
  end

  def nsq_stop
    if @nsq_pid != 0
      puts "Stopping NSQ service (pid #{@nsq_pid})"
      Process.kill('TERM', @nsq_pid)
      @nsq_pid = 0
    end
  end

  def pharos_reset_db
    puts "Resetting Pharos DB"
    env = env_hash
    cmd = 'rbenv exec rake pharos:empty_db'
    log_file = "#{@log_dir}/pharos.log"
    pid = Process.spawn(env, cmd, chdir: @pharos_root)
    Process.wait pid
    puts "Finished resetting Pharos DB"
  end

  def pharos_load_fixtures
    puts "Loading Pharos fixtures"
    env = env_hash
    cmd = 'rbenv exec rake db:fixtures:load'
    log_file = "#{@log_dir}/pharos.log"
    pid = Process.spawn(env, cmd, chdir: @pharos_root)
    Process.wait pid
    puts "Finished loading Pharos fixtures"
  end

  def pharos_start
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

  def pharos_stop
    if @pharos_pid != 0
      puts "Stopping Pharos (pid #{@pharos_pid})"
      Process.kill('TERM', @pharos_pid)
      @pharos_pid = 0
    end
  end

  private

  def start_go_service(name)
    env = env_hash
    cmd = "./#{name} -config #{@exchange_root}/config/integration.json"
    pid = Process.spawn(env, cmd, chdir: @go_bin_dir)
    Process.detach pid
    puts "Started #{name} with command '#{cmd}' and pid #{pid}"
    return pid
  end

  def stop_go_service(name, pid)
      puts "Stopping #{name} service (pid #{pid})"
      Process.kill('TERM', pid)
  end

end

if __FILE__ == $0
  s = Service.new
  s.apt_volume_service_start
  s.apt_fetch_start
  s.apt_record_start
  s.apt_store_start
  sleep 10
  s.apt_volume_service_stop
  s.apt_fetch_stop
  s.apt_record_stop
  s.apt_store_stop


end
