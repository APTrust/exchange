require 'fileutils'
require_relative 'context'

# The Serice class provides a means to start and stop services
# for APTrust and DPN integration tests. It also provides access
# to rake tasks that load fixtures and perform other fuctions
# required for integration testing.
#
# Note: Currently, for this to work, `rbenv local` must be 2.3.0
# in the exchange root dir, the pharos root dir and the dpn root dir.
class Service

  def initialize(context)
    @ctx = context
    @dpn_cluster_pid = 0
    @nsq_pid = 0
    @pharos_pid = 0
    @pids = {}
    @ctx.apps.values.each do |app|
      @pids[app.name] = 0 unless app.run_as == 'special'
    end
  end

  def env_hash
    env = {}
    ENV.each{ |k,v| env[k] = v }
    # Are APTrust and DPN on the same ruby verson?
    env['RBENV_VERSION'] = `cat #{@ctx.pharos_root}/.ruby-version`.chomp
    env['RAILS_ENV'] = 'integration'
    env
  end

  def app_start(app)
    if app.run_as == 'special'
      raise "Cannot run special app #{app.name}. There should be a custom method for that."
    end
    pid = @pids[app.name]
    if pid.nil?
      raise "Cannot start unknown app #{app.name}"
    end
    if pid == 0
      env = env_hash
      cmd = "./#{app.name} -config #{@ctx.exchange_root}/config/integration.json"
      pid = Process.spawn(env, cmd, chdir: @ctx.go_bin_dir)
      Process.detach pid
      puts "Started #{app.name} with command '#{cmd}' and pid #{pid}"
      @pids[app.name] = pid
    end
  end

  def app_stop(app)
    if app.run_as == 'special'
      raise "Cannot stop special app #{app.name}. There should be a custom method for that."
    end
    pid = @pids[app.name]
    if pid.nil? || pid == 0
      puts "Cannot stop app #{app_name} - no pid"
      return
    end
    puts "Stopping #{app.name} service (pid #{pid})"
    begin
      Process.kill('TERM', pid)
    rescue
      puts "#{app.name} wasn't even running."
    end
  end

  def dpn_cluster_init
    if @dpn_cluster_pid == 0
      env = env_hash
      puts "Setting up DPN cluster"
      cmd = "bundle exec ./script/setup_cluster.rb"
      log_file = "#{@ctx.log_dir}/dpn_cluster_setup.log"
      pid = Process.spawn(env,
                          cmd,
                          chdir: @ctx.dpn_server_root,
                          out: [log_file, 'w'],
                          err: [log_file, 'w'])
      Process.wait pid
      puts "Migrating DPN cluster"
      cmd = "bundle exec ./script/migrate_cluster.rb"
      log_file = "#{@ctx.log_dir}/dpn_cluster_migrate.log"
      pid = Process.spawn(env,
                          cmd,
                          chdir: @ctx.dpn_server_root,
                          out: [log_file, 'w'],
                          err: [log_file, 'w'])
      Process.wait pid
    end
  end

  def dpn_cluster_start
    if @dpn_cluster_pid == 0
      dpn_cluster_init
      puts "Deleting old DPN cluster log files"
      FileUtils.rm Dir.glob("#{@ctx.dpn_server_root}/impersonate*")
      env = env_hash
      # The -f flag tells Rails to load the test data fixtures
      # before it starts the cluster.
      cmd = "bundle exec ./script/run_cluster.rb -f"
      log_file = "#{@ctx.log_dir}/dpn_cluster.log"
      @dpn_cluster_pid = Process.spawn(env,
                                  cmd,
                                  chdir: @ctx.dpn_server_root,
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

  def nsq_start
    if @nsq_pid == 0
      env = env_hash
      cmd = "./nsq_service -config #{@ctx.exchange_root}/config/nsq/integration.config"
      log_file = '/dev/null'
      @nsq_pid = Process.spawn(env,
                                  cmd,
                                  chdir: @ctx.go_bin_dir,
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
    log_file = "#{@ctx.log_dir}/pharos.log"
    pid = Process.spawn(env, cmd, chdir: @ctx.pharos_root)
    Process.wait pid
    puts "Finished resetting Pharos DB"
  end

  def pharos_load_fixtures
    puts "Loading Pharos fixtures"
    env = env_hash
    cmd = 'rbenv exec rake db:fixtures:load'
    log_file = "#{@ctx.log_dir}/pharos.log"
    pid = Process.spawn(env, cmd, chdir: @ctx.pharos_root)
    Process.wait pid
    puts "Finished loading Pharos fixtures"
  end

  def pharos_start
    if @pharos_pid == 0
      env = env_hash
      cmd = 'rbenv exec rails server'
      log_file = "#{@ctx.log_dir}/pharos.log"
      @pharos_pid = Process.spawn(env,
                                  cmd,
                                  chdir: @ctx.pharos_root,
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

end

if __FILE__ == $0
  context = Context.new
  service = Service.new(context)
  context.apps.values.each { |app| service.app_start(app) unless app.run_as == 'special' }
  sleep 10
  context.apps.values.each { |app| service.app_stop(app) unless app.run_as == 'special' }
end
