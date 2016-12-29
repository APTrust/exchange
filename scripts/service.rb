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
    @context = context
    @dpn_cluster_pid = 0
    @nsq_pid = 0
    @pharos_pid = 0
    @pids = {}
    @context.apps.values.each do |app|
      @pids[app.name] = 0 unless app.run_as == 'special'
    end
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
      env = @context.env_hash
      cmd = "./#{app.name} -config #{@context.exchange_root}/config/integration.json"
      # HACK!
      if app.name == 'apt_bucket_reader'
        cmd += " -stats=#{@context.log_dir}/bucket_reader_stats.json"
      elsif app.name == 'apt_queue'
        cmd += " -stats=#{@context.log_dir}/apt_queue_stats.json"
      elsif app.name == 'dpn_queue'
        cmd += " -hours=240000"
      end

      if @context.verbose
        pid = Process.spawn(env, cmd, chdir: @context.go_bin_dir)
      else
        pid = Process.spawn(env, cmd, chdir: @context.go_bin_dir, out: '/dev/null', err: '/dev/null')
      end

      if app.run_as == 'service'
        Process.detach pid
      elsif app.run_as == 'application'
        Process.wait pid
      else
        puts "Don't know what to do with #{app.name}, run_as #{app.run_as}, so waiting..."
        Process.wait pid
      end
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
      env = @context.env_hash
      puts "Setting up DPN cluster"
      cmd = "bundle exec ./script/setup_cluster.rb"
      log_file = "#{@context.log_dir}/dpn_cluster_setup.log"
      pid = Process.spawn(env,
                          cmd,
                          chdir: @context.dpn_server_root,
                          out: [log_file, 'w'],
                          err: [log_file, 'w'])
      Process.wait pid
      puts "Migrating DPN cluster"
      cmd = "bundle exec ./script/migrate_cluster.rb"
      log_file = "#{@context.log_dir}/dpn_cluster_migrate.log"
      pid = Process.spawn(env,
                          cmd,
                          chdir: @context.dpn_server_root,
                          out: [log_file, 'w'],
                          err: [log_file, 'w'])
      Process.wait pid
    end
  end

  def dpn_cluster_start
    if @dpn_cluster_pid == 0

      # NOTE: We need to run dpn_cluster_init only if we've never run
      # the DPN cluster before on this machine, or if there are pending
      # DPN migrations.
      dpn_cluster_init if @context.run_dpn_cluster_init

      puts "Deleting old DPN cluster log files"
      FileUtils.rm Dir.glob("#{@context.dpn_server_root}/impersonate*")
      env = @context.env_hash
      # The -f flag tells Rails to load the test data fixtures
      # before it starts the cluster.
      cmd = "bundle exec ./script/run_cluster.rb -f"
      log_file = "#{@context.log_dir}/dpn_cluster.log"
      @dpn_cluster_pid = Process.spawn(env,
                                  cmd,
                                  chdir: @context.dpn_server_root,
                                  out: [log_file, 'w'],
                                  err: [log_file, 'w'])
      Process.detach @dpn_cluster_pid
      puts "Started DPN cluster with command '#{cmd}' and pid #{@dpn_cluster_pid}"
      puts "Waiting 30 seconds for all nodes to come online"
      sleep 30
    end
  end

  def dpn_cluster_stop
    if @dpn_cluster_pid != 0
      begin
        Process.kill('TERM', @dpn_cluster_pid)
        puts "Stopped DPN cluster (pid #{@dpn_cluster_pid})"
      rescue
        # DPN cluster wasn't running.
      end
      @dpn_cluster_pid = 0
    end
  end

  def nsq_start
    if @nsq_pid == 0
      env = @context.env_hash
      cmd = "./nsq_service -config #{@context.exchange_root}/config/nsq/integration.config"
      log_file = '/dev/null'
      @nsq_pid = Process.spawn(env,
                                  cmd,
                                  chdir: @context.go_bin_dir,
                                  out: [log_file, 'w'],
                                  err: [log_file, 'w'])
      Process.detach @nsq_pid
      puts "Started NSQ service with command '#{cmd}' and pid #{@nsq_pid}"
    end
  end

  def nsq_stop
    if @nsq_pid != 0
      begin
        Process.kill('TERM', @nsq_pid)
        puts "Stopped NSQ service (pid #{@nsq_pid})"
      rescue
        # nsqd wasn't running.
      end
      @nsq_pid = 0
    end
  end

  def pharos_reset_db
    puts "Resetting Pharos DB"
    env = @context.env_hash
    cmd = 'rbenv exec rake pharos:empty_db'
    log_file = "#{@context.log_dir}/pharos.log"
    pid = Process.spawn(env, cmd, chdir: @context.pharos_root)
    Process.wait pid
    puts "Finished resetting Pharos DB"
  end

  def pharos_load_fixtures
    puts "Loading Pharos fixtures"
    env = @context.env_hash
    cmd = 'rbenv exec rake db:fixtures:load'
    log_file = "#{@context.log_dir}/pharos.log"
    pid = Process.spawn(env, cmd, chdir: @context.pharos_root)
    Process.wait pid
    puts "Finished loading Pharos fixtures"
  end

  def pharos_start
    if @pharos_pid == 0
      env = @context.env_hash
      cmd = 'rbenv exec rails server'
      log_file = "#{@context.log_dir}/pharos.log"
      @pharos_pid = Process.spawn(env,
                                  cmd,
                                  chdir: @context.pharos_root,
                                  out: [log_file, 'w'],
                                  err: [log_file, 'w'])
      Process.detach @pharos_pid
      puts "Started Pharos with command '#{cmd}' and pid #{@pharos_pid}"
    end
  end

  def pharos_stop
    if @pharos_pid != 0
      begin
        Process.kill('TERM', @pharos_pid)
        puts "Stopped Pharos (pid #{@pharos_pid})"
      rescue
        # Service wasn't running
      end
      @pharos_pid = 0
    end
  end

  # Stops all services
  def stop_everything
    stop_apt_and_dpn_services
    dpn_cluster_stop
    pharos_stop
    nsq_stop
  end

  # Stops all apt_* and dpn_* services, but leaves external services
  # like NSQ, DPN REST and Pharos running.
  def stop_apt_and_dpn_services()
    @pids.each do |app_name, pid|
      begin
        if !pid.nil? && pid > 0
          Process.kill('TERM', pid)
          puts "Stopped #{app_name}"
        end
      rescue
        # Process wasn't running
      end
      @pids[app_name] = 0
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
