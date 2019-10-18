require 'fileutils'
require_relative 'context'

# The Serice class provides a means to start and stop services
# for APTrust integration tests. It also provides access
# to rake tasks that load fixtures and perform other fuctions
# required for integration testing.
#
class Service

  def initialize(context)
	@context = context
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
	  elsif app.name == 'apt_queue_fixity'
		# queue only 12 files, and choose them from bags we know
		# we stored in prior integration tests and did not delete
		cmd += " -maxfiles=12 -like=test.edu/ncsu.1840.16-100"
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
	  puts "[#{Time.now.strftime('%T.%L')}] Started #{app.name} with command '#{cmd}' and pid #{pid}"
	  @pids[app.name] = pid
	end
  end

  # Run a special bucket reader command to update a single bag.
  # This uses a different config file to check for bags in
  # a different bucket
  def run_bucket_reader_for_update()
    env = @context.env_hash
    cmd = "./apt_bucket_reader -config #{@context.exchange_root}/config/integration_update.json"
    cmd += " -stats=#{@context.log_dir}/bucket_reader_update_stats.json"
    if @context.verbose
      pid = Process.spawn(env, cmd, chdir: @context.go_bin_dir)
    else
      pid = Process.spawn(env, cmd, chdir: @context.go_bin_dir, out: '/dev/null', err: '/dev/null')
    end
    Process.wait pid
    puts "[#{Time.now.strftime('%T.%L')}] Started apt_bucket reader (for update) with command '#{cmd}' and pid #{pid}"
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
		puts "[#{Time.now.strftime('%T.%L')}] Stopped NSQ service (pid #{@nsq_pid})"
	  rescue
		# nsqd wasn't running.
	  end
	  @nsq_pid = 0
	end
  end

  def pharos_reset_db
	puts "Resetting Pharos DB"
	env = @context.env_hash
	cmd = 'bundle exec rake db:reset'
	log_file = "#{@context.log_dir}/pharos.log"
	pid = Process.spawn(env, cmd, chdir: @context.pharos_root)
	Process.wait pid
	puts "Finished resetting Pharos DB"
  end

  def pharos_load_fixtures
	puts "Loading Pharos fixtures"
	env = @context.env_hash
	cmd = 'bundle exec rake db:fixtures:load'
	log_file = "#{@context.log_dir}/pharos.log"
	pid = Process.spawn(env, cmd, chdir: @context.pharos_root)
	Process.wait pid
	puts "Finished loading Pharos fixtures"
  end

  def pharos_start
	if @pharos_pid == 0
	  env = @context.env_hash
	  cmd = 'bundle exec rails server'
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
		puts "[#{Time.now.strftime('%T.%L')}] Stopped Pharos (pid #{@pharos_pid})"
	  rescue
		# Service wasn't running
	  end
	  @pharos_pid = 0
	end
  end

  # Stops all services
  def stop_everything
	stop_apt_services
	pharos_stop
	nsq_stop
  end

  # Stops all apt_* services, but leaves external services
  # like NSQ and Pharos running.
  def stop_apt_services()
	@pids.each do |app_name, pid|
	  begin
		if !pid.nil? && pid > 0
		  Process.kill('TERM', pid)
		  puts "[#{Time.now.strftime('%T.%L')}] Stopped #{app_name}"
		end
	  rescue
		# Process wasn't running
	  end
	  @pids[app_name] = 0
	end
  end

  def linux_kill_process_tree(pid)
    child_pids = `pgrep -P #{pid}`.split("\n")
    child_pids.each do |child_pid|
      linux_kill_process_tree(child_pid.strip)
    end
    Process.kill('TERM', pid.to_i) rescue puts "No process #{pid}"
  end

end

if __FILE__ == $0
  context = Context.new
  service = Service.new(context)
  context.apps.values.each { |app| service.app_start(app) unless app.run_as == 'special' }
  sleep 10
  context.apps.values.each { |app| service.app_stop(app) unless app.run_as == 'special' }
end
