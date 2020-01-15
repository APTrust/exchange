require_relative 'app'

# The Context class contains some shared contextual information used
# for APTrust integration tests.
class Context

  attr_reader(:exchange_root, :pharos_root, :log_dir,
			  :restore_dir, :apps)
  attr_accessor(:verbose, :go_bin_dir)

  def initialize
	@exchange_root = ENV['EXCHANGE_ROOT'] || abort("Set env var EXCHANGE_ROOT")
	@pharos_root = ENV['PHAROS_ROOT'] || abort("Set env var PHAROS_ROOT")
	@log_dir = "#{ENV['HOME']}/tmp/logs"
	@tar_dir = "#{ENV['HOME']}/tmp/tar"
	@restore_dir = "#{ENV['HOME']}/tmp/restore"
	@nsq_data_dir = "#{ENV['HOME']}/tmp/nsq"
	@go_bin_dir = "#{ENV['HOME']}/tmp/bin"

	# The following are command-line options.
	# The verbose option prints log messages to STDERR in
	# addition to printing them to the usual logs.
	@verbose = false

	# This is the list of apps we can compile. The key is the app name,
	# and the value is the directory that contains the app's source code.
	# Services can be started and forcibly stopped by service.rb.
	# Applications can be started by service.rb and will run until done.
	# Special apps have special methods in service.rb to run them.
	@apps = {
	  'apt_audit_list' => App.new('apt_audit_list', 'application'),
	  'apt_bucket_reader' => App.new('apt_bucket_reader', 'application'),
      'apt_dump_files' => App.new('apt_dump_files', 'application'),
      'apt_dump_valdb' => App.new('apt_dump_valdb', 'application'),
	  'apt_fetch' => App.new('apt_fetch', 'service'),
	  'apt_file_delete' => App.new('apt_file_delete', 'service'),
	  'apt_file_restore' => App.new('apt_file_restore', 'service'),
	  'apt_fixity_check' => App.new('apt_fixity_check', 'service'),
	  'apt_glacier_restore_init' => App.new('apt_glacier_restore_init', 'service'),
	  'apt_json_extractor' => App.new('apt_json_extractor', 'application'),
	  'apt_queue' => App.new('apt_queue', 'application'),
	  'apt_queue_fixity' => App.new('apt_queue_fixity', 'application'),
	  'apt_record' => App.new('apt_record', 'service'),
	  'apt_restore' => App.new('apt_restore', 'service'),
	  'apt_restore_from_glacier' => App.new('apt_restore_from_glacier', 'application'),
	  'apt_spot_test_restore' => App.new('apt_spot_test_restore', 'application'),
	  'apt_store' => App.new('apt_store', 'service'),
	  'apt_volume_service' => App.new('apt_volume_service', 'service'),
	  'nsq_service' => App.new('nsq_service', 'special'),
	}
  end

  # env_hash returns a hash of environment variables that can be passed
  # to processes we spawn, such as REST services or go tests.
  def env_hash
	env = {}
	ENV.each{ |k,v| env[k] = v }
	env['RBENV_VERSION'] = `cat #{@pharos_root}/.ruby-version`.chomp
	env['RAILS_ENV'] = 'integration'
	env
  end

  def make_test_dirs
	FileUtils.mkdir_p @log_dir
	FileUtils.mkdir_p @tar_dir
	FileUtils.mkdir_p @go_bin_dir
	FileUtils.mkdir_p @nsq_data_dir
  end

  def clear_logs
	puts "Deleting old logs"
	FileUtils.remove(Dir.glob("#{@log_dir}/*"))
  end

  # Some tests use tar dir and some use staging dir
  # for temporary processing. Clear both before tests.
  def clear_staging
	puts "Deleting temporary staging"
	FileUtils.remove_dir(@tar_dir, force: true)
  end

  def clear_restore
	puts "Deleting temporary restore area"
	FileUtils.remove_dir(@restore_dir, force: true)
  end

  def clear_binaries
	puts "Deleting old binaries"
	FileUtils.remove(Dir.glob("#{@go_bin_dir}/*"))
  end

  def clear_nsq_data
	puts "Deleting old nsq data"
	FileUtils.remove(Dir.glob("#{@nsq_data_dir}/*"))
  end

end
