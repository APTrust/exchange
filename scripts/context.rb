require_relative 'app'

# The Context class contains some shared contextual information used
# for APTrust and DPN integration tests.
class Context

  attr_reader(:dpn_server_root, :exchange_root, :pharos_root, :log_dir,
              :go_bin_dir, :apps, :verbose)

  def initialize
    @dpn_server_root = ENV['DPN_SERVER_ROOT'] || abort("Set env var DPN_SERVER_ROOT")
    @exchange_root = ENV['EXCHANGE_ROOT'] || abort("Set env var EXCHANGE_ROOT")
    @pharos_root = ENV['PHAROS_ROOT'] || abort("Set env var PHAROS_ROOT")
    @log_dir = "#{ENV['HOME']}/tmp/logs"
    @nsq_data_dir = "#{ENV['HOME']}/tmp/nsq"
    @go_bin_dir = "#{ENV['HOME']}/tmp/bin"

    @verbose = false # make this a command-line option

    # This is the list of apps we can compile. The key is the app name,
    # and the value is the directory that contains the app's source code.
    # Services can be started and forcibly stopped by service.rb.
    # Applications can be started by service.rb and will run until done.
    # Special apps have special methods in service.rb to run them.
    @apps = {
      'apt_bucket_reader' => App.new('apt_bucket_reader', 'application'),
      'apt_fetch' => App.new('apt_fetch', 'service'),
      'apt_store' => App.new('apt_store', 'service'),
      'apt_record' => App.new('apt_record', 'service'),
      'apt_volume_service' => App.new('apt_volume_service', 'service'),
      'dpn_copy' => App.new('dpn_copy', 'service'),
      'dpn_queue' => App.new('dpn_queue', 'application'),
      'dpn_sync' => App.new('dpn_sync', 'application'),
      'nsq_service' => App.new('nsq_service', 'special'),
      'test_push_to_dpn' => App.new('test_push_to_dpn', 'application'),
    }
  end

  # env_hash returns a hash of environment variables that can be passed
  # to processes we spawn, such as REST services or go tests.
  def env_hash
    env = {}
    ENV.each{ |k,v| env[k] = v }
    # Are APTrust and DPN on the same ruby verson?
    env['RBENV_VERSION'] = `cat #{@pharos_root}/.ruby-version`.chomp
    env['RAILS_ENV'] = 'integration'
    env
  end

  def make_test_dirs
    FileUtils.mkdir_p @log_dir
    FileUtils.mkdir_p @go_bin_dir
    FileUtils.mkdir_p @nsq_data_dir
  end

  def clear_logs
    puts "Deleting old logs"
    FileUtils.remove(Dir.glob("#{@log_dir}/*"))
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
