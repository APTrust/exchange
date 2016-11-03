require 'fileutils'

# The Build class provides methods for building Go source files
# for APTrust and DPN integration tests.
class Build

  def initialize
    @exchange_root = ENV['EXCHANGE_ROOT'] || abort("Set env var EXCHANGE_ROOT")
    @pharos_root = ENV['PHAROS_ROOT'] || abort("Set env var PHAROS_ROOT")
    @log_dir = "#{ENV['HOME']}/tmp/logs"
    @go_bin_dir = "#{ENV['HOME']}/tmp/bin"
    FileUtils.mkdir_p @log_dir
    FileUtils.mkdir_p @go_bin_dir

    # This is the list of apps we can compile. The key is the app name,
    # and the value is the directory that contains the app's source code.
    @apps = {
      'apt_bucket_reader' => "#{@exchange_root}/apps/apt_bucket_reader",
      'apt_fetch' => "#{@exchange_root}/apps/apt_fetch",
      'apt_store' => "#{@exchange_root}/apps/apt_store",
      'apt_record' => "#{@exchange_root}/apps/apt_record",
      'apt_volume_service' => "#{@exchange_root}/apps/apt_volume_service",
      'dpn_copy' => "#{@exchange_root}/apps/dpn_copy",
      'dpn_queue' => "#{@exchange_root}/apps/dpn_queue",
      'dpn_sync' => "#{@exchange_root}/apps/dpn_sync",
      'nsq_service' => "#{@exchange_root}/apps/nsq_service",
      'test_push_to_dpn' => "#{@exchange_root}/apps/test_push_to_dpn",
    }
  end

  def build(app_name)
    source_dir = @apps[app_name]
    if source_dir == nil
      raise "Cannot build unknown app #{app_name}"
    end
    cmd = "go build -o #{@go_bin_dir}/#{app_name} #{app_name}.go"
    puts cmd
    pid = Process.spawn(cmd, chdir: source_dir)
    Process.wait pid
    if $?.exitstatus != 0
      raise "Build failed for #{app_name}"
    end
  end

end


if __FILE__ == $0
  b = Build.new
  b.build('apt_fetch')
  b.build('apt_store')
  b.build('apt_record')
  b.build('dpn_copy')
  b.build('dpn_queue')
end
