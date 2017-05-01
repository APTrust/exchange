require 'json'
require 'time'

class Downloader

  # Assumes the file restore.json is in the same directory,
  # and that apt_download is in $PATH. restore.json contains
  # a list of files that apt_restore_from_glacier has
  # requested for restoration. Those items should appear in the
  # Oregon S3 bucket after a few hours.
  #
  # Run this in screen, because it'll run for several hours, and
  # redirect stdout to a file, to capture the output data.
  #
  # ruby download_from_glacier.rb 0 200 2> glacier_download01.log 1> glacier_download01.json
  # 
  def initialize(offset, limit)
    @offset = offset || 0
    @limit = limit || 10000
    @end = offset + limit
    @data_file = "restore.json"
    @restore_dir = "/mnt/efs/apt/restore/"
    @six_hours_ago = Time.now.utc - (6 * 60 * 60)
  end

  def run
    STDERR.puts "Getting files from #{@offset} to #{@end}"
    count = 0
    File.open(@data_file, "r").each do |line|
      count += 1
      if count < @offset
        next
      elsif count > @end
        break
      end
      data = JSON.parse(line)
      request_time = Time.parse(data["RequestTime"])
      key = data["Key"]
      if request_time < @six_hours_ago
        local_file = File.join(@restore_dir, key)
        if !File.exist?(local_file)
          cmd = download_command(key)
          STDERR.puts cmd
          output = `#{cmd}`
          puts output
        else
          STDERR.puts "File #{local_file} is already on disk."
        end
      else
        STDERR.puts "File #{key} was requested less than six hours ago."
      end
    end
  end

  def download_command(key)
    # We need to JSON output because it includes metadata.
    return "apt_download -region='us-west-2' " + 
      "-bucket='aptrust.preservation.oregon' -format='json' " + 
      "-dir='#{@restore_dir}' -key='#{key}'"
  end

end

if __FILE__ == $0 
  if ARGV.count < 2
    STDERR.puts "Usage: ruby download_from_glacier.rb <offset> <limit>"
    exit(1)
  end
  d = Downloader.new(ARGV[0].to_i, ARGV[1].to_i)
  d.run
end
