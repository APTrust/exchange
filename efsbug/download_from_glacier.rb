require 'json'
require 'time'

class Downloader

  # Assumes the file restore.json is in the same directory,
  # and that apt_download is in $PATH. restore.json contains
  # a list of files that apt_restore_from_glacier has
  # requested for restoration. Those items should appear in the
  # Oregon S3 bucket after a few hours.
  def initialize
    @data_file = "restore.json"
    @restore_dir = "/mnt/efs/apt/restore/"
    @six_hours_ago = Time.now.utc - (6 * 60 * 60)
  end

  def run
    File.open(@data_file, "r").each do |line|
      data = json.parse(line)
      request_time = Time.parse(data["RequestTime"])
      key = data["Key"]
      if request_time < @six_hours_ago
        save_to = File.join(@restore_dir, key)
        cmd = download_command(key)
        puts cmd
        # `#{cmd}`
      end
    end
  end

  def download_command(key)
    return "apt_download -region='us-west-2' -bucket='apt.preservation.oregon' -dir='#{@restore_dir}' key='#{key}'"
  end

end
