# check_glacier.rb
#
# This script reads a text file containing UUIDs that should be
# stored in Glacer/Oregon. It queries the Glacier bucket to see
# if each key is present and writes its results to one of two
# files: glacier_present.txt or glacier_absent.txt.
# --------------------------------------------------------------------------

require 'open3'

class Glacier

  def initialize
    @uuid_file = 'glacier_uuids.txt'
    @present_file = '__glacier_present.txt'
    @absent_file = '__glacier_absent.txt'
    @error_file = '__glacier_errors.txt'
    #@base_cmd = '/home/ubuntu/go/bin/apt_audit_list -config=/home/ubuntu/go/src/github.com/APTrust/exchange/config/production.json -region="us-west-2" -bucket="aptrust.preservation.oregon"'
    @base_cmd = '/Users/apd4n/tmp/bin/apt_audit_list -config=/Users/apd4n/Desktop/audit/production.json -region="us-west-2" -bucket="aptrust.preservation.oregon"'
    self.write_present_file_headers()
  end

  def write_present_file_headers
    File.open(@present_file, 'w') do |f|
      f.puts("id,key,bucket,size,content_type,institution,bag_name,path_in_bag,md5,sha256,etag,last_modified,last_seen,created_at,updated_at,deleted_at")
    end
  end

  def check_uuids
    File.open(@uuid_file).each do |line|
      uuid = line.strip
      cmd = "#{@base_cmd} -prefix=#{uuid}"
      puts "Checking #{uuid}"
      stdout, stderr, status = Open3.capture3(cmd)
      process_output(uuid, stdout, stderr, status)
    end
  end

  def process_output(uuid, stdout, stderr, status)
    if stderr.strip.length > 0
      record_error(uuid, stderr)
    elsif stdout.strip.length == 0
      record_absent(uuid)
    else
      record_present(stdout)
    end
  end

  def record_error(uuid, stderr)
    File.open(@error_file, 'a') do |f|
      f.puts("#{uuid}\t#{stderr}")
    end
  end

  def record_absent(uuid)
    File.open(@absent_file, 'a') do |f|
      f.puts(uuid)
    end
  end

  def record_present(stdout)
    File.open(@present_file, 'a') do |f|
      f.puts(stdout)
    end
  end

end

if __FILE__ == $0
  glacier = Glacier.new
  glacier.check_uuids()
end
