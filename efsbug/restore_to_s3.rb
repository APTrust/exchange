require 'json'

# Restores files to S3 that we pulled down from Glacier.
# This assumes it's running from the same directory as
# the glacier_download JSON files.
class Restorer

  def initialize
    @bucket = 'aptrust.preservation.storage'
    @download_info = self.load_download_info
  end

  def load_download_info
    records = []
    for i in 1..6
      file = "glacier_download_0#{i}.json"
      if File.exist?(file)
        STDERR.puts "Reading #{file}"
        File.open(file, 'r').each do |line|
          next if line.strip == ''
          records.push(JSON.parse(line))
        end
      end
    end
    return records
  end

  def run
    download_info = self.load_download_info
    download_info.each do |record|
      if self.file_exists?(record) && self.file_is_valid?(record)
        copy_to_s3(record)
      else
        puts "File #{record['saved_to']} is missing or invalid"
      end
    end
  end

  def copy_to_s3(record)
    cmd = get_copy_command(record)
    STDERR.puts(cmd)
  end

  def get_copy_command(record)
    key = record['key']
    file_path = record['saved_to']
    meta_string = record['s3_metadata'].to_json
    return "apt_upload -bucket='#{@bucket}' -key='#{key}' -format='json' -metadata='#{meta_string}' #{file_path}"
  end

  def file_is_valid?(record)
    metadata = record['s3_metadata']
    size_ok = record['bytes_downloaded'] == record['s3_content_length']
    md5_ok = record['md5'] == metadata['Md5']
    sha_ok = record['sha256'] == metadata['Sha256']
    return size_ok && md5_ok && sha_ok
  end

  def file_exists?(record)
    File.exist?(record['saved_to'])
  end

end

if __FILE__ == $0
  d = Restorer.new()
  d.run
end
