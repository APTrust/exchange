# glacier_copy.rb
#
# Copy missing files from S3/VA to Glacier/Oregon.
# --------------------------------------------------------------------------

require 'open3'

class GlacierCopy
  def initialize
    @input_file = "resolution_glacier.txt"
    @output_file = "__glacier_copies.txt"
    @error_file = "__glacier_copy_errors.txt"
  end

  def run
    count = 0
    self.init_output_file
    File.open(@input_file).each do |line|
      cols = line.split(/\t/)
      uuid = cols[0].strip
      problem = cols[1].strip
      next if uuid == 'uuid' # first line has headers
      if problem == 'Missing'
        puts "Copying #{uuid}"
        copy_to_glacier(uuid)
        count += 1
        #break if count > 2
      end
    end
    self.finish_output_file
  end

  def init_output_file
    File.open(@output_file, "a") do |f|
      f.puts "["
    end
  end

  def finish_output_file
    File.open(@output_file, "a") do |f|
      f.puts "]"
    end
  end

  def copy_to_glacier(uuid)
    cmd = "mc cp --json --no-color s3/aptrust.preservation.storage/#{uuid} s3/aptrust.preservation.oregon/#{uuid}"
    stdout, stderr, status = Open3.capture3(cmd)
    process_output(uuid, stdout, stderr, status)
  end

  def process_output(uuid, stdout, stderr, status)
    if stderr.strip.length > 0
      record_error(uuid, stderr)
    else
      record_copy_result(uuid, stdout)
    end
  end

  def record_error(uuid, stderr)
    File.open(@error_file, 'a') do |f|
      f.puts("#{uuid}\t#{stderr}")
    end
  end

  def record_copy_result(uuid, stdout)
    File.open(@output_file, 'a') do |f|
      f.puts(stdout)
    end
  end
end

if __FILE__ == $0
  gcopy = GlacierCopy.new
  gcopy.run()
end
