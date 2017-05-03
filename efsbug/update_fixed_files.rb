require 'json'
require 'sqlite3'

# CREATE TABLE fixed_files (
#     id integer primary key autoincrement,
#     identifier varchar(255),
#     uuid varchar(36),
#     batch_number int not null default 0,
#     size bigint not null default 0,
#     error_message text,
#     restore_requested_at datetime,
#     restore_completed_at datetime,
#     copy_started_at datetime,
#     copy_completed_at datetime);

class Updater

  def initialize
    @db = SQLite3::Database.new('audit.db')
  end

  # Read in upload records from the uploader's JSON logs.
  def load_upload_records
    @uploaded = []
#    ['upload_01.json', 'upload_02.json'].each do |file|
    ['upload_03.json'].each do |file|
      File.open(file, 'r').each do |line|
        next if line.strip() == ''
        @uploaded.push(JSON.parse(line))
      end
    end
  end

  # Update one record in the database
  def update_db(record)
    fixed = false
    uuid = record['key']
    completed_at = record['s3_last_modified']
    size_in_s3 = record['s3_content_length']
    expected_size = get_expected_size(uuid)
    if size_in_s3 == expected_size
      @db.execute("update fixed_files set restore_completed_at = ? " + 
                   "where uuid = ?", completed_at, uuid)
      puts "Updated #{uuid} (#{size_in_s3} <-> #{expected_size})"
      fixed = true
    else
      puts "Wrong size for #{uuid}"
    end
    return uuid, fixed
  end

  def get_expected_size(uuid)
    result = @db.execute("select size from fixed_files where uuid=?", uuid)
    return result[0][0]
  end

  def run
    fixed_count = 0
    not_fixed = []
    load_upload_records()
    @uploaded.each do |record|
      uuid, fixed = update_db(record)
      if fixed
        fixed_count += 1
      else
        not_fixed.push(uuid)
      end
    end
    puts ""
    puts "Fixed #{fixed_count} files"
    if not_fixed.count > 0 
      puts "These files were not fixed"
      not_fixed.each do |uuid|
        puts uuid
      end
    end
  end

end

if __FILE__ == $0
  u = Updater.new
  u.run()
end
