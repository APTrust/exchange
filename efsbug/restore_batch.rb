require 'sqlite3'

class Restorer

  def initialize
    @db = SQLite3::Database.new('audit.db')
  end

  def restore_files_in_batch
    rows = @db.execute("select min(batch_number) from fixed_files " + 
                      "where restore_requested_at is null")
    if rows.count == 0
      STDERR.puts "No more batches to fetch"
      exit(0)
    end
    batch_number = rows[0]
    STDERR.puts "Fetching batch number #{batch_number}"

    rows_to_update = []
    rows = @db.execute("select id, uuid from fixed_files " + 
                      "where batch_number = ?", batch_number)
    rows.each do |row|
      (id, uuid) = row
      cmd = get_command(uuid)
      STDERR.puts cmd
      `#{cmd}`
      rows_to_update.push(id)
    end
    
    now = Time.now.utc.strftime("%Y-%m-%dT%H:%M:%S%z")
    rows_to_update.each do |id|
      STDERR.puts "Marking row #{id} as requested"
      @db.execute("update fixed_files set restore_requested_at = ? " + 
                 "where id = ?", now, id)
    end
  end

  def get_command(key)
    return "apt_restore_from_glacier -region='us-west-2' " + 
      "-bucket='aptrust.preservation.oregon' -days=14 " + 
      "-key='#{key}' 1>> restore.json 2>> restore.log"
  end

end

if __FILE__ == $0
  r = Restorer.new
  r.restore_files_in_batch
end
