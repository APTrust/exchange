require 'sqlite3'

db = SQLite3::Database.new('audit.db')

# Max total file size per batch is 6GB.
# If we move more than that in 4 hours,
# we start paying steep penalties.
max_size = 6291456000

remaining = max_size
batch = 1

while remaining > 0 do
  rows = db.execute("select id, size from fixed_files " + 
                    "where size < #{remaining} " + 
                    "and batch_number=0 order by size desc limit 1")
  if rows.count == 0
    if remaining == max_size
      break  # No more results to get
    end
    batch += 1
    remaining = max_size
    next
  end

  (id, size) = rows[0]
  puts("Id #{id}, Size #{size}, Batch #{batch}")
  db.execute("update fixed_files set batch_number = ? where id = ?", batch, id)
  remaining = remaining - size  
end

rows = db.execute("select batch_number, count(size), sum(size) " + 
                  "from fixed_files group by batch_number " + 
                  "order by batch_number")
puts "Batch\tFiles\tSize"
rows.each do |row|
  puts "#{row[0]}\t#{row[1]}\t#{row[2]}"
end
