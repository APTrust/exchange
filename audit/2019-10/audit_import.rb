require "sqlite3"
require "csv"

class Importer

  @@file_for_table = {
	'standard_s3' => 'aptrust.preservation.storage.txt',
	'standard_glacier' => 'aptrust.preservation.oregon.txt',
	'glacier_va' => 'aptrust.preservation.glacier.va.txt',
	'pharos' => 'pharos_files.txt'
  }

  # In Pharos file export, these records appear twice.
  # We want to import them only once. These duplicates
  # exist because we collected GenericFile records through
  # the REST API, which currently only allows ordering
  # by date and returns results one page at a time.
  # Some records were updated during data collection, causing
  # some pages to shift. This needs to be fixed on the
  # Pharos side, so we can order by GenericFile.Identifier,
  # or any other field we choose.
  @@duplicates = [
	"4220279","4220281","4220283","4220285",
	"4220286","4220287","4220288","4220289",
	"4220290","4220292","4220294","5391477",
	"5391478","5391479","5391480","5391481",
	"5391482","5391483","5391484","5391485",
	"5391486","5391487","5391488","5391489",
	"5391490","5391491","5391492","5391493",
	"5391494","5391495","5391496","5391497",
	"5391498","5391499","5391500","5391501",
	"5391502","5391503","5391504","5391505",
	"5391506","5391507","5391508","5391509",
	"5391510","5391511","5391512","5391513"
  ]

  # Keep track of duplicate GenericFile Ids that
  # have already been imported.
  @@already_imported = {}

  @@limit = 0

  def initialize()
	@db_file = File.join("audit.db")
	@db = SQLite3::Database.new(@db_file)
	puts "DB file: #{@db_file}"
  end

  def create_storage_table(table_name)
	puts "Creating table #{table_name}"
    @db.execute <<-SQL
		create table #{table_name} (
			id integer primary key,
			uuid varchar(80),
			bucket varchar(255),
			size integer,
			content_type varchar(255),
            gf_identifier varchar(400),
			md5 varchar(255),
			sha256 varchar(255),
			etag varchar(255),
			last_modified datetime,
			last_seen datetime
		)
		SQL
  end

  def create_pharos_table()
	puts "Creating table pharos"
    @db.execute <<-SQL
		create table pharos (
			id integer primary key,
			gfid int,
			identifier varchar(400),
			intellectual_object_id int,
			intellectual_object_identifier varchar(255),
			file_format varchar(255),
			uuid varchar(80),
			storage_option varchar(40),
			size integer,
			created_at datetime,
			updated_at datetime,
			state varchar(10),
			md5 varchar(255),
			sha256 varchar(255)
		)
		SQL
  end

  def storage_insert_statement(table_name)
	<<-SQL
		insert into #{table_name} (
			uuid,
			bucket,
			size,
			content_type,
            gf_identifier,
			md5,
			sha256,
			etag,
			last_modified,
			last_seen
		) values (?,?,?,?,?,?,?,?,?,?)
		SQL
  end


  def pharos_insert_statement()
	<<-SQL
		insert into pharos (
			gfid,
			identifier,
			intellectual_object_id,
			intellectual_object_identifier,
			file_format,
			uuid,
			storage_option,
			size,
			created_at,
			updated_at,
			state,
			md5,
			sha256
		) values (?,?,?,?,?,?,?,?,?,?,?,?,?)
		SQL
  end

  def create_summary_table()
    puts "Creating summary table"
	@db.execute <<-SQL
        create table summary (
        uuid varchar(80),
        in_s3 boolean,
        s3_size_matches boolean,
        s3_md5_matches boolean,
        s3_sha256_matches boolean,
        in_std_glacier boolean,
        std_glacier_size_matches boolean,
        std_glacier_md5_matches boolean,
        std_glacier_sha256_matches boolean,
        in_glacier_va boolean,
        glacier_va_size_matches boolean,
        glacier_va_md5_matches boolean,
        glacier_va_sha256_matches boolean
    )
    SQL
  end

  def populate_summary_table()
    puts "Populating summary table"
	@db.execute <<-SQL
        insert into summary
        select p.uuid,
        coalesce((ss3.uuid is not null), 0) as in_s3,
        coalesce((ss3.size = p.size), 0) as s3_size_matches,
        coalesce((ss3.md5 = p.md5), 0) as s3_md5_matches,
        coalesce((ss3.sha256 = p.sha256), 0) as s3_sha256_matches,
        coalesce((sgl.uuid is not null), 0) as in_std_glacier,
        coalesce((sgl.size = p.size), 0) as std_glacier_size_matches,
        coalesce((sgl.md5 = p.md5), 0) as std_glacier_md5_matches,
        coalesce((sgl.sha256 = p.sha256), 0) as std_glacier_sha256_matches,
        coalesce((gva.uuid is not null), 0) as in_gva_glacier,
        coalesce((gva.size = p.size), 0) as gva_size_matches,
        coalesce((gva.md5 = p.md5), 0) as gva_md5_matches,
        coalesce((gva.sha256 = p.sha256), 0) as gva_sha256_matches
        from pharos p
        left join standard_s3 ss3 on p.uuid = ss3.uuid
        left join standard_glacier sgl on p.uuid = sgl.uuid
        left join glacier_va gva on p.uuid = gva.uuid
     SQL
  end


  def import_data()
	@@file_for_table.each do |table_name, file_name|
	  if table_name == 'pharos'
		import_pharos_file(table_name, file_name)
	  else
		import_storage_file(table_name, file_name)
	  end
	end
  end

  def import_storage_file(table_name, file_name)
	opts = {col_sep: "\t", headers: false}
    row_count = 0
	@db.transaction
	CSV.foreach(file_name, opts) do |row|
	  row_count += import_storage_row(table_name, row);
	  break if @@limit > 0 && row_count >= @@limit
	end
	@db.commit
	puts "Imported #{row_count} records from #{file_name}"
  end

  def import_storage_row(table_name, row)
	sql = storage_insert_statement(table_name)
	data = clean_storage_row_data(row)
	return 0 if data.nil?
	@db.execute(sql, data)
	return 1
  end

  def clean_storage_row_data(row)
	if row[1].start_with?('logs/')
	  return nil
	end
	return [
	  row[1],        # uuid
	  row[2],        # bucket
	  row[3],        # size
	  row[4],        # content_type
      "#{row[6]}/#{row[7]}", # generic file identifier
	  row[8],        # md5
	  row[9],        # sha256
	  row[10],       # etag
	  row[11],       # last_modified
	  row[12]        # last_seen
	]
  end

  def import_pharos_file(table_name, file_name)
	opts = {col_sep: ",", headers: true}
	@db.transaction
    row_count = 0
	CSV.foreach(file_name, opts) do |row|
	  row_count += import_pharos_row(row);
	  break if @@limit > 0 && row_count >= @@limit
	end
	@db.commit
	puts "Imported #{row_count} records from #{file_name}"
  end

  def import_pharos_row(row)
	sql = pharos_insert_statement()
	data = xform_pharos_data(row)
	return 0 if data.nil?
	@db.execute(sql, data)
	return 1
  end

  def xform_pharos_data(row)
	id = row[0]
	if @@already_imported[id]
	  puts "Skipping duplicate #{id}."
	  return nil
	end
	if @@duplicates.include?(id)
	  @@already_imported[id] = true
	end

	uri = row[5]
	last_slash = uri.rindex('/')
	bucket = uri[0..(last_slash - 1)]
	uuid = uri[(last_slash + 1)..-1]

	storage_option = 'Standard'
	if bucket.end_with?('glacier.va')
	  storage_option = 'Glacier-VA'
	elsif bucket.end_with?('glacier.oh')
	  storage_option = 'Glacier-OH'
	end

	return [
	  row[0],   # Generic File Id (gfid)
	  row[1],   #identifier,
	  row[2],   #intellectual_object_id,
	  row[3],   #intellectual_object_identifier,
	  row[4],   #file_format,
	  uuid,
	  storage_option,
	  row[6],   #size,
	  row[9],   #created_at,
	  row[10],  #updated_at,
	  row[11],  #state,
	  row[12],  #md5,
	  row[13]   #sha256
	]
  end

  def create_indexes
	@@file_for_table.keys.each do |table_name|
	  if table_name == 'pharos'
        create_pharos_indexes()
	  else
		create_storage_table_indexes(table_name)
	  end
	end
  end

  # Generic File identifiers (gfid) should be unique in the Pharos table.
  def create_pharos_indexes()
    ix_uuid = "create unique index ix_pharos_uuid on pharos(uuid)"
    ix_gf_identifier = "create unique index ix_pharos_identifier on pharos(identifier)"
    puts ix_uuid
    @db.execute(ix_uuid)
    puts ix_gf_identifier
    @db.execute(ix_gf_identifier)
  end

  # Generic File identifiers (gf_identifier) will not be unique in all
  # cases in storage. We know we have some duplicates, and part of the
  # purpose of the audit is to find them.
  def create_storage_table_indexes(table_name)
    ix_uuid = "create unique index ix_#{table_name}_uuid on #{table_name}(uuid)"
    ix_gf_identifier = "create index ix_#{table_name}_gf_identifier on #{table_name}(gf_identifier)"
    puts ix_uuid
    @db.execute(ix_uuid)
    puts ix_gf_identifier
    @db.execute(ix_gf_identifier)
  end

  def create_summary_table_indexes()
    ix_uuid = "create unique index ix_summary_uuid on summary(uuid)"
    puts ix_uuid
    @db.execute(ix_uuid)
  end

  def run
	@@file_for_table.keys.each do |table_name|
	  if table_name == 'pharos'
		create_pharos_table()
	  else
		create_storage_table(table_name)
	  end
	end
	import_data()
    create_indexes()
    # Summary table is populated from main data tables
    # and needs indexes from create_indexes, or else it
    # will take forever to run.
    create_summary_table()
    populate_summary_table()
    create_summary_table_indexes()
  end

end

if __FILE__ == $0
  importer = Importer.new
  importer.run
end
