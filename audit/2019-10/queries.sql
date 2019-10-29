/*
    Queries to find orphan and missing files.
*/


-- Standard Storage, missing from S3
select
p.intellectual_object_id,
p.uuid,
p.identifier,
p.size,
p.created_at,
p.updated_at,
s.in_s3,
s.s3_size_matches,
s.s3_md5_matches,
s.s3_sha256_matches,
s.in_std_glacier,
s.std_glacier_size_matches,
s.std_glacier_md5_matches,
s.std_glacier_sha256_matches,
s.in_glacier_va,
s.glacier_va_size_matches,
s.glacier_va_md5_matches,
s.glacier_va_sha256_matches
from pharos p
left join summary s on p.uuid = s.uuid
where p.storage_option = 'Standard'
and p.state = 'A'
and s.in_s3 = 0


-- Standard Storage, missing from Glacier
select
p.intellectual_object_id,
p.uuid,
p.identifier,
p.size,
p.created_at,
p.updated_at,
s.in_s3,
s.s3_size_matches,
s.s3_md5_matches,
s.s3_sha256_matches,
s.in_std_glacier,
s.std_glacier_size_matches,
s.std_glacier_md5_matches,
s.std_glacier_sha256_matches,
s.in_glacier_va,
s.glacier_va_size_matches,
s.glacier_va_md5_matches,
s.glacier_va_sha256_matches
from pharos p
left join summary s on p.uuid = s.uuid
where p.storage_option = 'Standard'
and p.state = 'A'
and s.in_std_glacier = 0

-- Glacier-VA, not in Glacier-VA bucket
select
p.intellectual_object_id,
p.uuid,
p.identifier,
p.size,
p.created_at,
p.updated_at,
s.in_s3,
s.s3_size_matches,
s.s3_md5_matches,
s.s3_sha256_matches,
s.in_std_glacier,
s.std_glacier_size_matches,
s.std_glacier_md5_matches,
s.std_glacier_sha256_matches,
s.in_glacier_va,
s.glacier_va_size_matches,
s.glacier_va_md5_matches,
s.glacier_va_sha256_matches
from pharos p
left join summary s on p.uuid = s.uuid
where p.storage_option = 'Glacier-VA'
and p.state = 'A'
and s.in_glacier_va = 0

-- In S3 bucket but not in Pharos (S3 orphans)
select
ss3.uuid,
ss3.gf_identifier,
ss3.last_modified,
ss3.size
from standard_s3 ss3
left join pharos p on p.uuid = ss3.uuid
where p.uuid is null

-- S3 orphans count & size
select
count(ss3.uuid) as num_files,
sum(ss3.size) as total_size
from standard_s3 ss3
left join pharos p on p.uuid = ss3.uuid
where p.uuid is null

-- In Glacier Oregon (Std Storage Opt) but not Pharos
select
sgl.uuid,
sgl.gf_identifier,
sgl.last_modified,
sgl.size
from standard_glacier sgl
left join pharos p on p.uuid = sgl.uuid
where p.uuid is null

-- Glacier Oregon (Std Storage) orphans count & size
select
count(sgl.uuid) as num_files,
sum(sgl.size) as total_size
from standard_glacier sgl
left join pharos p on p.uuid = sgl.uuid
where p.uuid is null

-- In Glacier-VA but not in Pharos
select
gva.uuid,
gva.gf_identifier,
gva.last_modified,
gva.size
from glacier_va gva
left join pharos p on p.uuid = gva.uuid
where p.uuid is null
