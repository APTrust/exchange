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

----------------------------------------------------------------------------

-- S3 files where size of object in storage does not
-- match size in Pharos.
--
-- 1532: All but 3 are Columbia or UVA, from old known issue
-- 3 are from fulcrum, being updated 10/24/2019 while audit was running
--
-- Need to verify 3 Fulcrum files.
select s.uuid,
p.intellectual_object_id,
p.identifier,
p.size,
p.created_at,
p.updated_at
from summary s
left join pharos p on p.uuid = s.uuid
where s.in_s3=1 and s.s3_size_matches = 0
-- and p.identifier not like 'columbia%'

-- Glacier files where size of object in storage does not
-- match size in Pharos.
--
-- 1530: All but 1 are Columbia or UVA, from old known issue
-- 1 is from Fulcrum, being updated 10/24/2019 while audit was running
--
-- Need to verify 1 Fulcrum file.
select s.uuid,
p.intellectual_object_id,
p.identifier,
p.size,
p.created_at,
p.updated_at
from summary s
left join pharos p on p.uuid = s.uuid
where s.in_std_glacier = 1 and s.std_glacier_size_matches = 0
-- and p.identifier not like 'columbia%'

-- Glacier-VA files where size of object in storage does not
-- match size in Pharos.
--
-- Zero
select s.uuid,
p.intellectual_object_id,
p.identifier,
p.size,
p.created_at,
p.updated_at
from summary s
left join pharos p on p.uuid = s.uuid
where s.in_glacier_va = 1 and s.glacier_va_size_matches = 0

------------------------------------------------------------

-- S3 files where md5 metadata tag does not match md5 value
-- in Pharos.
--
-- 1376 - All but 8 are from legacy UVA/Columbia issue
-- 8 are from fulcrum, and were being written during audit
--
-- Need to verify 8 fulcrum files
select s.uuid,
p.intellectual_object_id,
p.identifier,
p.size,
p.created_at,
p.updated_at
from summary s
left join pharos p on p.uuid = s.uuid
where s.in_s3=1 and s.s3_md5_matches = 0
--and p.identifier not like 'columbia%'


-- Glacier files where md5 metadata tag does not match md5 value
-- in Pharos.
--
-- 1371: All UVA or Columbia from old issue, except
-- 5 from fulcrum that were being written during audit (10/24)
--
-- Need to verify 5 fulcrum files.
select s.uuid,
p.intellectual_object_id,
p.identifier,
p.size,
p.created_at,
p.updated_at
from summary s
left join pharos p on p.uuid = s.uuid
where s.in_std_glacier=1 and s.std_glacier_md5_matches = 0
--and p.identifier not like 'columbia%'

-- Glacier-VA files where md5 metadata tag does not match
-- Pharos md5.
--
-- Zero
select s.uuid,
p.intellectual_object_id,
p.identifier,
p.size,
p.created_at,
p.updated_at
from summary s
left join pharos p on p.uuid = s.uuid
where s.in_glacier_va=1 and s.glacier_va_md5_matches = 0

------------------------------------------------------------

-- S3 files where sha256 metadata tag value does not match
-- md5 in Pharos.
--
-- 1666: All Columbia or UVA from old known issue, except
-- 8 from fulcrum being written during audit (10/24)
--
-- Need to verify 8 Fulcrum files.
select s.uuid,
p.intellectual_object_id,
p.identifier,
p.size,
p.created_at,
p.updated_at
from summary s
left join pharos p on p.uuid = s.uuid
where s.in_s3=1 and s.s3_sha256_matches = 0
and p.identifier not like 'columbia%'

-- Glacier OR items where Pharos sha256 does not match
-- metadata tag sha256.
--
-- 600,239 total, but...
-- 623,575 items have null sha256 because they were ingested
-- before we started tagging Glacier files with sha256 metadata
--
-- Except for 5 Fulcrum files being written during the audit,
-- **ALL** Created on or before 2017-01-10, when we switched
-- to the new system. We were not recording the sha256 checksum
-- in Glacier in the old system.
--
-- 5 Fulcrum files being written on 10/24/2019 need to be verified.
select s.uuid,
p.intellectual_object_id,
p.identifier,
p.size,
p.created_at,
p.updated_at
from summary s
left join pharos p on p.uuid = s.uuid
where s.in_std_glacier=1 and s.std_glacier_sha256_matches = 0
--and p.identifier not like 'columbia%'
--and p.updated_at <= '2019-10-21'
order by p.created_at desc

-- Mismatch between Pharos sha256 and sha256 metadata tag value.
-- Zero
select s.uuid,
p.intellectual_object_id,
p.identifier,
p.size,
p.created_at,
p.updated_at
from summary s
left join pharos p on p.uuid = s.uuid
where s.in_glacier_va=1 and s.glacier_va_sha256_matches = 0

---------------------------------------------------------------------

-- Glacier files with no sha256 metadata.
-- 623,575 with null sha256
-- These were ingested before we moved to Exchange 2.0. (2017-01-10)
-- Prior to that date, we did not add sha256 checksum to Glacier metadata.
select count(*) from standard_glacier where sha256 is null or sha256 = ''

-- Glacier files with null md5 metadata.
-- Zero
select count(*) from standard_glacier where md5 is null or md5 = ''
