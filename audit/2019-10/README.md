# APTrust October 2019 Audit

This directory contains files related to a full audit of all APTrust storage
as of late October, 2019.

The purposes of the audit are:

1. to find the following and repair missing files.
2. to find and quarantine orphaned files.

Missing files are files that our Pharos registry says should be in S3 and/or
Glacier, but are not actually present. We should be able to fix these by copying
the S3 version to Glacier, or vice-versa.

Orphaned files are those that exist in S3 and/or Glacier, but not in Pharos. We
created quite a few of these between late September and mid October 2019. Once we
identify these, we can quarantine these by moving them to a separate storage area.
If we determine after several months that we don't need them, we can delete them.

## Data Collection Process

We collected data for the audit using the following commands on apt-prod-services:

```
# Dump list of Pharos Generic Files
./apt_dump_files --config=config/production.json --maxfiles=4000000000 > pharos_files.txt

# List Standard S3 bucket
/home/ubuntu/go/bin/apt_audit_list \
-config=/home/ubuntu/go/src/github.com/APTrust/exchange/config/production.json \
-region="us-east-1" -bucket="aptrust.preservation.storage" \
-limit=20000000 > apt.preservation.storage.txt

# List Standard Glacier bucket
# This also returns over 4.5 million log files
/home/ubuntu/go/bin/apt_audit_list \
-config=/home/ubuntu/go/src/github.com/APTrust/exchange/config/production.json \
-region="us-west-2" -bucket="aptrust.preservation.oregon" \
-limit=20000000 > apt.preservation.oregon.txt

# List Glacier VA bucket
/home/ubuntu/go/bin/apt_audit_list \
-config=/home/ubuntu/go/src/github.com/APTrust/exchange/config/production.json \
-region="us-east-1" -bucket="aptrust.preservation.glacier.va" \
-limit=20000000 > aptrust.preservation.glacier.va.txt

# List Glacier OH bucket
/home/ubuntu/go/bin/apt_audit_list \
-config=/home/ubuntu/go/src/github.com/APTrust/exchange/config/production.json \
-region="us-east-2" -bucket="aptrust.preservation.glacier.oh" \
-limit=20000000 > aptrust.preservation.glacier.oh.txt

# List Glacier OR bucket
/home/ubuntu/go/bin/apt_audit_list \
-config=/home/ubuntu/go/src/github.com/APTrust/exchange/config/production.json \
-region="us-west-2" -bucket="aptrust.preservation.glacier.or" \
-limit=20000000 > apt.preservation.glacier.or.txt

# List Glacier Deep VA bucket
/home/ubuntu/go/bin/apt_audit_list \
-config=/home/ubuntu/go/src/github.com/APTrust/exchange/config/production.json \
-region="us-east-1" -bucket="aptrust.preservation.glacier-deep.va" \
-limit=20000000 > aptrust.preservation.glacier-deep.va.txt

# List Glacier Deep OH bucket
/home/ubuntu/go/bin/apt_audit_list \
-config=/home/ubuntu/go/src/github.com/APTrust/exchange/config/production.json \
-region="us-east-2" -bucket="aptrust.preservation.glacier-deep.oh" \
-limit=20000000 > aptrust.preservation.glacier-deep.oh.txt

# List Glacier Deep OR bucket
/home/ubuntu/go/bin/apt_audit_list \
-config=/home/ubuntu/go/src/github.com/APTrust/exchange/config/production.json \
-region="us-west-2" -bucket="aptrust.preservation.glacier-deep.or" \
-limit=20000000 > apt.preservation.glacier-deep.or.txt
```

## Data Import

We imported the raw data into a SQLite database using the [audit_import.rb](audit_import.rb) script.

## Known Issues with the Data

* Generic file records created or updated after about Oct. 21, 2019 may be out of sync because the data collection scripts ran from Oct. 21 - Oct. 28, 2019. During that time, a handful (< 100) generic file records may have changed in Pharos and/or S3/Glacier.
* The data set includes no records for the following buckets, since there is no data in them:
  * Glacier-OH
  * Glacier-OR
  * Glacier-Deep-VA
  * Glacier-Deep-OH
  * Glacier-Deep-OR
* Pharos list three objects from the test account "test.edu" in the Glacier-OH bucket. Those objects were deleted when we turned on encryption in that bucket and were not replaced. Pharos should be updated to note the deletions.
* The Pharos raw data includes some duplicates. See comment in [audit_import.rb](audit_import.rb).

When analyzing the data with a GUI tool, avoid [DB Browser for SQLite](https://sqlitebrowser.org/dl/), as it crashes consistently and corrupts the database, even on read operations. Try [SQLite Studio](https://sqlitestudio.pl/) instead, or use command-line tools.

## Location of Raw Audit Data

The raw audit data is in the APTrust internal bucket (aptrust.internal) under the audit-oct-2019 folder. It includes the data dumped out by the `apt_dump_files` and `apt_audit_list` commands above. It also includes a copy of SQLite database, called `audit.db`.

## Preliminary Results

### Missing Files (According to data collected)

* 9 files missing from S3 Standard/VA
* 16,630 missing from S3 Glacier/Oregon
* 0 missing from Glacier-VA

Regarding the missing files:

* All of the missing Glacier files should be restorable from S3.
* All of the "missing" S3 files are actually present in S3.
  * 8 of the 9 were being written to S3 while we were collecting audit data,
    so they didn't appear in our data collection, but were confirmed afterward
    by a spot check.
  * The ninth item is in S3 and the checksum appears to be correct. Not sure
    why this appears as missing in the audit data.

#### Next Steps for Missing Files

* Identify commonalities among missing Glacier files.
* Copy missing Glacier files from S3/VA to Glacier/Oregon.
* Create Replication PREMIS event after successful copy.

### Orphan Files

* 364,405 in S3 Standard/VA (2.01 TB)
* 358,349 in Glacier Standard/OR (1.81 TB)
* 1 in Glacier-VA (3 KB)

#### Next Steps for Orphan Files

* Identify commonalities among orphan files.
* Move to quarantine area (separate S3 bucket, Glacier, or Wasabi).

## Tools for Cleanup

For copying files from S3 to Glacier, and to move orphan files to a
quarantine area, Minio client is simple and flexible.
The [download page](https://min.io/download) has packages for several operating
systems.

After download, the
[User Guide](https://docs.min.io/docs/minio-client-complete-guide)
provides setup instructions. The client is easy to configure. To add AWS as a remote
provider, simply run:

```
mc config host add s3 https://s3.amazonaws.com $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY --api S3v4
```