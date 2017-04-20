# EFS Bug

Items in this directory were created in response to the EFS zero-byte copy bug,
which resulted in a number of large files (50+ MB) being written to S3 as
empty files. This bug affected the production system from Feb. 22, 2017 to
April 17, 2017.

The program in efsbug.go is meant to, but does not, reproduce the problem. The
problem is 100% reproducible in our production environment. Before the fix in
the commits of April 17, 2017 (ending with commit e903cc4), the logs for
apt_storer showed a number of entries like this:

```
2017/04/11 23:28:33 [INFO] Starting to upload file PR_Joe_18Mbps_H264.mp4 (size: 2457787137)
2017/04/11 23:28:33 [INFO] Stored PR_Joe_18Mbps_H264.mp4 in s3 after 1 attempts
2017/04/11 23:28:41 [INFO] Starting to upload file PR_Joe_18Mbps_H264.mp4 (size: 2457787137)
2017/04/11 23:30:33 [INFO] Stored PR_Joe_18Mbps_H264.mp4 in glacier after 1 attempts


2017/04/11 23:48:59 [INFO] Starting to upload file PR_Debbie_18Mbps_H264.mp4 (size: 3123088573)
2017/04/11 23:48:59 [INFO] Stored PR_Debbie_18Mbps_H264.mp4 in s3 after 1 attempts
2017/04/11 23:49:04 [INFO] Starting to upload file PR_Debbie_18Mbps_H264.mp4 (size: 3123088573)
2017/04/11 23:52:23 [INFO] Stored PR_Debbie_18Mbps_H264.mp4 in glacier after 1 attempts

```

In the first case, a file of about 2.4 GB was uploaded to S3 in zero seconds.
In the second case, a file of about 3.1 GB was uploaded in zero seconds.
Both these files have a size of zero bytes in S3, and this problem affected
a number of other files as well.

Notice that the Glacier upload for the first file took 1 minute 52 seconds, and
the Glacier upload for the second file took 3 minutes 19 seconds. Those files
were traveling from Virginia to Oregon, so the copy took some time.

In all of these cases, apt_storer is reading a large file from inside a tar
archive, writing it to disk, and then telling the S3 uploader to send that
file to S3 in Virginia or S3 in Oregon (which is our gateway to Glacier). In
all cases, the first read of the file, during the copy to S3, resulted in a
zero-length file being stored, while the second read, during the copy to
Glacier, result in a file of the correct size being stored. In other words,
the first attempt to read any of these extracted files resulted in an empty
read (deep within the AWS S3 SDK), while all subsequent reads were non-empty.

We have not been able to fix or find the source of the problem. Our sample
program, efsbug.go, does not reproduce it. However, problem does still
occur consistently in production, and our work-around is to verify the size
of the file in S3 and to recopy the file if the first copy attempt does not
appear to be successful. We do this by fetching meta-information about the
S3 file immediately after uploading it.

While the S3 web console shows these initial uploads having zero bytes,
the AWS S3 SDK reports the same files as having a size of about 842 GB.
This behavior is consistent and reproducible.

From our apt_storer logs:

```
2017/04/17 20:58:10 [WARNING] s3 returned size 842354926880 for sample.mov, should be 642111856. Will retry.
2017/04/17 21:10:32 [WARNING] s3 returned size 842355066400 for sample.mov, should be 642111856. Will retry.
2017/04/17 21:24:32 [WARNING] s3 returned size 842355076608 for sample.mov, should be 642111856. Will retry.

```

When we get this bad size reading from S3, we simply re-upload the file and
recheck the result. Here's what a typical transaction looks like in the logs:

```
2017/04/17 21:24:32 [INFO] Starting to upload file sample.mov (size: 642111856) to s3
2017/04/17 21:24:32 [WARNING] s3 returned size 842355076608 for sample.mov, should be 642111856. Will retry.
2017/04/17 21:24:32 [INFO] Starting to upload file sample.mov (size: 642111856) to s3
2017/04/17 21:24:42 [INFO] Stored sample.mov in s3 after 2 attempts

```

Once again, note how the first attempt to copy a 600MB file to S3 took zero
seconds and returned a bad file size. The second attempt took 10 seconds, and
there's no message about a bad file size. The file was then checked manually
and confirmed to be correct.

This bug likely affected a large number of files. The code in the efsbug
directory will allow us to compare what Pharos says we've stored in S3
and Glacier against what's actually stored in S3 and Glacier. We will
overwrite bad S3 files with good copies from Glacier.

## Data for the Audit Database

Data for the audit database comes from the apt_audit_list program, which
dumps output to a tab-separated text file. We import that text file to the
stored_files table of a SQLite database.

We also extract data about GenericFiles from Pharos, dump it to a similar
text file, and then import that into the SQLite database. This is the code
to extract the data from Pharos. It runs in rails console:

```ruby

File.open('/home/adiamond/generic_files.txt', 'w') do |f|
  GenericFile.find_in_batches do |batch|
    batch.each do |gf|
      gone = gf.state == 'D'
      f.puts "#{gf.identifier}\t#{gf.uri.split("/").last}\t#{gf.size}\t#{gone}\t#{gf.created_at}\t#{gf.updated_at}"
    end
  end
end; 0

```

Once all the data is in the SQLite database, we can compare what *should* be
stored (pharos_files) to what's actually stored (stored_files). We can identify
bad copies in S3 and overwrite them with good copies from Glacier.
