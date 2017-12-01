APTrust Partner Tools - v. 2.2-beta - December 1, 2017

This package contains the tools listed below. The apt_upload program includes
some breaking changes. See the notes below.

To get specific help with any of these tools, invoke the tool with the
--help option.

For additional information, see https://wiki.aptrust.org/Partner_Tools.


apt_check_ingest v2.2-beta
--------------------------
Checks the ingest status of a bag. Requires a Pharos API key.

    v2.2-beta
    ---------

    * Updated --help doc and standardized exit codes.
    * Added -etag option to check bag version with specific etag.
    * Added etag to text output, and improved format and readability of text
      output.


apt_delete v2.2-beta
--------------------
Deletes files from your restoration bucket, or any other AWS bucket you have
access to. Requires AWS credentials.

    v2.2-beta
    ---------

    * Updated --help doc and standardized exit codes.
    * Replaced the old underlying crowdmob/goamz S3 library with Amazon's
      official S3 library.


apt_download v2.2-beta
----------------------
Downloads files from your restoration bucket, or any other bucket you have
access to. Requires AWS S3 credentials.

    v2.2-beta
    ---------

    * Updated --help doc and standardized exit codes.
    * New --dir flag lets you specify download directory.
    * New --format flag lets you specify whether the report of the download
      result should be printed in text or JSON.
    * Added --region flag to allow access to buckets in any AWS region.
    * No longer deletes files from restoration bucket.
    * Replaced the old underlying crowdmob/goamz S3 library with Amazon's
      official S3 library.


apt_list v2.2-beta
----------------------
Lists files in your receiving and restoration buckets, or any other bucket you
have access to. Requires AWS S3 credentials.

    v2.2-beta
    ---------

    * Updated --help doc and standardized exit codes.
    * Added --format flag with option to output results in JSON or plain text.
    * Added --region flag to allow access to buckets in any AWS region.
    * Minor changes to format of text output include expanded column size and
      quoted etags.
    * Replaced the old underlying crowdmob/goamz S3 library with Amazon's
      official S3 library.


apt_upload v2.2-beta
--------------------
Uploads files to your receiving bucket or any other bucket you have access to.
Requires AWS S3 credentials.

    v2.2-beta
    ---------

    ************************* BREAKING CHANGES ******************************
    The prior version of the uploader, v1.03, allowed you to specify multiple
    files to be uploaded in a single call. Because this version allows you to
    attach custom metadata to each upload, it will upload only a single file
    at a time.
    ************************* BREAKING CHANGES ******************************

    * Updated --help doc and standardized exit codes.
    * Added --format flag with option to output results in JSON or plain text.
    * Added --region flag to allow access to buckets in any AWS region.
    * Added --key flag to allow you to save a file in S3 under a name that
      differs from its name in the local file system.
    * Added --contentType flag that allows you to set the Content-Type metadata
      attribute of an uploaded object. That attribute also becomes the object's
      Content-Type header when people access it via HTTP(S).
    * Added --metadata flag that allows you to save custom metadata with your
      S3 upload.
    * Added --format flag with option to output results in JSON or plain text.
    * Replaced the old underlying crowdmob/goamz S3 library with Amazon's
      official S3 library.


apt_validate v2.2-beta
----------------------

Validates APTrust bags. Requires a configuration file, such as
aptrust_bag_validation_config.json or dpn_bag_validation_config.json, which are
included in this package.

    v2.2-beta
    ---------

    * Updated --help doc and standardized exit codes.
    * Fixed problem that caused validator to ask for EXCHANGE_HOME and GOPATH
    * Fixed a problem validating path of untarred bag on Windows.


aptrust_bag_validation_config.json
----------------------------------

Configuration file that tells apt_validate how to validate APTrust bags.


dpn_bag_validation_config.json
------------------------------

Configuration file that tells apt_validate how to validate DPN bags.



A. Diamond
support@aptrust.org
Last updated Dec. 1, 2017
