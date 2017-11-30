This package contains the following tools:

apt_check_ingest v1.1 - Check the ingest status of a bag. Requires a Pharos API key.
    * 2017-11-28: Added -etag option to check bag version with specific etag.
    * 2017-11-28: Added etag to text output, and improved format and readability of 
                  text output.

apt_delete v1.03 - Deletes files from your restoration bucket

apt_download v1.03 - Downloads files from your restoration bucket

apt_list v1.03 - Lists files in your receiving and restoration buckets

apt_upload v1.03 - Uploads files to your receiving bucket

apt_validate v2.1 - Validates APTrust bags
    * 2017-11-28: Fixed problem that caused validator to ask for EXCHANGE_HOME and GOPATH
    * 2017-11-28: Fixed a problem validating path of untarred bag on Windows.

aptrust_bag_validation_config.json - Configuration file that tells apt_validate how to 
validate APTrust bags.

dpn_bag_validation_config.json - Configuration file that tells apt_validate how to 
validate DPN bags.

Each of the partner tools will print a help/usage message to the console if run with no 
options or parameters. More info about partner tools is available at 
https://wiki.aptrust.org/Partner_Tools.

Last updated Nov. 28, 2017
