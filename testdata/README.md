# Exchange Test Data

This directory includes data for unit and integration testing.

* integration_results contains JSON files describing the expected outcomes of integration tests.
* json_objects contains JSON serializations of data fixtures used in unit tests.
* s3_bags contains files that belong in the S3 test receiving bucket called "aptrust.receiving.test.test.edu". Those files are used in integration tests. If they ever get wiped out, you can restore them by unzipping TestBags.zip and uploading the contents to the S3 test bucket. Note that there are only 14 tar files that matter for out integration tests, and they're all in the zip file. You may occasionally see a few additional files in the test bucket, left by other tests.
* unit_test_bags contains a set of bags used in unit tests. Most of these bags test our tar file reader and validator by presenting specific problems.
