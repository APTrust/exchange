[![Go Report Card](https://goreportcard.com/badge/github.com/APTrust/exchange)](https://goreportcard.com/report/github.com/APTrust/exchange)
[![Build Status](https://travis-ci.org/APTrust/exchange.svg?branch=master)](https://travis-ci.org/APTrust/exchange)

# Exchange

Exchange is a suite of back-end services that handle bag ingest, restoration and deletion for APTrust, along with ingest, replication and restoration for DPN. Each of these services is in turn composed of a number of micro services that represent a single step in the overall process of a larger service. The micro services allow the larger services to recover from failure without having to re-do large amounts of work.

Exchange is a rewrite of APTrust's original bagman code, which is functionally complete, well tested and proven, but also messy and hard to maintain. Exchange aims to replace bagman with an equally complete system that meets the following criteria:

* Code is divided into logical packages, is clear and easy to maintain and extend
* Code relies on the fewest possible external systems (i.e. remove NSQ)
* Code uses Amazon's Elastic File System, instead of EBS (i.g. remove volume manager)
* Code provides a simple, extensible REST client (instead of the messy Fluctus client)
* Code provides a straight-forward configuration system (currently a mess in bagman)
* Code provides a simple, consistent context manager for both cron jobs an microservices (also a mess in bagman)
* Code does not rely on any AWS services, other than S3 and Glacier

The first of these goals is the most important. The code must be clear and maintainable for the long term.

The existing bagman code will continue to run until this code is complete.

## Dependency Mangement

Go dependencies are managed using [glide](https://github.com/Masterminds/glide) and are stored in this source repo in the vendor directory. This ensures consistent and reproducable builds.

Glide reads glide.yml which entails all needed dependencies with their respective versions
`glide update` regenerates the dependency versions using scanning and rules, and `glide install` will install the versions listed in the glide.lock file, skipping scanning, unless the glide.lock file is not found in which case it will perform an update.

If you are using Go 1.5 ensure the environment variable GO15VENDOREXPERIMENT is set, for example by running export GO15VENDOREXPERIMENT=1. In Go 1.6 it is enabled by default and in Go 1.7 it is always enabled without the ability to turn it off.

### Usage

For first time usage install glide as follows.

The easiest way to install the latest release on Mac or Linux is with the following script:

```
curl https://glide.sh/get | sh
```

On Mac OS X you can also install the latest release via [Homebrew](https://github.com/Homebrew/homebrew):

```
$ brew install glide
```

On Ubuntu Precise(12.04), Trusty (14.04), Wily (15.10) or Xenial (16.04) you can install from our PPA:

```
sudo add-apt-repository ppa:masterminds/glide && sudo apt-get update
sudo apt-get install glide
```

```
$ glide create                            # Start a new workspace
$ open glide.yaml                         # and edit away!
$ glide get github.com/Masterminds/cookoo # Get a package and add to glide.yaml
$ glide install                           # Install packages and dependencies
# work, work, work
$ go build                                # Go tools work normally
$ glide up                                # Update to newest versions of the package
```

#### glide get [package name]

You can download one or more packages to your `vendor` directory and have it added to your
`glide.yaml` file with `glide get`.

```
$ glide get github.com/Masterminds/cookoo
```

## Unit Testing

```
go test $(glide novendor) github.com/APTrust/exchange/...
```
or
```
go test $(go list ./... | grep -v /vendor/)
```
or, if you have ruby installed
```
./scripts/test.rb units
```
## Integration Testing

To run integration tests, you'll need the following:

- A copy of the Pharos repo
- Environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY set to values that can access APTrust's test buckets
- Environment variables PHAROS_API_USER=system@aptrust.org and PHAROS_API_KEY=c3958c7b09e40af1d065020484dafa9b2a35cea0
- A copy of the develop branch of the [dpn-server repo](https://github.com/dpn-admin/dpn-server) if you want to do DPN integration testing
- A Postgres database (SQLite does not do well with the concurrent requests that the integration tests produce)
- The Postgres user 'pharos' must own the pharos_integration database and must have the CREATEDB privilege.

Hints for correctly configuring the Postgres DB for integration tests:

`grant all privileges on database pharos_integration to pharos;`
`alter database pharos_integration owner to pharos;`
`alter user pharos createdb;`

Once you have all that, simply run `ruby ./scripts/test.rb --help` to see which integration tests are available and what they do. Note that integration tests are cumulative, with each test bringing the various services into the state that the next test needs to start. The three most common options for integration testing are:

- `./scripts/tesh.sh apt_fixity` - This exercises all APTrust operations.
- `./scripts/tesh.sh dpn_ingest` - This exercises most APTrust operations, along with DPN ingest. (Pushing items from APTrust into DPN.)
- `./scripts/tesh.sh apt_fixity` - This exercises most APTrust operations, along with DPN replication. (Copying and storing items from other simulated DPN nodes.)

## Setting up Postgres

If you're on a Mac, get the Postgres app from https://postgresapp.com/.

Open psql, either from a terminal, or if that doesn't work, from the Postgres Apps's elephant menu at the top of your screen. While you're up there, you might want to tell the Postgres app to always start on startup or login.

In the psql shell, run the following commands:

1. create user pharos with password 'pharos';
2. create database pharos_integration;
3. grant all privileges on database pharos_integration to pharos;
4. alter user pharos createdb;
5. Change into the pharos repo directory and run `RAILS_ENV=integration rake db:schema:load`

That 4th command is required for integration tests because the test scripts drop and recreate the pharos_integration database at the start of the test cycle.

## Building the Go applications and services

You can build all of the Go applications and services with this command:

`ruby ./scripts/build.rb <path to output dir>`

Note that some applications build differently for Windows. For example, apt_validate
does not try to use mime magic when built for Windows. You can specify Windows, Mac,
or Linux builds using go build tags, e.g.:

`go build -tags="windows partners" apt_validate.go`

## TODO

Most of the TODOs are embedded in the code, and you can find them by running this command from the project's top-level directory:

```
grep -r TODO . --include=*.go --exclude=./vendor/*
```

The code in the /workers directory and in the /dpn/workers directory could use some cleanup. We added a number of logical tests and logging statements to that code after APTrust 2.0 went into production. In general, the fuctions in /workers and /dpn/workers should be broken up into shorter units, and some of it should be moved out into files and/or packages that can be unit-tested.

This is the list of global TODOs (not related to a specific package).

Aim for ~100% test coverage. Run the above test command with the -cover option to see coverage.
