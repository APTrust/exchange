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

Once you have all that, simple run `./scripts/test.rb --help` to see which integration tests are available. Note that
integration tests are cumulative, with each test bringing the various services into the state that the next
test needs to start. Currently, `./scripts/test.rb dpn_store` will run through all APTrust ingest and DPN replication
tests.

## TODO

Most of the TODOs are embedded in the code, and you can find them with this:

```
grep -r TODO . --include=*.go
```

This is the list of global TODOs (not related to a specific package).

 Aim for ~100% test coverage. Run the above test command with the -cover option to see coverage.
