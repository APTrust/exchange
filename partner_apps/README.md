# Partner Tools

This directory contains command-line applications that APTrust distributes
to depositors through the Partner Apps page at
https://wiki.aptrust.org/Partner_Tools.

For more info, see the [README.txt], which is distributed with the partner
tools.

## Building

Use the script in scripts/build_partner_tools.rb to build all of the partner
tools. Your build command must include a platform name and version number,
like this:

`./scripts/build_partner_tools.rb --version 2.2-beta --platform mac`

Build for the Mac on Mac, for Linux on Linux and for Windows on Windows.

To ensure the build gets stamped with the proper Git commit hash, make
your build *after* committing changes.

## Distribution

The build tool places the binaries in the parter_apps/bin directory, which
is not included in source control. It also places a copy of [README.txt] in
the bin directory. You can then zip that up and upload it to the
aptrust.public.download bucket for distribution. Be sure to update the
Wiki page as well.
