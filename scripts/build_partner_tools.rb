#!/usr/bin/env ruby

require 'optparse'
require 'fileutils'

# Don't include spaces in these vars, since they cause
# problems in the linker, even when quoted.
# Also note that double quotes and percent signs may
# cause some problems.
# See https://github.com/golang/go/wiki/GcToolchainTricks
REPO_URL = "https://github.com/APTrust/exchange"
WIKI_URL = "https://wiki.aptrust.org/Partner_Tools"
LICENSE = "Apache-2.0"
EMAIL = "support@aptrust.org"

@apps = ['apt_check_ingest',
        'apt_delete',
        'apt_download',
        'apt_list',
        'apt_upload',
        'apt_validate']

def run()
  options = parse_options()
  if !options['version'] || !options['platform']
    exit 1
  end

  # Set up substitution vars
  git_hash = `git rev-parse --short HEAD`.chomp
  build_date = Time.now.utc.strftime("%FT%TZ")
  ldflags = get_ld_flags(options['version'], build_date, git_hash)
  tags = "-tags='partners,#{options['platform']}'"
  build_dir = ensure_build_dir(options['exchange_root'])

  # Build each app, with substitution vars
  @apps.each do |app_name|
    source_dir = File.join(options['exchange_root'], 'partner_apps', app_name)
    cmd = "go build #{tags} #{ldflags} -o #{build_dir}/#{app_name} #{app_name}.go"
    puts "cd #{source_dir}"
    puts cmd
    pid = Process.spawn(cmd, chdir: source_dir)
    Process.wait pid
    if $?.exitstatus != 0
      raise "Build failed for #{app_name}"
    end
  end

  # Copy the README into the bin dir for zipping and distribution.
  puts "Copying README.txt"
  readMeSrc = File.join(options['exchange_root'], 'partner_apps', 'README.txt')
  readMeDest = File.join(options['exchange_root'], 'partner_apps', 'bin', 'README.txt')
  FileUtils.copy(readMeSrc, readMeDest)
end

def get_ld_flags(version, build_date, git_hash)
  pkg = "github.com/APTrust/exchange/partner_apps/common"
  # Single quotes around the entire arg that follows -X
  # helps prevent some errors.
  flags =  [ "-X '#{pkg}.Version=#{version}'",
             "-X '#{pkg}.BuildDate=#{build_date}'",
             "-X '#{pkg}.GitHash=#{git_hash}'",
             "-X '#{pkg}.RepoUrl=#{REPO_URL}'",
             "-X '#{pkg}.WikiUrl=#{WIKI_URL}'",
             "-X '#{pkg}.License=#{LICENSE}'",
             "-X '#{pkg}.Email=#{EMAIL}'"
           ]
  return "-ldflags \"#{flags.join(' ')}\""
end

def ensure_build_dir(exchange_root)
  build_dir = File.join(exchange_root, 'partner_apps', 'bin')
  if !Dir.exist?(build_dir)
    puts "Creating build directory #{build_dir}"
    Dir.mkdir(build_dir)
  end
  return build_dir
end

def parse_options()
  options = {}
  OptionParser.new do |opts|
    opts.on("-h", "--help", "Print help message and exit") do
      print_help
      exit!
    end
    opts.on("-p", "--platform platform", "Build for platform (mac, linux, windows)") do |p|
      if !['mac', 'linux', 'windows'].include?(p)
        puts "Required option -p must be one of 'mac', 'linux', or 'windows'"
        exit!
      end
      options['platform'] = p
    end
    opts.on("-v", "--version version", "Build version to assign to binaries (e.g. 2.3-beta)") do |v|
      if v.nil? || v == ""
        puts "Option -v (--version) is required. E.g. --version=2.3-beta"
        exit!
      end
      options['version'] = v
    end
  end.parse!
  options['exchange_root'] = ENV['EXCHANGE_ROOT'] || abort("Set env var EXCHANGE_ROOT")
  return options
end

def print_help()
  puts ""
  puts "build_partner_tools.rb --platform=< mac | linux | windows> --version=<version>"
  puts "Builds and packages all partner tools"
  puts "Option --p or --platform is the platform for which you are building."
  puts "Option --v or --version is the version number to stamp on this build."
  puts ""
  puts "Requires environment variable EXCHANGE_ROOT, which should be set to "
  puts "the directory containing the exchange source code."
  puts ""
end

if __FILE__ == $0
  run()
end
