#!/usr/bin/env ruby

require_relative 'context'

# The Build class provides methods for building Go source files
# for APTrust integration tests.
class Build

  def initialize(context)
    @context = context
  end

  def build(app)
    if app == nil
      raise "App cannot be nil"
    end
    cmd = "go build -o #{@context.go_bin_dir}/#{app.name} #{app.name}.go"
    source_dir = "#{@context.exchange_root}/apps/#{app.name}"
    puts cmd
    pid = Process.spawn(cmd, chdir: source_dir)
    Process.wait pid
    if $?.exitstatus != 0
      raise "Build failed for #{app.name}"
    end
  end

  def build_all()
    @context.apps.values.each do |app|
      build(app)
    end
  end

end


if __FILE__ == $0
  # context.rb expects the following ENV vars to be set for
  # integration testing, but if we're just doing a
  # command-line build, we don't need them.
  ENV['PHAROS_ROOT'] ||= '/dev/null'
  go_bin_dir = ARGV[0]
  if !go_bin_dir
    puts "Usage: ruby build.rb /path/bin/dir"
    puts "Binaries will be copied into /path/bin/dir"
    exit(1)
  end
  context = Context.new
  context.go_bin_dir = go_bin_dir
  build = Build.new(context)
  build.build_all()
  puts "Binaries are in #{go_bin_dir}"
end
