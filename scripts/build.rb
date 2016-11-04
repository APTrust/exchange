require_relative 'context'

# The Build class provides methods for building Go source files
# for APTrust and DPN integration tests.
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
  context = Context.new
  build = Build.new(context)
  build.build_all()
end
