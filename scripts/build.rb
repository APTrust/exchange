require_relative 'context'

# The Build class provides methods for building Go source files
# for APTrust and DPN integration tests.
class Build

  def initialize(context)
    @ctx = context
  end

  def build(app_name)
    app = @ctx.apps[app_name]
    if app == nil
      raise "Cannot build unknown app #{app_name}"
    end
    cmd = "go build -o #{@ctx.go_bin_dir}/#{app_name} #{app_name}.go"
    source_dir = "#{@ctx.exchange_root}/apps/#{app_name}"
    puts cmd
    pid = Process.spawn(cmd, chdir: source_dir)
    Process.wait pid
    if $?.exitstatus != 0
      raise "Build failed for #{app_name}"
    end
  end

  def build_all()
    @ctx.apps.values.each do |app|
      build(app.name)
    end
  end

end


if __FILE__ == $0
  context = Context.new
  build = Build.new(context)
  build.build_all()
end
