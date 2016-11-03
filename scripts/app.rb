# App describes an APTrust or DPN app or service used in integration testing.
class App

  attr_reader :name, :run_as

  # name is the name of the application's executable.
  # runs_as must be one of 'service' (which runs until we explicitly
  # kill it), 'application' (which exits on its own when it's done),
  # or 'special', which means Service.app_start should not try to
  # run it, because it requires special setup.
  def initialize(name, runs_as)
    @name = name
    @runs_as = runs_as
  end

end
