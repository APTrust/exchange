# App describes an APTrust app or service used in integration testing.
class App

  attr_reader :name, :run_as

  # name is the name of the application's executable.
  # run_as must be one of 'service' (which runs until we explicitly
  # kill it), 'application' (which exits on its own when it's done),
  # or 'special', which means Service.app_start should not try to
  # run it, because it requires special setup.
  def initialize(name, run_as)
    @name = name
    @run_as = run_as
  end

end
