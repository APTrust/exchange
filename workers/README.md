# APTrust Workers

APTrust workers perform the work required for ingest, restoration, file
deletion and ongoing fixity checks. The common.go file contains shared code.
All other files have a corresponding "main" file in the exchange/apps
directory. The main file compiles to a standalone binary, sets up the proper
context, instantiates a worker, and then runs as a service.

Because the workers run as services, and because they depend a number of
external services, they require integration tests rather than unit tests.
The integration tests for workers are in exchange/integration, and they
are set up and run by the Ruby tests scripts in the exchange/scripts
directory.
