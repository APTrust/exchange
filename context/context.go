package context

import (
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/logger"
	"github.com/op/go-logging"
	"os"
	"sync/atomic"
)

/*
Context sets up the items common to many of the bag
processing services (bag_processor, bag_restorer, cleanup,
etc.). It also encapsulates some functions common to all of
those services.
*/
type Context struct {
	Config          *models.Config
	MessageLog      *logging.Logger
	PharosClient    *network.PharosClient
//	Volume          *util.Volume            // *** TODO *** Re-add Volume as service!
	pathToLogFile   string
	succeeded       int64
	failed          int64
}

/*
Creates and returns a new Context object. Because some
items are absolutely required by this object and the processes
that use it, this method will panic if it gets an invalid
config param from the command line, or if it cannot set up some
essential services, such as logging.

This object is meant to used as a singleton with any of the
stand-along processing services (bag_processor, bag_restorer,
cleanup, etc.).
*/
func NewContext(config *models.Config) (context *Context) {
	context = &Context {
		succeeded: int64(0),
		failed: int64(0),
	}
	context.Config = config
	context.MessageLog, context.pathToLogFile = logger.InitLogger(config)
	context.initPharosClient()
	return context
}

// Sets up a new Volume object to track estimated disk usage.
// func (context *Context) initVolume() {
// 	volume, err := NewVolume(context.Config.TarDirectory, context.MessageLog)
// 	if err != nil {
// 		message := fmt.Sprintf("Exiting. Cannot init Volume object: %v", err)
// 		fmt.Fprintln(os.Stderr, message)
// 		context.MessageLog.Fatal(message)
// 	}
// 	context.Volume = volume
// }

// Initializes a reusable Pharos client.
func (context *Context) initPharosClient() {
	pharosClient, err := network.NewPharosClient(
		context.Config.PharosURL,
		context.Config.PharosAPIVersion,
		os.Getenv("PHAROS_API_USER"),
		os.Getenv("PHAROS_API_KEY"))
	if err != nil {
		message := fmt.Sprintf("Exiting. Cannot initialize Pharos Client: %v", err)
		fmt.Fprintln(os.Stderr, message)
		context.MessageLog.Fatal(message)
	}
	context.PharosClient = pharosClient
}

// Returns the number of processed items that succeeded.
func (context *Context) Succeeded() (int64) {
	return context.succeeded
}

// Returns the number of processed items that failed.
func (context *Context) Failed() (int64) {
	return context.failed
}

// Increases the count of successfully processed items by one.
func (context *Context) IncrementSucceeded() (int64) {
	atomic.AddInt64(&context.succeeded, 1)
	return context.succeeded
}

// Increases the count of unsuccessfully processed items by one.
func (context *Context) IncrementFailed() (int64) {
	atomic.AddInt64(&context.failed, 1)
	return context.succeeded
}

// Returns the path to this process' log file
func (context *Context) PathToLogFile() (string) {
	return context.pathToLogFile
}

// Logs info about the number of items that have succeeded and failed.
func (context *Context) LogStats() {
	context.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
		context.Succeeded(), context.Failed())
}
