package context

import (
	"fmt"
	"github.com/APTrust/exchange/config"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/network"
	"github.com/APTrust/exchange/util/logger"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/nsqio/go-nsq"
	"github.com/crowdmob/goamz/aws"
	"github.com/op/go-logging"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
)

/*
Context sets up the items common to many of the bag
processing services (bag_processor, bag_restorer, cleanup,
etc.). It also encapsulates some functions common to all of
those services.
*/
type Context struct {
	Config          config.Config
	JsonLog         *log.Logger
	MessageLog      *logging.Logger
	S3Client        *network.S3Client
	PharosClient    *network.PharosClient
//	Volume          *util.Volume
	syncMap         *models.SynchronizedMap
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
func NewContext(appConfig config.Config) (context *Context) {
	context = &Context {
		succeeded: int64(0),
		failed: int64(0),
	}
	context.Config = appConfig
	context.initLogging()
	context.initS3Client()
	context.initPharosClient()
	context.syncMap = models.NewSynchronizedMap()
	return context
}

// Initializes the loggers.
func (context *Context) initLogging() {
	context.MessageLog = logger.InitLogger(&context.Config)
	context.JsonLog = logger.InitJsonLogger(&context.Config)
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

// Initializes a reusable S3 client.
func (context *Context) initS3Client() {
	s3Client, err := network.NewS3Client(aws.USEast)
	if err != nil {
		message := fmt.Sprintf("Exiting. Cannot init S3 client: %v", err)
		fmt.Fprintln(os.Stderr, message)
		context.MessageLog.Fatal(message)
	}
	context.S3Client = s3Client
}

// Initializes a reusable Pharos client.
func (context *Context) initPharosClient() {
	pharosClient, err := network.NewPharosClient(
		context.Config.PharosURL,
		context.Config.PharosAPIVersion,
		os.Getenv("PHAROS_API_USER"),
		os.Getenv("PHAROS_API_KEY"),
		context.MessageLog)
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

/*
Registers an item currently being processed so we can keep track
of duplicates. Many requests for ingest, restoration, etc. may be
queued more than once. Register an item here to note that it is
being processed under a specific message id. If they item comes in
again before we're done processing, and you try to register it here,
you'll get an error saying the item is already in process.

The key should be a unique identifier. For intellectual objects,
this can be the IntellectualObject.Identifier. For S3 files, it can
be bucket_name/file_name.
*/
func (context *Context) RegisterItem(key string, messageId nsq.MessageID) (error) {
	messageIdString := context.MessageIdString(messageId)
	if context.syncMap.HasKey(key) {
		otherId := context.syncMap.Get(key)
		sameOrDifferent := "a different"
		if otherId == messageIdString {
			sameOrDifferent = "the same"
		}
		return fmt.Errorf("Item is already being processed under %s messageId (%s)",
			sameOrDifferent, otherId)
	}
	// Make a note that we're processing this file.
	context.syncMap.Add(key, messageIdString)
	return nil
}

/*
UnregisterItem removes the item with specified key from the list
of items we are currently processing. Be sure to call this when you're
done processing any item you've registered so we know we're finished
with it and we can reprocess it later, under a different message id.
*/
func (context *Context) UnregisterItem(key string) {
	context.syncMap.Delete(key)
}

/*
Returns the NSQ MessageId under which the current item is being
processed, or an empty string if no item with that key is currently
being processed.
*/
func (context *Context) MessageIdFor(key string) (string) {
	if context.syncMap.HasKey(key) {
		return context.syncMap.Get(key)
	}
	return ""
}

// Converts an NSQ MessageID to a string.
func (context *Context) MessageIdString(messageId nsq.MessageID) (string) {
	messageIdBytes := make([]byte, nsq.MsgIDLength)
	for i := range messageId {
		messageIdBytes[i] = messageId[i]
	}
	return string(messageIdBytes)
}

// Logs info about the number of items that have succeeded and failed.
func (context *Context) LogStats() {
	context.MessageLog.Info("**STATS** Succeeded: %d, Failed: %d",
		context.Succeeded(), context.Failed())
}


/*
Returns true if the bag is currently being processed. This handles a
special case where a very large bag is in process for a long time,
the NSQ message times out, then NSQ re-sends the same message with
the same ID to this worker. Without these checks, the worker will
accept the message and will be processing it twice. This causes
problems because the first working will be deleting files while the
second working is trying to run checksums on them.
*/
func (context *Context) BagAlreadyInProgress(s3File *models.S3File, currentMessageId string) (bool) {
	// Bag is in process if it's in the registry.
	messageId := context.MessageIdFor(s3File.BagName())
	if messageId != "" && messageId == currentMessageId {
		return true
	}

	re := regexp.MustCompile("\\.tar$")
	bagDir := re.ReplaceAllString(s3File.Key.Key, "")
	tarFilePath := filepath.Join(context.Config.TarDirectory, s3File.Key.Key)
	unpackDir := filepath.Join(context.Config.TarDirectory, bagDir)

	// Bag is in process if we have its files on disk.
	return fileutil.FileExists(unpackDir) || fileutil.FileExists(tarFilePath)
}
