package logger_test

import (
	"testing"
	"github.com/APTrust/exchange"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/logger"
	"github.com/op/go-logging"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
)


// Get a barebones config object with just enough info to
// set up logging. Log to a temp dir.
func getLoggingTestConfig(t *testing.T) (*exchange.Config) {
	logDir, err := ioutil.TempDir("", "exchange_log_test")
	if err != nil {
		t.Errorf("Can't create temp dir to test logging: %v", err)
	}
	return &exchange.Config{
		LogDirectory: logDir,
		LogLevel: logging.ERROR,
		LogToStderr: false,
	}
}

// Delete temp log dir after tests.
func teardownLoggerTest(config *exchange.Config) {
	os.RemoveAll(config.AbsLogDirectory())
}

func TestInitLogger(t *testing.T) {
	config := getLoggingTestConfig(t)
	defer teardownLoggerTest(config)
	log := logger.InitLogger(config)
	log.Error("Test Message")
	logFile := filepath.Join(config.AbsLogDirectory(), path.Base(os.Args[0])+".log")
	if !fileutil.FileExists(logFile) {
		t.Errorf("Log file does not exist at %s", logFile)
	}
	data, err := ioutil.ReadFile(logFile)
	if err != nil {
		t.Error(err)
	}
	if false == strings.HasSuffix(string(data), "Test Message\n") {
		t.Error("Expected message was not in the message log.")
	}
}

func TestInitJsonLogger(t *testing.T) {
	config := getLoggingTestConfig(t)
	defer teardownLoggerTest(config)
	log := logger.InitJsonLogger(config)
	log.Println("{a:100}")
	logFile := filepath.Join(config.AbsLogDirectory(), path.Base(os.Args[0])+".json")
	if !fileutil.FileExists(logFile) {
		t.Errorf("Log file does not exist at %s", logFile)
	}
	data, err := ioutil.ReadFile(logFile)
	if err != nil {
		t.Error(err)
	}
	if string(data) != "{a:100}\n" {
		t.Error("Expected message was not in the json log.")
	}
}

func TestDiscardLogger(t *testing.T) {
	log := logger.DiscardLogger("logger_test")
	if log == nil {
		t.Error("DiscardLogger returned nil")
	}
	log.Info("This should not cause an error!")
}
