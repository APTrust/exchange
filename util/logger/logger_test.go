package logger_test

import (
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/util/logger"
	"github.com/op/go-logging"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
)


// Get a barebones config object with just enough info to
// set up logging. Log to a temp dir.
func getLoggingTestConfig(t *testing.T) (*models.Config) {
	logDir, err := ioutil.TempDir("", "exchange_log_test")
	if err != nil {
		t.Errorf("Can't create temp dir to test logging: %v", err)
	}
	return &models.Config{
		TarDirectory: logDir,
		LogDirectory: logDir,
		RestoreDirectory: logDir,
		ReplicationDirectory: logDir,
		LogLevel: logging.ERROR,
		LogToStderr: false,
	}
}

// Delete temp log dir after tests.
func teardownLoggerTest(config *models.Config) {
	absLogDir := config.AbsLogDirectory()
	slashCount := (len(absLogDir) - len(strings.Replace(absLogDir, "/", "", -1)))
	if len(absLogDir) > 12 || slashCount < 3 {
		// Don't call remove all on "/" or "/usr" or anything like that.
		os.RemoveAll(absLogDir)
	} else {
		fmt.Printf("Not deleting log dir '%s' because it looks dangerous.\n" +
			"Delete that manually, if you thing it's safe.\n", absLogDir)
	}
}

func TestInitLogger(t *testing.T) {
	config := getLoggingTestConfig(t)
	defer teardownLoggerTest(config)
	log, filename := logger.InitLogger(config)
	log.Error("Test Message")
	logFile := filepath.Join(config.AbsLogDirectory(), path.Base(os.Args[0])+".log")
	if !fileutil.FileExists(logFile) {
		t.Errorf("Log file does not exist at %s", logFile)
	}
	if filename != logFile {
		t.Errorf("Expected log file path '%s', got '%s'", logFile, filename)
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
	log, filename := logger.InitJsonLogger(config)
	log.Println("{a:100}")
	logFile := filepath.Join(config.AbsLogDirectory(), path.Base(os.Args[0])+".json")
	if !fileutil.FileExists(logFile) {
		t.Errorf("Log file does not exist at %s", logFile)
	}
	if filename != logFile {
		t.Errorf("Expected log file path '%s', got '%s'", logFile, filename)
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
