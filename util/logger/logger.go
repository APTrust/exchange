package logger

import (
	"fmt"
	"github.com/APTrust/exchange"
	"github.com/op/go-logging"
	"io/ioutil"
	stdlog "log"
	"os"
	"path"
	"path/filepath"
)

/*
InitLogger creates and returns a logger suitable for logging
human-readable message.
*/
func InitLogger(config *exchange.Config) *logging.Logger {
	processName := path.Base(os.Args[0])
	filename := fmt.Sprintf("%s.log", processName)
	filename = filepath.Join(config.AbsLogDirectory(), filename)
	if config.LogDirectory != "" {
		// If this fails, getRotatingFileWriter will panic in just a second
		_ = os.Mkdir(config.LogDirectory, 0755)
	}
	writer, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644);
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot open log file '%s': %v\n", filename, err)
		os.Exit(1)
	}

	log := logging.MustGetLogger(processName)
	format := logging.MustStringFormatter("%{time} [%{level}] %{message}")
	logging.SetFormatter(format)
	logging.SetLevel(config.LogLevel, processName)

	logBackend := logging.NewLogBackend(writer, "", 0)
	if config.LogToStderr {
		// Log to BOTH file and stderr
		stderrBackend := logging.NewLogBackend(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile)
		stderrBackend.Color = true
		logging.SetBackend(logBackend, stderrBackend)
	} else {
		// Log to file only
		logging.SetBackend(logBackend)
	}

	return log
}

/*
InitLogger creates and returns a logger suitable for logging JSON
data. Bagman JSON logs consist of a single JSON object per line,
with no extraneous data. Because all of the data in the file is
pure JSON, with one record per line, these files are easy to parse.
*/
func InitJsonLogger(config *exchange.Config) *stdlog.Logger {
	processName := path.Base(os.Args[0])
	filename := fmt.Sprintf("%s.json", processName)
	filename = filepath.Join(config.AbsLogDirectory(), filename)
	writer, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644);
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot open log file '%s': %v", filename, err)
		os.Exit(1)
	}
	return stdlog.New(writer, "", 0)
}

/*
Discard logger returns a logger that writes to dev/null.
Suitable for use in testing.
*/
func DiscardLogger(module string) *logging.Logger {
	log := logging.MustGetLogger(module)
	devnull := logging.NewLogBackend(ioutil.Discard, "", 0)
	logging.SetBackend(devnull)
	logging.SetLevel(logging.INFO, "volume_test")
	return log
}
