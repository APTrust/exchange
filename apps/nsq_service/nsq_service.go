package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/util"
	"github.com/APTrust/exchange/util/fileutil"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Start the NSQ services. You can kill then all with Control-C
func main() {
	configFile := flag.String("config", "", "Path to nsqd config file")
	flag.Parse()
	fmt.Println("Config file =", *configFile)
	if configFile == nil {
		fmt.Println("Usage: go run service -config=/path/to/nsq/config")
		fmt.Println("    Starts nsqlookupd, nsqd, and nsqadmin.")
		fmt.Println("    Config files are in dir exchange/config/nsq")
		fmt.Println("    Ctrl-C stops all of those processes")
		os.Exit(1)
	}
	run(*configFile)
}

// Run each of the services...
func run(configFile string) {
	fmt.Println("Starting NSQ processes. Use Control-C to quit all")
	nsqlookupd := startProcess("nsqlookupd", "")

	expandedDataDir, err := expandedDataDir(configFile)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	var nsqd *exec.Cmd
	configArg := fmt.Sprintf("--config=%s", configFile)
	if expandedDataDir != "" {
		dataDirArg := fmt.Sprintf("--data-path=%s", expandedDataDir)
		nsqd = startProcess("nsqd", configArg, dataDirArg)
	} else {
		nsqd = startProcess("nsqd", configArg)
	}
	nsqadmin := startProcess("nsqadmin", "--lookupd-http-address=127.0.0.1:4161")

	nsqlookupd.Wait()
	nsqd.Wait()
	nsqadmin.Wait()
}

// Start a process, redirecting it's stderr & stdout so they show up
// in this process's terminal. Returns the command, so we can wait while
// it runs.
func startProcess(command string, arg ...string) *exec.Cmd {
	fmt.Println("Starting", command, arg)
	cmd := exec.Command(command, arg...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fmt.Println(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		fmt.Println(err)
	}
	go io.Copy(os.Stdout, stdout)
	go io.Copy(os.Stderr, stderr)
	err = cmd.Start()
	if err != nil {
		fmt.Println("Error starting", command, err)
	}
	return cmd
}

// Expand the tilde in the data_path setting to an absolute path.
// Returns the expanded path, or an error.
func expandedDataDir(configFile string) (string, error) {
	file, err := os.Open(configFile)
	if err != nil {
		return "", fmt.Errorf("Cannot open config file: %v\n", err)
	}
	defer file.Close()
	bufReader := bufio.NewReader(file)
	for {
		line, err := bufReader.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return "", err
		}
		cleanLine := strings.TrimSpace(line)
		if strings.HasPrefix(cleanLine, "data_path") {
			parts := strings.SplitN(cleanLine, "=", 2)
			if len(parts) < 2 {
				return "", fmt.Errorf("Config file setting for data_path is missing or malformed.")
			}
			expanded, err := fileutil.ExpandTilde(util.CleanString(parts[1]))
			if err != nil {
				return "", fmt.Errorf("Cannot expand data_dir setting '%s': %v", parts[1], err)
			}
			if !fileutil.FileExists(expanded) {
				fmt.Printf("Creating NSQ data directory %s \n", expanded)
				os.MkdirAll(expanded, 0755)
			}
			return expanded, nil
		}
	}
	return "", nil
}
