package main

import (
	"flag"
	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/validation"
	"os"
	"path/filepath"
)

func main() {
	pathToConfigFile, preserveAttrs := parseCommandLine()
	configAbsPath, err := filepath.Abs(pathToConfigFile)
	if err != nil {
		fmt.Println(os.Stderr, err.Error())
		os.Exit(1)
	}
	pathToBag, err := filepath.Abs(flag.Arg(0))
	if err != nil {
		fmt.Println(os.Stderr, err.Error())
		os.Exit(1)
	}
	conf, errors := validation.LoadBagValidationConfig(configAbsPath)
	if errors != nil && len(errors) > 0 {
		fmt.Println(os.Stderr, "Could not load bag validation config: %v", errors[0])
		os.Exit(1)
	}
	validator, err := validation.NewValidator(pathToBag, conf, preserveAttrs)
	if err != nil {
		fmt.Println(os.Stderr, "Error creating validator: %s", err.Error())
		os.Exit(1)
	}
	summary, err := validator.Validate()
	if err != nil {
		fmt.Println(os.Stderr, "The validator encountered an error: %s", err.Error())
		os.Exit(1)
	}
	if summary.HasErrors() {
		cleanup(validator.DBName())
		fmt.Println("Bag is not valid")
		fmt.Println(summary.AllErrorsAsString())
		os.Exit(2)
	}
	if !preserveAttrs {
		cleanup(validator.DBName())
	}
	fmt.Println("Bag is valid")
}

func cleanup(filePath string) {
	if fileutil.LooksSafeToDelete(filePath, 12, 3) {
		os.Remove(filePath)
	}
}

func parseCommandLine() (pathToConfigFile string, preserveAttrs bool) {
	var help bool
	flag.StringVar(&pathToConfigFile, "config", "", "Path to bag validation config file")
	flag.BoolVar(&preserveAttrs, "attrs", false, "Preserve attributes")
	flag.BoolVar(&help, "help", false, "Show help")

	flag.Parse()

	if help || pathToConfigFile == "" || flag.Arg(0) == "" {
		printUsage()
		os.Exit(0)
	}
	return pathToConfigFile, preserveAttrs
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_validate validates bags according to the specified config file.

Usage:

apt_validate -config=<config_file> -attrs=<true|false> path_to_bag

The path_to_bag parameter is required.

The -config param is required.

The -attrs param indicates whether you want to preserve extended
attributes. This uses more memory, and is generally necessary only
for APTrust ingest.
`
	fmt.Println(message)
}
