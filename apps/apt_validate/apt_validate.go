// +build partners

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/validation"
	"os"
)

func main() {
	configFile, outputFile, pathToBag := parseCommandLine()
	bagValidationConfig, errors := validation.LoadBagValidationConfig(configFile)
	if errors != nil && len(errors) > 0 {
		for _, err := range errors {
			fmt.Fprintln(os.Stderr, err.Error())
		}
		os.Exit(10)
	}
	validator, err := validation.NewBagValidator(pathToBag, bagValidationConfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating BagValidator:", err.Error())
		os.Exit(11)
	}
	exitCode := 0
	result := validator.Validate()
	if result.ParseSummary.HasErrors() {
		fmt.Println("Parse errors:")
		for _, err := range result.ParseSummary.Errors {
			fmt.Println(err)
		}
		fmt.Println("")
		exitCode = 12
	}
	if result.ValidationSummary.HasErrors() {
		fmt.Println("Validation errors:")
		for _, err := range result.ValidationSummary.Errors {
			fmt.Println(err)
		}
		fmt.Println("")
		exitCode = 13
	}
	if exitCode == 0 {
		fmt.Println("Bag is valid")
	}
	if outputFile != "" {
		err := dumpJson(result, outputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not write output file '%s': %v", outputFile, err)
			if exitCode == 0 {
				exitCode = 14
			}
		} else {
			fmt.Println("Wrote IntellectualObject JSON to", outputFile)
		}
	}
	os.Exit(exitCode)
}

func dumpJson(result *validation.ValidationResult, outputFile string) error {
	jsonData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	jsonFile, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer jsonFile.Close()
	jsonFile.Write(jsonData)
	return nil
}

func parseCommandLine() (configFile, outputFile, bagPath string) {
	var pathToConfigFile string
	var pathToOutputFile string
	var pathToBag string
	flag.StringVar(&pathToConfigFile, "config", "", "Path to bag validation config file")
	flag.StringVar(&pathToOutputFile, "output", "", "Path to bag validation output file")
	flag.StringVar(&pathToBag, "bag", "", "Path to bag directory or tar file")
	flag.Parse()
	if pathToConfigFile == "" || pathToBag == "" {
		printUsage()
		os.Exit(15)
	}
	return expandTilde(pathToConfigFile), expandTilde(pathToOutputFile), expandTilde(pathToBag)
}

func expandTilde(filePath string) string {
	expandedPath, err := fileutil.ExpandTilde(filePath)
	if err != nil {
		return filePath
	} else {
		return expandedPath
	}
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_validate validates aptrust bags.

Usage: apt_validate -config=<path> [-output=<path>] -bag=<path>

Param -config is required and should be the path to the JSON file
that describes APTrust or DPN bag validation rules.

Param -bag should be the path to the tar file or the directory
containing the bag you want to validate. E.g. /home/me/my_bag
for an untarred bag or /home/me/my_bag.tar for a tarred bag.

Param -output is not optional. If specified, the validator
will dump a JSON representation of the APTrust IntellectualObject
to the specified file. UUIDs in the JSON dump will be regenerated
each time validation runs, so don't depend on them.

apt_validate has the following exit codes:

 0 - Program completed normally and bag is valid.
10 - Bag validation config file is not valid
11 - Could not create bag validator
12 - Bag could not be parsed
13 - Bag was parsed, but is not valid
14 - Bag is valid, but validator could not dump JSON to output file
15 - Incorrect usage: required params missing.
`
	fmt.Println(message)
}
