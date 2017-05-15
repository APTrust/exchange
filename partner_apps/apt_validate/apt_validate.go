package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/exchange/models"
	"github.com/APTrust/exchange/util/fileutil"
	"github.com/APTrust/exchange/validation"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	pathToConfigFile, pathToOutFile, preserveAttrs := parseCommandLine()
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
		fmt.Fprintln(os.Stderr, "Could not load bag validation config: ", errors[0])
		os.Exit(1)
	}
	validator, err := validation.NewValidator(pathToBag, conf, preserveAttrs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating validator: ", err.Error())
		os.Exit(1)
	}
	summary, err := validator.Validate()
	if err != nil {
		fmt.Fprintln(os.Stderr, "The validator encountered an error: ", err.Error())
		os.Exit(1)
	}
	exitCode := 0
	if summary.HasErrors() {
		cleanup(validator.DBName())
		fmt.Println("Bag is not valid")
		fmt.Println(summary.AllErrorsAsString())
		exitCode = 2
	} else {
		fmt.Println("Bag is valid")
	}
	if pathToOutFile != "" {
		printOutput(validator, pathToOutFile)
	}
	cleanup(validator.DBName())
	os.Exit(exitCode)
}

func printOutput(validator *validation.Validator, pathToOutFile string) {
	file, err := os.Create(pathToOutFile)
	if err != nil {
		fmt.Println(os.Stderr, "Can't open output file: %v", err)
		return
	}
	defer file.Close()

	db := validator.DB()
	obj, err := db.GetIntellectualObject(validator.ObjIdentifier)
	if err != nil {
		fmt.Println(os.Stderr, "Can't get object from db: %v", err)
		return
	}
	objBytes, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		fmt.Println(os.Stderr, "Can't convert object to JSON: %v", err)
		return
	}
	objJson := strings.TrimSpace(string(objBytes))

	// Normally, we'd just add the generic files to the object
	// and serialize the whole thing, but when we have 200k files,
	// that causes an out-of-memory exception. So this hack...
	// Cut off the closing curly bracket, dump in the GenericFiles
	// one by one, and then re-add the curly bracket.
	objJson = objJson[:len(objJson)-2] + ",\n"
	objJson += `  "generic_files": [`
	_, err = file.WriteString(objJson)
	if err != nil {
		fmt.Println(os.Stderr, "Error printing to output file: %v", err)
		return
	}

	// Write out the GenericFiles one by one, without reading them
	// all into memory.
	count := 0
	err = db.ForEach(func(k, v []byte) error {
		if string(k) != validator.ObjIdentifier {
			gf := &models.GenericFile{}
			buf := bytes.NewBuffer(v)
			decoder := gob.NewDecoder(buf)
			err = decoder.Decode(gf)
			if err != nil {
				return fmt.Errorf("Error reading GenericFile from DB: %v", err)
			}
			gfBytes, err := json.MarshalIndent(gf, "    ", "  ")
			if err != nil {
				return fmt.Errorf("Can't convert generic file to JSON: %v", err)
			}
			if count > 0 {
				file.WriteString(",\n    ")
			}
			file.WriteString(string(gfBytes))
			count++
		}
		return nil
	})

	// Close up the JSON
	file.WriteString("\n  ]\n}\n")
}

func cleanup(filePath string) {
	if fileutil.LooksSafeToDelete(filePath, 12, 3) {
		os.Remove(filePath)
	}
}

func parseCommandLine() (pathToConfigFile, pathToOutFile string, preserveAttrs bool) {
	var help bool
	flag.StringVar(&pathToConfigFile, "config", "", "Path to bag validation config file")
	flag.StringVar(&pathToOutFile, "outfile", "", "Path to file for dumping JSON output")
	flag.BoolVar(&preserveAttrs, "attrs", false, "Preserve attributes")
	flag.BoolVar(&help, "help", false, "Show help")

	flag.Parse()

	if help || pathToConfigFile == "" || flag.Arg(0) == "" {
		printUsage()
		os.Exit(0)
	}
	return pathToConfigFile, pathToOutFile, preserveAttrs
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_validate validates bags according to the specified config file.

Usage:

apt_validate --config=<config_file> \
             --attrs=<true|false> \
             --outfile=<path_to_output_file> \
             path_to_bag

The path_to_bag parameter is required. It should be the absolute path
to the directory containing the untarred bag, or to a tarred bag file.

The --config option is required and should be the path to a bag validation
config file that describes the validation rules. Examples can be found at
https://github.com/APTrust/exchange/blob/master/config/aptrust_bag_validation_config.json
or https://github.com/APTrust/exchange/blob/master/config/dpn_bag_validation_config.json,
but the config file must exist on the local drive.

The --attrs option is not required. It indicates whether you want to preserve
detailed information when parsing the bag. This uses more memory, and is generally
necessary only for APTrust ingest.

The --outfile option is not required. If specified, the validator will dump
JSON information about the bag and its contents to this file. That info may be
useful, especially when combined with --attrs=true, in cases where you're trying
to debug your bagging process.

Exit codes:

0 - Bag is valid
1 - Validation could not be completed, typically because of a problem
    finding or reading the config file, or finding or reading the bag.
2 - Validation completed and bag is invalid.

`
	fmt.Println(message)
}