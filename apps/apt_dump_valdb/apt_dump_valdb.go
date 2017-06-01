package main

import (
	"fmt"
	"github.com/APTrust/exchange/util/storage"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
	}
	dbFile := os.Args[1]
	if dbFile == "" {
		printUsage()
	} else {
		db, err := storage.NewBoltDB(dbFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		db.DumpJson(os.Stdout)
	}
}

func printUsage() {
	msg := `
apt_dump_boltdb dumps the contents of a valdb database to STDOUT.
The contents will be an IntellectualObject, complete with GenericFiles,
Checksums and PremisEvents, if any exist, in JSON format.

Usage: apt_dump_valdb <path/to/file.valdb>
`
	fmt.Println(msg)
	os.Exit(0)
}
