package main

import (
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/exchange/util/fileutil"
	"io"
	"os"
	"path"
)

func main() {

	// Get the path to the tar file.
	filename := getFileName()

	// Extract the tagmanifest from the tar file.
	tfi, err := fileutil.NewTarFileIterator(filename)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	tagManifestPath := getTagManifestPath(filename)
	readCloser, err := tfi.Find(tagManifestPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		tfi.Close()
		os.Exit(1)
	}

	// Calculate the digest
	sha256Hash := sha256.New()
	_, err = io.Copy(sha256Hash, readCloser)
	if err != nil {
		readCloser.Close()
		tfi.Close()
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	digest := fmt.Sprintf("%x", sha256Hash.Sum(nil))

	// Clean up
	readCloser.Close()
	tfi.Close()

	// Print output
	fmt.Printf("%s\t%s\n", getBagName(filename), digest)
	os.Exit(0)
}

func getFileName() string {
	if len(os.Args) < 2 {
		printUsage()
	}
	return os.Args[1]
}

// Returns the path the tag manifest within the archive.
// The DPN tar file is named <uuid>.tar. The bag name is
// the uuid. The bag untars to a directory called <uuid>,
// and the tag manifest is inside of that.
func getTagManifestPath(filename string) string {
	bagname := getBagName(filename)
	return fmt.Sprintf("%s/tagmanifest-sha256.txt", bagname)
}

func getBagName(filename string) string {
	basename := path.Base(filename)
	ext := path.Ext(filename)
	end := len(basename) - len(ext)
	return basename[0:end]
}

func printUsage() {
	msg := `
dpn_tm_digest prints the sha256 digest of the tag manifest of a tarred DPN bag.

Usage: dpn_tm_digest /path/to/file.tar

Prints name of tar file, followed by a tab, followed by the sha256 checksum
of the bag's tagmanifest-sha256.txt file.
`
	fmt.Println(msg)
	os.Exit(0)
}
