package main

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
)

// Change this to point to an EFS directory that exists on your system.
const efsPath = "/Users/apd4n/tmp" // "/mnt/efs/tmp"

func main() {
	filePath1 := filepath.Join(efsPath, "file1.txt")
	filePath2 := filepath.Join(efsPath, "file2.txt")

	// Write ~200MB of data to a file on the EFS volume
	createFile(filePath1)

	// Stat the file and print the first few bytes to STDOUT
	statAndPrint(filePath1)

	// Now copy that file, and try to read the copy.
	copyFile(filePath2, filePath1)

	// Now see what we get out of that.
	statAndPrint(filePath2)

	// Cleanup
	os.Remove(filePath1)
	os.Remove(filePath2)
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomBytes(size int) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = letters[rand.Intn(len(letters))]
	}
	return buf
}

func createFile(pathToFile string) {
	// Get about 100k of data
	data := randomBytes(100000)
	file1, err := os.Create(pathToFile)
	if err != nil {
		die(err.Error())
	}
	defer file1.Close()
	for i := 0; i < 2000; i++ {
		n, err := file1.Write(data)
		if err != nil {
			die(err.Error())
		} else if n != len(data) {
			fmt.Fprintln(os.Stderr, "Create wrote only %d of %d bytes", n, len(data))
			os.Exit(1)
		}
	}
}

func copyFile(destPath, sourcePath string) {
	src, err := os.Open(sourcePath)
	if err != nil {
		die(err.Error())
	}
	defer src.Close()

	stat, err := src.Stat()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cant stat file: %s", err.Error())
		os.Exit(1)
	}
	srcSize := stat.Size()

	dest, err := os.Create(destPath)
	if err != nil {
		die(err.Error())
	}
	defer dest.Close()

	n, err := io.Copy(dest, src)
	if n != srcSize {
		fmt.Fprintln(os.Stderr, "Copy wrote only %d of %d bytes", n, srcSize)
		os.Exit(1)
	}

	// Rewind this file to the beginning.
	// dest.Seek(0, io.SeekStart)
	// return dest
}

func statAndPrint(filePath string) {
	f, err := os.Open(filePath)
	if err != nil {
		die(err.Error())
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cant stat file: %s", err.Error())
	} else {
		fmt.Println("File size of", filePath, stat.Size())
	}
	buf := make([]byte, 256)
	n, err := f.Read(buf)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cant read file: %s", err.Error())
	} else {
		fmt.Println("Bytes read from ", filePath, n)
		fmt.Println("Bytes:", string(buf))
	}
	fmt.Println("")
}

func die(errMsg string) {
	fmt.Fprintln(os.Stderr, errMsg)
	os.Exit(1)
}
