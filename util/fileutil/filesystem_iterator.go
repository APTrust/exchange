package fileutil

import (
	"fmt"
	"io"
	"os"
)

type FileSystemIterator struct {
	files      []string
	index      int
}

func NewFileSystemIterator(pathToDir string) (*FileSystemIterator, error) {
	var stat os.FileInfo
	var err error
	if stat, err = os.Stat(pathToDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("Directory '%s' does not exist.", pathToDir)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("Path '%s' is not a directory.", pathToDir)
	}
	files, err := RecursiveFileList(pathToDir)
	if err != nil {
		return nil, err
	}
	return &FileSystemIterator{
		files: files,
		index: -1,
	}, nil
}

// Returns an open reader for the next file, along with a FileSummary.
// Returns io.EOF when it reaches the last file.
// The caller is responsible for closing the reader.
func (iter *FileSystemIterator) Next() (io.Reader, *FileSummary, error) {
	iter.index += 1
	//filepath := iter.files[iter.index]

	return nil, nil, nil
}
