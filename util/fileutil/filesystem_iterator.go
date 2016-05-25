package fileutil

import (
	"fmt"
	"io"
	"os"
	"path"
	"syscall"
)

type FileSystemIterator struct {
	rootPath   string
	files      []string
	index      int
}

func NewFileSystemIterator(pathToDir string) (*FileSystemIterator, error) {
	if !path.IsAbs(pathToDir) {
		return nil, fmt.Errorf("Path '%s' must be absolute.", pathToDir)
	}
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
		rootPath: pathToDir,
		files: files,
		index: -1,
	}, nil
}

// Returns an open reader for the next file, along with a FileSummary.
// Returns io.EOF when it reaches the last file.
// The caller is responsible for closing the reader.
func (iter *FileSystemIterator) Next() (io.Reader, *FileSummary, error) {
	iter.index += 1
	filepath := iter.files[iter.index]
	var stat os.FileInfo
	var err error
	if stat, err = os.Stat(iter.rootPath); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("File '%s' does not exist.", iter.rootPath)
	}
	fs := &FileSummary{
		Name: stat.Name(),
		AbsPath: filepath,
		Mode: stat.Mode(),
		Size: stat.Size(),
		ModTime: stat.ModTime(),
		IsDir: stat.IsDir(),
	}
	systat := stat.Sys().(*syscall.Stat_t)
	if systat != nil {
		fs.Uid = int(systat.Uid)
		fs.Gid = int(systat.Gid)
	}
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fs, fmt.Errorf("Cannot read file '%s': %v", err)
	}
	return file, fs, nil
}
