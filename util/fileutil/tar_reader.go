package fileutil

import (
	"archive/tar"
	"io"
	"os"
)

type TarFileIterator struct {
	tarReader  *tar.Reader
	file       *os.File
}

func NewTarReader(pathToTarFile string) (*TarFileIterator, error) {
	file, err := os.Open(pathToTarFile)
	if err != nil {
		return nil, err
	}
	return &TarFileIterator{
		tarReader: tar.NewReader(file),
		file: file,
	}, nil
}

// Returns an open reader for the next file, along with a FileSummary.
// Returns io.EOF when it reaches the last file.
func (iter *TarFileIterator) Next() (io.ReadCloser, *FileSummary, error) {
	return nil, nil, nil
}

func (iter *TarFileIterator) Close() {
	if iter.file != nil {
		iter.file.Close()
	}
}

func (iter *TarFileIterator) Read(p []byte) (int, error) {
	return iter.tarReader.Read(p)
}
