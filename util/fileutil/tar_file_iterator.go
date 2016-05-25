package fileutil

import (
	"archive/tar"
	"io"
	"os"
)

// TarFileIterator lets us read tarred bags (or any other tarred files)
// without having to untar them.
type TarFileIterator struct {
	tarReader  *tar.Reader
	file       *os.File
}

func NewTarFileIterator(pathToTarFile string) (*TarFileIterator, error) {
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
	header, err := iter.tarReader.Next()
	if err != nil {
		// Error may be io.EOF, which just means we
		// reached the end of the headers.
		return nil, nil, err
	}
	finfo := header.FileInfo()
	fs := &FileSummary{
		Name: header.Name,
		AbsPath: "",
		Mode: finfo.Mode(),
		Size: header.Size,
		ModTime: header.ModTime,
		IsDir: header.Typeflag == tar.TypeDir,
		IsRegularFile: header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA,
		Uid: header.Uid,
		Gid: header.Gid,
	}

	// Wrap our tar reader in a TarReadCloser. When the caller
	// calls Read() on this object, it will read to the end
	// of whatever file the current header describes.
	tarReadCloser := TarReadCloser{
		tarReader: iter.tarReader,
	}
	return tarReadCloser, fs, nil
}

// Close the underlying tar file.
func (iter *TarFileIterator) Close() {
	if iter.file != nil {
		iter.file.Close()
	}
}

type TarReadCloser struct {
	tarReader  *tar.Reader
}

func (tarReadCloser TarReadCloser) Read(p []byte) (int, error) {
	return tarReadCloser.tarReader.Read(p)
}

func (tarReadCloser TarReadCloser) Close() (error) {
	return nil // noop
}
