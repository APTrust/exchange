package fileutil

import (
	"archive/tar"
	"io"
	"os"
	"strings"
)

// TarFileIterator lets us read tarred bags (or any other tarred files)
// without having to untar them.
type TarFileIterator struct {
	tarReader        *tar.Reader
	file             *os.File
	topLevelDirNames []string
}

func NewTarFileIterator(pathToTarFile string) (*TarFileIterator, error) {
	file, err := os.Open(pathToTarFile)
	if err != nil {
		return nil, err
	}
	return &TarFileIterator{
		tarReader: tar.NewReader(file),
		file: file,
		topLevelDirNames: make([]string, 0),
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
	iter.setTopLevelDirName(header.Name)
	finfo := header.FileInfo()
	// Path to file, minus the top-level directory name,
	// which is the name of the bag.
	relPathInArchive := (strings.Join(strings.Split(header.Name, "/")[1:], "/"))
	fs := &FileSummary{
		RelPath: relPathInArchive,
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

// Keep track of any top-level directory names we encounter.
// The BagIt spec says a tar file SHOULD untar to a directory with the
// same name as the tar file, minus the .tar extension. The APTrust
// spect says it MUST untar to a directory with that name. When we're
// validating bags, we'll want to know the name of the top-level
// directory, keeping in mind that the tar file may deserialize to
// multiple top-level directories.
func (iter *TarFileIterator) setTopLevelDirName(headerName string) {
	topLevelDir := strings.Split(headerName, "/")[0]
	for i := range iter.topLevelDirNames {
		if iter.topLevelDirNames[i] == topLevelDir {
			return
		}
	}
	iter.topLevelDirNames = append(iter.topLevelDirNames, topLevelDir)
}

// Returns the names of the top level directories to which the tar
// file expands. For APTrust purposes, the tar file should expand to
// one directory whose name matches that of the tar file, minus the
// .tar extension. In reality, tar files can expand to multiple
// top-level directories with any names.
//
// Note that you should read the entire tar file before calling
// this; otherwise, you may not get all the top-level dir names.
func (iter *TarFileIterator) GetTopLevelDirNames() ([]string) {
	return iter.topLevelDirNames
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
