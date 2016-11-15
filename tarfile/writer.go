package tarfile

import (
	"archive/tar"
	"fmt"
	"github.com/APTrust/exchange/platform"
	"io"
	"os"
)

type Writer struct {
	PathToTarFile string
	tarWriter     *tar.Writer
}

func NewWriter(pathToTarFile string) *Writer {
	return &Writer{
		PathToTarFile: pathToTarFile,
	}
}

func (writer *Writer) Open() error {
	tarFile, err := os.Create(writer.PathToTarFile)
	if err != nil {
		return fmt.Errorf("Error creating tar file: %v", err)
	}
	writer.tarWriter = tar.NewWriter(tarFile)
	return nil
}

func (writer *Writer) Close() error {
	if writer.tarWriter != nil {
		return writer.tarWriter.Close()
	}
	return nil
}

// Adds a file to a tar archive.
func (writer *Writer) AddToArchive(filePath, pathWithinArchive string) error {
	if writer.tarWriter == nil {
		return fmt.Errorf("Underlying TarWriter is nil. Has it been opened?")
	}
	finfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("Cannot add '%s' to archive: %v", filePath, err)
	}
	header := &tar.Header{
		Name:    pathWithinArchive,
		Size:    finfo.Size(),
		Mode:    int64(finfo.Mode().Perm()),
		ModTime: finfo.ModTime(),
	}

	// This call adds the owner and group info to the tar file header.
	// When running on *nix systems that support this call, we use
	// the definition in nix.go. On Windows, which does not support
	// the call, we use the no-op definition in windows.go.
	platform.GetOwnerAndGroup(finfo, header)

	// Write the header entry
	if err := writer.tarWriter.WriteHeader(header); err != nil {
		// Most likely error is archive/tar: write after close
		return err
	}

	// Open the file whose data we're going to add.
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		return err
	}

	// Copy the contents of the file into the tarWriter.
	bytesWritten, err := io.Copy(writer.tarWriter, file)
	if bytesWritten != header.Size {
		return fmt.Errorf("addToArchive() copied only %d of %d bytes for file %s",
			bytesWritten, header.Size, filePath)
	}
	if err != nil {
		return fmt.Errorf("Error copying %s into tar archive: %v",
			filePath, err)
	}

	return nil
}
