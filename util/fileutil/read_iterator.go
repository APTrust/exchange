package fileutil

import (
	"io"
)

type ReadIterator interface {
	Next() (io.ReadCloser, *FileSummary, error)
	GetTopLevelDirNames() ([]string)
}
