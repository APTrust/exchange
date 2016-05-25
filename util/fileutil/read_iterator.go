package fileutil

import (
	"io"
)

type ReadIterator interface {
	Next() (io.Reader, *FileSummary, error)
}
