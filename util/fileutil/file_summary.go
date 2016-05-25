package fileutil

import (
	"time"
)

// FileSummary includes the intersection of the set of
// file attributes available from os.FileInfo and tar.Header.
type FileSummary struct {
	Name       string
	Mode       int64
	Size       int64
	ModTime    time.Time
	IsDir      bool
	Uid        int
	Gid        int
	Uname      string
	Gname      string
}
