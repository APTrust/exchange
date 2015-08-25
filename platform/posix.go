// +build !partners

package platform

import (
	"archive/tar"
	"os"
	"syscall"
)
// We have a dummy version of this call in posix.go.
// Windows does not implement the syscall.Stat_t type we
// need, but the *nixes do. We use this in util.AddToArchive
// to set owner/group on files being added to a tar archive.
func GetOwnerAndGroup(finfo os.FileInfo, header *tar.Header) {
	systat := finfo.Sys().(*syscall.Stat_t)
	if systat != nil {
		header.Uid = int(systat.Uid)
		header.Gid = int(systat.Gid)
	}
}
