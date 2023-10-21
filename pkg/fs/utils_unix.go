//go:build !windows
// +build !windows

package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

func MakeDirent(offset fuseops.DirOffset, den types.Dirent) fuseutil.Dirent {
	typ := fuseutil.DT_File
	if den.Inode.IsDir() {
		typ = fuseutil.DT_Directory
	}

	return fuseutil.Dirent{
		Offset: offset,
		Inode:  fuseops.InodeID(den.Inode.Ino),
		Name:   den.Name,
		Type:   typ,
	}
}

func Error2Native(err error) error {
	switch err {
	case types.EEXIST:
		return fuse.EEXIST
	case types.EINVAL:
		return fuse.EINVAL
	case types.EIO:
		return fuse.EIO
	case types.ENOATTR:
		return fuse.ENOATTR
	case types.ENOENT:
		return fuse.ENOENT
	case types.ENOSYS:
		return fuse.ENOSYS
	case types.ENOTDIR:
		return fuse.ENOTDIR
	case types.EISDIR:
		return syscall.EISDIR
	case types.ENOTEMPTY:
		return fuse.ENOTEMPTY
	case types.EPERM:
		return syscall.EPERM
	}
	return err
}

func FillAttr(attr *fuseops.InodeAttributes, inode types.Inode) {
	attr.Mtime = inode.Mtime
	attr.Ctime = inode.Mtime
	attr.Atime = inode.Mtime
	attr.Size = inode.Size
	if inode.IsDir() {
		attr.Nlink = 2
	} else {
		attr.Nlink = 1
	}
	attr.Uid = inode.Uid
	attr.Gid = inode.Gid
	attr.Mode = inode.Mode
}

func RedirectStderr(dir, prefix, suffix string) (err error) {
	if strings.HasSuffix(dir, "/") {
		dir = dir + "crashlog"
	} else {
		dir = dir + "/" + "crashlog"
	}
	logPath := RedirectPath(dir, prefix, suffix)
	Dir := filepath.Dir(logPath)
	err = os.MkdirAll(Dir, os.ModePerm)
	if err != nil {
		return
	}
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0644)
	if err != nil {
		return
	}

	devNull, err := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return
	}

	err = syscall.Dup2(int(devNull.Fd()), syscall.Stdout)
	if err != nil {
		return
	}

	err = syscall.Dup2(int(logFile.Fd()), syscall.Stderr)
	if err != nil {
		return
	}
	return
}

func RedirectPath(dir, prefix, suffix string) (directpath string) {
	t := time.Now()
	filename := fmt.Sprintf("%s%d%02d%02d-%02d%02d%02d%s", prefix, t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), suffix)
	return filepath.Join(dir, filename)
}
