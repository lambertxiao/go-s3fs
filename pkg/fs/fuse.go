package fs

import (
	"os"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/types"
)

// cross-platform fuse types
type (
	DirentType uint32
	DirOffset  uint64
	HandleID   uint64
)

type StatFSOp struct {
	BlockSize       uint32
	Blocks          uint64
	BlocksFree      uint64
	BlocksAvailable uint64
	IoSize          uint32
	Inodes          uint64
	InodesFree      uint64
}

type LookUpInodeOp struct {
	Parent   types.InodeID
	Name     string
	Inode    types.Inode
	EntryExp time.Time
	AttrExp  time.Time
}

type GetInodeAttributesOp struct {
	Id      types.InodeID
	inode   types.Inode
	AttrExp time.Time
}

type SetInodeAttributesOp struct {
	Inode types.InodeID

	Handle *HandleID

	Size  *uint64
	Mode  *os.FileMode
	Atime *time.Time
	Mtime *time.Time

	Uid *uint32
	Gid *uint32

	inode   types.Inode
	AttrExp time.Time
}

type ForgetInodeOp struct {
	Inode types.InodeID
	N     uint64
}

type MkDirOp struct {
	Parent types.InodeID

	Name string
	Mode os.FileMode

	dirent   types.Dirent
	EntryExp time.Time
	AttrExp  time.Time
}

type MkNodeOp struct {
	Parent types.InodeID
	Name   string
	Mode   os.FileMode

	dirent   types.Dirent
	EntryExp time.Time
	AttrExp  time.Time
}

type CreateFileOp struct {
	Parent   types.InodeID
	Name     string
	Mode     os.FileMode
	dirent   types.Dirent
	handle   HandleID
	EntryExp time.Time
	AttrExp  time.Time
	ForWrite bool
}

type RenameOp struct {
	OldParent types.InodeID
	OldName   string
	NewParent types.InodeID
	NewName   string
}

type RmDirOp struct {
	Parent  types.InodeID
	Name    string
	RmCache bool
}

type UnlinkOp struct {
	Parent types.InodeID
	Name   string
	// for windows unlink for cache, because Fuse releases the reference count of the inode through forget to clean up
	// Windows is released directly through unlink
	UnlinkCache bool
}

type OpenDirOp struct {
	Inode  types.InodeID
	Handle HandleID
}

type ReleaseDirHandleOp struct {
	Handle HandleID
}

type OpenFileOp struct {
	Inode  types.InodeID
	Handle HandleID
	// FUSE on Linux/Unix
	KeepPageCache bool

	// for windows
	ForWrite bool
}

type ReadFileOp struct {
	Inode     types.InodeID
	Handle    HandleID
	Offset    int64
	Dst       []byte
	BytesRead int
}

type WriteFileOp struct {
	Inode  types.InodeID
	Handle HandleID
	Offset int64
	Data   []byte
}

type FlushFileOp struct {
	Handle HandleID
	Inode  types.InodeID
}

type ReleaseFileHandleOp struct {
	Handle HandleID
}
