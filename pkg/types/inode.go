package types

import (
	"os"
	"time"
)

type InodeID uint64

type Inode struct {
	Ino          InodeID
	Size         uint64
	Mtime        time.Time
	Ctime        time.Time
	Mode         os.FileMode
	Uid          uint32
	Gid          uint32
	StorageClass string
}

const (
	InvalidInodeID InodeID = 0
	RootInodeID    InodeID = 1
)

func (inode *Inode) IsDir() bool {
	return inode.Mode.IsDir()
}

// func (inode *Inode) IsFile() bool {
// 	return inode.Mode.IsRegular()
// }
