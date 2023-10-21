package meta

import (
	"os"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

type Meta interface {
	FindDentry(pino types.InodeID, name string, onlyLocal bool) (*types.Dirent, error)
	GetInode(ino types.InodeID) (types.Inode, bool)
	UpdateInodeSize(ino types.InodeID, size uint64, sync bool) (types.Inode, error)
	UpdateInode(ino types.InodeID, updateFields InodeUpdateAttr, sync bool) (types.Inode, error)
	CreateDentry(pino types.InodeID, name string, mode os.FileMode) (*types.Dirent, error)
	Rename(oldPino types.InodeID, oldName string, newPino types.InodeID, newName string) error
	RemoveDentry(pino types.InodeID, name string) error
	RemoveInode(inode types.InodeID) error
	LoadSubDentires(ino types.InodeID) ([]types.Dirent, error)
	GetDentry(ino types.InodeID) *types.Dirent
	GetDentryPath(ino types.InodeID) (string, error)
	RefreshInode(ino types.InodeID) error
	Destory()
	ReloadCfg(opt MetaOption)
}

// localMeta
type LocalMeta interface {
	FindDentry(pino types.InodeID, name string) (*types.Dentry, error)
	GetInode(ino types.InodeID) *types.Inode
	UpdateInode(ino types.InodeID, updateFields InodeUpdateAttr, sync bool) (*types.Inode, error)
	CreateDentry(pino types.InodeID, name string, inode *types.Inode) (*types.Dentry, error)
	CreateDentryFromObj(pino types.InodeID, name string, obj storage.ObjectInfo, isDir bool) (*types.Dentry, error)
	Rename(srcPino types.InodeID, srcName string, dstPino types.InodeID, dstName string) error
	RemoveDentry(pino types.InodeID, name string) error
	RemoveInode(ino types.InodeID) error
	GetDentry(ino types.InodeID) *types.Dentry
	BuildDentries(pino types.InodeID, childs map[string]*storage.FTreeNode)
	GetNextInodeID() types.InodeID
	CreateNewInode(mode os.FileMode, gid uint32, uid uint32) *types.Inode
	Destroy()
}

type InodeUpdateAttr struct {
	Size                *uint64
	Mode                *os.FileMode
	Ctime, Atime, Mtime *time.Time
	Uid, Gid            *uint32
}

type MetaOption struct {
	GID                    uint32
	UID                    uint32
	DCacheTTL              time.Duration
	SkipNotDirLookup       bool
	DisableCheckVirtualDir bool
	Bucket                 string
	BucketPrefix           string
	AllowOther             bool
	MpMask                 uint32
}
