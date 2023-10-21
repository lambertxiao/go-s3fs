package fs

import (
	"context"
	"runtime"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/meta"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

func (fs *FSR) lookUpInode(ctx context.Context, op *LookUpInodeOp) error {
	if op.Parent == types.RootInodeID {
		if op.Name == types.StatsInoName {
			op.Inode = *statsInode.Inode
			return nil
		}

		if op.Name == types.GcInoName {
			op.Inode = *gcInode.Inode
			runtime.GC()
			return nil
		}

		if op.Name == types.ReloadInoName {
			op.Inode = *reloadInode.Inode
			fs.reloadCfgCallback(fs)
			return nil
		}
	}

	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	dirent, err := fs.meta.FindDentry(op.Parent, op.Name, false)
	if err != nil {
		if err == types.ENOENT {
			logg.Dlog.Infof("lookUpInode pino:%d name:%s err:%+v", op.Parent, op.Name, err)
			now := fs.clock.Now()
			op.EntryExp = now.Add(time.Second)
			op.AttrExp = now.Add(time.Second)
			return types.ENOENT
		}
		logg.Dlog.Errorf("lookUpInode pino:%d name:%s err:%+v", op.Parent, op.Name, err)
		return err
	}

	now := fs.clock.Now()
	op.Inode = dirent.Inode
	op.EntryExp = now.Add(config.GetGConfig().Entry_ttl)
	op.AttrExp = now.Add(config.GetGConfig().Attr_ttl)

	return nil
}

func (fs *FSR) getInodeAttributes(ctx context.Context, op *GetInodeAttributesOp) error {
	if isInodeSpecial(op.Id) {
		inode := fs.getSpecialInode(op.Id)
		if inode == nil {
			return types.ENOENT
		}
		op.inode = *inode
		return nil
	}

	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	inode, exist := fs.meta.GetInode(op.Id)

	if !exist {
		logg.Dlog.Errorf("getInodeAttributes ino:%d ENOENT", op.Id)
		return types.ENOENT
	}

	op.inode = inode
	op.AttrExp = fs.clock.Now().Add(config.GetGConfig().Attr_ttl)
	return nil
}

func (fs *FSR) setInodeAttributes(ctx context.Context, op *SetInodeAttributesOp) error {
	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	inode, err := fs.meta.UpdateInode(op.Inode, meta.InodeUpdateAttr{
		Size:  op.Size,
		Mode:  op.Mode,
		Atime: op.Atime,
		Mtime: op.Mtime,
		Uid:   op.Uid,
		Gid:   op.Gid,
	}, true)

	if err != nil {
		logg.Dlog.Errorf("setInodeAttributes ino:%d err:%+v", op.Inode, err)
		return types.EIO
	}

	op.inode = inode
	op.AttrExp = fs.clock.Now().Add(config.GetGConfig().Attr_ttl)
	return nil
}

func (fs *FSR) forgetInode(ctx context.Context, op *ForgetInodeOp) error {
	logg.Dlog.Warnf("forgetInode ino:%d N:%d drop", op.Inode, op.N)
	return nil
}
