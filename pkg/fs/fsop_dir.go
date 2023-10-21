package fs

import (
	"context"
	"runtime"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

func (fs *FSR) openDir(ctx context.Context, op *OpenDirOp) error {
	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	dirent := fs.meta.GetDentry(op.Inode)
	if dirent == nil {
		logg.Dlog.Errorf("opendir ino:%d ENOENT", op.Inode)
		return types.ENOENT
	}

	fs.withLock(func() {
		handleID := fs.nextFhID
		op.Handle = handleID
		fs.nextFhID++

		opendir := &OpenDir{
			ino: op.Inode,
			hid: handleID,
		}
		fs.openDirs[handleID] = opendir
	})

	logg.Dlog.Infof("openDir ino:%d handle:%d", op.Inode, op.Handle)
	return nil
}

func (fs *FSR) readDir(ctx context.Context, handle HandleID,
	offset uint64, cb func(uint64, types.Dirent) (full bool)) error {

	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	fs.mutex.Lock()
	openDir, ok := fs.openDirs[handle]
	fs.mutex.Unlock()

	if !ok {
		logg.Dlog.Errorf("readdir ENOENT")
		return types.ENOENT
	}

	// 在macos系统中，在根目录readdir结束后，fuse不会发送releaseDirHandle请求，
	// 因此根目录的openDir会长期存在，导致readdir读到老的内容，所以增加该判断
	if offset == 0 && runtime.GOOS == "darwin" {
		openDir.dirents = nil
	}

	if openDir.dirents == nil {
		dirents, err := fs.meta.LoadSubDentires(openDir.ino)
		if err != nil {
			logg.Dlog.Errorf("readdir ino:%d handle:%d err:%v", openDir.ino, openDir.hid, err)
			return types.EIO
		}
		openDir.dirents = dirents
	}

	logg.Dlog.Debugf("dir dirents count:%d", len(openDir.dirents))

	if cb != nil {
		for idx, dirent := range openDir.dirents[offset:] {
			if cb(offset+uint64(idx), dirent) {
				break
			}
		}
	}

	return nil
}

func (fs *FSR) releaseDirHandle(ctx context.Context, op *ReleaseDirHandleOp) error {
	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	fs.mutex.Lock()
	dir := fs.openDirs[op.Handle]
	fs.mutex.Unlock()
	if dir == nil {
		logg.Dlog.Errorf("releaseDirHandle handle:%d ENOENT", op.Handle)
		return nil
	}
	fs.withLock(func() {
		delete(fs.openDirs, op.Handle)
	})
	return nil
}

func (fs *FSR) mkDir(ctx context.Context, op *MkDirOp) error {
	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	logg.Dlog.Infof("mkDir pino:%d name:%s mode:%v", op.Parent, op.Name, op.Mode)
	dirent, err := fs.meta.CreateDentry(op.Parent, op.Name, op.Mode)
	if err != nil {
		logg.Dlog.Errorf("mkDir pino:%d name:%s err:%+v", op.Parent, op.Name, err)
		return types.EIO
	}

	now := fs.clock.Now()
	op.dirent = *dirent
	op.EntryExp = now.Add(config.GetGConfig().Entry_ttl)
	op.AttrExp = now.Add(config.GetGConfig().Attr_ttl)
	return nil
}

func (fs *FSR) rmDir(ctx context.Context, op *RmDirOp) error {
	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	if config.GetGConfig().DisableRemove {
		return types.EPERM
	}
	err := fs.meta.RemoveDentry(op.Parent, op.Name)
	if err != nil {
		if err == types.ENOENT {
			logg.Dlog.Warnf("rmDir pino:%d name:%s err:%+v", op.Parent, op.Name, err)
			return err
		}
		logg.Dlog.Errorf("rmDir pino:%d name:%s err:%+v", op.Parent, op.Name, err)
		return err
	}

	return nil
}
