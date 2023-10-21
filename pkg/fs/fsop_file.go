package fs

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/fs/freader"
	"github.com/lambertxiao/go-s3fs/pkg/fs/fwriter"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

func (fs *FSR) unlink(ctx context.Context, op *UnlinkOp) error {
	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	if config.GetGConfig().DisableRemove {
		return types.EPERM
	}

	err := fs.meta.RemoveDentry(op.Parent, op.Name)
	if err != nil {
		logg.Dlog.Errorf("unlink pino:%d name:%s err:%+v", op.Parent, op.Name, err)
		return err
	}

	return nil
}

func (fs *FSR) mkNode(ctx context.Context, op *MkNodeOp) error {
	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	logg.Dlog.Infof("mkNode pino:%d name:%s mode:%v", op.Parent, op.Name, op.Mode)

	dirent, err := fs.meta.CreateDentry(op.Parent, op.Name, op.Mode)
	if err != nil {
		logg.Dlog.Errorf("mkNode pino:%d name:%s err:%+v", op.Parent, op.Name, err)
		return types.EIO
	}

	op.dirent = *dirent
	op.EntryExp = fs.clock.Now().Add(config.GetGConfig().Entry_ttl)
	op.AttrExp = fs.clock.Now().Add(config.GetGConfig().Attr_ttl)
	return nil
}

func (fs *FSR) createFile(ctx context.Context, op *CreateFileOp) error {
	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	//首先获取父亲节点的dirent
	parent := fs.meta.GetDentry(op.Parent)
	if parent == nil {
		logg.Dlog.Errorf("getFile Parient pino:%d name:%s ", op.Parent, op.Name)
		return types.ENOENT
	}

	dirent, err := fs.meta.CreateDentry(op.Parent, op.Name, op.Mode)
	if err != nil {
		logg.Dlog.Errorf("createFile pino:%d name:%s err:%+v", op.Parent, op.Name, err)
		return types.EIO
	}

	ino := dirent.Inode.Ino
	path, err := fs.meta.GetDentryPath(dirent.Inode.Ino)
	if err != nil {
		return err
	}

	handleID := fs.fileOpen(ino, path, op.ForWrite)
	op.handle = handleID
	op.dirent = *dirent
	op.EntryExp = fs.clock.Now().Add(config.GetGConfig().Entry_ttl)
	op.AttrExp = fs.clock.Now().Add(config.GetGConfig().Attr_ttl)

	return nil
}

func (fs *FSR) fileOpen(ino types.InodeID, path string, forWrite bool) HandleID {
	dirent := fs.meta.GetDentry(ino)
	file := &types.File{Size: dirent.Inode.Size, Path: path}
	of := NewOpenFile(
		ino, dirent.Name, dirent.PIno, file, storage.GetStorageClass(dirent.Inode.StorageClass),
	)

	if config.GetGConfig().Readahead == 0 {
		of.reader = freader.NewDirectReader(file)
	} else {
		fs.mutex.Lock()

		// 不存在fcache或者原本的fcache大小与现在不同，则创建一个新的fcache
		if fs.fcacheMap[ino] == nil {
			fs.fcacheMap[ino] = freader.InitFileCache(
				file.Path,
				types.DEFAULT_PAGE_SIZE,
				config.GetGConfig().MaxCachePerFile,
			)
		}

		of.reader = freader.NewDefaultReader(
			file,
			config.GetGConfig().Readahead,
			fs.fcacheMap[ino],
			fs.readCacheHitSizeHistogram,
			&fs.objInflightReadCnt,
		)
		fs.mutex.Unlock()
		// of.reader = freader.NewBufferReader(file, config.G_FSConfig.Readahead)
	}

	if forWrite {
		of.writer = fwriter.NewDefaultWriter(path, &fs.objInflightWriteCnt)
	}

	fs.withLock(func() {
		handleID := fs.nextFhID
		fs.nextFhID++
		of.hid = handleID

		fs.openFiles[handleID] = of
		if fs.ino2OpenFiles[ino] == nil {
			fs.ino2OpenFiles[ino] = map[HandleID]*OpenFile{}
		}
		fs.ino2OpenFiles[ino][handleID] = of
	})

	logg.Dlog.Infof("openFile ino:%d handle:%d forwrite:%t fpath:%s fsize:%d", ino, of.hid, forWrite, file.Path, file.Size)
	return of.hid
}

func (fs *FSR) openFile(ctx context.Context, op *OpenFileOp) error {
	if isInodeSpecial(op.Inode) {
		fs.openSpecialInode(op.Inode)
		op.Handle = virualHandleId
		return nil
	}

	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	fullpath, err := fs.meta.GetDentryPath(op.Inode)
	if err != nil {
		logg.Dlog.Errorf("get dentry path error, ino:%d err:%v", op.Inode, err)
		return err
	}

	handleID := fs.fileOpen(op.Inode, fullpath, op.ForWrite)
	op.Handle = handleID
	return nil
}

func (fs *FSR) readFile(ctx context.Context, op *ReadFileOp) error {
	if isInodeSpecial(op.Inode) {
		fs.readSpecialNode(op)
		return nil
	}

	atomic.AddInt64(&fs.fsInflightReadCnt, 1)
	st := time.Now()
	defer func() {
		atomic.AddInt64(&fs.fsInflightReadCnt, -1)
		fs.observeOP(FS_OP_READ, st)
	}()

	if config.GetGConfig().ReadAfterWriteFinish {
		fs.mutex.Lock()
		ofs := fs.ino2OpenFiles[op.Inode]
		fs.mutex.Unlock()

		for _, of := range ofs {
			if of.hid == op.Handle {
				continue
			}

			logg.Dlog.Infof("readfile wait write done, ino:%d hid:%d", of.ino, of.hid)
			of.WaitWriteDone()
		}
	}

	fs.mutex.RLock()
	of := fs.openFiles[op.Handle]
	fs.mutex.RUnlock()

	if of == nil {
		logg.Dlog.Errorf("no such file %v", op.Inode)
		return types.ENOENT
	}

	logg.Dlog.Debugf("readFile ino:%d handle:%d offset:%d len:%d", op.Inode, op.Handle, op.Offset, len(op.Dst))

	inode, _ := fs.meta.GetInode(of.ino)
	if inode.Size == 0 {
		err := fs.meta.RefreshInode(of.ino)
		if err != nil {
			logg.Dlog.Infof("ino:%d refresh error, err:%+v", of.ino, err)
			return err
		}

		inode, _ := fs.meta.GetInode(of.ino)
		if inode.Size != 0 {
			logg.Dlog.Infof("ino:%d update size from 0 to %d", of.ino, inode.Size)
			of.file = &types.File{Size: inode.Size, Path: of.file.Path}
		}
	}

	n, err := of.read(op.Offset, len(op.Dst), op.Dst)
	if err != nil {
		return err
	}

	op.BytesRead = n
	fs.readSizeHistogram.Observe(float64(n))
	return nil
}

func (fs *FSR) writeFile(ctx context.Context, op *WriteFileOp) error {
	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_WRITE, st)
	}()

	fs.mutex.RLock()
	of := fs.openFiles[op.Handle]
	fs.mutex.RUnlock()
	if of == nil {
		logg.Dlog.Errorf("write not find fd, fd=%v,inode=%v", op.Handle, op.Inode)
		return types.ENOENT
	}

	_, err := of.write(op.Offset, op.Data)
	if err != nil {
		logg.Dlog.Errorf("ino:%d handle write error %s, offset: %d, length: %d",
			op.Inode, err.Error(), op.Offset, len(op.Data))
		return err
	}

	if of.hasWrote {
		fsize := of.size()
		_, err := fs.meta.UpdateInodeSize(of.ino, fsize, false)
		if err != nil {
			logg.Dlog.Errorf("update inode error ino:%d fpath:%s err:%+v", op.Inode, of.file.Path, err)
			return err
		}
	}

	fs.writtenSizeHistogram.Observe(float64(len(op.Data)))
	logg.Dlog.Debugf("writeFile done ino:%d handle:%d fsize:%d", op.Inode, op.Handle, of.size())
	return nil
}

func (fs *FSR) syncFile(ctx context.Context) error {
	logg.Dlog.Warnf("fsync not support")
	return nil
}

func (fs *FSR) flushFile(ctx context.Context, op *FlushFileOp) error {
	if isInodeSpecial(op.Inode) {
		return nil
	}

	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	fs.mutex.RLock()
	of := fs.openFiles[op.Handle]
	fs.mutex.RUnlock()
	if of == nil {
		logg.Dlog.Errorf("flush No such inode %v", op.Inode)
		return types.ENOENT
	}

	if of.writer != nil {
		err := of.writer.Flush()
		if err != nil {
			logg.Dlog.Errorf("flush inode:%d handle:%d name:%s, %s", op.Inode, op.Handle, of.file.Path, err)
			return err
		}

		if of.hasWrote {
			fsize := of.size()
			logg.Dlog.Infof("ino:%d path:%s update size:%d", of.ino, of.file.Path, fsize)
			_, err := fs.meta.UpdateInodeSize(of.ino, fsize, true)
			if err != nil {
				logg.Dlog.Errorf("update inode error ino:%d name:%s err:%+v", op.Inode, of.file.Path, err)
				return err
			}
		}
	}

	return nil
}

func (fs *FSR) releaseFileHandle(ctx context.Context, op *ReleaseFileHandleOp) error {
	fs.mutex.Lock()
	of := fs.openFiles[op.Handle]
	fs.mutex.Unlock()

	if of == nil {
		if op.Handle == virualHandleId {
			return nil
		}
		logg.Dlog.Errorf("release file handle No such handle %v", op.Handle)
		return types.ENOENT
	}

	st := time.Now()
	defer func() {
		fs.observeOP(FS_OP_OTHER, st)
	}()

	defer func() {
		fs.mutex.Lock()
		delete(fs.openFiles, op.Handle)
		delete(fs.ino2OpenFiles[of.ino], op.Handle)
		fs.mutex.Unlock()
	}()

	err := of.Release()
	if err != nil {
		return err
	}

	fs.mutex.Lock()
	openCnt := len(fs.ino2OpenFiles[of.ino])
	// 只有当前打开的fd
	if openCnt == 1 {
		if fcache, ok := fs.fcacheMap[of.ino]; ok && fcache != nil {
			fcache.Release()
			fs.fcacheMap[of.ino] = nil
		}
	}
	fs.mutex.Unlock()

	// 查询节点在目录树上是否还存在
	dentry, _ := fs.meta.FindDentry(of.pino, of.name, true)
	if dentry == nil {
		logg.Dlog.Warnf("cannot found dentry in parent, ino:%d", of.ino)

		target := fs.meta.GetDentry(of.ino)
		refCnt := 0
		// 由于现在go-s3fs的dentry和inode是一一对应，只能通过比较target的name和openfile里的name，来决定这个dentry是否还是原来open出去的dentry
		// 如果是原来的dentry，且这个dentry已经在目录树上删除了，则这个dentry已经不指向inode了，不增加inode的引用计数
		if target != nil && target.Name != of.name {
			refCnt++
		}

		logg.Dlog.Infof("check inode status, ino:%d openCnt:%d refCnt:%d", of.ino, openCnt, refCnt)

		// 打开数为0，且没有任何dentry指向该inode，则删除该inode
		if openCnt-1 == 0 && refCnt == 0 {
			logg.Dlog.Infof("remove inode, ino:%d", of.ino)
			err := fs.meta.RemoveInode(of.ino)
			if err != nil {
				logg.Dlog.Error(err)
			}
			// todo 这里按照文件系统语义应该写入的文件删除，经讨论，暂时决定不删
		}
	}

	logg.Dlog.Debugf("ReleaseFileHandle done handle:%d", op.Handle)
	return nil
}
