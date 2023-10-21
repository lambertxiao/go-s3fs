//go:build !windows
// +build !windows

package fs

import (
	"context"

	_ "github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

type FSRX struct {
	fuseutil.NotImplementedFileSystem
	*FSR
}

func (fs *FSRX) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {
	logg.Dlog.Debugf("StatFS")
	var top StatFSOp

	err = fs.statFS(ctx, &top)
	if err != nil {
		return Error2Native(err)
	}

	op.BlockSize = top.BlockSize
	op.Blocks = top.Blocks
	op.BlocksFree = top.BlocksFree
	op.BlocksAvailable = top.BlocksAvailable
	op.IoSize = top.IoSize
	op.Inodes = top.Inodes
	op.InodesFree = top.InodesFree
	return
}

func (fs *FSRX) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {
	logg.Dlog.Debugf("LookUpInode pino:%d name:%s", op.Parent, op.Name)
	var top LookUpInodeOp
	top.Parent = types.InodeID(op.Parent)
	top.Name = op.Name
	err = fs.lookUpInode(ctx, &top)
	if err != nil {
		return Error2Native(err)
	}

	op.Entry.Child = fuseops.InodeID(top.Inode.Ino)
	op.Entry.EntryExpiration = top.EntryExp
	op.Entry.AttributesExpiration = top.AttrExp

	FillAttr(&op.Entry.Attributes, top.Inode)
	return nil
}

func (fs *FSRX) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	if !isInodeSpecial(types.InodeID(op.Inode)) {
		logg.Dlog.Debugf("GetInodeAttributes ino:%d", op.Inode)
	}
	var top GetInodeAttributesOp
	top.Id = types.InodeID(op.Inode)
	err := fs.getInodeAttributes(ctx, &top)
	if err != nil {
		return Error2Native(err)
	}
	FillAttr(&op.Attributes, top.inode)
	op.AttributesExpiration = top.AttrExp
	return nil
}

func (fs *FSRX) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) error {
	logg.Dlog.Debugf("SetInodeAttributes ino:%d mode:%+v uid:%v gid:%v", op.Inode, op.Mode, op.Uid, op.Gid)

	var top SetInodeAttributesOp
	top.Inode = types.InodeID(op.Inode)
	top.Handle = (*HandleID)(op.Handle)
	top.Size = op.Size
	top.Mode = op.Mode
	top.Atime = op.Atime
	top.Mtime = op.Mtime
	top.Uid = op.Uid
	top.Gid = op.Gid
	err := fs.setInodeAttributes(ctx, &top)
	if err != nil {
		return Error2Native(err)
	}
	FillAttr(&op.Attributes, top.inode)
	op.AttributesExpiration = top.AttrExp
	return nil
}

func (fs *FSRX) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) error {
	logg.Dlog.Debugf("ForgetInode ino:%d", op.Inode)
	var top ForgetInodeOp
	top.Inode = types.InodeID(op.Inode)
	top.N = op.N
	err := fs.forgetInode(ctx, &top)
	if err != nil {
		return Error2Native(err)
	}
	return nil
}

func (fs *FSRX) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) error {
	logg.Dlog.Infof("MkDir pino:%d name:%s mode:%v", op.Parent, op.Name, op.Mode)
	var top MkDirOp
	top.Parent = types.InodeID(op.Parent)
	top.Name = op.Name
	top.Mode = op.Mode
	err := fs.mkDir(ctx, &top)
	if err != nil {
		return Error2Native(err)
	}

	FillAttr(&op.Entry.Attributes, top.dirent.Inode)
	op.Entry.AttributesExpiration = top.AttrExp
	op.Entry.EntryExpiration = top.EntryExp
	op.Entry.Child = fuseops.InodeID(top.dirent.Inode.Ino)
	return nil
}

func (fs *FSRX) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) error {
	logg.Dlog.Infof("MkNode pino:%d name:%s mode:%v", op.Parent, op.Name, op.Mode)
	var top MkNodeOp
	top.Parent = types.InodeID(op.Parent)
	top.Name = op.Name
	top.Mode = op.Mode
	return fs.mkNode(ctx, &top)
}

func (fs *FSRX) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) error {
	logg.Dlog.Infof("CreateFile pino:%d name:%s mode:%v", op.Parent, op.Name, op.Mode)
	var top CreateFileOp
	top.Parent = types.InodeID(op.Parent)
	top.Name = op.Name
	top.Mode = op.Mode
	top.ForWrite = true
	err := fs.createFile(ctx, &top)
	if err != nil {
		return Error2Native(err)
	}

	FillAttr(&op.Entry.Attributes, top.dirent.Inode)
	op.Handle = fuseops.HandleID(top.handle)
	op.Entry.Child = fuseops.InodeID(top.dirent.Inode.Ino)
	op.Entry.EntryExpiration = top.EntryExp
	op.Entry.AttributesExpiration = top.AttrExp
	return nil
}

func (fs *FSRX) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) error {
	logg.Dlog.Debugf("CreateSymlink pino:%d name:%s target:%s", op.Parent, op.Name, op.Target)
	return fs.createSymlink(ctx)
}

func (fs *FSRX) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) error {
	return fs.createLink(ctx)
}

func (fs *FSRX) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) error {
	var top RenameOp
	top.OldParent = types.InodeID(op.OldParent)
	top.OldName = op.OldName
	top.NewParent = types.InodeID(op.NewParent)
	top.NewName = op.NewName
	logg.Dlog.Infof("Rename srcPino:%d srcName:%s dstPino:%d dstName:%s",
		op.OldParent, op.OldName, op.NewParent, op.NewName)
	return Error2Native(fs.rename(ctx, &top))
}

func (fs *FSRX) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) error {
	logg.Dlog.Infof("RmDir pino:%d name:%s", op.Parent, op.Name)
	var top RmDirOp
	top.Parent = types.InodeID(op.Parent)
	top.Name = op.Name
	top.RmCache = true
	return Error2Native(fs.rmDir(ctx, &top))
}

func (fs *FSRX) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) error {
	logg.Dlog.Infof("Unlink pino:%d name:%s", op.Parent, op.Name)
	var top UnlinkOp
	top.Parent = types.InodeID(op.Parent)
	top.Name = op.Name
	top.UnlinkCache = true
	return Error2Native(fs.unlink(ctx, &top))
}

func (fs *FSRX) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	logg.Dlog.Infof("OpenDir ino:%d", op.Inode)
	var top OpenDirOp
	top.Inode = types.InodeID(op.Inode)
	if err := fs.openDir(ctx, &top); err != nil {
		return Error2Native(err)
	}
	op.Handle = fuseops.HandleID(top.Handle)
	return nil
}

func (fs *FSRX) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	logg.Dlog.Infof("ReadDir ino:%d handle:%d offset:%d", op.Inode, op.Handle, op.Offset)
	limit := types.ReadDirLimitCount
	cb := func(offset uint64, d types.Dirent) bool {
		limit--
		if limit < 0 {
			// linux fuse kernel 通信消息长度限制不能太大, 一次填充了100条，就告诉调用者满了
			return true
		}

		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], MakeDirent(fuseops.DirOffset(offset)+1, d))
		if n == 0 {
			op.Dst = op.Dst[:op.BytesRead]
			return true
		}
		op.BytesRead += n
		return false
	}

	return fs.readDir(ctx, HandleID(op.Handle), uint64(op.Offset), cb)
}

func (fs *FSRX) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) error {
	logg.Dlog.Infof("ReleaseDirHandle handle:%d", op.Handle)
	var top ReleaseDirHandleOp
	top.Handle = HandleID(op.Handle)
	return Error2Native(fs.releaseDirHandle(ctx, &top))
}

func (fs *FSRX) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	if !isInodeSpecial(types.InodeID(op.Inode)) {
		logg.Dlog.Infof("OpenFile ino:%d", op.Inode)
	}

	var top OpenFileOp
	top.Inode = types.InodeID(op.Inode)
	top.ForWrite = op.OpenFlags.IsWriteOnly()
	err := fs.openFile(ctx, &top)
	if err != nil {
		return Error2Native(err)
	}
	op.Handle = fuseops.HandleID(top.Handle)
	op.KeepPageCache = false
	return nil
}

func (fs *FSRX) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	var top ReadFileOp
	top.Inode = types.InodeID(op.Inode)
	top.Handle = HandleID(op.Handle)
	top.Dst = op.Dst
	top.Offset = op.Offset
	err := fs.readFile(ctx, &top)
	if err != nil {
		return Error2Native(err)
	}
	op.BytesRead = top.BytesRead
	return nil
}

func (fs *FSRX) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	logg.Dlog.Debugf("writeFile ino:%d handle:%d offset:%d len:%d", op.Inode, op.Handle, op.Offset, len(op.Data))

	var top WriteFileOp
	top.Inode = types.InodeID(op.Inode)
	top.Handle = HandleID(op.Handle)
	top.Offset = op.Offset
	top.Data = op.Data
	err := fs.writeFile(ctx, &top)
	if err != nil {
		return Error2Native(err)
	}
	return nil
}

func (fs *FSRX) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) error {
	return Error2Native(fs.syncFile(ctx))
}

func (fs *FSRX) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) error {
	if !isInodeSpecial(types.InodeID(op.Inode)) {
		logg.Dlog.Infof("FlushFile ino:%d handle:%d", op.Inode, op.Handle)
	}
	var top FlushFileOp
	top.Handle = HandleID(op.Handle)
	top.Inode = types.InodeID(op.Inode)
	return Error2Native(fs.flushFile(ctx, &top))
}

func (fs *FSRX) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) error {
	if op.Handle != virualHandleId {
		logg.Dlog.Infof("ReleaseFileHandle handle:%d", op.Handle)
	}
	var top ReleaseFileHandleOp
	top.Handle = HandleID(op.Handle)
	return Error2Native(fs.releaseFileHandle(ctx, &top))
}

func (fs *FSRX) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) error {
	return Error2Native(fs.readSymlink(ctx))
}

func (fs *FSRX) RemoveXattr(
	ctx context.Context,
	op *fuseops.RemoveXattrOp) error {
	return Error2Native(fs.removeXattr(ctx))
}

func (fs *FSRX) GetXattr(
	ctx context.Context,
	op *fuseops.GetXattrOp) error {
	return Error2Native(fs.getXattr(ctx))
}

func (fs *FSRX) ListXattr(
	ctx context.Context,
	op *fuseops.ListXattrOp) error {
	return Error2Native(fs.listXattr(ctx))
}

func (fs *FSRX) SetXattr(
	ctx context.Context,
	op *fuseops.SetXattrOp) error {
	return Error2Native(fs.setXattr(ctx))
}

func (fs *FSRX) Fallocate(
	ctx context.Context,
	op *fuseops.FallocateOp) error {
	return Error2Native(fs.fallocate(ctx))
}

func (fs *FSRX) Destroy() {
	fs.destroy()
}
