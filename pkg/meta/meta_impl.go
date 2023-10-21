package meta

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

var _ Meta = (*DMeta)(nil)

type MetaMode string

const (
	MetaModeInternal MetaMode = "internal"
	MetaModeExternal MetaMode = "external"
)

// default meta impl
type DMeta struct {
	local   LocalMeta
	storage storage.Storage
	opt     MetaOption
}

func NewMeta(opt MetaOption, local LocalMeta, storage storage.Storage) (Meta, error) {
	meta := &DMeta{
		storage: storage,
		opt:     opt,
	}

	meta.local = local
	return meta, nil
}

func (m *DMeta) ReloadCfg(opt MetaOption) {
	m.opt.GID = opt.GID
	m.opt.UID = opt.UID
	m.opt.DCacheTTL = opt.DCacheTTL
	m.opt.SkipNotDirLookup = opt.SkipNotDirLookup
	m.opt.DisableCheckVirtualDir = opt.DisableCheckVirtualDir
}

func (m *DMeta) FindDentry(pino types.InodeID, name string, onlyLocal bool) (*types.Dirent, error) {
	d, err := m.findDentry(pino, name, onlyLocal)
	if err != nil {
		return nil, err
	}
	if d == nil {
		return nil, types.ENOENT
	}

	return m.dentry2Dirent(d), nil
}

func (m *DMeta) CreateDentry(pino types.InodeID, name string, mode os.FileMode) (*types.Dirent, error) {
	dentry, err := m.findDentry(pino, name, false)
	if err != nil && err != types.ENOENT {
		return nil, err
	}

	if dentry != nil {
		return m.dentry2Dirent(dentry), nil
	}

	dentry, err = m.createDentry(pino, name, mode)
	if err != nil {
		return nil, err
	}

	dentry.SetTTL(time.Now().Add(m.opt.DCacheTTL))
	return m.dentry2Dirent(dentry), err
}

func (m *DMeta) LoadSubDentires(ino types.InodeID) ([]types.Dirent, error) {
	dirents := []types.Dirent{}
	dentry := m.local.GetDentry(ino)
	dirents = append(dirents, types.Dirent{
		Name:  ".",
		Inode: *dentry.Inode,
	})

	dirents = append(dirents, types.Dirent{
		Name:  "..",
		Inode: *dentry.Inode,
	})

	logg.Dlog.Debugf("ino:%d load dentries, isComplete:%v isExpired:%v", ino, dentry.IsComplete(), dentry.IsExpired())

	if !dentry.IsComplete() || dentry.IsExpired() {
		err := m.refreshSubDentries(dentry, false)
		if err != nil {
			return nil, err
		}
	}

	for _, d := range dentry.Children() {
		dirents = append(dirents, *m.dentry2Dirent(d))
	}

	sort.Sort(types.SortDirents(dirents))
	return dirents, nil
}

func (m *DMeta) createDentry(pino types.InodeID, name string, mode os.FileMode) (*types.Dentry, error) {
	parent := m.local.GetDentry(pino)

	// create dentry in storage
	metaData := make(map[string]string, 1)
	metaData[storage.SETATTR_PERM] = strconv.FormatUint(uint64(mode), 10)
	metaData[storage.SETATTR_UID] = strconv.FormatUint(uint64(parent.Inode.Uid), 10)
	metaData[storage.SETATTR_GID] = strconv.FormatUint(uint64(parent.Inode.Gid), 10)

	key := ""
	if parent.IsRoot() {
		key = name
	} else {
		key = parent.GetFullPath() + "/" + name
	}

	if mode.IsDir() {
		key += "/"
	}

	putReq := &storage.PutFileRequest{
		Key:      key,
		Buf:      types.NullBuf{},
		MetaData: metaData,
	}
	_, err := m.storage.PutFile(putReq)
	if err != nil {
		return nil, err
	}

	inode := m.local.CreateNewInode(mode, parent.Inode.Gid, parent.Inode.Uid)
	dentry, err := m.local.CreateDentry(pino, name, inode)
	if err != nil {
		return nil, err
	}

	return dentry, nil
}

func (m *DMeta) findDentry(pino types.InodeID, name string, onlyLocal bool) (*types.Dentry, error) {
	dentry, err := m.local.FindDentry(pino, name)
	if err != nil {
		return nil, err
	}

	if dentry == nil {
		if onlyLocal {
			return nil, types.ENOENT
		}

		parent := m.local.GetDentry(pino)
		if parent == nil {
			return nil, fmt.Errorf("invalid pino:%d", pino)
		}

		// isRegularFile代表是否是常规文件
		obj, isDir, isRegularFile, err := m.getRemoteObject(parent, name)
		if err == nil {
			dentry, err := m.local.CreateDentryFromObj(pino, name, *obj, isDir)
			if err != nil {
				return nil, err
			}
			dentry.SetTTL(time.Now().Add(m.opt.DCacheTTL))
			return dentry, nil
		}

		if err != types.ENOENT {
			return nil, err
		}

		if isRegularFile && m.opt.DisableCheckVirtualDir {
			return nil, err
		}

		// 举个例子，如果当前ls的目录为a/b，但服务端存在以a/b/*为前缀的key，需要将a/b的目录在本地创建出来
		dirpath := parent.GetFullPathWithSlash() + name
		exist, err := m.isRemoteDirExist(dirpath + "/")
		if err != nil {
			return nil, err
		}

		if !exist {
			return nil, types.ENOENT
		}

		// add virtual dir in local
		dentry, _ = m.local.CreateDentry(parent.Inode.Ino, name, m.local.CreateNewInode(parent.Inode.Mode, parent.Inode.Gid, parent.Inode.Uid))
		dentry.SetTTL(time.Now().Add(m.opt.DCacheTTL))
		return dentry, nil
	} else {
		if !dentry.IsExpired() {
			return dentry, nil
		}

		var path string
		if dentry.IsDir() {
			path = dentry.GetFullPathWithSlash()
		} else {
			path = dentry.GetFullPath()
		}

		logg.Dlog.Debugf("pino:%d fpath:%s is expired", pino, path)
		reply, err := m.storage.HeadFile(&storage.HeadFileRequest{Key: path})
		if err != nil {
			if err == types.ENOENT && dentry.IsDir() {
				// local存的dentry可能是virtual dir, 服务端不存在对应的key
				dentry.SetTTL(time.Now().Add(m.opt.DCacheTTL))
				return dentry, nil
			}
			return nil, err
		}

		obj := reply.Info
		var mode os.FileMode
		if obj.Mode != nil {
			mode = (os.FileMode(*obj.Mode) & os.ModePerm)
		} else {
			if dentry.IsDir() {
				mode = os.ModeDir | 0755
			} else {
				mode = 0644
			}
		}

		update := InodeUpdateAttr{
			Size: &obj.Size, Uid: obj.Uid, Gid: obj.Gid, Mode: &mode,
		}

		_, err = m.local.UpdateInode(dentry.Inode.Ino, update, true)
		if err != nil {
			return nil, err
		}
		dentry.SetTTL(time.Now().Add(m.opt.DCacheTTL))
		return dentry, nil
	}
}

var (
	filterFileSuffix = map[string]struct{}{
		".jpeg": {},
		".jpg":  {},
		".png":  {},
		".gz":   {},
		".tgz":  {},
		".log":  {},
		".plot": {},
		".js":   {},
		".html": {},
		".css":  {},
		".apk":  {},
		".dat":  {},
	}
)

func (m *DMeta) getRemoteObject(parent *types.Dentry, name string) (*storage.ObjectInfo, bool, bool, error) {
	// 1. 检查是否有同名的文件
	// 2. 检查是否有同名的目录
	checkDentryExist := func(path string) (*storage.ObjectInfo, bool, bool, error) {
		req := &storage.HeadFileRequest{Key: path}
		checkFileResp, err := m.storage.HeadFile(req)
		if err != nil {
			if err == types.ENOENT {
				// 检查文件名后缀是否在过滤范围里
				if m.opt.SkipNotDirLookup {
					i := strings.LastIndex(path, ".")
					if i > 0 {
						suffix := path[i:]
						if _, exist := filterFileSuffix[suffix]; exist {
							return nil, false, true, err
						}
					}
				}

				req := &storage.HeadFileRequest{Key: path + "/"}
				checkDirResp, err := m.storage.HeadFile(req)
				if err != nil {
					return nil, false, false, err
				}
				return &checkDirResp.Info, checkDirResp.IsDir, false, nil
			}

			logg.Dlog.Errorf("headfile path:%s error:%+v", name, err)
			return nil, false, false, err
		}

		return &checkFileResp.Info, checkFileResp.IsDir, false, nil
	}

	fullname := parent.GetFullPathWithSlash() + name
	return checkDentryExist(fullname)
}

func (m *DMeta) isRemoteDirExist(prefix string) (bool, error) {
	req := &storage.ListObjectsRequest{
		Delimiter: "",
		Max:       1,
		Prefix:    prefix,
	}
	listRet, err := m.storage.ListObjects(req)
	if err != nil {
		return false, err
	}
	return len(listRet.Objects) > 0, nil
}

func (m *DMeta) GetInode(ino types.InodeID) (types.Inode, bool) {
	ret := types.Inode{}
	inode := m.local.GetInode(ino)
	if inode == nil {
		return ret, false
	}

	return *inode, true
}

func (m *DMeta) UpdateInodeSize(ino types.InodeID, size uint64, sync bool) (types.Inode, error) {
	inode, err := m.local.UpdateInode(ino, InodeUpdateAttr{Size: &size}, sync)
	return *inode, err
}

func (m *DMeta) UpdateInode(ino types.InodeID, updateFields InodeUpdateAttr, sync bool) (types.Inode, error) {
	dentry := m.local.GetDentry(ino)
	if dentry == nil {
		return types.Inode{}, types.ENOENT
	}

	inode := dentry.Inode
	if ino == types.RootInodeID {
		return *inode, syscall.ENOTSUP
	}

	var path string
	if dentry.IsDir() {
		path = dentry.GetFullPathWithSlash()
	} else {
		path = dentry.GetFullPath()
	}

	// refresh inode meta data
	req := &storage.HeadFileRequest{Key: path}
	reply, err := m.storage.HeadFile(req)

	metaData := make(map[string]string)
	if err != nil {
		// 这里的报错如果是ENOENT的情况下，可能是因为ino是个虚拟的目录，在存储端没有对应的key
		if inode.IsDir() && err == types.ENOENT {
			logg.Dlog.Infof("ino:%d path:%s maybe is a virtual dir, create it to remote", ino, path)
			// 准备创建对应的目录
			parent := dentry.Parent.Inode
			metaData[storage.SETATTR_UID] = strconv.FormatUint(uint64(parent.Uid), 10)
			metaData[storage.SETATTR_GID] = strconv.FormatUint(uint64(parent.Gid), 10)
			metaData[storage.SETATTR_PERM] = strconv.FormatUint(uint64(parent.Mode), 10)
		} else {
			return *inode, err
		}
	} else {
		// 即key已经在服务端存在
		if reply != nil {
			metaData[storage.SETATTR_MAGIC] = "REPLACE"
			metaData[storage.SETATTR_MAGIC2] = "/" + m.opt.Bucket + "/" + m.opt.BucketPrefix + path

			for k, v := range reply.Info.Metadata {
				metaData[k] = v
			}
		}
	}

	if updateFields.Uid != nil {
		metaData[storage.SETATTR_UID] = strconv.FormatUint(uint64(*updateFields.Uid), 10)
	}
	if updateFields.Gid != nil {
		metaData[storage.SETATTR_GID] = strconv.FormatUint(uint64(*updateFields.Gid), 10)
	}
	if updateFields.Mode != nil {
		metaData[storage.SETATTR_PERM] = strconv.FormatUint(uint64(*updateFields.Mode), 10)
	}

	if len(metaData) == 0 {
		return *inode, nil
	}

	logg.Dlog.Infof("ino:%d setattr %v", ino, metaData)

	preq := &storage.PutFileRequest{
		Buf:      types.DefaultNullBuf,
		MetaData: metaData,
		Key:      path,
	}
	_, err = m.storage.PutFile(preq)
	if err != nil {
		logg.Dlog.Errorf("ino:%d setattr error %v", ino, err)
		return *inode, err
	}

	inode, err = m.local.UpdateInode(ino, updateFields, true)
	return *inode, err
}

func (m *DMeta) Rename(srcPino types.InodeID, srcName string, dstPino types.InodeID, dstName string) error {
	srcDentry, err := m.local.FindDentry(srcPino, srcName)
	if err != nil || srcDentry == nil {
		return types.ENOENT
	}

	src_is_dir := srcDentry.IsDir()
	dstParent := m.local.GetDentry(dstPino)
	if dstParent == nil {
		return types.ENOENT
	}

	dstDentry := dstParent.GetChild(dstName)
	if dstDentry != nil && dstDentry.IsDir() && dstDentry.ChildCount() != 0 {
		return types.ENOTEMPTY
	}

	var dst_is_dir bool
	// 如果dstDentry不存在，则直接创建一个同名的目录或文件
	if dstDentry == nil {
		dst_is_dir = src_is_dir
	} else {
		dst_is_dir = dstDentry.IsDir()
	}

	if src_is_dir && !dst_is_dir {
		logg.Dlog.Errorf("src dentry is dir but dst dentry is a file")
		return types.ENOTDIR
	}

	if !src_is_dir && dst_is_dir {
		logg.Dlog.Errorf("src dentry is a file but dst dentry is dir")
		return types.ENOTDIR
	}

	if src_is_dir {
		// 提前递归刷新一遍最新的子目录
		err := m.refreshSubDentries(srcDentry, true)
		if err != nil {
			return fmt.Errorf("refresh src dentry error, srcpino: %d, err: %v", srcPino, err)
		}

		srcPath := srcDentry.GetFullPathWithSlash()
		subDentries := srcDentry.GetSubDentries()
		dstPath := dstParent.GetFullPathWithSlash() + dstName + "/"

		for _, sd := range subDentries {
			srcKey := sd.Path
			if sd.IsDir {
				srcKey += "/"
			}

			dstKey := dstPath + srcKey[len(srcPath):]

			err := m.storage.Copy(&storage.CopyRequest{Src: srcKey, Dst: dstKey})

			// src是个目录的情况下，在对象存储上可能是不存在的，因此这里应该忽略404错误
			if err != nil {
				if sd.IsDir && err == types.ENOENT {
					continue
				}
				logg.Dlog.Errorf("copy err, srcKey:%s dstKey:%s err:%v", srcKey, dstKey, err)
				return err
			}

			if _, err = m.storage.DeleteFile(&storage.DeleteFileRequest{Key: srcKey}); err != nil {
				logg.Dlog.Errorf("delete err, srcKey:%s err:%v", srcKey, err)
				return err
			}
		}

		err = m.storage.Copy(&storage.CopyRequest{
			Src: srcPath,
			Dst: dstPath,
		})
		if err != nil && err != types.ENOENT {
			logg.Dlog.Errorf("copy err, srcPath:%s dstPath:%s err:%v", srcPath, dstPath, err)
			return err
		}

		if err == nil {
			_, err = m.storage.DeleteFile(&storage.DeleteFileRequest{Key: srcPath})
			if err != nil {
				logg.Dlog.Errorf("delete err, srcPath:%s err:%v", srcPath, err)
				return err
			}
		}
	} else {
		srcPath := srcDentry.GetFullPath()
		dstPath := dstParent.GetFullPathWithSlash() + dstName

		err := m.storage.Copy(&storage.CopyRequest{
			Src: srcPath,
			Dst: dstPath,
		})
		if err != nil {
			return err
		}

		_, err = m.storage.DeleteFile(&storage.DeleteFileRequest{Key: srcPath})
		if err != nil {
			return err
		}
	}

	err = m.local.Rename(srcPino, srcName, dstPino, dstName)
	if err != nil {
		return err
	}
	return nil
}

func (m *DMeta) RemoveDentry(pino types.InodeID, name string) error {
	err := m.removeDentry(pino, name)
	if err != nil && err == types.ENOENT {
		m.local.RemoveDentry(pino, name)
	}
	return err
}

func (m *DMeta) removeDentry(pino types.InodeID, name string) error {
	dentry, err := m.findDentry(pino, name, false)
	if err != nil {
		return err
	}
	if dentry == nil {
		return types.ENOENT
	}

	fullpath := dentry.GetFullPath()
	if !dentry.Inode.IsDir() {
		_, err = m.storage.DeleteFile(&storage.DeleteFileRequest{Key: fullpath})
		if err != nil {
			return err
		}
	} else {
		logg.Dlog.Infof("sub dentries len:%d", dentry.ChildCount())
		if dentry.ChildCount() != 0 {
			return types.ENOTEMPTY
		}

		_, err = m.storage.DeleteFile(&storage.DeleteFileRequest{Key: fullpath + "/"})
		if err != nil {
			return err
		}
	}

	return m.local.RemoveDentry(pino, name)
}

func (m *DMeta) GetDentry(ino types.InodeID) *types.Dirent {
	dentry := m.local.GetDentry(ino)
	if dentry == nil {
		return nil
	}

	return m.dentry2Dirent(dentry)
}

func (m *DMeta) GetDentryPath(ino types.InodeID) (string, error) {
	dentry := m.local.GetDentry(ino)
	if dentry == nil {
		return "", types.ENOENT
	}

	return dentry.GetFullPath(), nil
}

func (m *DMeta) dentry2Dirent(dentry *types.Dentry) *types.Dirent {
	var pino types.InodeID
	if dentry.Inode.Ino != types.RootInodeID {
		pino = dentry.Parent.Inode.Ino
	}
	d := &types.Dirent{
		Name:  dentry.Name,
		Inode: *dentry.Inode,
		PIno:  pino,
	}

	return d
}

func (m *DMeta) refreshSubDentries(dentry *types.Dentry, recursive bool) error {
	prefix := ""
	if !dentry.IsRoot() {
		prefix = dentry.GetFullPath() + "/"
	}

	logg.Dlog.Infof("listfiles prefix:%v", prefix)
	listMarker := ""

	for {
		req := &storage.GetFileListRequest{
			Prefix:    prefix,
			Marker:    listMarker,
			Max:       1000,
			Recursive: recursive,
		}
		reply, err := m.storage.GetFileList(req)
		if err != nil {
			logg.Dlog.Error(err)
			return err
		}

		m.local.BuildDentries(dentry.Inode.Ino, reply.FTree.Children) // 递归生成目录树

		if reply.IsTrunc {
			listMarker = reply.Marker
			logg.Dlog.Infof("readdir is trunc %v", reply.Marker)
			continue
		} else {
			listMarker = ""
		}
		break
	}

	m.clearUnSyncDentry(dentry)

	return nil
}

func (m *DMeta) clearUnSyncDentry(parent *types.Dentry) {
	// todo: 下面没加锁
	queue := []*types.Dentry{parent}
	dcacheTime := time.Now().Add(m.opt.DCacheTTL)

	for len(queue) != 0 {
		d := queue[0]
		queue = queue[1:]
		if !d.Synced {
			logg.Dlog.Debugf("ino:%d ignore not synced dir, name:%s ", d.Inode.Ino, d.Name)
			continue
		}

		d.Synced = false
		d.SetTTL(dcacheTime)

		if d.ChildCount() != 0 {
			d.Complete()
		}

		for _, dentry := range d.Children() {
			dname := dentry.Name
			if !dentry.Synced {
				d.RemoveChild(dname)
				logg.Dlog.Infof("ino:%d remove missing dentry: %s", d.Inode.Ino, dname)
				continue
			}

			dentry.Synced = false
			if dentry.Inode.IsDir() {
				queue = append(queue, dentry)
				// 防止有子目录但是还没有拉取的情况，如果拉取过，认为一定拉取全了
				dentry.SetTTL(dcacheTime)

				if dentry.ChildCount() != 0 {
					dentry.Complete()
				}
			} else {
				dentry.SetTTL(dcacheTime)
			}
		}
	}
}

func (m *DMeta) RefreshInode(ino types.InodeID) error {
	dentry := m.local.GetDentry(ino)
	if dentry == nil {
		return fmt.Errorf("refresh inode error, invalid ino:%d", ino)
	}

	if dentry.Inode.IsDir() {
		logg.Dlog.Infof("ignore dir ino:%d refresh", ino)
		return nil
	}

	req := &storage.HeadFileRequest{Key: dentry.GetFullPath()}
	reply, err := m.storage.HeadFile(req)
	if err != nil {
		return err
	}

	m.local.UpdateInode(ino, InodeUpdateAttr{
		Size: &reply.Info.Size,
	}, true)

	return nil
}

func (m *DMeta) RemoveInode(ino types.InodeID) error {
	return m.local.RemoveInode(ino)
}

func (m *DMeta) Destory() {
	m.local.Destroy()
}
