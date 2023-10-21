package meta

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

var _ LocalMeta = (*MemMeta)(nil)

const (
	LOCK_COUNT         = 64
	INO_KEY_PREFIX     = "ino-"
	LOAD_CACHE_CH_SIZE = 100000

	S_IRWXU = 0000700 /* [XSI] RWX mask for owner */
	S_IRWXG = 0000070 /* [XSI] RWX mask for group */
	S_IRWXO = 0000007 /* [XSI] RWX mask for other */

	DEFAULT_MP_MODE = 0766
)

type dentryMap map[types.InodeID]*types.Dentry

type MemMeta struct {
	mutex         sync.Mutex
	nextInodeID   types.InodeID
	locks         [LOCK_COUNT]sync.RWMutex
	dentryBuckets [LOCK_COUNT]dentryMap
	rootDentry    *types.Dentry
	opt           MetaOption
}

func NewMemMeta(opt MetaOption) (LocalMeta, error) {
	m := &MemMeta{
		dentryBuckets: [LOCK_COUNT]dentryMap{},
		nextInodeID:   types.RootInodeID,
	}

	var mp_mode uint32 = 0
	if opt.AllowOther {
		if opt.MpMask != types.DEFAULT_MP_MASK {
			mp_mode = (^opt.MpMask & (S_IRWXU | S_IRWXG | S_IRWXO))
		} else {
			mp_mode = (S_IRWXU | S_IRWXG | S_IRWXO)
		}
	} else {
		mp_mode = DEFAULT_MP_MODE
	}

	now := time.Now()
	m.rootDentry = types.NewDentry("", &types.Inode{
		Ino:   m.GetNextInodeID(),
		Mode:  os.ModeDir | os.FileMode(mp_mode),
		Mtime: now,
		Ctime: now,
		Uid:   opt.UID,
		Gid:   opt.GID,
	})

	m.saveDentryIndex(m.rootDentry)
	return m, nil
}

func getInodeCacheKey(pino types.InodeID, name string) string {
	return fmt.Sprintf(INO_KEY_PREFIX+"%d/%s", pino, name)
}

func parseInoKey(key string) (types.InodeID, string, error) {
	k := key[len(INO_KEY_PREFIX):]
	parts := strings.Split(k, "/")
	_pino, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("parse ino error, key:%s ino:%s err:%v", k, parts[0], err)
	}

	pino, name := types.InodeID(_pino), parts[1]
	return pino, name, nil
}

func max(ino1 int64, ino2 int64) int64 {
	if ino1 > ino2 {
		return ino1
	}
	return ino2
}

// 将dentry加入全局索引
func (m *MemMeta) saveDentryIndex(dentry *types.Dentry) {
	ino := dentry.Inode.Ino
	m.wLockInode(ino)
	defer m.wUnlockInode(ino)

	idx := ino % LOCK_COUNT
	dmap := m.dentryBuckets[idx]
	if dmap == nil {
		dmap = dentryMap{}
	}
	dmap[ino] = dentry
	m.dentryBuckets[idx] = dmap
}

// 将inode从全局索引中删除
func (m *MemMeta) RemoveInode(ino types.InodeID) error {
	m.wLockInode(ino)
	defer m.wUnlockInode(ino)

	idx := ino % LOCK_COUNT
	dmap := m.dentryBuckets[idx]
	if dmap == nil {
		return nil
	}

	delete(dmap, ino)
	return nil
}

func (m *MemMeta) GetDentry(ino types.InodeID) *types.Dentry {
	m.rLockInode(ino)
	defer m.rUnlockInode(ino)

	return m.dentryBuckets[ino%LOCK_COUNT][ino]
}

func (m *MemMeta) rLockInode(ino types.InodeID) {
	m.locks[ino%LOCK_COUNT].RLock()
}

func (m *MemMeta) rUnlockInode(ino types.InodeID) {
	m.locks[ino%LOCK_COUNT].RUnlock()
}

func (m *MemMeta) wLockInode(ino types.InodeID) {
	m.locks[ino%LOCK_COUNT].Lock()
}

func (m *MemMeta) wUnlockInode(ino types.InodeID) {
	m.locks[ino%LOCK_COUNT].Unlock()
}

func (m *MemMeta) GetNextInodeID() types.InodeID {
	m.mutex.Lock()
	ino := m.nextInodeID
	m.nextInodeID++
	m.mutex.Unlock()
	return ino
}

func (m *MemMeta) FindDentry(pino types.InodeID, name string) (*types.Dentry, error) {
	parent := m.GetDentry(pino)
	if parent == nil {
		return nil, fmt.Errorf("invalid pino:%d", pino)
	}

	return parent.GetChild(name), nil
}

func (m *MemMeta) GetInode(ino types.InodeID) *types.Inode {
	dentry := m.GetDentry(ino)
	if dentry == nil {
		return nil
	}

	return dentry.Inode
}

func (m *MemMeta) UpdateInode(ino types.InodeID, updateFields InodeUpdateAttr, sync bool) (*types.Inode, error) {
	dentry := m.GetDentry(ino)
	if dentry == nil {
		return nil, nil
	}

	inode := dentry.Inode

	if updateFields.Size != nil {
		if sync {
			logg.Dlog.Debugf("change size %v->%v", inode.Size, *updateFields.Size)
		}
		inode.Size = *updateFields.Size
	}
	if updateFields.Mtime != nil {
		logg.Dlog.Debugf("change mtime %v->%v", inode.Mtime, *updateFields.Mtime)
		inode.Mtime = *updateFields.Mtime
	}
	if updateFields.Mode != nil {
		newMode := (inode.Mode & ^os.ModePerm) | (*updateFields.Mode & os.ModePerm)
		logg.Dlog.Debugf("change mode %v->%v", inode.Mode, newMode)
		inode.Mode = newMode
	}
	if updateFields.Uid != nil {
		logg.Dlog.Debugf("change uid %v->%v", inode.Uid, *updateFields.Uid)
		inode.Uid = *updateFields.Uid
	}
	if updateFields.Gid != nil {
		logg.Dlog.Debugf("change gid %v->%v", inode.Gid, *updateFields.Gid)
		inode.Gid = *updateFields.Gid
	}

	return inode, nil
}

func (m *MemMeta) addChildForDentry(parent *types.Dentry, name string, inode *types.Inode) *types.Dentry {
	dst := parent.AddChild(name, inode)
	m.saveDentryIndex(dst)
	return dst
}

func (m *MemMeta) removeChildForDentry(parent *types.Dentry, name string) {
	// 这里只将节点从parent上移除，不释放inode，因为可能有正在进行的write
	// 在releaseFileHandle的时候会check inode是否还在parent上，不存在了代表被删掉了，则释放inode自身
	parent.RemoveChild(name)
}

func (m *MemMeta) updateDentryAttr(dentry *types.Dentry, size uint64, ctime, mtime time.Time) {
	dentry.Update(size, ctime, mtime)
}

func (m *MemMeta) CreateDentry(pino types.InodeID, name string, inode *types.Inode) (*types.Dentry, error) {
	parent := m.GetDentry(pino)
	if parent == nil {
		return nil, fmt.Errorf("invalid pino:%d", pino)
	}

	dst := m.addChildForDentry(parent, name, inode)
	return dst, nil
}

func (m *MemMeta) CreateDentryFromObj(pino types.InodeID, name string, obj storage.ObjectInfo, isDir bool) (*types.Dentry, error) {
	parent := m.GetDentry(pino)
	if parent == nil {
		return nil, fmt.Errorf("invalid pino:%d", pino)
	}

	inode := m.obj2Inode(obj, isDir)
	dst := m.addChildForDentry(parent, name, &inode)
	return dst, nil
}

func (m *MemMeta) Rename(srcPino types.InodeID, srcName string, dstPino types.InodeID, dstName string) error {
	srcParent := m.GetDentry(srcPino)
	if srcParent == nil {
		return fmt.Errorf("invalid src pino:%d ", srcPino)
	}

	dstParent := m.GetDentry(dstPino)
	if dstParent == nil {
		return fmt.Errorf("invalid dst pino:%d ", dstPino)
	}

	src := srcParent.GetChild(srcName)
	if src == nil {
		return fmt.Errorf("invalid src name src_pino:%d src_name:%s", srcPino, srcName)
	}

	m.removeChildForDentry(srcParent, srcName)
	m.addChildForDentry(dstParent, dstName, src.Inode)

	return nil
}

func (m *MemMeta) RemoveDentry(pino types.InodeID, name string) error {
	parent := m.GetDentry(pino)
	if parent == nil {
		return fmt.Errorf("invalid pino:%d", pino)
	}

	m.removeChildForDentry(parent, name)
	return nil
}

func (m *MemMeta) BuildDentries(pino types.InodeID, childs map[string]*storage.FTreeNode) {
	if childs == nil {
		return
	}

	var build func(parent *types.Dentry, childs map[string]*storage.FTreeNode)
	build = func(parent *types.Dentry, childs map[string]*storage.FTreeNode) {
		parent.Synced = true // 标记dir刚从服务端同步过
		for _, child := range childs {
			oinfo := child.Oinfo
			childDentry := parent.GetChild(child.Name)

			if childDentry == nil {
				isDir := child.Typ == storage.FTTDir
				inode := m.obj2Inode(child.Oinfo, isDir)
				childDentry = m.addChildForDentry(parent, child.Name, &inode)
			} else {
				m.updateDentryAttr(childDentry, oinfo.Size, oinfo.Ctime, oinfo.Mtime)
			}

			if child.Typ == storage.FTTDir {
				build(childDentry, child.Children)
			}

			childDentry.Synced = true // 标记dentry是本次从服务端同步到的
		}
	}

	parent := m.GetDentry(pino)
	build(parent, childs)
}

func (m *MemMeta) obj2Inode(obj storage.ObjectInfo, isDir bool) types.Inode {
	inode := &types.Inode{
		Ino:          m.GetNextInodeID(),
		Size:         obj.Size,
		Mtime:        obj.Mtime,
		Ctime:        obj.Ctime,
		StorageClass: obj.Storage_class,
	}

	if obj.Uid != nil {
		inode.Uid = *obj.Uid
	} else {
		inode.Uid = m.opt.UID
	}

	if obj.Gid != nil {
		inode.Gid = *obj.Gid
	} else {
		inode.Gid = m.opt.GID
	}

	if isDir {
		inode.Mode = os.ModeDir | 0755
	} else {
		inode.Mode = 0644
	}

	if obj.Mode != nil {
		inode.Mode = (inode.Mode & ^os.ModePerm) | (os.FileMode(*obj.Mode) & os.ModePerm)
	}

	return *inode
}

func (m *MemMeta) CreateNewInode(mode os.FileMode, gid uint32, uid uint32) *types.Inode {
	now := time.Now()
	inode := &types.Inode{
		Ino:   m.GetNextInodeID(),
		Mtime: now,
		Ctime: now,
		Mode:  mode,
		Uid:   gid,
		Gid:   uid,
	}
	return inode
}

func (m *MemMeta) Destroy() {}
