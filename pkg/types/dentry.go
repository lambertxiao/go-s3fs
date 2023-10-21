package types

import (
	"sync"
	"time"
)

const ZERO_SIZE_FILE_TTL = 0 * time.Second

type Dentry struct {
	Mutex    sync.RWMutex
	Name     string
	Parent   *Dentry
	children map[string]*Dentry
	Inode    *Inode
	ttl      time.Time
	flags    uint32
	Synced   bool
}

const (
	D_COMPLETE  uint32 = 1
	SUFFIX_LOGO        = "."
)

func NewDentry(name string, inode *Inode) *Dentry {
	d := &Dentry{
		Name:     name,
		Inode:    inode,
		children: make(map[string]*Dentry),
	}
	return d
}

func (d *Dentry) IsRoot() bool {
	return d.Inode.Ino == RootInodeID
}

// 子dentry存在的情况下，parent不可能被删除
func (d *Dentry) GetFullPath() string {
	// root name is "", not "/"
	if d.Inode.Ino == RootInodeID {
		return ""
	}

	fullpath := d.Name
	parent := d.Parent

	for parent != nil && parent.Inode.Ino != RootInodeID {
		fullpath = parent.Name + "/" + fullpath
		parent = parent.Parent
	}

	return fullpath
}

func (d *Dentry) GetFullPathWithSlash() string {
	if d.Inode.Ino == RootInodeID {
		return ""
	}

	return d.GetFullPath() + "/"
}

func (d *Dentry) GetChild(name string) *Dentry {
	d.Mutex.RLock()
	defer d.Mutex.RUnlock()

	return d.children[name]
}

func (d *Dentry) AddChild(name string, inode *Inode) *Dentry {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	child := &Dentry{
		Parent:   d,
		Name:     name,
		Inode:    inode,
		children: make(map[string]*Dentry),
	}

	d.children[name] = child
	return child
}

func (d *Dentry) RemoveChild(name string) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	delete(d.children, name)
}

func (d *Dentry) Children() []*Dentry {
	d.Mutex.RLock()
	defer d.Mutex.RUnlock()

	denties := []*Dentry{}
	for _, child := range d.children {
		denties = append(denties, child)
	}
	return denties
}

func (d *Dentry) IsExpired() bool {
	return d.ttl.Before(time.Now())
}

func (d *Dentry) IsComplete() bool {
	return d.flags&D_COMPLETE == D_COMPLETE
}

func (d *Dentry) ClearComplete() {
	d.flags &= ^D_COMPLETE
}

func (d *Dentry) Complete() {
	d.flags |= D_COMPLETE
}

func (d *Dentry) Update(size uint64, ctime, mtime time.Time) {
	d.Inode.Size = size
	d.Inode.Ctime = ctime
	d.Inode.Mtime = mtime
}

func (d *Dentry) SetTTL(ttl time.Time) {
	if !d.Inode.IsDir() && d.Inode.Size == 0 {
		// 由于go-s3fs可能拉取到其他端正在写入的文件(表现为size为0的文件)，为了在上层做lookupInode等操作时能及时获取到正确的size，将ttl尽量设置为0
		d.ttl = time.Now().Add(ZERO_SIZE_FILE_TTL)
	} else {
		d.ttl = ttl
	}
}

func (d *Dentry) IsDir() bool {
	return d.Inode.IsDir()
}

func (d *Dentry) ChildCount() int {
	d.Mutex.RLock()
	defer d.Mutex.RUnlock()
	return len(d.children)
}

type SubDentry struct {
	Path  string
	IsDir bool
}

// 深度优先，获取所有子孙节点的路径
func (d *Dentry) GetSubDentries() []SubDentry {
	paths := []SubDentry{}
	basePath := d.GetFullPathWithSlash()

	var recur func(parent *Dentry, basePath string)
	recur = func(parent *Dentry, basePath string) {
		for _, child := range parent.children {
			paths = append(paths, SubDentry{
				Path:  basePath + child.Name,
				IsDir: child.IsDir(),
			})

			if child.ChildCount() != 0 {
				recur(child, basePath+child.Name+"/")
			}
		}
	}

	recur(d, basePath)
	return paths
}
