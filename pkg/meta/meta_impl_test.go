package meta_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/meta"
	"github.com/lambertxiao/go-s3fs/pkg/mocks"
	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
	"github.com/stretchr/testify/suite"
)

func TestMetaImplTestSuite(t *testing.T) {
	suite.Run(t, new(MetaImplTestSuite))
}

const (
	mockName    = "testname"
	mockPino    = types.RootInodeID
	mockInodeID = types.InodeID(123)
)

var (
	mockInode  = &types.Inode{Ino: mockInodeID}
	mockDentry = &types.Dentry{Inode: mockInode, Name: mockName}
	mockGID    = uint32(8)
	mockUID    = uint32(16)
	mockFMode  = os.FileMode(0755)
)

type MetaImplTestSuite struct {
	suite.Suite
	mockCtrl   *gomock.Controller
	storage    *mocks.MockStorage
	localMeta  *mocks.MockLocalMeta
	meta       meta.Meta
	rootInode  *types.Inode
	rootDentry *types.Dentry
}

func (s *MetaImplTestSuite) SetupTest() {
	logg.InitLogger()
	s.mockCtrl = gomock.NewController(s.T())
	s.storage = mocks.NewMockStorage(s.mockCtrl)
	s.localMeta = mocks.NewMockLocalMeta(s.mockCtrl)
	s.meta, _ = meta.NewMeta(meta.MetaOption{
		Bucket: "mockBucketName",
	}, s.localMeta, s.storage)
	s.rootInode = &types.Inode{
		Ino: types.RootInodeID,
		Gid: mockGID,
		Uid: mockUID,
	}
	s.rootDentry = types.NewDentry("", s.rootInode)
}

func (s *MetaImplTestSuite) AfterTest() {
	s.mockCtrl.Finish()
}

func (s *MetaImplTestSuite) BeforeTest() {
}

func (s *MetaImplTestSuite) expectFindDentry(name string) {
	// 1. 先检查local里是否有dentry
	s.localMeta.EXPECT().
		FindDentry(types.RootInodeID, name).
		Return(nil, nil)

	// 2. 没有dentry，需要查找parent，拿到dentry的全路径
	s.localMeta.EXPECT().
		GetDentry(types.RootInodeID).
		Return(s.rootDentry)

	// 3. 先查对应文件在不在
	s.storage.EXPECT().
		HeadFile(&storage.HeadFileRequest{Key: name}).
		Return(nil, types.ENOENT)

	// 4. 再查对应目录在不在
	s.storage.EXPECT().
		HeadFile(&storage.HeadFileRequest{Key: name + "/"}).
		Return(nil, types.ENOENT)

	// 5. 最后检查是否有以testname为前缀的文件，有的话，可以为testname本地构建一个目录
	s.storage.EXPECT().
		ListObjects(&storage.ListObjectsRequest{Prefix: name + "/", Max: 1}).
		Return(&storage.ListObjectsReply{}, nil)
}

// dentry local和remote都不存在
func (s *MetaImplTestSuite) TestFindDentryNotExist() {
	s.expectFindDentry(mockName)
	dentry, err := s.meta.FindDentry(types.RootInodeID, mockName, false)
	s.Nil(dentry)
	s.Equal(types.ENOENT, err)
}

// dentry local存在
func (s *MetaImplTestSuite) TestFindDentryLocalExist() {
	target := &types.Dentry{
		Name:   mockName,
		Inode:  &types.Inode{Ino: 123, Size: 1024},
		Parent: s.rootDentry,
	}
	target.SetTTL(time.Now().Add(time.Second * 5))

	s.localMeta.EXPECT().
		FindDentry(types.RootInodeID, mockName).
		Return(target, nil)

	dirent, err := s.meta.FindDentry(types.RootInodeID, mockName, false)
	s.Nil(err)
	s.Equal(target.Inode.Ino, dirent.Inode.Ino)
	s.Equal(target.Name, dirent.Name)
}

// dentry local存在, 但ttl已失效
func (s *MetaImplTestSuite) TestFindDentryLocalExistButExpired() {
	target := &types.Dentry{
		Name:   mockName,
		Inode:  &types.Inode{Ino: mockInodeID, Size: 1024},
		Parent: s.rootDentry,
	}

	s.localMeta.EXPECT().
		FindDentry(types.RootInodeID, mockName).
		Return(target, nil)

	mode := uint32(mockFMode)
	var size uint64 = 2048
	s.storage.EXPECT().
		HeadFile(&storage.HeadFileRequest{Key: mockName}).
		Return(&storage.HeadFileReply{
			Info: storage.ObjectInfo{
				Size: size,
				Uid:  &mockUID,
				Gid:  &mockGID,
				Mode: &mode,
			},
		}, nil)

	s.localMeta.EXPECT().
		UpdateInode(mockInodeID, meta.InodeUpdateAttr{
			Size: &size,
			Uid:  &mockUID,
			Gid:  &mockGID,
			Mode: &mockFMode,
		}, true)

	dirent, err := s.meta.FindDentry(types.RootInodeID, mockName, false)
	s.Nil(err)
	s.Equal(target.Inode.Ino, dirent.Inode.Ino)
	s.Equal(target.Name, dirent.Name)
}

// dentry local不存在，remote存在
func (s *MetaImplTestSuite) TestFindDentryLocalNotExistRemoteExist() {
	s.localMeta.EXPECT().
		FindDentry(types.RootInodeID, mockName).
		Return(nil, nil)
	s.localMeta.EXPECT().
		GetDentry(types.RootInodeID).
		Return(s.rootDentry)

	obj := storage.ObjectInfo{}
	s.storage.EXPECT().
		HeadFile(&storage.HeadFileRequest{Key: mockName}).
		Return(&storage.HeadFileReply{Info: obj, IsDir: false}, nil)

	target := &types.Dentry{
		Name:   mockName,
		Inode:  &types.Inode{Ino: 123},
		Parent: s.rootDentry,
	}
	s.localMeta.EXPECT().
		CreateDentryFromObj(types.RootInodeID, mockName, obj, false).
		Return(target, nil)

	dirent, err := s.meta.FindDentry(types.RootInodeID, mockName, false)
	s.Nil(err)
	s.Equal(mockName, dirent.Name)
	s.Equal(types.InodeID(123), dirent.Inode.Ino)
}

// 测试skip_ne_dir_lookup + regularFile
func (s *MetaImplTestSuite) TestFindDentrySkipNotDir() {
	mockName := "testname.dat"
	var dir1Ino types.InodeID = 2
	dir1Dentry := s.rootDentry.AddChild("dir1", &types.Inode{Ino: dir1Ino, Mode: os.ModePerm})

	s.localMeta.EXPECT().
		FindDentry(dir1Ino, mockName).
		Return(nil, nil)
	s.localMeta.EXPECT().
		GetDentry(dir1Ino).
		Return(dir1Dentry)

	// 只开启skip_ne_dir_lookup的情况下会有一次headfile和一次listobject
	s.storage.EXPECT().
		HeadFile(&storage.HeadFileRequest{Key: "dir1/" + mockName}).
		Return(nil, types.ENOENT)

	s.storage.EXPECT().ListObjects(&storage.ListObjectsRequest{
		Max:    1,
		Prefix: "dir1/" + mockName + "/",
	}).Return(&storage.ListObjectsReply{}, nil)

	ameta, _ := meta.NewMeta(meta.MetaOption{
		Bucket:           "mockBucketName",
		SkipNotDirLookup: true,
	}, s.localMeta, s.storage)

	_, err := ameta.FindDentry(dir1Ino, mockName, false)
	s.Equal(types.ENOENT, err)
}

// 测试skip_ne_dir_lookup + disable_check_vdir + regularFile
func (s *MetaImplTestSuite) TestFindDentrySkipNotDirAndDisableCheckVDir() {
	mockName := "testname.dat"
	var dir1Ino types.InodeID = 2
	dir1Dentry := s.rootDentry.AddChild("dir1", &types.Inode{Ino: dir1Ino, Mode: os.ModePerm})

	s.localMeta.EXPECT().
		FindDentry(dir1Ino, mockName).
		Return(nil, nil)
	s.localMeta.EXPECT().
		GetDentry(dir1Ino).
		Return(dir1Dentry)

	// 开启skip_ne_dir_lookup + disable_check_vdir的情况下会有一次headfile
	s.storage.EXPECT().
		HeadFile(&storage.HeadFileRequest{Key: "dir1/" + mockName}).
		Return(nil, types.ENOENT)

	ameta, _ := meta.NewMeta(meta.MetaOption{
		Bucket:                 "mockBucketName",
		SkipNotDirLookup:       true,
		DisableCheckVirtualDir: true,
	}, s.localMeta, s.storage)

	_, err := ameta.FindDentry(dir1Ino, mockName, false)
	s.Equal(types.ENOENT, err)
}

// 测试skip_ne_dir_lookup + 非regularFile
func (s *MetaImplTestSuite) TestFindDentrySkipNotDirAndNotRegularFile() {
	mockName := "testname"
	var dir1Ino types.InodeID = 2
	dir1Dentry := s.rootDentry.AddChild("dir1", &types.Inode{Ino: dir1Ino, Mode: os.ModePerm})

	s.localMeta.EXPECT().
		FindDentry(dir1Ino, mockName).
		Return(nil, nil)
	s.localMeta.EXPECT().
		GetDentry(dir1Ino).
		Return(dir1Dentry)

	s.storage.EXPECT().
		HeadFile(&storage.HeadFileRequest{Key: "dir1/" + mockName}).
		Return(nil, types.ENOENT)

	s.storage.EXPECT().
		HeadFile(&storage.HeadFileRequest{Key: "dir1/" + mockName + "/"}).
		Return(nil, types.ENOENT)

	s.storage.EXPECT().ListObjects(&storage.ListObjectsRequest{
		Max:    1,
		Prefix: "dir1/" + mockName + "/",
	}).Return(&storage.ListObjectsReply{}, nil)

	ameta, _ := meta.NewMeta(meta.MetaOption{
		Bucket:                 "mockBucketName",
		SkipNotDirLookup:       true,
		DisableCheckVirtualDir: true,
	}, s.localMeta, s.storage)

	_, err := ameta.FindDentry(dir1Ino, mockName, false)
	s.Equal(types.ENOENT, err)
}

func (s *MetaImplTestSuite) TestCreateDentry() {
	pino := types.RootInodeID
	name := mockName

	s.expectFindDentry(name)

	s.localMeta.EXPECT().
		GetDentry(pino).
		Return(s.rootDentry)

	s.storage.EXPECT().
		PutFile(&storage.PutFileRequest{
			Key: name, Buf: types.NullBuf{},
			MetaData: map[string]string{
				"X-Amz-Meta-Fsperm": "493",
				"X-Amz-Meta-Fsgid":  fmt.Sprintf("%d", mockGID), // 使用parent的uid和gid
				"X-Amz-Meta-Fsuid":  fmt.Sprintf("%d", mockUID),
			},
		}).
		Return(&storage.PutFileReply{}, nil)

	mode := os.FileMode(0755)
	inode := &types.Inode{Ino: types.InodeID(123)}

	s.localMeta.EXPECT().
		CreateNewInode(mode, mockGID, mockUID).
		Return(inode)

	dentry := &types.Dentry{
		Parent: s.rootDentry,
		Name:   name,
		Inode:  inode,
	}

	s.localMeta.EXPECT().
		CreateDentry(pino, name, inode).
		Return(dentry, nil)

	ret, err := s.meta.CreateDentry(pino, mockName, mode)
	s.Nil(err)
	s.NotNil(ret)
}

func (s *MetaImplTestSuite) TestGetUnknowInode() {
	ino := types.InodeID(123)
	s.localMeta.EXPECT().
		GetInode(ino).
		Return(nil)

	_, exist := s.meta.GetInode(ino)
	s.False(exist)
}

func (s *MetaImplTestSuite) TestRemoveInode() {
	ino := types.InodeID(123)
	s.localMeta.EXPECT().
		RemoveInode(ino).
		Return(nil)
	err := s.meta.RemoveInode(ino)
	s.Nil(err)
}

func (s *MetaImplTestSuite) TestRemoveDentry() {
	pino := types.RootInodeID
	name := mockName

	dentry := &types.Dentry{
		Name:   mockName,
		Parent: s.rootDentry,
		Inode:  &types.Inode{Ino: types.InodeID(123), Size: 1024},
	}
	dentry.SetTTL(time.Now().Add(time.Second * 10))

	s.localMeta.EXPECT().
		FindDentry(pino, name).
		Return(dentry, nil)

	s.storage.EXPECT().
		DeleteFile(&storage.DeleteFileRequest{Key: mockName}).
		Return(nil, nil)

	s.localMeta.EXPECT().
		RemoveDentry(pino, name)

	err := s.meta.RemoveDentry(pino, name)
	s.Nil(err)
}

func (s *MetaImplTestSuite) TestGetRootDentry() {
	s.localMeta.EXPECT().
		GetDentry(types.RootInodeID).
		Return(&types.Dentry{
			Inode: &types.Inode{Ino: types.RootInodeID},
		})

	dirent := s.meta.GetDentry(types.RootInodeID)
	s.NotNil(dirent)
	s.Equal("", dirent.Name)
}

func (s *MetaImplTestSuite) TestGetNormalDentry() {
	ino := types.InodeID(123)
	s.localMeta.EXPECT().
		GetDentry(ino).
		Return(&types.Dentry{
			Name:   mockName,
			Parent: s.rootDentry,
			Inode:  &types.Inode{Ino: ino},
		})

	dirent := s.meta.GetDentry(ino)
	s.NotNil(dirent)
	s.Equal(mockName, dirent.Name)
	s.Equal(types.RootInodeID, dirent.PIno)
}

func (s *MetaImplTestSuite) TestGetDentryPath() {
	ino := types.InodeID(123)
	s.localMeta.EXPECT().
		GetDentry(ino).
		Return(&types.Dentry{
			Name:   mockName,
			Parent: s.rootDentry,
			Inode:  &types.Inode{Ino: ino},
		})

	path, err := s.meta.GetDentryPath(ino)
	s.Nil(err)
	s.Equal(mockName, path)
}

func (s *MetaImplTestSuite) TestGetMultiLevelDentryPath() {
	// root->n1->n2
	parent := types.NewDentry("n1", &types.Inode{Ino: types.InodeID(10)})
	parent.Parent = s.rootDentry

	ino := types.InodeID(100)
	s.localMeta.EXPECT().
		GetDentry(ino).
		Return(&types.Dentry{
			Name:   "n2",
			Parent: parent,
			Inode:  &types.Inode{Ino: ino},
		})

	path, err := s.meta.GetDentryPath(ino)
	s.Nil(err)
	s.Equal("n1/n2", path)
}

// 创建一个已经存在的dentry
func (s *MetaImplTestSuite) TestCreateExistDentry() {
	target := &types.Dentry{
		Name:   mockName,
		Inode:  &types.Inode{Ino: 123, Size: 1024},
		Parent: s.rootDentry,
	}
	target.SetTTL(time.Now().Add(time.Second * 5))
	s.localMeta.EXPECT().
		FindDentry(types.RootInodeID, mockName).
		Return(target, nil)

	dirent, err := s.meta.CreateDentry(types.RootInodeID, mockName, os.FileMode(0755))
	s.Nil(err)
	s.Equal(target.Name, dirent.Name)
	s.Equal(target.Inode.Ino, dirent.Inode.Ino)
}

// refreshInode会触发headFile操作，并更新local的inode size
func (s *MetaImplTestSuite) TestRefreshInode() {
	ino := types.InodeID(123)
	target := &types.Dentry{
		Name:   mockName,
		Inode:  &types.Inode{Ino: 123},
		Parent: s.rootDentry,
	}
	s.localMeta.EXPECT().GetDentry(ino).Return(target)

	var expectSize uint64 = 1024
	s.storage.EXPECT().
		HeadFile(&storage.HeadFileRequest{Key: mockName}).
		Return(&storage.HeadFileReply{Info: storage.ObjectInfo{Size: expectSize}}, nil)

	s.localMeta.EXPECT().
		UpdateInode(ino, meta.InodeUpdateAttr{Size: &expectSize}, true)
	s.meta.RefreshInode(ino)
}

// 测试src不存在
func (s *MetaImplTestSuite) TestRenameSrcNotExist() {
	srcPino, dstPino := types.InodeID(1), types.InodeID(1)
	srcName, dstName := "test1", "test2"

	s.localMeta.EXPECT().
		FindDentry(srcPino, srcName).
		Return(nil, nil)

	err := s.meta.Rename(srcPino, srcName, dstPino, dstName)
	s.NotNil(err)
	s.Equal(types.ENOENT, err)
}

// 测试rename时srcDentry为文件且dstDentry不存在
func (s *MetaImplTestSuite) TestRenameFileWithDstNotExist() {
	srcPino, dstPino := types.InodeID(1), types.InodeID(1)
	srcName, dstName := "test1", "test2"
	srcDentry := &types.Dentry{Name: "test1", Inode: &types.Inode{Mode: os.ModePerm}}

	s.localMeta.EXPECT().
		FindDentry(srcPino, srcName).
		Return(srcDentry, nil)

	s.localMeta.EXPECT().
		GetDentry(dstPino).
		Return(s.rootDentry)

	// 先copy
	s.storage.EXPECT().Copy(&storage.CopyRequest{
		Src: "test1", Dst: "test2",
	}).Return(nil)

	// 再删除
	s.storage.EXPECT().
		DeleteFile(&storage.DeleteFileRequest{Key: "test1"}).
		Return(nil, nil)

	// 最后修改内存
	s.localMeta.EXPECT().Rename(srcPino, srcName, dstPino, dstName)

	err := s.meta.Rename(srcPino, srcName, dstPino, dstName)
	s.Nil(err)
}

// 测试rename时srcDentry为文件，dstDentry存在且为文件，则dstFile会被覆盖
func (s *MetaImplTestSuite) TestRenameFileWithDstFileExist() {
	srcPino, dstPino := types.InodeID(1), types.InodeID(1)
	srcName, dstName := "test1", "test2"

	srcDentry := &types.Dentry{Name: "test1", Inode: &types.Inode{Mode: os.ModePerm}}
	s.localMeta.EXPECT().
		FindDentry(srcPino, srcName).
		Return(srcDentry, nil)

	// 使dstDentry已存在且为文件
	dstParentDentry := types.NewDentry("", s.rootInode)
	dstParentDentry.AddChild("test2", &types.Inode{Mode: os.ModePerm})
	s.localMeta.EXPECT().
		GetDentry(dstPino).
		Return(dstParentDentry)

	// 先copy
	s.storage.EXPECT().Copy(&storage.CopyRequest{
		Src: "test1", Dst: "test2",
	}).Return(nil)

	// 再删除
	s.storage.EXPECT().
		DeleteFile(&storage.DeleteFileRequest{Key: "test1"}).
		Return(nil, nil)

	// 最后修改内存
	s.localMeta.EXPECT().
		Rename(srcPino, srcName, dstPino, dstName)

	err := s.meta.Rename(srcPino, srcName, dstPino, dstName)
	s.Nil(err)
}

// 测试rename时srcDentry为文件，dstDentry存在且为目录，则报错：not a directory
func (s *MetaImplTestSuite) TestRenameFileWithDstDirExist() {
	srcPino, dstPino := types.InodeID(1), types.InodeID(1)
	srcName, dstName := "test1", "test2"

	srcDentry := &types.Dentry{Name: "test1", Inode: &types.Inode{Mode: os.ModePerm}}
	s.localMeta.EXPECT().
		FindDentry(srcPino, srcName).
		Return(srcDentry, nil)

	// 使dstDentry已存在且为目录
	dstParentDentry := types.NewDentry("", s.rootInode)
	dstParentDentry.AddChild("test2", &types.Inode{Mode: os.ModeDir})
	s.localMeta.EXPECT().
		GetDentry(dstPino).
		Return(dstParentDentry)

	err := s.meta.Rename(srcPino, srcName, dstPino, dstName)
	s.Equal(types.ENOTDIR, err)
}

// 测试rename时srcDentry为目录，dstDentry不存在
func (s *MetaImplTestSuite) TestRenameDirWithDstNotExist() {
	srcPino, dstPino := types.InodeID(1), types.InodeID(1)
	srcName, dstName := "test1", "test2"

	srcDentry := types.NewDentry("test1", &types.Inode{Ino: types.InodeID(122), Mode: os.ModeDir})
	srcDentry.AddChild("child1", &types.Inode{})

	s.localMeta.EXPECT().
		FindDentry(srcPino, srcName).
		Return(srcDentry, nil)

	dstParentDentry := types.NewDentry("", s.rootInode)
	s.localMeta.EXPECT().
		GetDentry(dstPino).
		Return(dstParentDentry)

	// srcDentry是目录的情况下，会先触发拉文件列表
	s.storage.EXPECT().
		GetFileList(&storage.GetFileListRequest{Prefix: "test1/", Recursive: true, Max: 1000}).
		Return(&storage.GetFileListResp{FTree: &storage.FTreeNode{}}, nil)
	s.localMeta.EXPECT().BuildDentries(types.InodeID(122), gomock.Any())

	// 先处理目录底下的child dentry
	s.storage.EXPECT().Copy(&storage.CopyRequest{Src: "test1/child1", Dst: "test2/child1"}).Return(nil)
	s.storage.EXPECT().DeleteFile(&storage.DeleteFileRequest{Key: "test1/child1"}).Return(nil, nil)

	// 最后处理目录本身
	s.storage.EXPECT().Copy(&storage.CopyRequest{Src: "test1/", Dst: "test2/"}).Return(nil)
	s.storage.EXPECT().DeleteFile(&storage.DeleteFileRequest{Key: "test1/"}).Return(nil, nil)

	// 修改本地内存目录树
	s.localMeta.EXPECT().
		Rename(srcPino, srcName, dstPino, dstName)

	err := s.meta.Rename(srcPino, srcName, dstPino, dstName)
	s.Nil(err)
}

// 测试rename时srcDentry为目录，dstDentry存在且为文件
func (s *MetaImplTestSuite) TestRenameDirWithDstIsFile() {
	srcPino, dstPino := types.InodeID(1), types.InodeID(1)
	srcName, dstName := "test1", "test2"

	// src是目录
	srcDentry := types.NewDentry("test1", &types.Inode{Ino: types.InodeID(122), Mode: os.ModeDir})
	s.localMeta.EXPECT().
		FindDentry(srcPino, srcName).
		Return(srcDentry, nil)

	// 使dstDentry已存在且为文件
	dstParentDentry := types.NewDentry("", s.rootInode)
	dstParentDentry.AddChild("test2", &types.Inode{Mode: os.ModePerm})
	s.localMeta.EXPECT().
		GetDentry(dstPino).
		Return(dstParentDentry)

	err := s.meta.Rename(srcPino, srcName, dstPino, dstName)
	s.Equal(types.ENOTDIR, err)
}

// 测试rename时srcDentry为目录，dstDentry存在且为非空目录
func (s *MetaImplTestSuite) TestRenameDirWithDstIsAEmptyDir() {
	srcPino, dstPino := types.InodeID(1), types.InodeID(1)
	srcName, dstName := "test1", "test2"

	// src是目录
	srcDentry := types.NewDentry("test1", &types.Inode{Ino: types.InodeID(122), Mode: os.ModeDir})
	s.localMeta.EXPECT().
		FindDentry(srcPino, srcName).
		Return(srcDentry, nil)

	// 使dstDentry已存在且为非空目录
	dstParentDentry := types.NewDentry("", s.rootInode)
	dstDentry := dstParentDentry.AddChild("test2", &types.Inode{Mode: os.ModeDir})
	dstDentry.AddChild("subfile", &types.Inode{Mode: os.ModePerm})

	s.localMeta.EXPECT().
		GetDentry(dstPino).
		Return(dstParentDentry)

	err := s.meta.Rename(srcPino, srcName, dstPino, dstName)
	s.Equal(types.ENOTEMPTY, err)
}

func (s *MetaImplTestSuite) TestLocalMetaUpdateInode() {
	local, err := meta.NewMemMeta(meta.MetaOption{})
	s.Nil(err)

	ino := types.InodeID(123)
	inode := &types.Inode{
		Ino:  ino,
		Mode: os.FileMode(0777),
		Uid:  4,
		Gid:  6,
	}
	_, err = local.CreateDentry(mockPino, mockName, inode)
	s.Nil(err)

	updateFileds := meta.InodeUpdateAttr{
		Mode: &mockFMode,
		Gid:  &mockGID,
		Uid:  &mockUID,
	}

	retInode, err := local.UpdateInode(ino, updateFileds, true)
	s.Nil(err)

	s.Equal(mockGID, retInode.Gid)
	s.Equal(mockUID, retInode.Uid)
	s.Equal(mockFMode, retInode.Mode)
}

func (s *MetaImplTestSuite) TestUpdateInode() {
	s.localMeta.EXPECT().
		GetDentry(mockInodeID).
		Return(mockDentry)

	s.storage.EXPECT().
		HeadFile(&storage.HeadFileRequest{Key: mockName}).
		Return(&storage.HeadFileReply{
			Info: storage.ObjectInfo{
				Metadata: map[string]string{
					"k1": "v1",
				},
			},
		}, nil)

	s.storage.EXPECT().
		PutFile(&storage.PutFileRequest{
			Buf: types.DefaultNullBuf, Key: mockName,
			MetaData: map[string]string{
				"k1":                       "v1",
				"X-Amz-Meta-Fsgid":         "16",
				"X-Amz-Meta-Fsuid":         "8",
				"X-Amz-Meta-Fsperm":        "493",
				"X-Amz-Copy-Source":        "/mockBucketName/testname",
				"X-Amz-Metadata-Directive": "REPLACE",
			},
		}).
		Return(&storage.PutFileReply{}, nil)

	updateFileds := meta.InodeUpdateAttr{
		Mode: &mockFMode,
		Gid:  &mockUID,
		Uid:  &mockGID,
	}

	s.localMeta.EXPECT().
		UpdateInode(mockInodeID, updateFileds, true).
		Return(&types.Inode{}, nil)

	_, err := s.meta.UpdateInode(mockInodeID, updateFileds, false)
	s.Nil(err)
}
