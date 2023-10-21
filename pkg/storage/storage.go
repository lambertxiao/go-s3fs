package storage

import (
	"io"
	"time"
)

var Instance Storage

func InitStorage(sto Storage) {
	Instance = sto
}

type Storage interface {
	HeadFile(*HeadFileRequest) (*HeadFileReply, error)
	PutFile(*PutFileRequest) (*PutFileReply, error)
	ListObjects(*ListObjectsRequest) (*ListObjectsReply, error)
	GetFileList(*GetFileListRequest) (*GetFileListResp, error)
	InitMPut(*InitMPutRequest) (*InitMPutReply, error)
	MputUpload(*MPutUploadRequest) (*MPutUploadReply, error)
	MputAbort(*MPutAbortRequest) (*MPutAbortReply, error)
	MputFinish(*MPutFinishRequest) (*MPutFinishReply, error)
	GetFile(*GetFileRequest) (*GetFileReply, error)
	DeleteFile(*DeleteFileRequest) (*DeleteFileReply, error)
	Rename(*RenameRequest) (*RenameReply, error)
	Copy(*CopyRequest) error
	Restore(*RestoreRequest) (*RestoreReply, error)
}

type ListBackend int

const (
	LBFileMGR ListBackend = iota
	LBListProxy
)

type ListObjectsRequest struct {
	Delimiter string
	Prefix    string
	Max       uint32
	Marker    string
}
type ListObjectsReply struct {
	IsTrunc        bool
	Objects        []*ObjectInfo
	CommonPrefixes []string
	Marker         string
}

type GetFileListRequest struct {
	Prefix    string
	Max       uint32
	Marker    string
	Recursive bool // true会拉取prefix下的所有子孙目录
}

type GetFileListResp struct {
	IsTrunc bool
	Marker  string
	FTree   *FTreeNode
}

type FTreeNodeType int

const (
	FTTFile FTreeNodeType = iota
	FTTDir
)

type FTreeNode struct {
	Name string // 节点名称，目录名/文件名
	// Path     string                // 节点路径
	Typ      FTreeNodeType         // 目录/文件
	Children map[string]*FTreeNode // 子节点，当typ是目录是有效
	Oinfo    ObjectInfo
}

type ObjectInfo struct {
	Key           string
	Size          uint64
	Mtime         time.Time
	Ctime         time.Time
	Uid           *uint32
	Gid           *uint32
	Mode          *uint32
	Metadata      map[string]string
	Storage_class string
}

type HeadFileRequest struct {
	Key string
}
type HeadFileReply struct {
	Info  ObjectInfo
	IsDir bool
}

type PutFileRequest struct {
	Buf      io.ReadSeeker
	BufLen   int // only for statistic
	Key      string
	MetaData map[string]string
}
type PutFileReply struct {
	Etag string
}

type StatImpl interface {
	getEtags() map[int]string
}

type MPutStat struct {
	PartSize int
	Stat     interface{}
	MputId   string
}

//func (stat *MPutStat) getEtags() map[int]string {
//	return stat.getEtags()
//}

type InitMPutRequest struct {
	Key string
}

type InitMPutReply struct {
	Stat *MPutStat
}

type MPutUploadRequest struct {
	Stat   *MPutStat
	PartNo int
	Data   *MPutFile
	Key    string
}

type MPutFile struct {
	Buffer []byte
	IsLast bool
}

type MPutUploadReply struct {
	Etags       []string
	PartSizeErr bool
}

type MPutAbortRequest struct {
	Key  string
	Stat *MPutStat
}
type MPutAbortReply struct {
}

type MPutFinishRequest struct {
	Key  string
	Stat *MPutStat
}

type MPutFinishReply struct {
}

type GetFileRequest struct {
	Offset uint64
	Length int
	Key    string
}

type GetFileReply struct {
	Body io.ReadCloser
}

type DeleteFileRequest struct {
	Key string
}

type DeleteFileReply struct {
}

type RenameRequest struct {
	Src string
	Dst string
}
type RenameReply struct {
}

type CopyRequest struct {
	Src string
	Dst string
}

type RestoreRequest struct {
	Name string
}
type RestoreReply struct{}
