package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/types"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	S3_META_UID  = "x-amz-meta-uid"
	S3_META_GID  = "x-amz-meta-gid"
	S3_META_PERM = "x-amz-meta-perm"
)

const (
	SETATTR_UID  = "X-Amz-Meta-Fsuid"
	SETATTR_GID  = "X-Amz-Meta-Fsgid"
	SETATTR_PERM = "X-Amz-Meta-Fsperm"

	SETATTR_MAGIC  = "X-Amz-Metadata-Directive"
	SETATTR_MAGIC2 = "X-Amz-Copy-Source"

	STORAGE_STANDARD = 1
	STORAGE_IA       = 2
	STORAGE_ARCHIVE  = 3
	STORAGE_UNKNOWN  = 4
)

func GetStorageClass(storage string) int {
	switch storage {
	case "STANDARD":
		return STORAGE_STANDARD
	case "IA":
		return STORAGE_IA
	case "ARCHIVE":
		return STORAGE_ARCHIVE
	default:
		return STORAGE_UNKNOWN
	}
}

const RETRY_INTERVAL = 500 * time.Millisecond

type S3Storage struct {
	minioClient *minio.Core
	conf        config.StorageConf

	objectReqsHistogram *prometheus.HistogramVec
	objectDataBytes     *prometheus.CounterVec
}

func NewS3Storage(conf config.StorageConf, reg prometheus.Registerer) (*S3Storage, error) {
	sto := &S3Storage{
		conf: conf,
	}

	minioClient, err := minio.NewCore(conf.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(conf.AccessKey, conf.SecertKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}

	sto.minioClient = minioClient
	sto.initMetrics(reg)
	return sto, nil
}

func (s *S3Storage) initMetrics(reg prometheus.Registerer) {
	s.objectReqsHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "object_request_durations_histogram_seconds",
		Help:    "Object requests latency distributions.",
		Buckets: prometheus.ExponentialBuckets(0.01, 1.5, 25),
	}, []string{"method"})

	s.objectDataBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "object_request_data_bytes",
		Help: "Object requests size in bytes.",
	}, []string{"method"})

	if reg == nil {
		return
	}

	reg.MustRegister(s.objectReqsHistogram)
	reg.MustRegister(s.objectDataBytes)
}

func (u *S3Storage) ReloadCfg(cfg config.StorageConf) {}

func (u *S3Storage) ListObjects(req *ListObjectsRequest) (reply *ListObjectsReply, err error) {
	return u.list_objects(req)
}

func (u *S3Storage) GetFileList(req *GetFileListRequest) (reply *GetFileListResp, err error) {
	return u.list_objects_v2(req)
}

func (u *S3Storage) HeadFile(req *HeadFileRequest) (reply *HeadFileReply, err error) {
	return u.innerheadFile(req)
}

func (u *S3Storage) innerheadFile(req *HeadFileRequest) (*HeadFileReply, error) {
	obj, err := u.minioClient.StatObject(context.Background(), u.conf.Bucket, req.Key, minio.StatObjectOptions{})
	if err != nil {
		if err.Error() == "The specified key does not exist." {
			return nil, types.ENOENT
		}
		return nil, err
	}

	reply := &HeadFileReply{}

	reply.IsDir = strings.HasSuffix(req.Key, "/")
	reply.Info.Key = req.Key
	reply.Info.Size = uint64(obj.Size)
	reply.Info.Mtime = obj.LastModified

	if len(obj.Metadata.Get(S3_META_GID)) > 0 {
		gid_val, err := strconv.ParseUint(obj.Metadata.Get(S3_META_GID), 10, 32)
		if err != nil {
			logg.Dlog.Errorf("HeadFile parse Gid error: %v", err)
		} else {
			gid32 := uint32(gid_val)
			reply.Info.Gid = &gid32
		}
	}

	if len(obj.Metadata.Get(S3_META_UID)) > 0 {
		uid_val, err := strconv.ParseUint(obj.Metadata.Get(S3_META_UID), 10, 32)
		if err != nil {
			logg.Dlog.Errorf("HeadFile parse Uid error: %v", err)
		} else {
			uid32 := uint32(uid_val)
			reply.Info.Uid = &uid32
		}
	}

	if len(obj.Metadata.Get(S3_META_PERM)) > 0 {
		perm_val, err := strconv.ParseUint(obj.Metadata.Get(S3_META_PERM), 10, 32)
		if err != nil {
			logg.Dlog.Errorf("HeadFile parse Perm error: %v", err)
		} else {
			perm := uint32(perm_val)
			reply.Info.Mode = &perm
			if (os.FileMode(perm) & os.ModeDir) > 0 {
				reply.IsDir = true
			}
		}
	}

	reply.Info.Metadata = make(map[string]string)
	for k, v := range obj.Metadata {
		if strings.HasPrefix(k, "x-amz-meta-") {
			reply.Info.Metadata[k] = v[0]
		}
	}

	reply.Info.Storage_class = obj.StorageClass
	reply.Info.Ctime = obj.LastModified
	return reply, nil
}

func (u *S3Storage) PutFile(req *PutFileRequest) (*PutFileReply, error) {
	st := time.Now()

	mimeType := getMime(req.Key)
	logg.Dlog.Infof("putFile %s with mimeType %s", req.Key, mimeType)

	info, err := u.minioClient.PutObject(
		context.Background(),
		u.conf.Bucket,
		req.Key,
		req.Buf,
		int64(req.BufLen),
		"", "",
		minio.PutObjectOptions{
			UserMetadata: req.MetaData,
			ContentType:  mimeType,
		},
	)

	if err != nil {
		return nil, err
	}

	reply := &PutFileReply{
		Etag: info.ETag,
	}

	if req.BufLen != 0 {
		used := time.Since(st)
		u.objectReqsHistogram.WithLabelValues("WRITE").Observe(used.Seconds())
		u.objectDataBytes.WithLabelValues("WRITE").Add(float64(req.BufLen))
	}
	return reply, nil
}

func (u *S3Storage) InitMPut(req *InitMPutRequest) (reply *InitMPutReply, err error) {
	return u.innerInitMPut(req)
}

func (u *S3Storage) innerInitMPut(req *InitMPutRequest) (*InitMPutReply, error) {
	mimeType := getMime(req.Key)
	uploadId, err := u.minioClient.NewMultipartUpload(
		context.Background(), u.conf.Bucket, req.Key,
		minio.PutObjectOptions{
			StorageClass: u.conf.Storage_class,
			ContentType:  mimeType,
		},
	)

	if err != nil {
		logg.Dlog.Errorf("init mput %v %v", req.Key, err)
		return nil, err
	}

	parts := []minio.CompletePart{}
	reply := &InitMPutReply{
		Stat: &MPutStat{
			MputId:   uploadId,
			PartSize: 1024 * 1024 * 8,
			Stat:     parts,
		},
	}

	return reply, nil
}

func (u *S3Storage) MputUpload(req *MPutUploadRequest) (*MPutUploadReply, error) {
	logg.Dlog.Debugf("uploadPart key:%s partNum:%v", req.Key, req.PartNo)
	part, err := u.minioClient.PutObjectPart(
		context.Background(),
		u.conf.Bucket,
		req.Key,
		req.Stat.MputId,
		req.PartNo+1,
		bytes.NewReader(req.Data.Buffer),
		int64(len(req.Data.Buffer)),
		minio.PutObjectPartOptions{},
	)
	if err != nil {
		return nil, err
	}
	st := time.Now()

	used := time.Since(st)
	u.objectReqsHistogram.WithLabelValues("WRITE").Observe(used.Seconds())
	u.objectDataBytes.WithLabelValues("WRITE").Add(float64(len(req.Data.Buffer)))

	reply := &MPutUploadReply{
		Etags: make([]string, 0),
	}

	parts := req.Stat.Stat.([]minio.CompletePart)
	parts = append(parts, minio.CompletePart{
		PartNumber: part.PartNumber,
		ETag:       part.ETag,
	})

	reply.Etags = append(reply.Etags, part.ETag)
	return reply, nil
}

func (u *S3Storage) MputAbort(req *MPutAbortRequest) (*MPutAbortReply, error) {
	err := u.minioClient.AbortMultipartUpload(
		context.Background(),
		u.conf.Bucket,
		req.Key,
		req.Stat.MputId,
	)

	if err == nil {
		reply := &MPutAbortReply{}
		return reply, nil
	}
	return nil, err
}

func (u *S3Storage) MputFinish(req *MPutFinishRequest) (*MPutFinishReply, error) {
	if req.Stat == nil || req.Stat.Stat == nil {
		return nil, errors.New("mput not init")
	}

	parts := req.Stat.Stat.([]minio.CompletePart)

	_, err := u.minioClient.CompleteMultipartUpload(
		context.Background(),
		u.conf.Bucket,
		req.Key,
		req.Stat.MputId,
		parts,
		minio.PutObjectOptions{},
	)

	if err == nil {
		reply := &MPutFinishReply{}
		return reply, nil
	}
	return nil, err
}

func (u *S3Storage) GetFile(req *GetFileRequest) (*GetFileReply, error) {
	st := time.Now()
	logg.Dlog.Debugf("get_range %v~%v", req.Offset, req.Length)

	options := minio.GetObjectOptions{}
	options.SetRange(int64(req.Offset), int64(req.Offset)+int64(req.Length-1))

	var err error
	var r io.ReadCloser
	var obj minio.ObjectInfo

	for i := 0; i < config.GetGConfig().Retry; i++ {
		r, obj, _, err = u.minioClient.GetObject(
			context.Background(),
			u.conf.Bucket,
			req.Key,
			options,
		)
		if err != nil {
			logg.Dlog.Errorf("GetFile Error: %v; retry", err)
			time.Sleep(time.Duration((i+1)*500) * time.Millisecond)
			continue
		}

		reply := &GetFileReply{
			Body: r,
		}

		used := time.Since(st)
		u.objectReqsHistogram.WithLabelValues("READ").Observe(used.Seconds())
		u.objectDataBytes.WithLabelValues("READ").Add(float64(obj.Size))
		return reply, nil
	}

	return nil, err
}

func (u *S3Storage) DeleteFile(req *DeleteFileRequest) (*DeleteFileReply, error) {
	logg.Dlog.Infof("deleteBlob %v", req.Key)
	err := u.minioClient.RemoveObject(
		context.Background(),
		u.conf.Bucket,
		req.Key,
		minio.RemoveObjectOptions{},
	)
	return nil, err
}

func (u *S3Storage) Rename(req *RenameRequest) (*RenameReply, error) {
	return nil, errors.New("rename not supported")
}

func (u *S3Storage) Restore(req *RestoreRequest) (*RestoreReply, error) {
	return nil, errors.New("restore not supported")
}

func (u *S3Storage) make_request(f func() error) error {
	interval := RETRY_INTERVAL
	var err error
	for i := 0; i <= config.GetGConfig().Retry; i++ {
		err = f()
		if err == nil {
			break
		}
		switch err {
		case types.ENOENT, syscall.EPERM:
			return err
		}
		time.Sleep(interval)
		interval = interval * time.Duration(2)
	}
	return err
}

func (u *S3Storage) list_objects(req *ListObjectsRequest) (*ListObjectsReply, error) {
	resp, err := u.minioClient.ListObjectsV2(
		u.conf.Bucket,
		req.Prefix,
		"",
		req.Marker,
		req.Delimiter,
		int(req.Max),
	)

	if err != nil {
		logg.Dlog.Errorf("listObjects %v", err)
		return nil, err
	}
	reply := &ListObjectsReply{
		Objects:        make([]*ObjectInfo, 0, len(resp.Contents)),
		CommonPrefixes: make([]string, 0, len(resp.CommonPrefixes)),
	}

	hits := make(map[string]struct{}, len(resp.CommonPrefixes))
	for _, i := range resp.CommonPrefixes {
		reply.CommonPrefixes = append(reply.CommonPrefixes, i.Prefix)
		l := len(i.Prefix)
		if l > 0 {
			hits[i.Prefix[:len(i.Prefix)-1]] = struct{}{}
		}
	}
	for _, obj := range resp.Contents {
		if _, exist := hits[obj.Key]; exist {
			continue
		}

		obj := &ObjectInfo{
			Key:           obj.Key,
			Size:          uint64(obj.Size),
			Mtime:         obj.LastModified,
			Ctime:         obj.LastModified,
			Metadata:      obj.UserMetadata,
			Storage_class: obj.StorageClass,
		}
		// for k, v := range obj.Metadata {
		// 	if k == GO_S3FSGID {
		// 		gid, err := strconv.ParseUint(v, 10, 32)
		// 		if err != nil {
		// 			logg.Dlog.Errorf("ls parse gid error, key=%v, v=%v, err=%v", k, v, err)
		// 			continue
		// 		} else {
		// 			gid32 := uint32(gid)
		// 			obj.Gid = &gid32
		// 		}
		// 	} else if k == GO_S3FSUID {
		// 		uid, err := strconv.ParseUint(v, 10, 32)
		// 		if err != nil {
		// 			logg.Dlog.Errorf("ls parse uid error, key=%v, v=%v, err=%v", k, v, err)
		// 			continue
		// 		} else {
		// 			uid32 := uint32(uid)
		// 			obj.Uid = &uid32
		// 		}
		// 	} else if k == GO_S3FSPERM {
		// 		perm, err := strconv.ParseUint(v, 10, 32)
		// 		if err != nil {
		// 			logg.Dlog.Errorf("ls parse perm error, key=%v, v=%v, err=%v", k, v, err)
		// 			continue
		// 		} else {
		// 			perm32 := uint32(perm)
		// 			obj.Mode = &perm32
		// 		}
		// 	}
		// }
		reply.Objects = append(reply.Objects, obj)
	}
	reply.IsTrunc = resp.IsTruncated
	reply.Marker = resp.NextContinuationToken
	return reply, nil
}

// todo 修改接口名称
func (u *S3Storage) list_objects_v2(req *GetFileListRequest) (*GetFileListResp, error) {
	resp, err := u.minioClient.ListObjectsV2(
		u.conf.Bucket,
		req.Prefix,
		"",
		req.Marker,
		"/",
		int(req.Max),
	)

	if err != nil {
		logg.Dlog.Errorf("listObjects %v", err)
		return nil, err
	}
	fTree := &FTreeNode{
		Typ: FTTDir,
		// Path:     req.Prefix,
		Children: make(map[string]*FTreeNode),
	}
	reply := &GetFileListResp{
		FTree: fTree,
	}

	currNode := fTree
	// listobject只处理下一级目录
	for _, i := range resp.CommonPrefixes {
		dirName := i.Prefix[len(req.Prefix) : len(i.Prefix)-1]
		// 忽略 "/" 目录
		if dirName == "" {
			continue
		}
		dirNode := &FTreeNode{
			Name: dirName,
			// Path: req.Prefix + dirName,
			Typ: FTTDir,
		}
		currNode.Children[dirName] = dirNode
	}

	// listobject只处理下一级文件
	for _, i := range resp.Contents {
		if err != nil {
			return nil, err
		}
		fileName := i.Key[len(req.Prefix):]
		obj := &ObjectInfo{
			Key:           i.Key,
			Size:          uint64(i.Size),
			Mtime:         i.LastModified,
			Ctime:         i.LastModified,
			Metadata:      i.UserMetadata,
			Storage_class: i.StorageClass,
		}

		// for k, v := range obj.Metadata {
		// 	if k == GO_S3FSGID {
		// 		gid, err := strconv.ParseUint(v, 10, 32)
		// 		if err != nil {
		// 			logg.Dlog.Errorf("ls parse gid error, key=%v, v=%v, err=%v", k, v, err)
		// 			continue
		// 		} else {
		// 			gid32 := uint32(gid)
		// 			obj.Gid = &gid32
		// 		}
		// 	} else if k == GO_S3FSUID {
		// 		uid, err := strconv.ParseUint(v, 10, 32)
		// 		if err != nil {
		// 			logg.Dlog.Errorf("ls parse uid error, key=%v, v=%v, err=%v", k, v, err)
		// 			continue
		// 		} else {
		// 			uid32 := uint32(uid)
		// 			obj.Uid = &uid32
		// 		}
		// 	} else if k == GO_S3FSPERM {
		// 		perm, err := strconv.ParseUint(v, 10, 32)
		// 		if err != nil {
		// 			logg.Dlog.Errorf("ls parse perm error, key=%v, v=%v, err=%v", k, v, err)
		// 			continue
		// 		} else {
		// 			perm32 := uint32(perm)
		// 			obj.Mode = &perm32
		// 		}
		// 	}
		// }

		node := &FTreeNode{
			Name:  fileName,
			Typ:   FTTFile,
			Oinfo: *obj,
		}
		currNode.Children[fileName] = node
	}

	reply.IsTrunc = resp.IsTruncated
	reply.Marker = resp.ContinuationToken
	return reply, nil
}

func (u *S3Storage) Copy(req *CopyRequest) error {
	logg.Dlog.Infof("copy %s to %s", req.Src, req.Dst)

	_, err := u.minioClient.CopyObject(
		context.Background(),
		u.conf.Bucket,
		req.Src,
		u.conf.Bucket,
		req.Dst,
		nil,
		minio.CopySrcOptions{},
		minio.PutObjectOptions{},
	)
	return err
}
