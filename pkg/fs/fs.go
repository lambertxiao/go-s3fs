package fs

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/common"
	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/fs/freader"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/meta"
	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
)

// file system runtime
type FSR struct {
	meta          meta.Meta
	mutex         sync.RWMutex
	nextFhID      HandleID
	openFiles     map[HandleID]*OpenFile
	ino2OpenFiles map[types.InodeID]map[HandleID]*OpenFile
	openDirs      map[HandleID]*OpenDir
	clock         common.Clock

	fcacheMap map[types.InodeID]*freader.FileCache

	registry *prometheus.Registry

	// 采集指标
	handlersGause             prometheus.GaugeFunc
	readSizeHistogram         prometheus.Histogram
	writtenSizeHistogram      prometheus.Histogram
	opsDurationsHistogram     *prometheus.HistogramVec
	readCacheHitSizeHistogram prometheus.Histogram

	fsInflightReadCntGauge   prometheus.GaugeFunc
	objInflightReadCntGauge  prometheus.GaugeFunc
	objInflightWriteCntGauge prometheus.GaugeFunc

	fsInflightReadCnt   int64
	objInflightReadCnt  int64
	objInflightWriteCnt int64

	reloadCfgCallback func(fs *FSR)
}

func InitFS(
	conf *config.FSConfig,
	bucket string,
	registry *prometheus.Registry,
	registerer prometheus.Registerer,
	reloadCfgCallback func(fs *FSR),
) (*FSR, error) {
	stowr, err := storage.NewS3Storage(conf.StorageConfig, registerer)
	if err != nil {
		return nil, err
	}

	storage.InitStorage(stowr)

	// if !conf.StorageConfig.No_check {
	// 	err = stowr.CheckConnectable()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	start := time.Now()
	metaOpt := meta.MetaOption{
		GID:                    conf.Gid,
		UID:                    conf.Uid,
		DCacheTTL:              conf.Dcache_ttl,
		SkipNotDirLookup:       conf.Skip_ne_dir_lookup,
		DisableCheckVirtualDir: conf.Disable_check_vdir,
		Bucket:                 conf.StorageConfig.Bucket,
		BucketPrefix:           conf.Prefix,
		AllowOther:             conf.Allow_other,
		MpMask:                 conf.MpMask,
	}
	localMeta, err := meta.NewMemMeta(metaOpt)
	if err != nil {
		return nil, err
	}

	metaSvc, err := meta.NewMeta(metaOpt, localMeta, stowr)
	if err != nil {
		return nil, err
	}

	end := time.Now()
	logg.Dlog.Info("init meta cost time:", end.Unix()-start.Unix())

	fsx, err := NewFS(metaSvc, common.NewDefaultClock(), registry, registerer)
	if err != nil {
		return nil, err
	}

	fsx.reloadCfgCallback = reloadCfgCallback
	return fsx, nil
}

func (fs *FSR) ReloadFS(newCfg *config.FSConfig) {
	// bucket和bucketPrefix不允许修改
	fs.meta.ReloadCfg(meta.MetaOption{
		GID:                    newCfg.Gid,
		UID:                    newCfg.Uid,
		DCacheTTL:              newCfg.Dcache_ttl,
		SkipNotDirLookup:       newCfg.Skip_ne_dir_lookup,
		DisableCheckVirtualDir: newCfg.Disable_check_vdir,
	})
}

func NewFS(
	meta meta.Meta,
	clock common.Clock,
	registry *prometheus.Registry,
	registerer prometheus.Registerer,
) (*FSR, error) {
	fs := &FSR{
		meta:          meta,
		openFiles:     make(map[HandleID]*OpenFile),
		ino2OpenFiles: make(map[types.InodeID]map[HandleID]*OpenFile),
		openDirs:      make(map[HandleID]*OpenDir),
		fcacheMap:     make(map[types.InodeID]*freader.FileCache),
		clock:         clock,
		registry:      registry,
	}
	fs.initMetrics(registerer)
	fs.initSpecialInode()

	if config.GetGConfig().Enable_load_dentries {
		op := &OpenDirOp{Inode: types.RootInodeID}
		err := fs.openDir(context.Background(), op)
		if err != nil {
			logg.Dlog.Error(err)
		} else {
			logg.Dlog.Info("auto load dentries")
			go func() {
				err = fs.readDir(context.Background(), op.Handle, 0, nil)
				if err != nil {
					logg.Dlog.Error(err)
				} else {
					logg.Dlog.Info("auto load dentries done")
				}
			}()
		}
	}

	common.InitParallelPool(config.GetGConfig().Parallel)

	return fs, nil
}

func (fs *FSR) initMetrics(registerer prometheus.Registerer) {
	fs.readSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fuse_read_size_bytes",
		Help:    "size of read distributions.",
		Buckets: prometheus.LinearBuckets(4096, 4096, 32),
	})

	fs.writtenSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fuse_written_size_bytes",
		Help:    "size of write distributions.",
		Buckets: prometheus.LinearBuckets(4096, 4096, 32),
	})

	fs.opsDurationsHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fuse_ops_durations_histogram_seconds",
		Help:    "Operations latency distributions.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 1.5, 30),
	}, []string{"method"})

	fs.handlersGause = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "fuse_open_handlers",
		Help: "number of open files.",
	}, func() float64 {
		fs.mutex.RLock()
		defer fs.mutex.RUnlock()
		return float64(len(fs.openFiles))
	})

	fs.readCacheHitSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "read_cache_hit_size_bytes",
		Help:    "size of read distributions.",
		Buckets: prometheus.LinearBuckets(4096, 4096, 32),
	})

	fs.fsInflightReadCntGauge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "fs_inflight_read_cnt",
		Help: "fs_inflight_read_cnt",
	}, func() float64 {
		return float64(atomic.LoadInt64(&fs.fsInflightReadCnt))
	})

	fs.objInflightReadCntGauge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "storage_inflight_read_cnt",
		Help: "storage inflight read cnt",
	}, func() float64 {
		return float64(atomic.LoadInt64(&fs.objInflightReadCnt))
	})

	fs.objInflightWriteCntGauge = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "storage_inflight_write_cnt",
		Help: "storage inflight write cnt",
	}, func() float64 {
		return float64(atomic.LoadInt64(&fs.objInflightWriteCnt))
	})

	if registerer == nil {
		return
	}

	registerer.MustRegister(
		fs.readSizeHistogram,
		fs.writtenSizeHistogram,
		fs.opsDurationsHistogram,
		fs.readCacheHitSizeHistogram,
		fs.handlersGause,
		fs.fsInflightReadCntGauge,
		fs.objInflightReadCntGauge,
		fs.objInflightWriteCntGauge,
	)
}

type FS_OP uint8

const (
	FS_OP_WRITE FS_OP = iota + 0
	FS_OP_READ
	FS_OP_OTHER
)

func (fs *FSR) observeOP(op FS_OP, beginTime time.Time) {
	var opstr string
	if op == FS_OP_READ {
		opstr = "READ"
	} else if op == FS_OP_WRITE {
		opstr = "WRITE"
	} else {
		opstr = "OTHER"
	}

	fs.opsDurationsHistogram.WithLabelValues(opstr).Observe(float64(time.Since(beginTime).Seconds()))
}

func (fs *FSR) statFS(ctx context.Context, op *StatFSOp) error {
	const BLOCK_SIZE = 4096
	const TOTAL_SPACE = 1 * 1024 * 1024 * 1024 * 1024 * 1024 // 1PB
	const TOTAL_BLOCKS = TOTAL_SPACE / BLOCK_SIZE
	const INODES = 1 * 1000 * 1000 * 1000 // 1 billion
	op.BlockSize = BLOCK_SIZE
	op.Blocks = TOTAL_BLOCKS
	op.BlocksFree = TOTAL_BLOCKS
	op.BlocksAvailable = TOTAL_BLOCKS
	op.IoSize = 1 * 1024 * 1024 // 1MB
	op.Inodes = INODES
	op.InodesFree = INODES
	return nil
}

func (fs *FSR) createSymlink(ctx context.Context) error {
	logg.Dlog.Warnf("ln -s ENOSYS")
	return types.ENOSYS
}

func (fs *FSR) createLink(ctx context.Context) error {
	logg.Dlog.Warnf("ln ENOSYS")
	return types.ENOSYS
}

// only support file to file
func (fs *FSR) rename(ctx context.Context, op *RenameOp) error {
	if config.GetGConfig().DisableRemove {
		return types.EPERM
	}

	if config.GetGConfig().ReadAfterWriteFinish {
		srcDentry, err := fs.meta.FindDentry(op.OldParent, op.OldName, false)
		if err != nil || srcDentry == nil {
			return types.ENOENT
		}
		fs.mutex.Lock()
		ofs := fs.ino2OpenFiles[srcDentry.Inode.Ino]
		fs.mutex.Unlock()

		for _, of := range ofs {
			logg.Dlog.Infof("readfile wait write done, ino:%d hid:%d", of.ino, of.hid)
			of.WaitWriteDone()
		}
	}

	err := fs.meta.Rename(op.OldParent, op.OldName, op.NewParent, op.NewName)
	if err != nil {
		logg.Dlog.Errorf("rename srcPino:%d srcName:%s dstPino:%d dstName:%s err:%+v", op.OldParent, op.OldName, op.NewParent, op.NewName, err)
		return err
	}

	return nil
}

func (fs *FSR) readSymlink(ctx context.Context) error {
	logg.Dlog.Warnf("symlink not support")
	return types.ENOSYS
}

func (fs *FSR) fallocate(ctx context.Context) error {
	logg.Dlog.Warnf("fallocate not support")
	return types.ENOSYS
}

func (fs *FSR) destroy() {
	logg.Dlog.Infof("destroy")
	fs.meta.Destory()
}

func (fs *FSR) withLock(f func()) {
	fs.mutex.Lock()
	f()
	fs.mutex.Unlock()
}

const O_ACCMODE = 0x3

func ForWrite(flags uint32) bool {
	switch flags & O_ACCMODE {
	case syscall.O_WRONLY, syscall.O_RDWR:
		return true
	}
	return false
}
