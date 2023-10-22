package config

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type StorageConf struct {
	Bucket    string
	AccessKey string
	SecertKey string
	Endpoint  string
	Hosts     []string

	Storage_class string
}

type FSConfig struct {
	ModifiedTime  time.Time
	StorageConfig StorageConf
	MountPoint    string
	Foreground    bool

	// fuse
	Entry_ttl          time.Duration
	Attr_ttl           time.Duration
	Disable_async_read bool
	Writeback          bool
	Async_dio          bool
	// fuse -o
	Allow_other bool

	// winfsp
	// in seconds
	Dir_info_ttl     int
	File_info_ttl    int
	Volume_info_ttl  int
	Case_insensitive bool

	// fuse & winfsp
	Keep_pagecache bool
	DebugFuse      bool

	// os
	Dcache_ttl         time.Duration
	Retry              int
	Parallel           int
	DisableRemove      bool
	Etag               int
	Passwd_file        string
	Enable_md5         bool
	Uid                uint32
	Gid                uint32
	MpMask             uint32
	Disable_check_vdir bool
	MaxPutSize         uint64
	Prefix             string

	Readahead uint64

	MaxCachePerFile uint64

	Log_level logrus.Level

	FuseOptions map[string]string

	Enable_direct_read bool

	Skip_ne_dir_lookup bool

	LogDir          string
	LogMaxAge       time.Duration
	LogRotationTime time.Duration

	Enable_load_dentries bool
	MaxLocalFileSize     int64

	// 由于部分writer是在release的时候才结束写入，而release本身是异步的
	// 所以存在刚写入的文件马上来读会读取不到文件内容，因为此时写入可能还未结束
	// 针对这种场景，给定一个可选的功能，可以让读端等待写入全部结束后才开始读
	// 由于一个文件的fd可以被dup多个，如果有某个dup出来的fd被close了，fuse会发起flush请求，
	// 这种情况下不应该在flush的时候去结束整个写入流程，因为其余的fd还可能继续写入数据。
	// 由于s3服务端目前不支持追加写入，因此针对这种场景，给定一个可选的功能，在release时结束写入整个写入流程。
	// 并配合ReadAfterWriteFinish参数保证在写入后马上读，能读到正确的内容
	EnableAsyncFlush bool
}

var (
	fsConfig *FSConfig
	cfgLock  sync.RWMutex
)

func GetGConfig() *FSConfig {
	cfgLock.RLock()
	defer cfgLock.RUnlock()

	return fsConfig
}

func SetGConfig(cfg *FSConfig) {
	cfgLock.Lock()
	defer cfgLock.Unlock()

	fsConfig = cfg
}

type BucketConfig struct {
	Access_key           string   `yaml:"access_key"`
	Secret_key           string   `yaml:"secret_key"`
	Endpoint             string   `yaml:"endpoint"`
	Hosts                []string `yaml:"hosts"`
	Retry                int      `yaml:"retry"`
	Get_file_list        bool     `yaml:"get_file_list"`
	Keep_pagecache       bool     `yaml:"keep_pagecache"`
	No_check             bool     `yaml:"no_check"`
	Parallel             int      `yaml:"parallel"`
	Disable_check_vdir   bool     `yaml:"disable_check_vdir"`
	Readahead            string   `yaml:"readahead"`
	MaxCachePerFile      string   `yaml:"max_cache_per_file"`
	Dcache_ttl           string   `yaml:"dcache_timeout"`
	Entry_ttl            string   `yaml:"entry_timeout"`
	Attr_ttl             string   `yaml:"attr_timeout"`
	Congestion_threshold uint16   `yaml:"congestion_threshold"`
	Max_background       uint16   `yaml:"max_background"`
	LogDir               string   `yaml:"log_dir"`
	Allow_other          bool     `yaml:"allow_other"`
	Async_dio            bool     `yaml:"async_dio"`
	Skip_ne_dir_lookup   bool     `yaml:"skip_ne_dir_lookup"`
	//////////////////////////////////////////////////////
	Writeback            bool   `yaml:"writeback"`
	Enable_direct_read   bool   `yaml:"direct_read"`
	Disable_async_read   bool   `yaml:"disable_async_read"`
	DebugFuse            bool   `yaml:"debug"`
	Enable_md5           bool   `yaml:"enable_md5"`
	LogMaxAge            string `yaml:"log_max_age"`
	LogRotationTime      string `yaml:"log_rotation_time"`
	Uid                  uint32 `yaml:"uid"`
	Gid                  uint32 `yaml:"gid"`
	MpMask               uint32 `yaml:"mp_mask"`
	Prefix               string `yaml:"prefix"`
	Etag                 int    `yaml:"etag"`
	Log_level            string `yaml:"level"`
	Storage_class        string `yaml:"storage_class"`
	Enable_load_dentries bool   `yaml:"enable_load_dentries"`
	EnableAsyncFlush     bool   `yaml:"enable_async_flush"`
	CacheDB              string `yaml:"cache_db"`
	DisableRemove        bool   `yaml:"disable_remove"`
}
