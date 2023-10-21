package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/types"
	"gopkg.in/yaml.v2"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	// flags
	C_HELP                      = "help, h"
	C_F                         = "f"
	C_RETRY                     = "retry"
	C_PARALLEL                  = "parallel"
	C_LEVEL                     = "level"
	C_DISABLE_REMOVE            = "disable_remove"
	C_DEBUG                     = "debug"
	C_READAHEAD                 = "readahead"
	C_MAX_CACHE_PER_FILE        = "max_cache_per_file"
	C_ETAG                      = "etag"
	C_PASSWD                    = "passwd"
	C_UID                       = "uid"
	C_GID                       = "gid"
	C_MP_MASK                   = "mp_mask"
	C_DISABLE_CHECK_VDIR        = "disable_check_vdir"
	C_PREFIX                    = "prefix"
	C_ALLOW_OTHER               = "allow_other"
	C_DIRECT_READ               = "direct_read"
	C_SKIP_NE_DIR_LOOKUP        = "skip_ne_dir_lookup"
	C_STORAGE_CLASS             = "storage_class"
	C_LOG_DIR                   = "log_dir"
	C_LOG_MAX_AGE               = "log_max_age"
	C_LOG_ROTATION_TIME         = "log_rotation_time"
	C_ENABLE_LOAD_DENTRIES      = "enable_load_dentries"
	C_READ_AFTER_WRITE_FINISH   = "read_after_write_finish"
	C_FINISH_WRITE_WHEN_RELEASE = "finish_write_when_release"
	C_DCACHETIMEOUT             = "dcache_timeout"
	C_O                         = "o"
	C_ENTRYTIMEOUT              = "entry_timeout"
	C_ATTRTIMEOUT               = "attr_timeout"
	C_DISABLE_ASYNC_READ        = "disable_async_read"
	C_MAX_BACKGROUND            = "max_background"
	C_CONGESTION_THRESHOLD      = "congestion_threshold"
	C_ASYNC_DIO                 = "async_dio"
	C_KEEP_PAGECACHE            = "keep_pagecache"
	C_MOUNTPOINT                = "mp"
)

var allFlags map[string]string

func init() {
	cli.VersionPrinter = VersionPointer
	allFlags = make(map[string]string)
	for _, v := range []string{C_HELP, C_F} {
		allFlags[v] = "misc"
	}

	FillPlatformFlags(allFlags)

	for _, v := range []string{
		C_DCACHETIMEOUT, C_RETRY, C_PARALLEL, C_LEVEL, C_DISABLE_REMOVE, C_DEBUG, C_READAHEAD, C_MAX_CACHE_PER_FILE,
		C_ETAG, C_PASSWD,
		C_UID, C_GID, C_MP_MASK, C_DISABLE_CHECK_VDIR,
		C_PREFIX, C_DIRECT_READ, C_SKIP_NE_DIR_LOOKUP, C_STORAGE_CLASS,
		C_LOG_DIR, C_LOG_MAX_AGE, C_LOG_ROTATION_TIME, C_ENABLE_LOAD_DENTRIES,
		C_READ_AFTER_WRITE_FINISH,
		C_FINISH_WRITE_WHEN_RELEASE,
	} {
		allFlags[v] = "os"
	}
}

func VersionPointer(c *cli.Context) {
	fmt.Printf("%v", c.App.Version)
}

func NewApp() *cli.App {
	version := "GO_S3FS Version: " + types.GO_S3FS_VERSION + "\n" +
		"  Commit ID: " + types.COMMIT_ID + "\n" +
		"  Build: " + types.BUILD_TIME + "\n" +
		"  Go Version: " + types.GO_VERSION + "\n"

	app := &cli.App{
		Name:     "go-s3fs",
		HideHelp: false,
		Version:  version,
		Usage:    "go-s3fs [global options] <bucket> <mountpoint>",
		Writer:   os.Stderr,
		Commands: []cli.Command{
			{
				Name:  "stats",
				Usage: "show stats",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:     C_MOUNTPOINT,
						Usage:    "specify mountpoint",
						Required: true,
					},
				},
				Action: func(c *cli.Context) error {
					return ShowStats(c.String(C_MOUNTPOINT))
				},
			},
			{
				Name:  "reload",
				Usage: "reload configuration",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:     C_MOUNTPOINT,
						Usage:    "specify mountpoint",
						Required: true,
					},
				},
				Action: func(c *cli.Context) error {
					_, err := os.Stat(filepath.Join(c.String(C_MOUNTPOINT), types.ReloadInoName))
					return err
				},
			},
			{
				Name:  "gc",
				Usage: "execute golang rumtime.GC()",
				Flags: []cli.Flag{
					cli.StringFlag{
						Name:     C_MOUNTPOINT,
						Usage:    "specify mountpoint",
						Required: true,
					},
				},
				Action: func(c *cli.Context) error {
					_, err := os.Stat(filepath.Join(c.String(C_MOUNTPOINT), types.GcInoName))
					return err
				},
			},
		},
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  C_HELP,
				Usage: "show help",
			},
			cli.BoolFlag{
				Name:  C_F,
				Usage: "foreground",
			},

			cli.DurationFlag{
				Name:  C_DCACHETIMEOUT,
				Value: types.DEFAULT_DCACHE_TTL,
				Usage: "How long to cache dentry for go-s3fs",
			},

			cli.IntFlag{
				Name:  C_RETRY,
				Value: types.DEFAULT_RETRY,
				Usage: "Number of times to retry a failed I/O",
			},

			cli.IntFlag{
				Name:  C_PARALLEL,
				Value: types.DEFAULT_PARALLEL,
				Usage: "Number of parallel I/O thread",
			},
			cli.BoolFlag{
				Name:  C_DISABLE_REMOVE,
				Usage: "Disable remove op, such as unlink, rmdir, rename",
			},
			cli.BoolFlag{
				Name:  C_DEBUG,
				Usage: "Set debug level for fuse/winfsp",
			},
			cli.StringFlag{
				Name:  C_LEVEL,
				Usage: "Set log level: error/warn/info/debug",
				Value: types.DEFAULT_LEVEL,
			},
			cli.StringFlag{
				Name:  C_LOG_DIR,
				Usage: "Set log dir",
				Value: "",
			},
			cli.DurationFlag{
				Name:  C_LOG_MAX_AGE,
				Usage: "Set log max age",
				Value: types.DEFAULT_LOG_MAX_AGE,
			},
			cli.DurationFlag{
				Name:  C_LOG_ROTATION_TIME,
				Usage: "Set log rotation time",
				Value: types.DEFAULT_LOG_ROTATION_TIME,
			},
			cli.BoolFlag{
				Name:  C_ENABLE_LOAD_DENTRIES,
				Usage: "enable auto init dentries in memory",
			},
			cli.BoolFlag{
				Name:  C_READ_AFTER_WRITE_FINISH,
				Usage: "read operation will wait all write operation done",
			},
			cli.BoolFlag{
				Name:  C_FINISH_WRITE_WHEN_RELEASE,
				Usage: "all written data will be uploaded when release",
			},

			cli.StringFlag{
				Name:  C_READAHEAD,
				Value: types.DEFAULT_READAHEAD,
				Usage: "Readahead size. e.g.: 1m/1k/1 ",
			},
			cli.StringFlag{
				Name:  C_MAX_CACHE_PER_FILE,
				Value: types.DEFAULT_MAX_CACHE_PER_FILE,
				Usage: "Max cache per file when enable readahead. e.g.: 32m/64m/128m ",
			},
			cli.IntFlag{
				Name:  C_ETAG,
				Usage: "Check etag for part. value is percent(0~100)",
				Value: types.DEFAULT_ETAG,
			},
			cli.StringFlag{
				Name:  C_PASSWD,
				Usage: "specify access file",
				Value: "/etc/go-s3fs/go-s3fs.yaml",
			},
			cli.StringSliceFlag{
				Name:  C_O,
				Usage: "Specify fuse/winfsp option",
			},
			cli.IntFlag{
				Name:  C_UID,
				Usage: "Specify default uid",
				Value: os.Getuid(),
			},
			cli.IntFlag{
				Name:  C_GID,
				Usage: "Specify default gid",
				Value: os.Getgid(),
			},
			cli.IntFlag{
				Name:  C_MP_MASK,
				Usage: "Specify mountpoint mask",
				Value: types.DEFAULT_MP_MASK,
			},
			cli.BoolFlag{
				Name:  C_DISABLE_CHECK_VDIR,
				Usage: "disable detection of virtual directories",
			},
			cli.StringFlag{
				Name:  C_PREFIX,
				Usage: "Specify bucket prefix path",
				Value: "",
			},
			cli.BoolFlag{
				Name:  C_DIRECT_READ,
				Usage: "Enable cache bypass read",
			},
			cli.BoolFlag{
				Name:  C_SKIP_NE_DIR_LOOKUP,
				Usage: "Skip non-essential directory checking, such as files ending in \".log\",\".png\",\".jpg\", etc.",
			},
			cli.StringFlag{
				Name:  C_STORAGE_CLASS,
				Usage: "Storage type, including \"STANDARD\", \"IA\"",
				Value: "STANDARD",
			},
		},
	}

	AppendAppFlags(app)
	return app
}

func PopulateConfig(c *cli.Context) (*config.FSConfig, error) {
	cfg := &config.FSConfig{
		// common
		Foreground: c.Bool("f"),

		// fuse & winfsp
		Keep_pagecache: c.Bool(C_KEEP_PAGECACHE),
		DebugFuse:      c.Bool(C_DEBUG),

		// os
		Dcache_ttl:         c.Duration(C_DCACHETIMEOUT),
		Retry:              c.Int(C_RETRY),
		Parallel:           c.Int(C_PARALLEL),
		DisableRemove:      c.Bool(C_DISABLE_REMOVE),
		Etag:               c.Int(C_ETAG),
		Passwd_file:        c.String(C_PASSWD),
		Uid:                uint32(c.Int(C_UID)),
		Gid:                uint32(c.Int(C_GID)),
		MpMask:             uint32(c.Int(C_MP_MASK)),
		Disable_check_vdir: c.Bool(C_DISABLE_CHECK_VDIR), // check empty dir
		MaxPutSize:         types.DEFAULT_PART_SIZE,
		Prefix:             c.String(C_PREFIX),

		Enable_direct_read: c.Bool(C_DIRECT_READ),

		Skip_ne_dir_lookup: c.Bool(C_SKIP_NE_DIR_LOOKUP),

		LogDir:          c.String(C_LOG_DIR),
		LogMaxAge:       c.Duration(C_LOG_MAX_AGE),
		LogRotationTime: c.Duration(C_LOG_ROTATION_TIME),

		Enable_load_dentries:   c.Bool(C_ENABLE_LOAD_DENTRIES),
		ReadAfterWriteFinish:   c.Bool(C_READ_AFTER_WRITE_FINISH),
		WriteFinishWhenRelease: c.Bool(C_FINISH_WRITE_WHEN_RELEASE),
	}

	cfg.StorageConfig = config.StorageConf{
		Bucket:        c.Args()[0],
		Storage_class: c.String(C_STORAGE_CLASS),
	}

	bucketCof, err := parseConfig(cfg)
	if err != nil {
		return cfg, err
	}
	FillConfig(c, cfg)
	err = updateConfig(cfg, bucketCof)
	if err != nil {
		return cfg, err
	}
	cfg.MountPoint = c.Args()[1]

	if cfg.Parallel < 0 {
		cfg.Parallel = 20
	}

	cfg.FuseOptions = make(map[string]string)
	for _, s := range c.StringSlice(C_O) {
		parseFuseOption(cfg.FuseOptions, s)
	}

	if _, exist := cfg.FuseOptions[C_ALLOW_OTHER]; exist || bucketCof.Allow_other {
		cfg.Allow_other = true

		// 仅在配置文件中配置allowOther时，还需要将该参数同时放到cfg.FuseOptions，在挂载的时候传递给fuse层
		if !exist {
			cfg.FuseOptions[C_ALLOW_OTHER] = ""
		}
	}

	if cfg.Retry <= 0 {
		cfg.Retry = 1
	}
	// 这是终端readhead
	readahead := c.String(C_READAHEAD)
	if readahead == types.DEFAULT_READAHEAD && bucketCof.Readahead != "" {
		readahead = strings.ToLower(bucketCof.Readahead)
	}
	readaheadInt, err := ParseStringToSize(readahead)
	if err != nil {
		return cfg, err
	}
	cfg.Readahead = uint64(readaheadInt)

	maxCachePerFile := c.String(C_MAX_CACHE_PER_FILE)
	if maxCachePerFile == types.DEFAULT_MAX_CACHE_PER_FILE && bucketCof.MaxCachePerFile != "" {
		maxCachePerFile = strings.ToLower(bucketCof.MaxCachePerFile)
	}

	maxCachePerFileInt, err := ParseStringToSize(maxCachePerFile)
	if err != nil {
		return cfg, err
	}

	if maxCachePerFileInt < readaheadInt {
		return cfg, fmt.Errorf("max_cache_per_file must be greater than readahead")
	}

	cfg.MaxCachePerFile = uint64(maxCachePerFileInt)

	//to-do
	level_str := c.String(C_LEVEL)
	if level_str == types.DEFAULT_LEVEL && bucketCof.Log_level != types.DEFAULT_LEVEL {
		level_str = bucketCof.Log_level
	}
	switch level_str {
	case "error":
		cfg.Log_level = logrus.ErrorLevel
	case "warn":
		cfg.Log_level = logrus.WarnLevel
	case "info":
		cfg.Log_level = logrus.InfoLevel
	case "debug":
		cfg.Log_level = logrus.DebugLevel
	default:
		cfg.Log_level = logrus.InfoLevel
	}

	logg.SetLevel(cfg.Log_level)

	if cfg.Etag < 0 {
		cfg.Etag = 50
	}
	if cfg.Etag > 100 {
		cfg.Etag = 100
	}

	if cfg.Prefix != "" {
		if !strings.HasSuffix(cfg.Prefix, "/") {
			return cfg, errors.New("prefix must end up with  /")
		}
	}

	var ok bool
	cfg.StorageConfig.Storage_class, ok = StorageClass(cfg.StorageConfig.Storage_class)
	if !ok {
		return cfg, errors.New("wrong storage class")
	}

	return cfg, nil
}

func updateConfig(conf *config.FSConfig, bucketCof *config.BucketConfig) error {
	sc := &conf.StorageConfig
	sc.AccessKey = bucketCof.Access_key
	sc.SecertKey = bucketCof.Secret_key
	sc.Endpoint = bucketCof.Endpoint
	sc.Hosts = bucketCof.Hosts
	conf.StorageConfig = *sc

	if conf.Attr_ttl == types.DEFAULT_ATTR_TTL && bucketCof.Attr_ttl != "" {
		attr_ttl, err := time.ParseDuration(bucketCof.Attr_ttl)
		if err != nil {
			errorStr := fmt.Sprintf("invalid value %s for attr_timeout :parse error", bucketCof.Attr_ttl)
			return errors.New(errorStr)
		}
		conf.Attr_ttl = attr_ttl
	}
	if conf.Entry_ttl == types.DEFAULT_ENTRY_TTL && bucketCof.Entry_ttl != "" {
		entry_ttl, err := time.ParseDuration(bucketCof.Entry_ttl)
		if err != nil {
			errorStr := fmt.Sprintf("invalid value %s for entry_timeout :parse error", bucketCof.Entry_ttl)
			return errors.New(errorStr)
		}
		conf.Entry_ttl = entry_ttl
	}
	if conf.Congestion_threshold == types.DEFAULT_CONGESTION_THRESHOLD && bucketCof.Congestion_threshold != 0 {
		conf.Congestion_threshold = bucketCof.Congestion_threshold
	}
	if conf.Max_background == types.DEFAULT_BACKGROUND && bucketCof.Max_background != 0 {
		conf.Max_background = bucketCof.Max_background
	}
	if conf.Keep_pagecache || bucketCof.Keep_pagecache {
		conf.Keep_pagecache = true
	}
	if conf.Async_dio || bucketCof.Async_dio {
		conf.Async_dio = true
	}
	if conf.Disable_async_read || bucketCof.Disable_async_read {
		conf.Disable_async_read = true
	}
	if conf.Writeback || bucketCof.Writeback {
		conf.Writeback = true
	}

	return nil
}

func parseConfig(conf *config.FSConfig) (*config.BucketConfig, error) {
	y, err := ioutil.ReadFile(conf.Passwd_file)
	var bucketConfig config.BucketConfig

	if err != nil {
		fmt.Printf("parse config read file error: %v, %v", err, conf.Passwd_file)
		return nil, err
	}

	err = yaml.Unmarshal(y, &bucketConfig)
	if err != nil {
		fmt.Printf("parse config yaml error %v, %v", err, conf.Passwd_file)
		return nil, err
	}

	if bucketConfig.Access_key == "" {
		err = errors.New("parse config error, access_key is required")
		return nil, err
	}

	if bucketConfig.Secret_key == "" {
		err = errors.New("parse config error, secret_key is required")
		return nil, err
	}

	if bucketConfig.Endpoint == "" {
		err = errors.New("parse config error, endpoint key is required")
		return nil, err
	}

	//string  类型
	if conf.LogDir == "" && bucketConfig.LogDir != "" {
		conf.LogDir = bucketConfig.LogDir
	}
	if conf.Prefix == "" && bucketConfig.Prefix != "" {
		conf.Prefix = bucketConfig.Prefix
	}
	if conf.StorageConfig.Storage_class == standard_class && bucketConfig.Storage_class != "" {
		conf.StorageConfig.Storage_class = bucketConfig.Storage_class
	}

	//bool 类型
	if conf.Enable_direct_read || bucketConfig.Enable_direct_read {
		conf.Enable_direct_read = true
	}
	if conf.Skip_ne_dir_lookup || bucketConfig.Skip_ne_dir_lookup {
		conf.Skip_ne_dir_lookup = true
	}
	if conf.DebugFuse || bucketConfig.DebugFuse {
		conf.DebugFuse = true
	}
	if conf.Disable_check_vdir || bucketConfig.Disable_check_vdir {
		conf.Disable_check_vdir = true
	}
	if conf.Enable_md5 || bucketConfig.Enable_md5 {
		conf.Enable_md5 = true
	}
	if conf.Enable_load_dentries || bucketConfig.Enable_load_dentries {
		conf.Enable_load_dentries = true
	}
	if conf.ReadAfterWriteFinish || bucketConfig.ReadAfterWriteFinish {
		conf.ReadAfterWriteFinish = true
	}
	if conf.WriteFinishWhenRelease || bucketConfig.FinishWriteWhenRelease {
		conf.WriteFinishWhenRelease = true
	}
	if conf.DisableRemove || bucketConfig.DisableRemove {
		conf.DisableRemove = true
	}

	// int  类型
	if conf.Uid == uint32(os.Getuid()) && bucketConfig.Uid != 0 {
		conf.Uid = uint32(bucketConfig.Uid)
	}
	if conf.Gid == uint32(os.Getgid()) && bucketConfig.Gid != 0 {
		conf.Gid = uint32(bucketConfig.Gid)
	}
	if conf.MpMask == types.DEFAULT_MP_MASK && bucketConfig.MpMask != 0 {
		conf.MpMask = uint32(bucketConfig.MpMask)
	}

	if conf.Etag == types.DEFAULT_ETAG && bucketConfig.Etag != 0 {
		conf.Etag = bucketConfig.Etag
	}
	if conf.Retry == types.DEFAULT_RETRY && bucketConfig.Retry != 0 {
		conf.Retry = bucketConfig.Retry
	}
	if conf.Parallel == types.DEFAULT_PARALLEL && bucketConfig.Parallel != 0 {
		conf.Parallel = bucketConfig.Parallel
	}
	if conf.Dcache_ttl == types.DEFAULT_DCACHE_TTL && bucketConfig.Dcache_ttl != "" {
		dcache_ttl, err := time.ParseDuration(bucketConfig.Dcache_ttl)
		if err != nil {
			errorStr := fmt.Sprintf("invalid value %s for dcache_timeout :parse error", bucketConfig.Dcache_ttl)
			return nil, errors.New(errorStr)
		}
		conf.Dcache_ttl = dcache_ttl
	}
	if conf.LogMaxAge == types.DEFAULT_LOG_MAX_AGE && bucketConfig.LogMaxAge != "" {
		LogMaxAge, err := time.ParseDuration(bucketConfig.LogMaxAge)
		if err != nil {
			errorStr := fmt.Sprintf("invalid value %s for log_max_age :parse error", bucketConfig.LogMaxAge)
			return nil, errors.New(errorStr)
		}
		conf.LogMaxAge = LogMaxAge
	}
	if conf.LogRotationTime == types.DEFAULT_LOG_ROTATION_TIME && bucketConfig.LogRotationTime != "" {
		LogRotationTime, err := time.ParseDuration(bucketConfig.LogRotationTime)
		if err != nil {
			errorStr := fmt.Sprintf("invalid value %s for log_rotation_time :parse error", bucketConfig.LogRotationTime)
			return nil, errors.New(errorStr)
		}
		conf.LogRotationTime = LogRotationTime
	}

	fi, err := os.Stat(conf.Passwd_file)
	if err != nil {
		return nil, err
	}
	conf.ModifiedTime = fi.ModTime()
	return &bucketConfig, nil
}

func parseFuseOption(m map[string]string, s string) {
	for _, v := range strings.Split(s, ",") {
		var key string
		var value string
		if equal := strings.IndexByte(v, '='); equal != -1 {
			key = v[:equal]
			value = v[equal+1:]
		} else {
			key = v
		}
		m[key] = value
	}
}

const (
	standard_class = "STANDARD"
	ia_class       = "IA"
)

func StorageClass(s string) (string, bool) {
	s = strings.ToUpper(s)
	switch s {
	case standard_class:
		return standard_class, true
	case ia_class:
		return ia_class, true
	}
	return "", false
}
func ParseStringToSize(str string) (int, error) {

	size_reg, err := regexp.Compile("([0-9][0-9]*)([mk]*)")
	if err != nil {
		return 0, err
	}
	ret := size_reg.FindAllStringSubmatch(str, -1)
	if len(ret) != 1 || len(ret[0]) != 3 || len(ret[0][1])+len(ret[0][2]) != len(str) {
		return 0, fmt.Errorf("parse string to size error %s", str)
	}
	size, err := strconv.ParseUint(ret[0][1], 10, 64)
	if err != nil {
		return -1, err
	}
	unit := ret[0][2]
	switch unit {
	case "k":
		size *= 1024
	case "m":
		size *= 1 << 20
	case "":
		break
	default:
		return 0, fmt.Errorf("invalid  size unit error %s", unit)
	}
	return int(size), err
}
