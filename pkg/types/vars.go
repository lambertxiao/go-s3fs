package types

import (
	"time"
)

var (
	GO_S3FS_VERSION string
	GO_VERSION      string
	COMMIT_ID       string
	BUILD_TIME      string
)

const (
	PANIC_LOG_PREFIX = "go-s3fs-"
	PANIC_LOG_SUFFIX = "-stderr.log"

	// default value
	DEFAULT_RETRY                = 5
	DEFAULT_PARALLEL             = 32
	DEFAULT_DCACHE_TTL           = 5 * time.Minute
	DEFAULT_ATTR_TTL             = 5 * time.Minute
	DEFAULT_ENTRY_TTL            = 5 * time.Minute
	DEFAULT_ETAG                 = 50
	DEFAULT_CONGESTION_THRESHOLD = 48
	DEFAULT_BACKGROUND           = 64
	DEFAULT_LOG_MAX_AGE          = 72 * time.Hour
	DEFAULT_LOG_ROTATION_TIME    = 1 * time.Hour
	DEFAULT_READAHEAD            = "0"
	DEFAULT_MAX_CACHE_PER_FILE   = "1024m"
	DEFAULT_PERF_DUMP            = 1 * time.Hour
	DEFAULT_LEVEL                = "info"
	DEFAULT_PART_SIZE            = 4 * 1024 * 1024 // 4MB
	ETAG_PART_SIZE               = 4 * 1024 * 1024 //
	MAX_SMALL_SIZE               = 4 * 1024 * 1024 // 4MB
	DEFAULT_PAGE_SIZE            = 1024 * 1024
	DEFAULT_MP_MASK              = 0000

	ReadDirLimitCount = 100

	StatsInoName  = "__stats__"
	GcInoName     = "__gc__"
	ReloadInoName = "__reload__"
)
