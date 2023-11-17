package freader

import (
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
)

// 本地会缓存一部分预读的数据
type DefaultReader struct {
	file *types.File
	fc   *FileCache

	lastReadPos        uint64
	readaheadSize      uint64 // 当前预读的大小
	readaheadSizeLimit uint64 // 预读上限

	// 统计缓存命中率
	readCnt uint64
	hitCnt  uint64

	readCacheHitSizeHistogram  prometheus.Histogram
	inflightobjInflightReadCnt *int64
}

const RETRY_INTERVAL = 500 * time.Millisecond

func NewDefaultReader(
	file *types.File,
	readaheadSizeLimit uint64,
	fcache *FileCache,
	readCacheHitSizeHistogram prometheus.Histogram,
	inflightobjInflightReadCnt *int64,
) FileReader {
	r := &DefaultReader{
		file:                       file,
		readaheadSizeLimit:         readaheadSizeLimit,
		fc:                         fcache,
		readCacheHitSizeHistogram:  readCacheHitSizeHistogram,
		inflightobjInflightReadCnt: inflightobjInflightReadCnt,
		lastReadPos:                0,
	}
	return r
}

func (r *DefaultReader) Read(offset uint64, length int, data []byte) (int, error) {
	readBytes, err := r.innerRead(offset, uint64(length), data)
	return int(readBytes), err
}

func (reader *DefaultReader) directRead(offset uint64, length int, data []byte) (uint64, error) {
	req := &storage.GetFileRequest{
		Key:    reader.file.Path,
		Offset: offset,
		Length: length,
	}
	var (
		c, n  int
		err   error
		reply *storage.GetFileReply
	)
	reply, err = storage.Instance.GetFile(req)
	if err != nil {
		return 0, err
	}

	for {
		c, err = reply.Body.Read(data[n:])
		n += c
		if err == io.EOF {
			break
		}

		if err != nil {
			logg.Dlog.Errorf("read body error %v", err)
			break
		}

		if n >= len(data) {
			break
		}
	}

	reply.Body.Close()
	if err == io.EOF {
	} else if err != nil {
		return 0, err
	}

	return uint64(n), nil
}

func (r *DefaultReader) Release() error {
	if r.readCnt != 0 {
		logg.Dlog.Infof("fpath:%s read_cnt:%d hit_cnt:%d hit_rate:%f",
			r.file.Path, r.readCnt, r.hitCnt, float64(r.hitCnt)/float64(r.readCnt))
	}

	return nil
}

// 准备数据
func (r *DefaultReader) innerRead(offset, length uint64, data []byte) (uint64, error) {
	fc := r.fc

	// 读的临界区开始
	fc.pageStateLock.Lock()

	lastReadPos := r.lastReadPos
	r.lastReadPos = offset + length

	// 更新readPost
	r.readCnt++

	// 1. 完成缺页检查，这一步同时会初始化缺页
	_, missingPages, inflightPages := fc.checkPages(offset, length)

	// 2. 判断cache里的数据是否完整
	if len(missingPages) == 0 && len(inflightPages) == 0 {
		if r.readCacheHitSizeHistogram != nil {
			r.readCacheHitSizeHistogram.Observe(float64(length))
		}
		r.hitCnt++
		logg.Dlog.Debugf("cache hit, %d~%d, len:%d", offset, offset+length-1, length)

		// 先加上数据的锁，再释放掉状态锁
		fc.pageDataLock.RLock()
		defer fc.pageDataLock.RUnlock()

		fc.pageStateLock.Unlock()

		err := fc.loadData(offset, length, data)
		if err != nil {
			return 0, err
		}
		return length, nil
	}

	// 判断是否是顺序读
	if !r.observeReadPos(offset, lastReadPos, length) {
		fc.pageStateLock.Unlock()
		logg.Dlog.Debugf("not sequence read, last:%d now:%d", lastReadPos, offset)
		return r.directRead(offset, int(length), data)
	}

	// 3. 如果有数据缺失，计算需要预读的范围
	rOffset, rLength := r.getReadaheadRange(offset, length)
	logg.Dlog.Debugf("readahead offset:%d length:%d", rOffset, rLength)

	// 4. 根据要预读的数据重新进行缺页检查
	readyPages, missingPages, inflightPages := fc.checkPages(rOffset, rLength)

	// 5. 将page合并成group, 方便后面发送固定大小的getrange
	missingGroups := mergeToFileGroup(missingPages, PAGE_GROUP_SIZE)
	for _, group := range missingGroups {
		group.enterInflight()
	}

	// 6. 检查lru是否足够分配当前需要的页面
	if fc.lru.Len()+len(missingPages) > fc.lru.capacity {
		fc.pageDataLock.Lock()
		logg.Dlog.Debugf("lru cache is full, start recycle")

		pages := []*FilePage{}
		pages = append(pages, readyPages...)
		pages = append(pages, inflightPages...)

		fc.recycle(len(missingPages), pages)
		logg.Dlog.Debugf("recycle done")
		fc.pageDataLock.Unlock()
	}

	// 同样，先加上读数据的锁
	fc.pageDataLock.RLock()
	defer fc.pageDataLock.RUnlock()

	fc.pageStateLock.Unlock()
	// 读的临界区结束

	var (
		op          sync.WaitGroup
		missingErr  error
		inflightErr error
	)

	if len(missingGroups) != 0 {
		op.Add(1)
		go func() {
			defer op.Done()
			missingErr = r.fetchMissing(missingGroups)
		}()
	}

	if len(inflightPages) != 0 {
		op.Add(1)
		go func() {
			defer op.Done()
			inflightErr = r.waitInflight(inflightPages)
		}()
	}

	op.Wait()

	if missingErr != nil {
		logg.Dlog.Errorf("get missing pages error, %v", missingErr)
		return 0, missingErr
	}
	if inflightErr != nil {
		logg.Dlog.Errorf("get inflight pages error, %v", inflightErr)
		return 0, inflightErr
	}

	// data is ready
	err := fc.loadData(offset, length, data)
	if err != nil {
		return 0, err
	}

	return length, nil
}

// 判断是否是顺序读
func (r *DefaultReader) observeReadPos(offset, lastReadPos, currIOSize uint64) bool {
	if offset == lastReadPos {
		return true
	}

	if math.Abs(float64(offset)-float64(lastReadPos)) <= float64(currIOSize)*8 {
		return true
	}

	r.readaheadSize = 0
	return false
}

// 获取缺少的页面的内容
func (r *DefaultReader) fetchMissing(groups []*FilePageGroup) error {
	var (
		wg       sync.WaitGroup
		asyncErr error
	)

	// 找服务端补齐缺少的页的数据，并填充到pageCache里
	for _, group := range groups {
		wg.Add(1)

		go func(group *FilePageGroup) {
			atomic.AddInt64(r.inflightobjInflightReadCnt, 1)
			defer func() {
				wg.Done()
				atomic.AddInt64(r.inflightobjInflightReadCnt, -1)
			}()
			err := r.loadPageData(group)
			if err != nil {
				logg.Dlog.Error(err)
				asyncErr = err
			}
		}(group)
	}
	wg.Wait()
	return asyncErr
}

// 等待正在inflight的页面返回
func (r *DefaultReader) waitInflight(inflightP []*FilePage) error {
	inflightReqs := map[*InflightReq]struct{}{}
	for _, page := range inflightP {
		inflightReqs[page.req] = struct{}{}
	}

	var (
		wg       sync.WaitGroup
		asyncErr error
	)
	for req := range inflightReqs {
		wg.Add(1)
		go func(req *InflightReq) {
			defer wg.Done()
			req.waitFinish()

			if req.err != nil {
				logg.Dlog.Error(req.err)
				asyncErr = req.err
			}
		}(req)
	}
	wg.Wait()
	return asyncErr
}

// 计算需要预读的范围
func (r *DefaultReader) getReadaheadRange(offset, length uint64) (uint64, uint64) {
	realLength := length + r.readaheadSize

	if r.readaheadSize == 0 {
		// 预读的大小从第一个IO的Size开始逐步翻倍
		r.readaheadSize = length
	} else {
		// 指数翻倍
		r.readaheadSize = 2 * uint64(r.readaheadSize)
		if r.readaheadSize > r.readaheadSizeLimit {
			r.readaheadSize = r.readaheadSizeLimit
		}
	}

	if offset+realLength >= r.file.Size {
		realLength = r.file.Size - offset
	}

	return offset, realLength
}

func (r *DefaultReader) loadPageData(group *FilePageGroup) error {
	expectReadLen := group.size
	if group.offset+group.size > r.file.Size {
		expectReadLen = r.file.Size - group.offset
	}

	var (
		err         error
		reply       *storage.GetFileReply
		realReadLen uint64
	)

	interval := RETRY_INTERVAL
	req := &storage.GetFileRequest{
		Key:    r.file.Path,
		Offset: group.offset,
		Length: int(expectReadLen),
	}

	// 这个重试主要针对的是读respBody时发生的错误
	for i := 0; i < config.GetGConfig().Retry+1; i++ {
		reply, err = storage.Instance.GetFile(req)
		// getFile的err已经是底下经过多次重试的结果了，这里不需要进行重试
		if err != nil {
			group.done(err)
			return err
		}

		// 如果是读取respBody出错的时候，需要进行重试
		realReadLen, err = group.fillPages(reply.Body)
		if err == nil && expectReadLen == realReadLen {
			group.done(nil)
			return nil
		}

		if err != nil {
			logg.Dlog.Error(err)
		}

		reply.Body.Close()

		if expectReadLen != realReadLen {
			err = fmt.Errorf("missing read len, expected %d, got %d", expectReadLen, realReadLen)
			logg.Dlog.Error(err)
		}

		time.Sleep(interval)
		interval = interval * time.Duration(2)
	}

	group.done(err)
	return err
}
