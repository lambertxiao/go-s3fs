package freader

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/lambertxiao/go-s3fs/pkg/common"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
)

type FileCache struct {
	fpath    string
	pageSize uint64
	pages    map[int]*FilePage
	// 这把锁仅仅用来限制对pages这个非线程安全的map的并发读写
	// loadData和checkPages
	mapLock      sync.RWMutex
	maxCachePage int

	// 这把锁用来保证page的读流程和gc流程是隔离开的，不会出现正在读page的时候，page被回收了
	pageDataLock sync.RWMutex
	// 这把锁用来保证同一时刻只有一个goroutine能修改page的状态
	pageStateLock sync.Mutex
	lru           *LruCache
	recycleFactor float32
}

func InitFileCache(fpath string, pageSize uint64, maxCache uint64) *FileCache {
	var maxCachePage int
	if maxCache%pageSize == 0 {
		maxCachePage = int(maxCache / pageSize)
	} else {
		maxCachePage = int(maxCache/pageSize + 1)
	}

	pages := make(map[int]*FilePage)
	c := &FileCache{
		fpath:         fpath,
		pageSize:      pageSize,
		pages:         pages,
		maxCachePage:  maxCachePage,
		recycleFactor: 0.75,
	}

	logg.Dlog.Infof(
		"init_file_cache fpath:%s page_size:%d max_cache:%d max_cache_page:%d", fpath, pageSize, maxCache, maxCachePage)

	c.lru = newLruCache(maxCachePage)
	return c
}

// 获取涉及的页码范围
func (c *FileCache) getPageRange(offset, length uint64) (int, int) {
	from := c.getPageNum(offset)
	to := c.getPageNum(offset + length - 1)

	return int(from), int(to)
}

// 确定哪些page是缺失的，哪些是inflight的
// 这个方法不会并发调用
func (c *FileCache) checkPages(offset, length uint64) (readyP, missingP, inflightP []*FilePage) {
	from, to := c.getPageRange(offset, length)

	c.mapLock.Lock()
	defer c.mapLock.Unlock()

	for i := from; i <= to; i++ {
		page := c.pages[i]
		if page == nil {
			page = c.initPage(i)
			c.pages[i] = page
		}

		c.lru.Add(strconv.Itoa(page.idx), struct{}{})

		switch page.status {
		case FPS_EMPTY:
			missingP = append(missingP, page)
		case FPS_INFLIGHT:
			inflightP = append(inflightP, page)
		case FPS_READY:
			readyP = append(readyP, page)
		}
	}

	logg.Dlog.Debugf(
		"from_page: %d to_page:%d missing_page_cnt: %d, inflight_page_cnt: %d",
		from, to, len(missingP), len(inflightP),
	)

	return
}

func (c *FileCache) initPage(idx int) *FilePage {
	page := new(FilePage)
	page.status = FPS_EMPTY
	page.idx = idx
	page.size = c.pageSize
	return page
}

// 这个方法会被多个读端并发调用
func (c *FileCache) loadData(offset, length uint64, data []byte) error {
	c.mapLock.RLock()
	defer c.mapLock.RUnlock()

	originLength := length
	var dataIdx int
	var dataLen uint64
	from, to := c.getPageRange(offset, length)

	for i := from; i <= to; i++ {
		page := c.pages[i]

		if page == nil {
			// 预期是不可能进入这个c分支的
			logg.Dlog.Errorf("unexpected nil page %d", page.idx)
			return errors.New("unexpected nil page")
		}

		c.lru.Add(strconv.Itoa(page.idx), struct{}{})

		pageOffset := offset - page.getOffset()
		startIdx := pageOffset
		endIdx := minInt(int(pageOffset+length), len(page.data))

		n := copy(data[dataIdx:], page.data[startIdx:endIdx])
		dataIdx += n
		dataLen += uint64(n)
		length -= uint64(n)
		offset += uint64(n)
		if length == 0 {
			break
		}
	}
	if dataLen != originLength {
		err := fmt.Errorf("could not read all requested data, expect_len:%d old_len:%d real_len:%d", len(data), originLength, dataLen)
		return err
	}
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// offset向下取整
func (c *FileCache) getPageNum(offset uint64) int {
	if offset%c.pageSize == 0 {
		return int(offset / c.pageSize)
	}
	return int(offset / c.pageSize)
}

// 进入该方法前需先加pageDataLock写锁
// excludePages表示此次不可被回收的page
func (c *FileCache) recycle(cnt int, excludePages []*FilePage) {
	total := float32(c.lru.Len())
	expect := int(total * c.recycleFactor)

	if cnt < expect {
		cnt = expect
	}

	logg.Dlog.Debugf("recycle page_cnt %d", cnt)

	excludePageMap := map[int]struct{}{}
	for _, page := range excludePages {
		excludePageMap[page.idx] = struct{}{}
	}

	for cnt > 0 {
		key := c.lru.removeOldest()
		if key == "" {
			break
		}

		pidx, _ := strconv.Atoi(key)
		page := c.pages[pidx]
		_, ok := excludePageMap[pidx]

		if page.status == FPS_READY && !ok {
			page.data = page.data[0:]
			common.MMP.PutData(page.data)
			c.pages[pidx] = nil
			logg.Dlog.Debugf("recycle page %d", pidx)
		}

		cnt--
	}
}

func (c *FileCache) Release() {
	logg.Dlog.Infof("release fcache fpath:%s, page_cnt:%d",
		c.fpath, len(c.pages))

	c.mapLock.Lock()
	defer c.mapLock.Unlock()

	for _, p := range c.pages {
		if p != nil {
			if cap(p.data) == 0 {
				logg.Dlog.Debugf("ingore empty page, page_num:%d", p.idx)
			} else {
				logg.Dlog.Debugf("release page data, page_num:%d cap:%d", p.idx, cap(p.data))
				common.MMP.PutData(p.data)
			}
		}
	}
	if c.lru != nil {
		c.lru.Clear()
	}
}
