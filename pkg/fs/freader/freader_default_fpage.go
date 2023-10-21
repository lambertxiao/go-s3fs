package freader

import (
	"io"
	"sync"

	"github.com/lambertxiao/go-s3fs/pkg/common"
)

type FilePage struct {
	idx    int
	size   uint64
	data   []byte
	status FilePageStatus
	req    *InflightReq // 每一个group都对应着一个inflight req
}

type FilePageGroup struct {
	offset uint64
	size   uint64
	pages  []*FilePage
	req    *InflightReq
}

type FilePageStatus int

const (
	FPS_EMPTY    FilePageStatus = 0 + iota // 页面初始状态
	FPS_INFLIGHT                           // 页面数据正在加载中
	FPS_READY                              // 页面数据完成
)

type InflightReqStatus int

const (
	PAGE_GROUP_SIZE = 8 * 1024 * 1024

	IRS_INFLIGHT InflightReqStatus = 0 + iota
	IRS_DONE
)

type InflightReq struct {
	cond   sync.Cond
	from   int // 开始页码
	to     int // 结束页码
	status InflightReqStatus
	err    error
}

func (req *InflightReq) waitFinish() {
	req.cond.L.Lock()
	for req.status == IRS_INFLIGHT {
		req.cond.Wait()
	}
	req.cond.L.Unlock()
}

func (req *InflightReq) done() {
	req.cond.L.Lock()
	req.status = IRS_DONE
	req.cond.L.Unlock()
	req.cond.Broadcast()
}

func (p *FilePage) getOffset() uint64 {
	return uint64(p.idx) * p.size
}

// page进入inflight状态
func (g *FilePageGroup) enterInflight() {
	req := &InflightReq{
		from:   g.fisrtPage(),
		to:     g.lastPage(),
		cond:   *sync.NewCond(&sync.Mutex{}),
		status: IRS_INFLIGHT,
	}
	g.req = req
	for _, p := range g.pages {
		p.status = FPS_INFLIGHT
		p.req = req
		buf, _ := common.MMP.GetData(int64(p.size))
		p.data = buf[:p.size]
	}
}

func (g *FilePageGroup) done(err error) {
	status := FPS_READY
	if err != nil {
		status = FPS_EMPTY
		g.req.err = err
	}

	for _, p := range g.pages {
		p.status = status
	}

	g.req.done()
}

func (g *FilePageGroup) fisrtPage() int {
	if len(g.pages) == 0 {
		return 0
	}
	return g.pages[0].idx
}

func (g *FilePageGroup) lastPage() int {
	if len(g.pages) == 0 {
		return 0
	}
	return g.pages[len(g.pages)-1].idx
}

// 将reader里的内容填充到pages中
func (g *FilePageGroup) fillPages(reader io.Reader) (readBytes uint64, err error) {
	for _, page := range g.pages {
		remaining := len(page.data)
		for remaining > 0 {
			n, err := reader.Read(page.data[len(page.data)-remaining:])
			readBytes += uint64(n)
			remaining -= n
			if err != nil {
				if err == io.EOF {
					return readBytes, nil
				}
				return readBytes, err
			}
		}
	}
	return readBytes, nil
}

func mergeToFileGroup(pages []*FilePage, maxSize uint64) (groups []*FilePageGroup) {
	if len(pages) == 0 {
		return
	}
	var (
		curGroup *FilePageGroup
		curSize  uint64
	)

	for _, page := range pages {
		isNotSequential := curGroup != nil && curGroup.pages[len(curGroup.pages)-1].idx+1 != page.idx

		if curGroup == nil || isNotSequential {
			// 创建一个新的 FilePageGroup
			curGroup = &FilePageGroup{
				offset: page.getOffset(),
				size:   page.size,
				pages:  []*FilePage{page},
			}
			groups = append(groups, curGroup)
			curSize = page.size
		} else {
			// 将 page 加入当前 FilePageGroup
			curGroup.size += page.size
			curGroup.pages = append(curGroup.pages, page)
			curSize += page.size

			if curSize == maxSize {
				// 当前 FilePageGroup 的大小等于 maxSize，需要重新创建一个新的 FilePageGroup
				curGroup = nil
				curSize = 0
			}
		}
	}

	return
}
