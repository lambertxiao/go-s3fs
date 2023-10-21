package fwriter

import (
	"sync"

	"github.com/lambertxiao/go-s3fs/pkg/types"
)

type Pool struct {
	ps map[int64]*sync.Pool
	sync.RWMutex
}

var (
	SeqWritePool *Pool = &Pool{
		ps: make(map[int64]*sync.Pool),
	}
)

func init() {
	SeqWritePool.RegisterSpec(types.DEFAULT_PART_SIZE)
}

func (p *Pool) Malloc(size int64) *Part {
	p.RLock()
	defer p.RUnlock()
	return p.ps[size].Get().(*Part)
}

func (p *Pool) Free(part *Part) {
	part.woff = 0
	p.RLock()
	defer p.RUnlock()
	p.ps[int64(len(part.buf))].Put(part)
}

func (p *Pool) RegisterSpec(size int64) {
	p.Lock()
	defer p.Unlock()
	if _, exist := p.ps[size]; exist {
		return
	}
	p.ps[size] = &sync.Pool{
		New: func() interface{} {
			return &Part{
				buf:  make([]byte, size),
				woff: 0,
			}
		},
	}
}
