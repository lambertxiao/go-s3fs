package common

import (
	"errors"
	"sort"
	"sync"
)

const (
	KB = 1024
	MB = 1024 * 1024
)

/*
	实现多级内存池
*/
var MMP = NewMultiMemPool()

var defaultPool = []int{
	4, 8, 16, 32, 64, 128, 256, 512,
	1 * KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 32 * KB, 64 * KB, 128 * KB, 256 * KB, 512 * KB,
	1 * MB, 2 * MB, 3 * MB, 4 * MB, 5 * MB, 8 * MB, 16 * MB, 32 * MB, 64 * MB,
}

//新建多级内存池
func NewMultiMemPool() *MultiMemPool {
	mmp := &MultiMemPool{}
	for _, v := range defaultPool {
		mmp.Add(int64(v))
	}
	sort.Sort(PoolList(mmp.Poollist))
	return mmp
}

//内存池的新建方法
func NewBuffer(chunkSize int64) []byte {
	buf := make([]byte, chunkSize)
	return buf
}

type MultiMemPool struct {
	Poollist []*MemPool
}

//为内存池添加指定大小的内存块
func (mmp *MultiMemPool) Add(ChunkSize int64) {
	syncPool := &sync.Pool{
		New: func() interface{} {
			return NewBuffer(ChunkSize)
		},
	}
	MemPool := &MemPool{
		Pool:      syncPool,
		ChunkSize: ChunkSize,
	}
	mmp.Poollist = append(mmp.Poollist, MemPool)
}

//根据指定的chunksize，返回对应级别的内存池
func (mmp *MultiMemPool) Get(ChunkSize int64) *MemPool {
	for i := 0; i < len(mmp.Poollist); i++ {
		if ChunkSize <= mmp.Poollist[i].ChunkSize {
			return mmp.Poollist[i]
		}
	}
	return mmp.Poollist[len(mmp.Poollist)-1]
}

//根据指定的chunksize，返回对应大小的内存块
func (mmp *MultiMemPool) GetData(ChunkSize int64) ([]byte, error) {
	if mmp.Get(ChunkSize).ChunkSize < ChunkSize {
		return make([]byte, ChunkSize), nil
	}
	return mmp.Get(ChunkSize).Get(ChunkSize)
}

func (mmp *MultiMemPool) PutData(data []byte) {
	chunkSize := int64(cap(data))
	if mmp.Get(chunkSize).ChunkSize >= chunkSize {
		mmp.Get(chunkSize).Put(data[:cap(data)])
	}
}

type PoolList []*MemPool

func (pl PoolList) Len() int           { return len(pl) }
func (pl PoolList) Swap(i, j int)      { pl[i], pl[j] = pl[j], pl[i] }
func (pl PoolList) Less(i, j int) bool { return pl[i].ChunkSize < pl[j].ChunkSize }

type MemPool struct {
	Pool      *sync.Pool
	ChunkSize int64
}

func (mp *MemPool) Get(chunkSize int64) ([]byte, error) {
	buf := mp.Pool.Get()
	switch data := buf.(type) {
	case []byte:
		if chunkSize > int64(len(data)) {
			return make([]byte, chunkSize), nil
		}
		return data, nil
	default:
		return nil, errors.New("unknow type")
	}
}

func (mp *MemPool) Put(buf []byte) {
	mp.Pool.Put(buf)
}

func GetData(ChunkSize int64) ([]byte, error) {
	return MMP.GetData(ChunkSize)
}
