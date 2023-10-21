package common

import (
	"sync"
)

var defaultParallelPool *ParallelPool
var poolLock sync.RWMutex

type ParallelPool struct {
	Waitfor   *sync.WaitGroup
	WriteChan chan struct{}
	ReadChan  chan struct{}
}

func InitParallelPool(parallel int) {
	poolLock.Lock()
	defer poolLock.Unlock()

	defaultParallelPool = &ParallelPool{
		Waitfor:   &sync.WaitGroup{},
		WriteChan: make(chan struct{}, parallel),
		ReadChan:  make(chan struct{}, parallel),
	}
}

func GetParallelPool() *ParallelPool {
	poolLock.RLock()
	defer poolLock.RUnlock()
	return defaultParallelPool
}

func (u *ParallelPool) GetWrite(c int) {
	for i := 0; i < c; i++ {
		u.WriteChan <- struct{}{}
	}
	u.Waitfor.Add(c)
}

func (u *ParallelPool) PutWrite() {
	<-u.WriteChan
	u.Waitfor.Done()
}

func (u *ParallelPool) GetRead() {
	u.ReadChan <- struct{}{}
	u.Waitfor.Add(1)

}

func (u *ParallelPool) PutRead() {
	<-u.ReadChan
	u.Waitfor.Done()
}

func (u *ParallelPool) Wait() {
	u.Waitfor.Wait()
}
