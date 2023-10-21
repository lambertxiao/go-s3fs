package fwriter

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/lambertxiao/go-s3fs/pkg/common"
	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

// 顺序写writer
type DefaultWriter struct {
	sync.Mutex
	hid                 uint64
	fpath               string
	partNo              int
	stat                *storage.MPutStat
	asyncErr            error
	wg                  sync.WaitGroup
	nwo                 int64 // next write offset
	writeable           bool
	typ                 UpType
	small               *Part
	big                 *Part
	isWriting           bool
	objInflightWriteCnt *int64
}

func NewDefaultWriter(fpath string, objInflightWriteCnt *int64) FileWriter {
	return &DefaultWriter{
		fpath:               fpath,
		writeable:           true,
		typ:                 SMALL,
		isWriting:           true,
		objInflightWriteCnt: objInflightWriteCnt,
	}
}

type UpType uint8

const (
	SMALL UpType = iota
	BIG
)

func (w *DefaultWriter) Write(offset int64, data []byte) (int, error) {
	w.Lock()
	defer w.Unlock()

	if w.nwo != offset {
		logg.Dlog.Errorf("only support sequential write %s, %d != %d, size:%d", w.fpath, w.nwo, offset, len(data))
		return 0, syscall.ENOTSUP
	}

	if !w.IsWriteable() {
		logg.Dlog.Errorf("fpath:%s handle:%d is not writeable", w.fpath, w.hid)
		return 0, syscall.EIO
	}

	err := w.writeData(data)
	if err != nil {
		return 0, err
	}
	w.nwo += int64(len(data))
	return len(data), nil
}

func (w *DefaultWriter) writeData(data []byte) (err error) {
	if w.asyncErr != nil {
		return w.asyncErr
	}

	for len(data) > 0 {
		switch w.typ {
		case SMALL:
			if w.small == nil {
				w.small = SeqWritePool.Malloc(types.DEFAULT_PART_SIZE)
			}

			n := w.small.Write(data)
			if w.small.Full() {
				if len(data) > n {
					// 证明还没拷贝完，且超过了4MiB，需要切换到分片上传模式
					err = w.Switch2BigMode()
					if err != nil {
						return
					}
				} else {
					// 证明刚好写完，需不需要切换，还得看接下来的行为
					return nil
				}
			} else if n == len(data) {
				// 拷贝结束
				return
			}
			data = data[n:]
		case BIG:
			n := w.big.Write(data)
			if w.big.Full() {
				w.BigUpload(w.big, false)
				w.big = SeqWritePool.Malloc(int64(w.stat.PartSize))
			}
			data = data[n:]
		}
	}
	return
}

func (w *DefaultWriter) BigUpload(p *Part, isLast bool) {
	atomic.AddInt64(w.objInflightWriteCnt, 1)
	no := w.partNo
	w.partNo++
	req := &storage.MPutUploadRequest{
		Stat:   w.stat,
		PartNo: no,
		Data: &storage.MPutFile{
			Buffer: p.buf[:p.woff],
			IsLast: isLast,
		},
		Key: w.fpath,
	}
	common.GetParallelPool().GetWrite(1)
	w.wg.Add(1)
	go func() {
		defer common.GetParallelPool().PutWrite()
		defer w.wg.Done()
		defer SeqWritePool.Free(p)
		defer atomic.AddInt64(w.objInflightWriteCnt, -1)

		reply, err := storage.Instance.MputUpload(req)
		if err != nil {
			logg.Dlog.Errorf("upload failed error %v", err)
			w.asyncErr = err
			return
		}

		if reply.PartSizeErr {
			logg.Dlog.Errorf("upload failed error %v", reply.PartSizeErr)
			w.asyncErr = types.ErrPartSize
			return
		}
	}()
}

func (w *DefaultWriter) Switch2BigMode() (err error) {
	req := &storage.InitMPutRequest{
		Key: w.fpath,
	}
	reply, err := storage.Instance.InitMPut(req)
	if err != nil {
		logg.Dlog.Errorf("init mput error %v", err)
		return err
	}

	w.stat = reply.Stat
	w.partNo = 0
	if w.stat.PartSize != types.DEFAULT_PART_SIZE {
		logg.Dlog.Infof("%v Init mput partsize=%v", req.Key, w.stat.PartSize)
	}

	SeqWritePool.RegisterSpec(int64(w.stat.PartSize))
	w.big = SeqWritePool.Malloc(int64(w.stat.PartSize))
	w.typ = BIG
	if w.big.Cap() == w.small.Len() {
		// spec is same
		w.BigUpload(w.small, false)
		w.small = nil
	} else {
		var sum int64
		for sum < w.small.Len() {
			sum += w.big.Copy(w.small, int(sum))
			if w.big.Full() {
				w.BigUpload(w.big, false)
				w.big = SeqWritePool.Malloc(int64(w.stat.PartSize))
			}
		}
	}
	return
}
func (w *DefaultWriter) BigUploadCommit() (bool, error) {
	if w.big != nil {
		if w.big.Len() > 0 {
			// 有效数据
			w.BigUpload(w.big, true)
			w.big = nil
			w.wg.Wait()
			if w.asyncErr != nil {
				w.BigUploadAbort()
				return false, w.asyncErr
			}
		} else {
			w.wg.Wait()
			SeqWritePool.Free(w.big)
		}
		w.big = nil
	}

	req := &storage.MPutFinishRequest{
		Key:  w.fpath,
		Stat: w.stat,
	}

	_, err := storage.Instance.MputFinish(req)
	if err != nil {
		logg.Dlog.Errorf("big upload commit fail, key:%s uploadid:%s, %s", w.fpath, w.stat.MputId, err)
		w.BigUploadAbort()
		return false, err
	}

	return true, nil
}

func (w *DefaultWriter) BigUploadAbort() error {
	req := &storage.MPutAbortRequest{
		Key:  w.fpath,
		Stat: w.stat,
	}

	_, err := storage.Instance.MputAbort(req)
	if err != nil {
		logg.Dlog.Errorf("big upload abort fail, key:%s uploadid:%s, %s", w.fpath, w.stat.MputId, err)
		return err
	}
	return nil
}
func (w *DefaultWriter) smallUpload() (bool, error) {
	atomic.AddInt64(w.objInflightWriteCnt, 1)
	if w.small == nil {
		return false, nil
	}
	defer func() {
		SeqWritePool.Free(w.small)
		w.small = nil
		atomic.AddInt64(w.objInflightWriteCnt, -1)
	}()
	req := &storage.PutFileRequest{
		Buf: types.DefaultNullBuf,
		Key: w.fpath,
	}

	if w.small != nil {
		req.Buf = bytes.NewReader(w.small.buf[:w.small.woff])
		req.BufLen = len(w.small.buf[:w.small.woff])
	}

	_, err := storage.Instance.PutFile(req)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (w *DefaultWriter) FlushPart() (bool, error) {
	switch w.typ {
	case SMALL:
		return w.smallUpload()
	case BIG:
		return w.BigUploadCommit()
	}
	return false, fmt.Errorf("unknow writer type")
}

func (w *DefaultWriter) Flush() error {
	if config.GetGConfig().WriteFinishWhenRelease {
		return nil
	}

	w.Lock()
	defer w.Unlock()

	return w.finishWrite()
}

// 将内存中遗留的数据上传完
func (w *DefaultWriter) finishWrite() error {
	if !w.IsWriteable() {
		logg.Dlog.Errorf("fpath:%s handle:%d is not writeable", w.fpath, w.hid)
		return syscall.EIO
	}

	if w.nwo == 0 {
		return nil
	}

	// 开始进行flush则设置为不可写
	w.writeable = false
	uploaded, err := w.FlushPart()
	if err != nil {
		// 如果flushPart报错，重新设置writeable状态
		w.writeable = true
		logg.Dlog.Errorf("flush fpath:%s handle:%d, err:%+v", w.fpath, w.hid, err)
		return err
	}

	if uploaded {
		logg.Dlog.Debugf("handle:%d is set to unwriteable", w.hid)
	} else {
		// 理论上走不到这个分支，仅在没有任何writeFile时来flushFile会触发
		logg.Dlog.Warnf("unexpect code branch, handle:%d", w.hid)
		w.writeable = true
	}

	return nil
}

func (w *DefaultWriter) IsWriteable() bool {
	return w.writeable
}

func (w *DefaultWriter) Release() error {
	w.Lock()
	defer w.Unlock()

	if config.GetGConfig().WriteFinishWhenRelease {
		err := w.finishWrite()
		if err != nil {
			return err
		}
	}

	if w.small != nil {
		SeqWritePool.Free(w.small)
		w.small = nil
	}

	if w.big != nil {
		SeqWritePool.Free(w.big)
		w.big = nil
	}

	w.stat = nil
	w.isWriting = false
	return nil
}

func (w *DefaultWriter) FileSize() uint64 {
	return uint64(w.nwo)
}
