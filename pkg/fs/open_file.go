package fs

import (
	"sync"
	"sync/atomic"

	"github.com/lambertxiao/go-s3fs/pkg/fs/freader"
	"github.com/lambertxiao/go-s3fs/pkg/fs/fwriter"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

type OpenFile struct {
	ino           types.InodeID
	name          string
	pino          types.InodeID
	hid           HandleID
	file          *types.File
	reader        freader.FileReader
	writer        fwriter.FileWriter
	storageClass  int
	restored      bool
	hasWrote      bool // 是否有写更新
	isWriting     bool
	writeDoneCond sync.Cond // 用来通知读端写入已经结束
	firstWrite    int32
}

func NewOpenFile(ino types.InodeID, name string, pino types.InodeID, file *types.File, storageClass int) *OpenFile {
	of := &OpenFile{
		ino:           ino,
		name:          name,
		pino:          pino,
		file:          file,
		writeDoneCond: *sync.NewCond(&sync.Mutex{}),
		storageClass:  storageClass,
	}
	return of
}

func (of *OpenFile) write(offset int64, data []byte) (int, error) {
	// 在第一次写入时将openfile标记为writing
	if atomic.LoadInt32(&of.firstWrite) == 0 {
		if atomic.CompareAndSwapInt32(&of.firstWrite, 0, 1) {
			of.isWriting = true
		}
	}

	n, err := of.writer.Write(offset, data)
	if err != nil {
		return n, err
	}

	of.hasWrote = true
	// 写完更新fileSize, 以便支持一个openFile写后再读
	of.file.Size = of.writer.FileSize()
	return n, nil
}

func (of *OpenFile) read(offset int64, length int, data []byte) (int, error) {
	if uint64(offset) >= of.file.Size {
		logg.Dlog.Errorf("ino:%d: path:%s read offset exceed file size %v>=%v ,len:%v finally", of.ino, of.file.Path, offset, of.file.Size, length)
		return 0, nil
	}

	if uint64(offset)+uint64(length) > of.file.Size {
		length = int(of.file.Size - uint64(offset))
	}

	if length == 0 {
		logg.Dlog.Infof("%v read size==0", of.file.Path)
		return 0, nil
	}

	// todo 实现逻辑有问题
	// restore archile files
	if of.storageClass == storage.STORAGE_ARCHIVE && !of.restored {
		req := &storage.RestoreRequest{Name: of.file.Path}
		_, err := storage.Instance.Restore(req)
		if err != nil {
			return 0, types.EIO
		}
		of.restored = true
	}

	return of.reader.Read(uint64(offset), length, data)
}

func (of *OpenFile) size() uint64 {
	return of.file.Size
}

func (of *OpenFile) Release() error {
	if of.writer != nil {
		err := of.writer.Release()
		defer func() {
			of.isWriting = false
			of.writeDoneCond.Broadcast()
		}()

		if err != nil {
			logg.Dlog.Errorf("writer release error: %v", err)
			return err
		}
	}

	if of.reader != nil {
		err := of.reader.Release()
		if err != nil {
			logg.Dlog.Warnf("reader release error, handle:%d error: %v", of.hid, err)
			return err
		}
	}
	return nil
}

func (of *OpenFile) WaitWriteDone() {
	if of.writer != nil {
		of.writeDoneCond.L.Lock()
		for of.isWriting {
			of.writeDoneCond.Wait()
		}
		of.writeDoneCond.L.Unlock()
	}
}
