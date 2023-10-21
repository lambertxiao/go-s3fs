package freader

import (
	"io"

	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

// 直读backend，本地不缓存内容
type DirectReader struct {
	file *types.File
}

func NewDirectReader(file *types.File) FileReader {
	return &DirectReader{file: file}
}

func (reader *DirectReader) Read(offset uint64, length int, data []byte) (int, error) {
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

	return n, nil
}

func (reader *DirectReader) Release() error {
	return nil
}
