package freader

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/mocks"
	"github.com/lambertxiao/go-s3fs/pkg/storage"
	"github.com/lambertxiao/go-s3fs/pkg/types"
	"github.com/stretchr/testify/suite"
)

func TestDefaultReaderSuite(t *testing.T) {
	suite.Run(t, new(DefaultReaderSuite))
}

type DefaultReaderSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	storage     *mocks.MockStorage
	fr          FileReader
	inflightCnt int64
}

func (s *DefaultReaderSuite) SetupTest() {
	cfg := &config.FSConfig{}
	cfg.Retry = 2
	config.SetGConfig(cfg)

	logg.InitLogger()
	s.mockCtrl = gomock.NewController(s.T())
	s.storage = mocks.NewMockStorage(s.mockCtrl)
	storage.InitStorage(s.storage)
}

const (
	CACHE_LIMIT = 1024 * 1024 * 1024
)

func (s *DefaultReaderSuite) TestReadBasic() {
	var readahead uint64 = 0
	var pageSize uint64 = 4

	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 0,
		Length: 16,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(bytes.NewReader(buf)),
	}, nil)

	fcache := InitFileCache("foo", pageSize, 100000)
	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 8192}, readahead, fcache, nil, &s.inflightCnt,
	)

	readBuf := make([]byte, 16)
	n, err := s.fr.Read(0, 16, readBuf)
	s.Equal(16, n)
	s.Nil(err)
	s.Equal(buf[:16], readBuf)
}

// 读取0~3号页面，cache里0和2号页面已经存在
func (s *DefaultReaderSuite) TestReadBasic2() {
	var readahead uint64 = 0
	var pageSize uint64 = 4

	fcache := InitFileCache("foo", pageSize, 100000)
	fcache.pages[0] = &FilePage{
		idx:    0,
		status: FPS_READY,
		data:   []byte{1, 2, 3, 4},
		size:   4,
	}
	fcache.pages[2] = &FilePage{
		idx:    2,
		status: FPS_READY,
		data:   []byte{9, 10, 11, 12},
		size:   4,
	}

	// 缺了两页
	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 4,
		Length: 4,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(bytes.NewReader([]byte{5, 6, 7, 8})),
	}, nil)

	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 12,
		Length: 4,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(bytes.NewReader([]byte{13, 14, 15, 16})),
	}, nil)

	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 100}, readahead, fcache, nil, &s.inflightCnt,
	)

	readBuf := make([]byte, 16)
	n, err := s.fr.Read(0, 16, readBuf)
	s.Equal(16, n)
	s.Nil(err)
	s.Equal([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, readBuf)
}

func (s *DefaultReaderSuite) TestReadBasic3() {
	// 开了预读
	var readahead uint64 = 12
	var pageSize uint64 = 4

	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 0,
		Length: 4,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(bytes.NewReader(buf[0:4])),
	}, nil)

	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 4,
		Length: 8,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(bytes.NewReader(buf[4:])),
	}, nil)

	fcache := InitFileCache("foo", pageSize, 100000)
	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 8192}, readahead, fcache, nil, &s.inflightCnt,
	)

	// 第一个IO
	readBuf := make([]byte, 4)
	n, err := s.fr.Read(0, 4, readBuf)
	s.Equal(4, n)
	s.Nil(err)
	s.Equal(buf[:4], readBuf)

	// 第二个IO
	n, err = s.fr.Read(4, 4, readBuf)
	s.Equal(4, n)
	s.Nil(err)
	s.Equal(buf[4:8], readBuf)
}

func (s *DefaultReaderSuite) TestReadParallel() {
	var readahead uint64 = 0
	var pageSize uint64 = 4

	fcache := InitFileCache("foo", pageSize, CACHE_LIMIT)
	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 8192},
		readahead,
		fcache,
		nil,
		&s.inflightCnt,
	)
	buf := make([]byte, 16)
	rand.Read(buf)
	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 0,
		Length: 16,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(bytes.NewReader(buf)),
	}, nil)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		readBuf := make([]byte, 16)
		n, err := s.fr.Read(0, 16, readBuf)
		s.Equal(16, n)
		s.Nil(err)
		s.Equal(buf[:16], readBuf)
	}()

	go func() {
		time.Sleep(1 * time.Second)
		defer wg.Done()

		readBuf := make([]byte, 16)
		n, err := s.fr.Read(0, 16, readBuf)
		s.Equal(16, n)
		s.Nil(err)
		s.Equal(buf[:16], readBuf)
	}()

	wg.Wait()
}

func (s *DefaultReaderSuite) TestReadWithInflightPage() {
	var readahead uint64 = 0
	var pageSize uint64 = 4

	fcache := InitFileCache("foo", pageSize, CACHE_LIMIT)
	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 100},
		readahead,
		fcache,
		nil,
		&s.inflightCnt,
	)

	inflightPage3Req := &InflightReq{
		cond:   *sync.NewCond(&sync.Mutex{}),
		status: IRS_INFLIGHT,
	}
	page3 := &FilePage{
		idx:    3,
		status: FPS_INFLIGHT,
		size:   4,
		req:    inflightPage3Req,
	}
	fcache.pages[3] = page3

	go func() {
		time.Sleep(1 * time.Second)
		// 延迟设置上数据
		page3.data = []byte{13, 14, 15, 16}
		inflightPage3Req.done()
	}()

	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 0,
		Length: 12,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(bytes.NewReader(buf)),
	}, nil)

	readBuf := make([]byte, 16)
	n, err := s.fr.Read(0, 16, readBuf)
	s.Equal(16, n)
	s.Nil(err)
	s.Equal([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, readBuf)
}

// 这个case设置了，lru只缓存2个页面，但上层要读3个页面，这种情况下，
// lru也需要支持正常读，不能将已经ready的页面回收
func (s *DefaultReaderSuite) TestReadWithRecycle() {
	var readahead uint64 = 0
	var cacheLimit uint64 = 8
	var pageSize uint64 = 4

	// 只缓存2个页面
	fcache := InitFileCache("foo", pageSize, cacheLimit)
	fcache.pages[0] = &FilePage{
		idx:    0,
		status: FPS_READY,
		data:   []byte{1, 2, 3, 4},
		size:   4,
	}
	fcache.pages[1] = &FilePage{
		idx:    1,
		status: FPS_READY,
		data:   []byte{5, 6, 7, 8},
		size:   4,
	}

	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 100},
		readahead,
		fcache,
		nil,
		&s.inflightCnt,
	)

	buf := []byte{9, 10, 11, 12}

	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 8,
		Length: 4,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(bytes.NewReader(buf)),
	}, nil)

	readBuf := make([]byte, 12)
	n, err := s.fr.Read(0, 12, readBuf)

	s.Equal(12, n)
	s.Nil(err)
	s.Equal([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, readBuf)
}

func (s *DefaultReaderSuite) TestReadWithServerError() {
	var readahead uint64 = 0
	var pageSize uint64 = 4

	fcache := InitFileCache("foo", pageSize, CACHE_LIMIT)
	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 100},
		readahead,
		fcache,
		nil,
		&s.inflightCnt,
	)

	fakeErr := fmt.Errorf("fake err")
	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 0,
		Length: 16,
		Key:    "foo",
	}).Return(nil, fakeErr)

	readBuf := make([]byte, 16)
	n, err := s.fr.Read(0, 16, readBuf)
	s.Equal(0, n)
	s.Equal(fakeErr, err)
	s.Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, readBuf)
}

func (s *DefaultReaderSuite) TestReadWithInflightPageServerError() {
	var readahead uint64 = 0
	var pageSize uint64 = 4

	fcache := InitFileCache("foo", pageSize, CACHE_LIMIT)
	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 100},
		readahead,
		fcache,
		nil,
		&s.inflightCnt,
	)

	inflightPage3Req := &InflightReq{
		cond:   *sync.NewCond(&sync.Mutex{}),
		status: IRS_INFLIGHT,
	}
	page3 := &FilePage{
		idx:    3,
		status: FPS_INFLIGHT,
		size:   4,
		req:    inflightPage3Req,
	}
	fcache.pages[3] = page3

	fakeErr := fmt.Errorf("fake err")

	go func() {
		time.Sleep(1 * time.Second)
		// 延迟设置上数据
		inflightPage3Req.err = fakeErr
		inflightPage3Req.done()
	}()

	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 0,
		Length: 12,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(bytes.NewReader(buf)),
	}, nil)

	readBuf := make([]byte, 16)
	n, err := s.fr.Read(0, 16, readBuf)
	s.Equal(0, n)
	s.Equal(fakeErr, err)
	s.Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, readBuf)
}

// getFile直接返回了错误, 这种情况下不进行重试
func (s *DefaultReaderSuite) TestReadWithGetFileError() {
	var readahead uint64 = 0
	var pageSize uint64 = 4

	fcache := InitFileCache("foo", pageSize, CACHE_LIMIT)
	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 100},
		readahead,
		fcache,
		nil,
		&s.inflightCnt,
	)

	fakeErr := fmt.Errorf("fake err")
	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 0,
		Length: 16,
		Key:    "foo",
	}).Return(nil, fakeErr)

	readBuf := make([]byte, 16)
	n, err := s.fr.Read(0, 16, readBuf)
	s.Equal(0, n)
	s.Equal(fakeErr, err)
}

type MockGetFileReader struct{}

func (r MockGetFileReader) Read(data []byte) (int, error) {
	return 0, fmt.Errorf("fake read error")
}

// getFile正常返回, 但read resp body时出错
func (s *DefaultReaderSuite) TestReadWithGetFileReadBodyError() {
	var readahead uint64 = 0
	var pageSize uint64 = 4

	fcache := InitFileCache("foo", pageSize, CACHE_LIMIT)
	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 100},
		readahead,
		fcache,
		nil,
		&s.inflightCnt,
	)

	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 0,
		Length: 8,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(MockGetFileReader{}),
	}, nil).Times(3) // 会执行1次 + 重试2次

	readBuf := make([]byte, 8)
	n, err := s.fr.Read(0, 8, readBuf)
	s.Equal(0, n)
	s.NotNil(err)
}

// getFile正常返回, 但read resp body时出错，最后重试成功
func (s *DefaultReaderSuite) TestReadWithGetFileReadBodyErrorButRetrySuccess() {
	var readahead uint64 = 0
	var pageSize uint64 = 4

	fcache := InitFileCache("foo", pageSize, CACHE_LIMIT)
	s.fr = NewDefaultReader(
		&types.File{Path: "foo", Size: 100},
		readahead,
		fcache,
		nil,
		&s.inflightCnt,
	)

	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 0,
		Length: 8,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(MockGetFileReader{}),
	}, nil).Times(2) // 重试2次

	// 最后一次成功
	buf := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	s.storage.EXPECT().GetFile(&storage.GetFileRequest{
		Offset: 0,
		Length: 8,
		Key:    "foo",
	}).Return(&storage.GetFileReply{
		Body: ioutil.NopCloser(bytes.NewReader(buf)),
	}, nil).Times(1)

	readBuf := make([]byte, 8)
	n, err := s.fr.Read(0, 8, readBuf)
	s.Equal(8, n)
	s.Nil(err)
}
