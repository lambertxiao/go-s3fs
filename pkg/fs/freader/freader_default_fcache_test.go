package freader

import (
	"bytes"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/stretchr/testify/suite"
)

func TestFileCacheSuite(t *testing.T) {
	suite.Run(t, new(FileCacheSuite))
}

type FileCacheSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller
	fc       *FileCache
}

func (s *FileCacheSuite) SetupTest() {
	logg.InitLogger()
	s.mockCtrl = gomock.NewController(s.T())
	// 文件大小8K，page大小512B
	s.fc = InitFileCache("foo", 512, CACHE_LIMIT)
}

func (s *FileCacheSuite) TestGetPageRange() {
	from, to := s.fc.getPageRange(0, 4096)
	s.Equal(0, from)
	s.Equal(7, to)

	from1, to1 := s.fc.getPageRange(100, 200)
	s.Equal(0, from1)
	s.Equal(0, to1)

	from2, to2 := s.fc.getPageRange(100, 600)
	s.Equal(0, from2)
	s.Equal(1, to2)

	from3, to3 := s.fc.getPageRange(200, 3000)
	s.Equal(0, from3)
	s.Equal(6, to3)

	from4, to4 := s.fc.getPageRange(0, 4097)
	s.Equal(0, from4)
	s.Equal(8, to4)
}

func (s *FileCacheSuite) TestCheckPages_MissingPages1() {
	readyP, missingP, inflightP := s.fc.checkPages(0, 4096)
	s.Equal(8, len(missingP))
	s.Empty(inflightP)
	s.Empty(readyP)
}

func (s *FileCacheSuite) TestCheckPages_MissingPages2() {
	s.fc.pages[2] = &FilePage{status: FPS_EMPTY}
	s.fc.pages[3] = &FilePage{status: FPS_EMPTY}
	s.fc.pages[4] = &FilePage{status: FPS_EMPTY}

	readyP, missingP, inflightP := s.fc.checkPages(0, 4096)
	s.Equal(8, len(missingP))
	s.Empty(inflightP)
	s.Empty(readyP)
}

func (s *FileCacheSuite) TestCheckPages_MissingPages3() {
	s.fc.pages[2] = &FilePage{status: FPS_INFLIGHT}
	s.fc.pages[3] = &FilePage{status: FPS_INFLIGHT}
	s.fc.pages[4] = &FilePage{status: FPS_INFLIGHT}

	readyP, missingP, inflightP := s.fc.checkPages(0, 4096)
	s.Equal(5, len(missingP))
	s.Equal(3, len(inflightP))
	s.Empty(readyP)
}

func (s *FileCacheSuite) TestCheckPages_MissingPages4() {
	s.fc.pages[2] = &FilePage{status: FPS_INFLIGHT}
	s.fc.pages[3] = &FilePage{status: FPS_READY}
	s.fc.pages[4] = &FilePage{status: FPS_INFLIGHT}

	readyP, missingP, inflightP := s.fc.checkPages(0, 4096)
	s.Equal(5, len(missingP))
	s.Equal(2, len(inflightP))
	s.Equal(1, len(readyP))
}

func (s *FileCacheSuite) TestMergeToPageGroup() {
	pages := []*FilePage{
		{idx: 0, size: 4},
		{idx: 1, size: 4},
		//
		{idx: 2, size: 4},
		{idx: 3, size: 4},
		//
		{idx: 4, size: 4},
		//
		{idx: 7, size: 4},
		{idx: 8, size: 4},
		//
		{idx: 9, size: 4},
	}
	groups := mergeToFileGroup(pages, 8)
	s.Equal(5, len(groups))
}

func (s *FileCacheSuite) TestMergeToPageGroup2() {
	pages := []*FilePage{
		{idx: 2, size: 4},
		{idx: 3, size: 4},
		//
		{idx: 4, size: 4},
		//
		{idx: 7, size: 4},
		{idx: 8, size: 4},
		//
		{idx: 9, size: 4},
	}
	groups := mergeToFileGroup(pages, 8)
	s.Equal(4, len(groups))
}

func (s *FileCacheSuite) TestMergeToPageGroup3() {
	pages := []*FilePage{
		{idx: 2, size: 4},
		//
		{idx: 4, size: 4},
		//
		{idx: 7, size: 4},
		//
		{idx: 9, size: 4},
	}
	groups := mergeToFileGroup(pages, 8)
	s.Equal(4, len(groups))
}

func (s *FileCacheSuite) TestFillPages1() {
	group := &FilePageGroup{}
	buf := []byte{1, 2, 3, 4}
	n, err := group.fillPages(bytes.NewReader(buf))
	s.Equal(uint64(0), n)
	s.Nil(err)
}

func (s *FileCacheSuite) TestFillPages2() {
	group := &FilePageGroup{
		pages: []*FilePage{
			{size: 4, data: make([]byte, 4)},
		},
	}
	buf := []byte{1, 2, 3, 4}
	n, err := group.fillPages(bytes.NewReader(buf))
	s.Equal(uint64(4), n)
	s.Nil(err)
	s.Equal([]byte{1, 2, 3, 4}, group.pages[0].data)
}

func (s *FileCacheSuite) TestFillPages3() {
	group := &FilePageGroup{
		pages: []*FilePage{
			{size: 4, data: make([]byte, 4)},
			{size: 4, data: make([]byte, 4)},
		},
	}
	buf := []byte{1, 2, 3, 4, 5, 6}
	n, err := group.fillPages(bytes.NewReader(buf))
	s.Equal(uint64(6), n)
	s.Nil(err)
	s.Equal([]byte{1, 2, 3, 4}, group.pages[0].data)
	s.Equal([]byte{5, 6, 0, 0}, group.pages[1].data)
}
