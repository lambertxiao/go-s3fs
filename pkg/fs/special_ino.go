package fs

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

const (
	minInternalNode = 0x7FFFFFFF00000000
	virualHandleId  = 0x7FFFFFFF00000000

	statsIno  = minInternalNode + 1
	gcIno     = minInternalNode + 2
	reloadIno = minInternalNode + 3
)

type SpecialInode struct {
	Inode *types.Inode
	Data  []byte
}

var (
	statsInode  *SpecialInode
	gcInode     *SpecialInode
	reloadInode *SpecialInode
)

func (fs *FSR) initSpecialInode() {
	statsInode = &SpecialInode{Inode: &types.Inode{Ino: statsIno, Mode: 0644, Size: 1024 * 1024}}
	gcInode = &SpecialInode{Inode: &types.Inode{Ino: gcIno, Mode: 0644, Size: 1024 * 1024}}
	reloadInode = &SpecialInode{Inode: &types.Inode{Ino: reloadIno, Mode: 0644, Size: 1024 * 1024}}
}

func (fs *FSR) getSpecialInode(ino types.InodeID) *types.Inode {
	switch ino {
	case statsIno:
		return statsInode.Inode
	case gcIno:
		return gcInode.Inode
	case reloadIno:
		return reloadInode.Inode
	}
	return nil
}

func (fs *FSR) openSpecialInode(ino types.InodeID) {
	switch ino {
	case statsIno:
		fs.openStatsInode()
		return
	}
}

func (fs *FSR) readSpecialNode(op *ReadFileOp) {
	switch op.Inode {
	case statsIno:
		readLen := len(op.Dst)
		fsize := uint64(len(statsInode.Data))
		if int(op.Offset) >= len(statsInode.Data) {
			op.BytesRead = 0
			return
		}

		if uint64(op.Offset)+uint64(readLen) > fsize {
			readLen = int(fsize - uint64(op.Offset))
		}

		off := int(op.Offset) + readLen
		copy(op.Dst, statsInode.Data[op.Offset:off])
		op.BytesRead = readLen
		return
	}
}

// 准备stats统计信息
func (fs *FSR) openStatsInode() {
	data := collectMetrics(fs.registry)
	statsInode.Data = data
	statsInode.Inode.Size = uint64(len(data))
}

func isInodeSpecial(ino types.InodeID) bool {
	return ino >= minInternalNode
}

func collectMetrics(registry *prometheus.Registry) []byte {
	if registry == nil {
		return []byte("")
	}
	mfs, err := registry.Gather()
	if err != nil {
		logg.Dlog.Errorf("collect metrics: %s", err)
		return nil
	}
	w := bytes.NewBuffer(nil)
	format := func(v float64) string {
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			var name string = *mf.Name
			for _, l := range m.Label {
				if *l.Name != "mp" && *l.Name != "bucket" {
					name += "_" + *l.Value
				}
			}
			switch *mf.Type {
			case io_prometheus_client.MetricType_GAUGE:
				_, _ = fmt.Fprintf(w, "%s %s\n", name, format(*m.Gauge.Value))
			case io_prometheus_client.MetricType_COUNTER:
				_, _ = fmt.Fprintf(w, "%s %s\n", name, format(*m.Counter.Value))
			case io_prometheus_client.MetricType_HISTOGRAM:
				_, _ = fmt.Fprintf(w, "%s_total %d\n", name, *m.Histogram.SampleCount)
				_, _ = fmt.Fprintf(w, "%s_sum %s\n", name, format(*m.Histogram.SampleSum))
			case io_prometheus_client.MetricType_SUMMARY:
			}
		}
	}
	return w.Bytes()
}
