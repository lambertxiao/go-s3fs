package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/types"
	"github.com/mattn/go-isatty"
)

const (
	BLACK = 30 + iota
	RED
	GREEN
	YELLOW
	BLUE
	MAGENTA
	CYAN
	WHITE
	DEFAULT = "00"
)

const (
	RESET_SEQ      = "\033[0m"
	COLOR_SEQ      = "\033[1;" // %dm
	COLOR_DARK_SEQ = "\033[0;" // %dm
	UNDERLINE_SEQ  = "\033[4m"
	// BOLD_SEQ       = "\033[1m"
)

type statsWatcher struct {
	colorful bool
	interval uint
	header   string
	path     string
	sections []*section
}

func (w *statsWatcher) colorize(msg string, color int, dark bool, underline bool) string {
	if !w.colorful || msg == "" || msg == " " {
		return msg
	}
	var cseq, useq string
	if dark {
		cseq = COLOR_DARK_SEQ
	} else {
		cseq = COLOR_SEQ
	}
	if underline {
		useq = UNDERLINE_SEQ
	}
	return fmt.Sprintf("%s%s%dm%s%s", useq, cseq, color, msg, RESET_SEQ)
}

const (
	metricByte = 1 << iota
	metricCount
	metricTime
	metricCPU
	metricGauge
	metricCounter
	metricHist
)

type item struct {
	nick string // must be size <= 5
	name string
	typ  uint8
}

type section struct {
	name  string
	items []*item
}

func (w *statsWatcher) buildSchema(schema string, verbosity uint) {
	for _, r := range schema {
		var s section
		switch r {
		case 'u':
			s.name = "process"
			s.items = append(s.items, &item{"cpu", "gos3fs_cpu_usage", metricCPU | metricCounter})
			s.items = append(s.items, &item{"mem", "gos3fs_memory", metricGauge})
		case 'f':
			s.name = "fs"
			s.items = append(s.items, &item{"write_ops", "gos3fs_fuse_ops_durations_histogram_seconds_WRITE", metricTime | metricHist})
			s.items = append(s.items, &item{"read_ops", "gos3fs_fuse_ops_durations_histogram_seconds_READ", metricTime | metricHist})
			// s.items = append(s.items, &item{"op_other", "gos3fs_fuse_ops_durations_histogram_seconds_OTHER", metricTime | metricHist})
			// 带宽
			s.items = append(s.items, &item{"read", "gos3fs_fuse_read_size_bytes_sum", metricByte | metricCounter})
			s.items = append(s.items, &item{"write", "gos3fs_fuse_written_size_bytes_sum", metricByte | metricCounter})
			s.items = append(s.items, &item{"read_infl", "gos3fs_fs_inflight_read_cnt", metricGauge})
		case 'c':
			s.name = "readahead"
			s.items = append(s.items, &item{"read_bw", "gos3fs_read_cache_hit_size_bytes_sum", metricByte | metricCounter})
		case 'o':
			s.name = "object storage"
			// 带宽
			s.items = append(s.items, &item{"write", "gos3fs_object_request_data_bytes_WRITE", metricByte | metricCounter})
			// 次数
			s.items = append(s.items, &item{"write_req", "gos3fs_object_request_durations_histogram_seconds_WRITE", metricTime | metricHist})
			s.items = append(s.items, &item{"read", "gos3fs_object_request_data_bytes_READ", metricByte | metricCounter})
			s.items = append(s.items, &item{"read_req", "gos3fs_object_request_durations_histogram_seconds_READ", metricTime | metricHist})
			s.items = append(s.items, &item{"read_infl", "gos3fs_storage_inflight_read_cnt", metricGauge})
			s.items = append(s.items, &item{"write_infl", "gos3fs_storage_inflight_write_cnt", metricGauge})
		case 'g':
			s.name = "go"
			s.items = append(s.items, &item{"alloc", "gos3fs_go_memstats_alloc_bytes", metricGauge})
			s.items = append(s.items, &item{"sys", "gos3fs_go_memstats_sys_bytes", metricGauge})
		default:
			fmt.Printf("Warning: no item defined for %c\n", r)
			continue
		}
		w.sections = append(w.sections, &s)
	}
	if len(w.sections) == 0 {
		log.Fatalln("no section to watch, please check the schema string")
	}
}

func padding(name string, width int, char byte) string {
	pad := width - len(name)
	if pad < 0 {
		pad = 0
		name = name[0:width]
	}
	prefix := (pad + 1) / 2
	buf := make([]byte, width)
	for i := 0; i < prefix; i++ {
		buf[i] = char
	}
	copy(buf[prefix:], name)
	for i := prefix + len(name); i < width; i++ {
		buf[i] = char
	}
	return string(buf)
}

func (w *statsWatcher) formatHeader() {
	headers := make([]string, len(w.sections))
	subHeaders := make([]string, len(w.sections))
	for i, s := range w.sections {
		subs := make([]string, 0, len(s.items))
		for _, it := range s.items {
			subs = append(subs, w.colorize(padding(it.nick, 9, ' '), BLUE, false, true))

			if it.typ&metricHist != 0 {
				if it.typ&metricTime != 0 {
					subs = append(subs, w.colorize("  lat_ms ", BLUE, false, true))
				} else {
					subs = append(subs, w.colorize("    avg  ", BLUE, false, true))
				}
			}
		}
		width := 10*len(subs) - 1 // nick(5) + space(1)
		subHeaders[i] = strings.Join(subs, " ")
		headers[i] = w.colorize(padding(s.name, width, '-'), BLUE, false, false)
	}

	blueLine := w.colorize("|", BLUE, false, false)
	w.header = fmt.Sprintf("%s\n%s", strings.Join(headers, blueLine),
		strings.Join(subHeaders, blueLine))
}

func (w *statsWatcher) formatU64(v float64, dark, isByte bool) string {
	if v <= 0.0 {
		return w.colorize("       0 ", BLACK, false, false)
	}
	var vi uint64
	var unit string
	var color int
	switch vi = uint64(v); {
	case vi < 10000:
		if isByte {
			unit = "B"
		} else {
			unit = " "
		}
		color = RED
	case vi>>10 < 10000:
		vi, unit, color = vi>>10, "K", YELLOW
	case vi>>20 < 10000:
		vi, unit, color = vi>>20, "M", GREEN
	case vi>>30 < 10000:
		vi, unit, color = vi>>30, "G", BLUE
	case vi>>40 < 10000:
		vi, unit, color = vi>>40, "T", MAGENTA
	default:
		vi, unit, color = vi>>50, "P", CYAN
	}
	return w.colorize(fmt.Sprintf("%8d", vi), color, dark, false) +
		w.colorize(unit, BLACK, false, false)
}

func (w *statsWatcher) formatTime(v float64, dark bool) string {
	var ret string
	var color int
	switch {
	case v <= 0.0:
		ret, color, dark = "       0 ", BLACK, false
	case v < 10.0:
		ret, color = fmt.Sprintf("%8.2f ", v), GREEN
	case v < 100.0:
		ret, color = fmt.Sprintf("%8.1f ", v), YELLOW
	case v < 10000.0:
		ret, color = fmt.Sprintf("%8.f ", v), RED
	default:
		ret, color = fmt.Sprintf("%8.e", v), MAGENTA
	}
	return w.colorize(ret, color, dark, false)
}

func (w *statsWatcher) formatCPU(v float64, dark bool) string {
	var ret string
	var color int
	switch v = v * 100.0; {
	case v <= 0.0:
		ret, color = "     0.0", WHITE
	case v < 30.0:
		ret, color = fmt.Sprintf("%8.1f", v), GREEN
	case v < 100.0:
		ret, color = fmt.Sprintf("%8.1f", v), YELLOW
	default:
		ret, color = fmt.Sprintf("%8.f", v), RED
	}
	return w.colorize(ret, color, dark, false) +
		w.colorize("%", BLACK, false, false)
}

func (w *statsWatcher) printDiff(left, right map[string]float64, dark bool) {
	if !w.colorful && dark {
		return
	}
	values := make([]string, len(w.sections))
	for i, s := range w.sections {
		vals := make([]string, 0, len(s.items))
		for _, it := range s.items {
			switch it.typ & 0xF0 {
			case metricGauge: // currently must be metricByte
				vals = append(vals, w.formatU64(right[it.name], dark, true))
			case metricCounter:
				v := (right[it.name] - left[it.name])
				if !dark {
					v /= float64(w.interval)
				}
				if it.typ&metricByte != 0 {
					vals = append(vals, w.formatU64(v, dark, true))
				} else if it.typ&metricCPU != 0 {
					vals = append(vals, w.formatCPU(v, dark))
				} else { // metricCount
					vals = append(vals, w.formatU64(v, dark, false))
				}
			case metricHist: // metricTime
				count := right[it.name+"_total"] - left[it.name+"_total"]
				var avg float64
				if count > 0.0 {
					cost := right[it.name+"_sum"] - left[it.name+"_sum"]
					if it.typ&metricTime != 0 {
						cost *= 1000 // s -> ms
					}
					avg = cost / count
				}
				if !dark {
					count /= float64(w.interval)
				}
				vals = append(vals, w.formatU64(count, dark, false), w.formatTime(avg, dark))
			}
		}
		values[i] = strings.Join(vals, " ")
	}
	if w.colorful && dark {
		fmt.Printf("%s\r", strings.Join(values, w.colorize("|", BLUE, false, false)))
	} else {
		fmt.Printf("%s\n", strings.Join(values, w.colorize("|", BLUE, false, false)))
	}
}

func readStats(path string) map[string]float64 {
	f, err := os.Open(path)
	if err != nil {
		fmt.Printf("open %s: %s\n", path, err)
		os.Exit(0)
	}
	defer f.Close()
	d, err := io.ReadAll(f)
	if err != nil {
		fmt.Printf("read %s: %s\n", path, err)
		return nil
	}

	stats := make(map[string]float64)
	lines := strings.Split(string(d), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			stats[fields[0]], err = strconv.ParseFloat(fields[1], 64)
			if err != nil {
				log.Printf("parse %s: %s\n", fields[1], err)
			}
		}
	}
	return stats
}

func ShowStats(mountpoint string) error {
	path := path.Join(mountpoint, types.StatsInoName)

	watcher := &statsWatcher{
		colorful: true,
		interval: 1,
	}
	watcher.buildSchema("ufcog", 1)
	watcher.formatHeader()

	var tick uint
	var start, last, current map[string]float64
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	current = readStats(path)
	start = current
	last = current

	for {
		if tick%(watcher.interval*30) == 0 {
			fmt.Println(watcher.header)
		}
		if tick%watcher.interval == 0 {
			watcher.printDiff(start, current, false)
			start = current
		} else {
			watcher.printDiff(last, current, true)
		}
		last = current
		tick++
		<-ticker.C
		current = readStats(path)
	}
}

func SupportANSIColor(fd uintptr) bool {
	return isatty.IsTerminal(fd) && runtime.GOOS != "windows"
}
