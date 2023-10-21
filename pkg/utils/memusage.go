//go:build !windows
// +build !windows

package utils

import (
	"bytes"
	"os"
	"strconv"
	"syscall"
)

func MemoryUsage() (virt, rss uint64) {
	stat, err := os.ReadFile("/proc/self/stat")
	if err == nil {
		stats := bytes.Split(stat, []byte(" "))
		if len(stats) >= 24 {
			v, _ := strconv.ParseUint(string(stats[22]), 10, 64)
			r, _ := strconv.ParseUint(string(stats[23]), 10, 64)
			return v, r * 4096
		}
	}

	var ru syscall.Rusage
	err = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	if err == nil {
		return uint64(ru.Maxrss), uint64(ru.Maxrss)
	}
	return
}
