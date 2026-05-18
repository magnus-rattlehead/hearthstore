//go:build linux

package server

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

func processRSSBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(syscall.Getpagesize())
}
