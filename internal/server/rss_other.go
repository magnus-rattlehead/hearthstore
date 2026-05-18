//go:build !linux && !darwin

package server

func processRSSBytes() uint64 { return 0 }
