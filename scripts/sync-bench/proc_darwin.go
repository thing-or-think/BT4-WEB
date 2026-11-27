//go:build darwin

package main

import (
	"os"
	"syscall"
)

func peakRAMFromProcessState(ps *os.ProcessState) uint64 {
	if ps == nil {
		return 0
	}
	ru, ok := ps.SysUsage().(*syscall.Rusage)
	if !ok || ru == nil || ru.Maxrss <= 0 {
		return 0
	}
	// On macOS, Rusage.Maxrss is in bytes.
	return uint64(ru.Maxrss)
}
