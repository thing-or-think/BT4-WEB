//go:build unix && !linux && !darwin

package main

import "os"

func peakRAMFromProcessState(ps *os.ProcessState) uint64 {
	_ = ps
	return 0
}
