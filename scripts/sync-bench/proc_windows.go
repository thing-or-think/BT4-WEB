//go:build windows

package main

import (
	"fmt"
	"os/exec"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	modPsapi                 = windows.NewLazySystemDLL("psapi.dll")
	procGetProcessMemoryInfo = modPsapi.NewProc("GetProcessMemoryInfo")
)

type processMemoryCounters struct {
	CB                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
}

func configureChildCmd(cmd *exec.Cmd) {
	_ = cmd
}

func killChildProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}

type processTracker struct {
	handle windows.Handle
}

// startProcessTracker opens a handle to the started child process before Wait
// reaps it, keeping the kernel process object alive so PeakWorkingSetSize can
// be queried after the child exits.
func startProcessTracker(cmd *exec.Cmd) *processTracker {
	if cmd == nil || cmd.Process == nil {
		return &processTracker{}
	}
	pid := uint32(cmd.Process.Pid)
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_INFORMATION|windows.PROCESS_VM_READ, false, pid)
	if err != nil {
		h, err = windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
		if err != nil {
			return &processTracker{}
		}
	}
	return &processTracker{handle: h}
}

func (t *processTracker) waitAndPeakRAM(cmd *exec.Cmd) (uint64, error) {
	defer t.close()
	waitErr := cmd.Wait()
	if t == nil || t.handle == 0 {
		return 0, waitErr
	}
	peak, err := queryPeakWorkingSet(t.handle)
	if err != nil {
		return 0, waitErr
	}
	return peak, waitErr
}

func (t *processTracker) close() {
	if t != nil && t.handle != 0 {
		_ = windows.CloseHandle(t.handle)
		t.handle = 0
	}
}

func queryPeakWorkingSet(h windows.Handle) (uint64, error) {
	var pmc processMemoryCounters
	pmc.CB = uint32(unsafe.Sizeof(pmc))
	r1, _, err := procGetProcessMemoryInfo.Call(
		uintptr(h),
		uintptr(unsafe.Pointer(&pmc)),
		uintptr(pmc.CB),
	)
	if r1 == 0 {
		if err != nil && err != windows.ERROR_SUCCESS {
			return 0, err
		}
		return 0, fmt.Errorf("GetProcessMemoryInfo failed")
	}
	return uint64(pmc.PeakWorkingSetSize), nil
}
