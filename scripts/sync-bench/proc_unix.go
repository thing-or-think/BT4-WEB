//go:build unix

package main

import (
	"os/exec"
	"syscall"
)

func configureChildCmd(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killChildProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	if pid > 0 {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}
	_ = cmd.Process.Kill()
}

type processTracker struct{}

func startProcessTracker(cmd *exec.Cmd) *processTracker {
	_ = cmd
	return &processTracker{}
}

func (t *processTracker) waitAndPeakRAM(cmd *exec.Cmd) (uint64, error) {
	err := cmd.Wait()
	return peakRAMFromProcessState(cmd.ProcessState), err
}
