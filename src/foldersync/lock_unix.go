//go:build unix

package foldersync

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

var errWouldBlock = unix.EWOULDBLOCK

func tryLock(f *os.File) error {
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EAGAIN) {
		return errWouldBlock
	}
	return err
}

func unlock(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
