//go:build !unix && !windows

package foldersync

import (
	"errors"
	"os"
)

// Nền tảng không có khóa tệp: chỉ dựa vào việc người dùng không chạy song song.
var errWouldBlock = errors.New("would block")

func tryLock(*os.File) error { return nil }
func unlock(*os.File) error  { return nil }
