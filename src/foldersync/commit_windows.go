//go:build windows

package foldersync

import (
	"errors"

	"golang.org/x/sys/windows"
)

// isTransientFSError nhận lỗi do tệp đang bị tiến trình khác mở (trình soạn thảo,
// antivirus, indexer). Các lỗi này thường hết sau vài trăm mili giây. Rename đè lên
// tệp đang mở không có FILE_SHARE_DELETE trả ERROR_ACCESS_DENIED; tệp chỉ-đọc đã được
// makeWritable xử lý trước, nên thử lại lỗi này chỉ tốn tối đa fsRetries*fsRetryDelay.
func isTransientFSError(err error) bool {
	return errors.Is(err, windows.ERROR_SHARING_VIOLATION) ||
		errors.Is(err, windows.ERROR_LOCK_VIOLATION) ||
		errors.Is(err, windows.ERROR_ACCESS_DENIED)
}
