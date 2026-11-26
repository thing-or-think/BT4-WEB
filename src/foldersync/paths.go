package foldersync

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"strings"

	"github.com/schollz/croc/v11/src/receivefs"
)

var (
	// ErrManifestTooLarge báo manifest vượt quá giới hạn MaxManifestEntries.
	ErrManifestTooLarge = errors.New("foldersync: manifest exceeds maximum entry limit")

	// ErrUnsupportedKind báo entry mang loại chưa hỗ trợ trong bản MVP (ví dụ symlink trong Entries).
	ErrUnsupportedKind = errors.New("foldersync: unsupported entry kind")

	// ErrStagingPath báo đường dẫn nằm trong thư mục staging nội bộ .croc-sync-tmp-*.
	ErrStagingPath = errors.New("foldersync: reserved staging path")

	// ErrInvalidEntry báo metadata của entry không hợp lệ (size, mode, hash, cha thiếu...).
	ErrInvalidEntry = errors.New("foldersync: invalid manifest entry")
)

// NormalizeWirePath chuẩn hóa một đường dẫn tương đối thành dạng chuẩn trên dây:
// dấu phân tách '/', chuẩn Unicode NFC, chặn traversal (..), đường dẫn tuyệt đối,
// tên thiết bị Windows (CON, NUL, COM1...) và thư mục nhạy cảm (.ssh, .gnupg).
//
// '\' được coi là dấu phân tách, nên hàm này chỉ dùng cho dữ liệu đã ở dạng dây
// hoặc đến từ mạng. Đường dẫn đọc từ đĩa phải đi qua NormalizeWirePathFromOS.
//
// Hàm luôn truyền allowRoot=false vì bản thân thư mục gốc ("" hoặc ".")
// không được phép là một mục trong Manifest.Entries.
func NormalizeWirePath(raw string) (string, error) {
	return receivefs.Normalize(raw, false)
}

// NormalizeWirePathFromOS chuyển đường dẫn tương đối từ hệ điều hành (có thể dùng '\'
// trên Windows hoặc NFD trên macOS) sang đường dẫn chuẩn trên dây.
func NormalizeWirePathFromOS(relDiskPath string) (string, error) {
	return normalizeFromOS(relDiskPath, filepath.Separator)
}

// normalizeFromOS tách riêng separator để test được hành vi Linux trên Windows và ngược lại.
func normalizeFromOS(relDiskPath string, separator rune) (string, error) {
	// Trên Unix '\' là ký tự hợp lệ trong tên tệp. Nếu đổi nó thành '/', tệp
	// "a\b" sẽ bị hiểu thành "a/b" ở máy đích, nên coi đây là tên không hợp lệ.
	if separator != '\\' && strings.Contains(relDiskPath, `\`) {
		return "", fmt.Errorf("%w: backslash in file name %q", receivefs.ErrUnsafePath, relDiskPath)
	}
	if separator != '/' {
		relDiskPath = strings.ReplaceAll(relDiskPath, string(separator), "/")
	}
	return NormalizeWirePath(relDiskPath)
}

// PathKey trả về khóa so sánh không phân biệt hoa/thường và không phụ thuộc NFC/NFD.
// Dùng để phát hiện đụng độ tên (VD: "README.md" và "readme.md") trên Windows/macOS.
func PathKey(wirePath string) string {
	return receivefs.CollisionKey(wirePath)
}

// stagingKey là StagingDirPrefix ở dạng khóa so sánh.
var stagingKey = receivefs.CollisionKey(StagingDirPrefix)

// IsStagingPath kiểm tra đường dẫn có thành phần nào bắt đầu bằng ".croc-sync-tmp-" không,
// không phân biệt hoa/thường và NFC/NFD (cùng quy tắc với PathKey).
// Cả filter.go và ValidateManifest đều dùng hàm này để chặn thư mục staging nội bộ.
func IsStagingPath(p string) bool {
	if p == "" {
		return false
	}
	normalized := strings.ReplaceAll(p, "\\", "/")
	for _, part := range strings.Split(normalized, "/") {
		if strings.HasPrefix(receivefs.CollisionKey(part), stagingKey) {
			return true
		}
	}
	return false
}

// PathDepth trả về độ sâu của đường dẫn chuẩn trên dây (số thành phần).
// Ví dụ: "a.txt" -> 1, "dir/sub/a.txt" -> 3.
// Hữu ích khi sắp xếp danh sách xóa từ sâu lên nông (con trước, cha sau).
func PathDepth(wirePath string) int {
	if wirePath == "" || wirePath == "." {
		return 0
	}
	return strings.Count(wirePath, "/") + 1
}

// ValidateManifest xác thực toàn bộ Manifest nhận từ mạng trước khi diff hoặc chạm vào đĩa:
//   - Giới hạn tổng số Entries + Skipped.
//   - Chỉ cho phép tệp và thư mục (symlink phải nằm ở Skipped).
//   - Chặn thư mục staging (.croc-sync-tmp-*).
//   - Đường dẫn phải ở dạng chuẩn; chặn traversal, tên thiết bị Windows, trùng
//     hoa/thường, trùng NFC/NFD và xung đột tệp/thư mục (receivefs.ValidateEntries).
//   - Metadata hợp lệ: Size không âm (0 với thư mục), Mode chỉ chứa bit quyền,
//     Hash rỗng hoặc đúng HashSize byte.
//   - Mọi thư mục cha phải có mặt dưới dạng KindDirectory. Nhờ vậy "Docs/a" và
//     "docs/b" buộc phải khai báo cả "Docs" lẫn "docs" và bị phát hiện là trùng.
//   - Skipped: lý do hợp lệ, Path chuẩn (chỉ SkipInvalidName được để rỗng).
func ValidateManifest(m *Manifest) error {
	if m == nil {
		return errors.New("foldersync: nil manifest")
	}
	if len(m.Entries) > MaxManifestEntries || len(m.Skipped) > MaxManifestEntries-len(m.Entries) {
		return fmt.Errorf("%w: %d entries + %d skipped > %d",
			ErrManifestTooLarge, len(m.Entries), len(m.Skipped), MaxManifestEntries)
	}

	rfEntries := make([]receivefs.Entry, len(m.Entries))
	directories := make(map[string]struct{})
	for i, e := range m.Entries {
		if err := validateEntryShape(e); err != nil {
			return err
		}
		rfEntries[i] = receivefs.Entry{Path: e.Path, Kind: e.Kind}
		if e.IsDir() {
			directories[e.Path] = struct{}{}
		}
	}

	if _, err := receivefs.ValidateEntries(rfEntries); err != nil {
		return fmt.Errorf("foldersync: %w", err)
	}

	for _, e := range m.Entries {
		if parent := path.Dir(e.Path); parent != "." {
			if _, ok := directories[parent]; !ok {
				return fmt.Errorf("%w: %q has no parent directory entry %q", ErrInvalidEntry, e.Path, parent)
			}
		}
	}

	for _, s := range m.Skipped {
		if err := validateSkipped(s); err != nil {
			return err
		}
	}
	return nil
}

func validateEntryShape(e Entry) error {
	if e.Kind != receivefs.KindFile && e.Kind != receivefs.KindDirectory {
		return fmt.Errorf("%w: %q (kind=%d)", ErrUnsupportedKind, e.Path, e.Kind)
	}
	if err := validateCanonicalPath(e.Path); err != nil {
		return err
	}
	switch {
	case e.Size < 0:
		return fmt.Errorf("%w: %q has negative size %d", ErrInvalidEntry, e.Path, e.Size)
	case e.IsDir() && e.Size != 0:
		return fmt.Errorf("%w: directory %q has size %d", ErrInvalidEntry, e.Path, e.Size)
	case e.Mode&^fs.ModePerm != 0:
		return fmt.Errorf("%w: %q mode %v has non-permission bits", ErrInvalidEntry, e.Path, e.Mode)
	case len(e.Hash) != 0 && len(e.Hash) != HashSize:
		return fmt.Errorf("%w: %q hash length %d, want %d", ErrInvalidEntry, e.Path, len(e.Hash), HashSize)
	case e.IsDir() && len(e.Hash) != 0:
		return fmt.Errorf("%w: directory %q has a hash", ErrInvalidEntry, e.Path)
	}
	return nil
}

func validateSkipped(s SkippedPath) error {
	switch s.Reason {
	case SkipFiltered, SkipSymlink, SkipReadError, SkipSpecial:
	case SkipInvalidName:
		if s.Path == "" {
			return nil
		}
	default:
		return fmt.Errorf("%w: skipped %q has unknown reason %q", ErrInvalidEntry, s.Path, s.Reason)
	}
	return validateCanonicalPath(s.Path)
}

// validateCanonicalPath yêu cầu p đã ở dạng chuẩn trên dây và không trỏ vào staging.
func validateCanonicalPath(p string) error {
	if IsStagingPath(p) {
		return fmt.Errorf("%w: %q", ErrStagingPath, p)
	}
	clean, err := NormalizeWirePath(p)
	if err != nil {
		return fmt.Errorf("foldersync: invalid path %q: %w", p, err)
	}
	if clean != p {
		return fmt.Errorf("%w: non-canonical path %q (want %q)", receivefs.ErrUnsafePath, p, clean)
	}
	return nil
}
