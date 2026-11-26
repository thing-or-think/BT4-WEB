package foldersync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/schollz/croc/v11/src/receivefs"
)

var (
	// ErrNotDirectory báo thư mục gốc cần quét không phải thư mục.
	ErrNotDirectory = errors.New("foldersync: scan root is not a directory")

	// ErrFileChanged báo tệp đã đổi (size/ModTime/loại) giữa lúc quét và lúc hash.
	ErrFileChanged = errors.New("foldersync: file changed during scan")
)

// ScanOptions điều khiển ScanDir.
type ScanOptions struct {
	// Filter áp dụng khi quét. Nil nghĩa là không lọc gì ngoài thư mục staging.
	// Máy nguồn dùng bộ lọc từ cờ CLI; máy đích dùng NewFilter(src.Filter) để
	// các mục bị lọc ở đích nằm trong Skipped và được bảo vệ khỏi --delete.
	// Scanner nạp thêm các lớp .gitignore vào Filter khi đi qua từng thư mục.
	Filter *Filter

	// MaxEntries giới hạn tổng Entries + Skipped; 0 nghĩa là MaxManifestEntries.
	MaxEntries int
}

// scanTestHook được gọi trước khi đọc một thư mục (stage "readdir") và trước khi
// Lstat một mục (stage "stat"). Chỉ dùng trong test để mô phỏng tệp bị đổi hoặc biến
// mất giữa các bước quét.
var scanTestHook func(stage, diskRel string)

type scanner struct {
	root       string
	filter     *Filter
	maxEntries int
	m          *Manifest
	keys       map[string]string // PathKey -> đường dẫn dây đầu tiên dùng khóa đó
}

// ScanDir duyệt đệ quy thư mục root và trả về Manifest đã sắp xếp ổn định.
//
// Hành vi:
//   - Không đi theo symlink: symlink nằm trong Skipped (SkipSymlink), nên vòng symlink
//     không làm quét lặp vô hạn.
//   - Thư mục staging .croc-sync-tmp-* luôn bị bỏ qua và không được ghi nhận.
//   - Tên không chuẩn hóa được (CON, "a:b", kết thúc bằng '.') hoặc trùng tên theo
//     hoa/thường, NFC/NFD với một mục trước đó: SkipInvalidName, không đi vào cây con.
//   - Lỗi đọc thư mục con, lỗi Lstat hoặc tệp đổi loại trong lúc quét: SkipReadError
//     và Complete=false (máy đích sẽ cấm --delete trong lượt này).
//   - Lỗi đọc chính thư mục gốc, ctx bị hủy hoặc vượt giới hạn mục: trả lỗi.
//
// Hash không được tính ở đây (hash lười); dùng HashEntries cho các tệp cần so tầng 2.
func ScanDir(ctx context.Context, root string, o ScanOptions) (*Manifest, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("foldersync: scan root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: %s", ErrNotDirectory, root)
	}
	filter := o.Filter
	if filter == nil {
		filter = NewFilter(FilterSpec{})
	}
	maxEntries := o.MaxEntries
	if maxEntries <= 0 || maxEntries > MaxManifestEntries {
		maxEntries = MaxManifestEntries
	}
	s := &scanner{
		root:       root,
		filter:     filter,
		maxEntries: maxEntries,
		m:          &Manifest{Complete: true},
		keys:       make(map[string]string),
	}
	s.loadGitIgnore("", "")
	if err := s.walkDir(ctx, "", ""); err != nil {
		return nil, err
	}
	s.m.Filter = filter.Spec()
	s.m.Sort()
	return s.m, nil
}

func (s *scanner) count() int { return len(s.m.Entries) + len(s.m.Skipped) }

func (s *scanner) skip(reason SkipReason, wire, disk string, detail string) error {
	if s.count() >= s.maxEntries {
		return fmt.Errorf("%w: more than %d entries", ErrManifestTooLarge, s.maxEntries)
	}
	if reason == SkipReadError {
		s.m.Complete = false
	}
	s.m.Skipped = append(s.m.Skipped, SkippedPath{Path: wire, DiskPath: disk, Reason: reason, Detail: detail})
	return nil
}

// loadGitIgnore nạp .gitignore của thư mục (nếu --git). Lỗi đọc làm manifest không đầy đủ
// vì có thể đã bỏ sót quy tắc lọc, khiến máy đích xóa nhầm mục lẽ ra được bảo vệ.
func (s *scanner) loadGitIgnore(diskDir, wireDir string) {
	if !s.filter.GitEnabled() {
		return
	}
	ignorePath := filepath.Join(s.root, diskDir, ".gitignore")
	if err := s.filter.LoadGitIgnoreFile(wireDir, ignorePath); err != nil {
		// Không thể vượt giới hạn ở đây mà không trả lỗi; lỗi giới hạn sẽ bị bắt ở mục kế tiếp.
		_ = s.skip(SkipReadError, path.Join(wireDir, ".gitignore"), filepath.Join(diskDir, ".gitignore"), err.Error())
	}
}

func (s *scanner) walkDir(ctx context.Context, diskDir, wireDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if scanTestHook != nil {
		scanTestHook("readdir", diskDir)
	}
	dirEntries, readErr := os.ReadDir(filepath.Join(s.root, diskDir))
	if readErr != nil {
		if diskDir == "" {
			return fmt.Errorf("foldersync: read scan root: %w", readErr)
		}
		// ReadDir có thể trả một phần kết quả kèm lỗi: ghi nhận lỗi rồi xử lý phần đã đọc.
		if err := s.skip(SkipReadError, wireDir, diskDir, readErr.Error()); err != nil {
			return err
		}
	}

	for _, d := range dirEntries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := d.Name()
		if IsStagingPath(name) {
			continue
		}
		diskRel := filepath.Join(diskDir, name)
		wire, err := NormalizeWirePathFromOS(diskRel)
		if err != nil {
			if err := s.skip(SkipInvalidName, "", diskRel, fmt.Sprintf("%q: %v", diskRel, err)); err != nil {
				return err
			}
			continue
		}
		key := PathKey(wire)
		if first, dup := s.keys[key]; dup {
			detail := fmt.Sprintf("%q collides with %q (case or Unicode normalization)", diskRel, first)
			if err := s.skip(SkipInvalidName, wire, diskRel, detail); err != nil {
				return err
			}
			continue
		}
		s.keys[key] = wire

		typ := d.Type()
		if typ&fs.ModeSymlink != 0 {
			if err := s.skip(SkipSymlink, wire, diskRel, ""); err != nil {
				return err
			}
			continue
		}
		isDir := d.IsDir()
		if !isDir && !typ.IsRegular() {
			if err := s.skip(SkipSpecial, wire, diskRel, typ.String()); err != nil {
				return err
			}
			continue
		}
		if s.filter.Match(wire, isDir) {
			if err := s.skip(SkipFiltered, wire, diskRel, ""); err != nil {
				return err
			}
			continue
		}

		if scanTestHook != nil {
			scanTestHook("stat", diskRel)
		}
		// Lstat lại thay vì d.Info(): trên Windows DirEntry.Info trả metadata đã lưu từ
		// lúc ReadDir, nên không phát hiện được tệp vừa biến mất hoặc đổi loại.
		info, err := os.Lstat(filepath.Join(s.root, diskRel))
		if err == nil && info.Mode().Type() != typ {
			err = fmt.Errorf("%w: type changed from %v to %v", ErrFileChanged, typ, info.Mode().Type())
		}
		if err != nil {
			if err := s.skip(SkipReadError, wire, diskRel, err.Error()); err != nil {
				return err
			}
			continue
		}

		if s.count() >= s.maxEntries {
			return fmt.Errorf("%w: more than %d entries", ErrManifestTooLarge, s.maxEntries)
		}
		entry := Entry{
			Path:     wire,
			DiskPath: diskRel,
			ModTime:  info.ModTime().UTC(),
			Mode:     info.Mode().Perm(),
			Kind:     receivefs.KindFile,
		}
		if isDir {
			entry.Kind = receivefs.KindDirectory
		} else {
			entry.Size = info.Size()
		}
		s.m.Entries = append(s.m.Entries, entry)

		if isDir {
			s.loadGitIgnore(diskRel, wire)
			if err := s.walkDir(ctx, diskRel, wire); err != nil {
				return err
			}
		}
	}
	return nil
}

// HashFile tính xxhash64 toàn bộ nội dung tệp (big-endian, HashSize byte).
// xxhash chỉ để phát hiện thay đổi; tính xác thực do lớp mã hóa của croc đảm nhận.
func HashFile(ctx context.Context, fullPath string) ([]byte, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return hashReader(ctx, f)
}

func hashReader(ctx context.Context, r io.Reader) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	h := xxhash.New()
	buf := make([]byte, 1<<20)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := r.Read(buf)
		if n > 0 {
			_, _ = h.Write(buf[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return binary.BigEndian.AppendUint64(nil, h.Sum64()), nil
}

// HashEntries tính Hash cho các tệp wirePaths của manifest m (quét từ root).
//
// Mỗi tệp được kiểm size và ModTime trước và sau khi hash so với entry; nếu khác,
// Hash giữ rỗng và lỗi ErrFileChanged được trả về (gộp bằng errors.Join) để bên gọi
// thử lại lượt sau thay vì gửi một hash không khớp với metadata.
// Đường dẫn không có trong manifest hoặc không phải tệp cũng là lỗi.
func HashEntries(ctx context.Context, root string, m *Manifest, wirePaths []string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	index := make(map[string]int, len(m.Entries))
	for i := range m.Entries {
		index[m.Entries[i].Path] = i
	}
	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("foldersync: open root for hashing: %w", err)
	}
	defer rootFS.Close()

	var errs []error
	for _, p := range wirePaths {
		if err := ctx.Err(); err != nil {
			return err
		}
		i, ok := index[p]
		if !ok || !m.Entries[i].IsFile() {
			errs = append(errs, fmt.Errorf("%w: %q is not a file in the manifest", ErrInvalidEntry, p))
			continue
		}
		sum, err := hashEntry(ctx, rootFS, &m.Entries[i])
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			errs = append(errs, fmt.Errorf("hash %q: %w", p, err))
			continue
		}
		m.Entries[i].Hash = sum
	}
	return errors.Join(errs...)
}

func hashEntry(ctx context.Context, rootFS *os.Root, e *Entry) ([]byte, error) {
	diskPath := e.DiskPath
	if diskPath == "" {
		diskPath = filepath.FromSlash(e.Path)
	}
	before, err := rootFS.Lstat(diskPath)
	if err != nil {
		return nil, err
	}
	if err := sameFileMeta(e, before); err != nil {
		return nil, err
	}
	f, err := rootFS.Open(diskPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sum, err := hashReader(ctx, f)
	if err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if err := sameFileMeta(e, after); err != nil {
		return nil, err
	}
	return sum, nil
}

func sameFileMeta(e *Entry, info fs.FileInfo) error {
	switch {
	case !info.Mode().IsRegular():
		return fmt.Errorf("%w: no longer a regular file", ErrFileChanged)
	case info.Size() != e.Size:
		return fmt.Errorf("%w: size %d -> %d", ErrFileChanged, e.Size, info.Size())
	case !info.ModTime().Equal(e.ModTime):
		return fmt.Errorf("%w: modification time changed", ErrFileChanged)
	}
	return nil
}

// String trả về mô tả ngắn để hiển thị cảnh báo cho người dùng.
func (s SkippedPath) String() string {
	name := s.Path
	if name == "" {
		name = s.DiskPath
	}
	var b strings.Builder
	b.WriteString(name)
	b.WriteString(" (")
	b.WriteString(string(s.Reason))
	if s.Detail != "" {
		b.WriteString(": ")
		b.WriteString(s.Detail)
	}
	b.WriteString(")")
	return b.String()
}
