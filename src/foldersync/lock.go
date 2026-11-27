package foldersync

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ErrDestinationLocked báo một tiến trình croc sync khác đang ghi vào cùng thư mục đích.
var ErrDestinationLocked = errors.New("foldersync: another croc sync is using this destination")

const (
	// LockFileName là tệp khóa ở gốc thư mục đích. Tên bắt đầu bằng StagingDirPrefix
	// nên scanner và bộ lọc luôn bỏ qua nó. Tệp được giữ lại sau khi xong: xóa một
	// tệp khóa đang có người mở sẽ cho phép hai tiến trình cùng giữ hai khóa khác nhau.
	LockFileName = StagingDirPrefix + "lock"

	stagingLockName = ".lock"
)

// DestLock là khóa độc quyền trên một thư mục đích, giữ suốt phiên sync.
type DestLock struct {
	root string
	f    *os.File
}

// AcquireLock khóa thư mục đích root; trả ErrDestinationLocked nếu đã có người giữ.
func AcquireLock(root string) (*DestLock, error) {
	f, err := lockFile(filepath.Join(root, LockFileName))
	if err != nil {
		return nil, err
	}
	return &DestLock{root: root, f: f}, nil
}

// Release mở khóa. Gọi nhiều lần an toàn.
func (l *DestLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := unlockFile(l.f)
	l.f = nil
	return err
}

// lockFile mở (tạo nếu cần) và khóa độc quyền không chờ.
func lockFile(name string) (*os.File, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("foldersync: open lock %s: %w", name, err)
	}
	if err := tryLock(f); err != nil {
		_ = f.Close()
		if errors.Is(err, errWouldBlock) {
			return nil, fmt.Errorf("%w (%s)", ErrDestinationLocked, filepath.Dir(name))
		}
		return nil, fmt.Errorf("foldersync: lock %s: %w", name, err)
	}
	return f, nil
}

func unlockFile(f *os.File) error {
	err := unlock(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// Staging là thư mục tạm .croc-sync-tmp-<id>/ nằm trong thư mục đích. Nằm cùng hệ tệp
// nên Rename từ staging vào đích là nguyên tử. Thư mục có khóa riêng để lần dọn sau
// chỉ xóa staging không còn tiến trình nào dùng.
type Staging struct {
	Root string // thư mục đích
	Name string // tên thư mục staging (tương đối so với Root)
	lock *os.File
}

// Dir trả đường dẫn tuyệt đối của thư mục staging.
func (s *Staging) Dir() string { return filepath.Join(s.Root, s.Name) }

// NewStaging tạo và khóa một thư mục staging mới trong root.
func NewStaging(root string) (*Staging, error) {
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	name := StagingDirPrefix + hex.EncodeToString(id[:])
	dir := filepath.Join(root, name)
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, fmt.Errorf("foldersync: create staging: %w", err)
	}
	f, err := lockFile(filepath.Join(dir, stagingLockName))
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return &Staging{Root: root, Name: name, lock: f}, nil
}

// Remove mở khóa và xóa thư mục staging cùng mọi thứ còn lại bên trong.
// Staging do croc tạo nên được phép RemoveAll (không phải dữ liệu người dùng);
// os.Root giữ thao tác trong thư mục đích dù có symlink bên trong.
func (s *Staging) Remove() error {
	if s == nil {
		return nil
	}
	var err error
	if s.lock != nil {
		err = unlockFile(s.lock)
		s.lock = nil
	}
	if rerr := removeStagingDir(s.Root, s.Name); err == nil {
		err = rerr
	}
	return err
}

func removeStagingDir(root, name string) error {
	r, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer r.Close()
	err = r.RemoveAll(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// CleanOrphanStaging xóa các thư mục staging mồ côi (tiến trình trước bị dừng giữa chừng).
// Chỉ xóa thư mục mà khóa riêng của nó lấy được, tức không còn tiến trình nào dùng.
// Bên gọi nên giữ DestLock. Trả danh sách đã xóa.
func CleanOrphanStaging(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var removed []string
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, StagingDirPrefix) || name == LockFileName || !e.IsDir() {
			continue
		}
		f, err := lockFile(filepath.Join(root, name, stagingLockName))
		if errors.Is(err, ErrDestinationLocked) {
			continue // đang được dùng
		}
		if f != nil {
			_ = unlockFile(f)
		}
		if err := removeStagingDir(root, name); err != nil {
			errs = append(errs, err)
			continue
		}
		removed = append(removed, name)
	}
	return removed, errors.Join(errs...)
}
