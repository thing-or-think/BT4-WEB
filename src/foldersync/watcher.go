package foldersync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/fsnotify/fsnotify"
)

const (
	defaultWatchQuiet        = 400 * time.Millisecond
	defaultWatchMaxWait      = 2 * time.Second
	defaultWatchPollInterval = 2 * time.Second

	watchEventBufferSize = 4096
	maxQueuedWatchEvents = 16384

	watchModeEvents  = "events"
	watchModePolling = "polling"

	modeCodeEvents  int32 = 0
	modeCodePolling int32 = 1
)

// fsWatcherNew tạo fsnotify.Watcher; test có thể ghi đè để mô phỏng lỗi khởi tạo
// (ví dụ vượt giới hạn inotify/file descriptor) nhằm kiểm tra fallback sang polling.
var fsWatcherNew = func() (*fsnotify.Watcher, error) {
	return fsnotify.NewBufferedWatcher(watchEventBufferSize)
}

// watchAddHook cho phép test mô phỏng lỗi khi gắn watch vào một thư mục (ví dụ ENOSPC).
var watchAddHook func(dir string) error

// WatchOptions điều khiển Watcher cho chế độ `croc sync --watch`.
type WatchOptions struct {
	Filter       *Filter       // Thư mục khớp bộ lọc không được theo dõi; staging (.croc-sync-tmp-*) luôn bị bỏ qua
	Quiet        time.Duration // Debounce: phát batch sau khoảng yên tĩnh không có sự kiện mới (mặc định 400ms)
	MaxWait      time.Duration // Phát batch chậm nhất sau khoảng này kể từ sự kiện đầu tiên (mặc định 2s)
	PollInterval time.Duration // Chu kỳ quét dự phòng ở chế độ polling (mặc định 2s)
}

// watchEventQueue đệm sự kiện và lỗi từ goroutine rút kênh fsnotify sang vòng lặp xử lý chính.
// Tách bước đọc fsw.Events/fsw.Errors giúp fsw.Add/fsw.Remove/fsw.Close không bao giờ bị
// deadlock trên Windows khi kênh sự kiện đầy.
type watchEventQueue struct {
	mu     sync.Mutex
	events []fsnotify.Event
	hasErr bool
	notify chan struct{}
}

func newWatchEventQueue() *watchEventQueue {
	return &watchEventQueue{
		notify: make(chan struct{}, 1),
	}
}

func (q *watchEventQueue) pushEvent(ev fsnotify.Event) {
	q.mu.Lock()
	if len(q.events) >= maxQueuedWatchEvents {
		// Hàng đợi nội bộ tràn: coi như overflow để phát batch quét lại toàn bộ ngay.
		q.hasErr = true
	} else {
		q.events = append(q.events, ev)
	}
	q.mu.Unlock()
	q.signal()
}

func (q *watchEventQueue) pushError(_ error) {
	q.mu.Lock()
	q.hasErr = true
	q.mu.Unlock()
	q.signal()
}

func (q *watchEventQueue) drain() ([]fsnotify.Event, bool) {
	q.mu.Lock()
	events := q.events
	q.events = nil
	hasErr := q.hasErr
	q.hasErr = false
	q.mu.Unlock()
	return events, hasErr
}

func (q *watchEventQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Watcher theo dõi đệ quy một thư mục gốc và phát tín hiệu đã gộp (debounce) qua Changes().
// Sự kiện chỉ là gợi ý để bên gọi quét lại và diff toàn bộ cây thư mục.
type Watcher struct {
	root         string
	baseFilter   *Filter // Bản sao bất biến của bộ lọc gốc (dùng khi quét lại ở chế độ polling)
	filter       *Filter // Bản sao riêng của goroutine sự kiện (có thể nạp thêm lớp .gitignore)
	quiet        time.Duration
	maxWait      time.Duration
	pollInterval time.Duration

	ctx      context.Context
	cancel   context.CancelFunc
	changes  chan struct{}
	done     chan struct{}
	mode     atomic.Int32
	closeErr error

	// Các trường bên dưới chỉ do goroutine xử lý sự kiện sử dụng sau khi khởi tạo.
	fsw      *fsnotify.Watcher
	queue    *watchEventQueue
	pumpDone chan struct{}
	watched  map[string]struct{}
}

// NewWatcher khởi tạo Watcher theo dõi đệ quy thư mục root.
// Nếu không thể mở watcher của hệ điều hành hoặc gắn watch ban đầu thất bại
// (ví dụ hết giới hạn inotify ENOSPC hoặc quá nhiều tệp mở), Watcher tự chuyển sang polling.
func NewWatcher(ctx context.Context, root string, o WatchOptions) (*Watcher, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("foldersync: watch root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: %s", ErrNotDirectory, root)
	}
	if abs, absErr := filepath.Abs(root); absErr == nil {
		root = abs
	}
	root = filepath.Clean(root)

	quiet := o.Quiet
	if quiet <= 0 {
		quiet = defaultWatchQuiet
	}
	maxWait := o.MaxWait
	if maxWait <= 0 {
		maxWait = defaultWatchMaxWait
	}
	pollInterval := o.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultWatchPollInterval
	}

	wctx, cancel := context.WithCancel(ctx)
	w := &Watcher{
		root:         root,
		baseFilter:   cloneFilter(o.Filter),
		filter:       cloneFilter(o.Filter),
		quiet:        quiet,
		maxWait:      maxWait,
		pollInterval: pollInterval,
		ctx:          wctx,
		cancel:       cancel,
		changes:      make(chan struct{}, 1),
		done:         make(chan struct{}),
		watched:      make(map[string]struct{}),
	}

	fsw, err := fsWatcherNew()
	if err == nil {
		w.fsw = fsw
		w.queue = newWatchEventQueue()
		w.pumpDone = make(chan struct{})
		go w.pumpEvents(fsw, w.queue, w.pumpDone)

		if addErr := w.addDirTree(w.root); addErr != nil {
			_ = w.stopFSWatcher()
			err = addErr
		}
	}

	if err != nil {
		w.mode.Store(modeCodePolling)
		prev, fpErr := treeFingerprint(w.ctx, w.root, w.baseFilter)
		if w.ctx.Err() != nil {
			cancel()
			close(w.changes)
			close(w.done)
			return nil, w.ctx.Err()
		}
		go w.runPolling(prev, fpErr == nil)
		return w, nil
	}

	w.mode.Store(modeCodeEvents)
	go w.runEvents()
	return w, nil
}

// Changes trả về kênh nhận một tín hiệu cho mỗi batch thay đổi đã debounce.
// Kênh có buffer 1 và tự gộp tín hiệu nên không bao giờ làm nghẽn Watcher.
func (w *Watcher) Changes() <-chan struct{} {
	return w.changes
}

// Mode trả về chế độ theo dõi hiện tại: "events" hoặc "polling".
func (w *Watcher) Mode() string {
	if w.mode.Load() == modeCodePolling {
		return watchModePolling
	}
	return watchModeEvents
}

// Close dừng mọi goroutine của Watcher và đóng fsnotify watcher (nếu đang mở).
func (w *Watcher) Close() error {
	w.cancel()
	<-w.done
	return w.closeErr
}

// injectError bơm một lỗi vào đường xử lý lỗi nội bộ để test tình huống overflow/lỗi backend.
func (w *Watcher) injectError(err error) {
	if err == nil || w.queue == nil {
		return
	}
	w.queue.pushError(err)
}

func (w *Watcher) emit() {
	select {
	case w.changes <- struct{}{}:
	default:
	}
}

func (w *Watcher) pumpEvents(fsw *fsnotify.Watcher, q *watchEventQueue, done chan struct{}) {
	defer close(done)
	events := fsw.Events
	errs := fsw.Errors
	for events != nil || errs != nil {
		select {
		case ev, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			q.pushEvent(ev)
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			q.pushError(err)
		}
	}
}

func (w *Watcher) stopFSWatcher() error {
	if w.fsw == nil {
		return nil
	}
	err := w.fsw.Close()
	if w.pumpDone != nil {
		<-w.pumpDone
	}
	w.fsw = nil
	w.pumpDone = nil
	clear(w.watched)
	return err
}

func (w *Watcher) addWatch(dir string) error {
	if watchAddHook != nil {
		if err := watchAddHook(dir); err != nil {
			return err
		}
	}
	return w.fsw.Add(dir)
}

// addDirTree duyệt đệ quy từ startDir và gắn watch cho mọi thư mục con hợp lệ,
// không đi theo symlink và bỏ qua thư mục bị lọc hoặc staging.
func (w *Watcher) addDirTree(startDir string) error {
	startDir = filepath.Clean(startDir)
	if startDir != w.root {
		info, err := os.Lstat(startDir)
		if err != nil || !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
			return nil
		}
	}

	return filepath.WalkDir(startDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			if p != w.root {
				if d != nil && d.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			return walkErr
		}
		if p != w.root {
			if d.Type()&fs.ModeSymlink != 0 {
				return nil
			}
			if !d.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(w.root, p)
			if err != nil {
				return fs.SkipDir
			}
			if IsStagingPath(rel) {
				return fs.SkipDir
			}
			wire, normErr := NormalizeWirePathFromOS(rel)
			if normErr == nil {
				if w.filter.Match(wire, true) {
					return fs.SkipDir
				}
				if w.filter.GitEnabled() {
					_ = w.filter.LoadGitIgnoreFile(wire, filepath.Join(p, ".gitignore"))
				}
			}
		} else if w.filter.GitEnabled() {
			_ = w.filter.LoadGitIgnoreFile("", filepath.Join(p, ".gitignore"))
		}

		clean := filepath.Clean(p)
		if _, exists := w.watched[clean]; !exists {
			if err := w.addWatch(clean); err != nil {
				if clean != w.root {
					if _, statErr := os.Lstat(clean); errors.Is(statErr, os.ErrNotExist) {
						return fs.SkipDir
					}
				}
				return err
			}
			w.watched[clean] = struct{}{}
		}
		return nil
	})
}

// removeWatchedSubtree gỡ bỏ target và mọi thư mục con của nó khỏi danh sách theo dõi.
func (w *Watcher) removeWatchedSubtree(target string) {
	if target == w.root {
		return
	}
	prefix := target + string(filepath.Separator)
	for p := range w.watched {
		if p == target || strings.HasPrefix(p, prefix) {
			if w.fsw != nil {
				_ = w.fsw.Remove(p)
			}
			delete(w.watched, p)
		}
	}
}

func (w *Watcher) runEvents() {
	defer func() {
		w.closeErr = w.stopFSWatcher()
		close(w.changes)
		close(w.done)
	}()

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	var (
		timerCh    <-chan time.Time
		pending    bool
		firstEvent time.Time
	)

	fireNow := func() {
		pending = false
		timer.Stop()
		timerCh = nil
		w.emit()
	}

	recordChange := func(now time.Time) {
		if !pending {
			pending = true
			firstEvent = now
		}
		deadline := now.Add(w.quiet)
		maxDeadline := firstEvent.Add(w.maxWait)
		if deadline.After(maxDeadline) {
			deadline = maxDeadline
		}
		delay := deadline.Sub(now)
		if delay <= 0 {
			fireNow()
			return
		}
		timer.Stop()
		timer.Reset(delay)
		timerCh = timer.C
	}

	for {
		select {
		case <-w.ctx.Done():
			return

		case <-timerCh:
			pending = false
			timerCh = nil
			w.emit()

		case <-w.queue.notify:
			events, hasErr := w.queue.drain()
			if hasErr {
				// ErrEventOverflow hoặc lỗi khác từ fsnotify: phát batch ngay để bên gọi quét lại toàn bộ.
				fireNow()
				// Bổ sung watch cho thư mục mới có thể đã bỏ lỡ trong lúc tràn hàng đợi.
				if err := w.addDirTree(w.root); err != nil {
					w.fallbackToPolling(false)
					return
				}
			}

			var sawChange bool
			for _, ev := range events {
				changed, fallback := w.handleEvent(ev)
				if fallback {
					w.fallbackToPolling(!hasErr)
					return
				}
				if changed {
					sawChange = true
				}
			}
			if sawChange && !hasErr {
				recordChange(time.Now())
			}
		}
	}
}

// handleEvent xử lý một sự kiện fsnotify: cập nhật danh sách thư mục đang theo dõi,
// lọc sự kiện staging/bộ lọc, và cho biết có cần kích hoạt batch hay chuyển sang polling không.
func (w *Watcher) handleEvent(ev fsnotify.Event) (changed bool, fallback bool) {
	if ev.Name == "" {
		return true, false
	}
	clean := filepath.Clean(ev.Name)
	_, wasWatched := w.watched[clean]

	// Trên Windows và kqueue, tạo/sửa/xóa mục con sẽ phát thêm Write trên chính thư mục cha.
	// Bỏ qua Write thuần túy trên thư mục đang theo dõi để tệp bị lọc hoặc staging bên trong
	// không gây ra batch giả (mọi mục con hợp lệ đã có sự kiện riêng từ watch của thư mục đó).
	if ev.Op == fsnotify.Write && wasWatched {
		return false, false
	}

	// Khi thư mục đang theo dõi bị xóa hoặc đổi tên, gỡ khỏi danh sách watch mà không báo lỗi.
	if ev.Has(fsnotify.Remove) || ev.Has(fsnotify.Rename) {
		w.removeWatchedSubtree(clean)
	}

	if clean == w.root {
		return true, false
	}

	rel, err := filepath.Rel(w.root, clean)
	if err != nil || rel == "." {
		return true, false
	}

	// Luôn bỏ qua thư mục staging (.croc-sync-tmp-*), kể cả khi đường dẫn không chuẩn hóa được.
	if IsStagingPath(rel) {
		return false, false
	}

	isDir := wasWatched
	var info fs.FileInfo
	var statErr error
	if !ev.Has(fsnotify.Remove) {
		info, statErr = os.Lstat(clean)
		if statErr == nil {
			isDir = info.IsDir()
		}
	}

	wire, normErr := NormalizeWirePathFromOS(rel)
	if normErr != nil {
		// Không chuẩn hóa được: vẫn coi là thay đổi theo thiết kế.
		return true, false
	}

	if w.filter.Match(wire, isDir) {
		return false, false
	}

	if w.filter.GitEnabled() && !isDir && filepath.Base(clean) == ".gitignore" {
		_ = w.filter.LoadGitIgnoreFile(parentDir(wire), clean)
	}

	// Thư mục mới tạo (hoặc vừa di chuyển vào): gắn watch đệ quy cho nó và mọi thư mục con
	// đã kịp tạo trước khi gắn watch.
	if ev.Has(fsnotify.Create) && statErr == nil && info.IsDir() && info.Mode()&fs.ModeSymlink == 0 {
		if addErr := w.addDirTree(clean); addErr != nil {
			return true, true
		}
	}

	return true, false
}

func (w *Watcher) fallbackToPolling(emitFirst bool) {
	_ = w.stopFSWatcher()
	w.mode.Store(modeCodePolling)
	if emitFirst {
		w.emit()
	}
	prev, err := treeFingerprint(w.ctx, w.root, w.baseFilter)
	if w.ctx.Err() != nil {
		return
	}
	w.pollLoop(prev, err == nil)
}

func (w *Watcher) runPolling(prev uint64, hasPrev bool) {
	defer func() {
		close(w.changes)
		close(w.done)
	}()
	w.pollLoop(prev, hasPrev)
}

func (w *Watcher) pollLoop(prev uint64, hasPrev bool) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			fp, err := treeFingerprint(w.ctx, w.root, w.baseFilter)
			if w.ctx.Err() != nil {
				return
			}
			if err != nil {
				if hasPrev {
					hasPrev = false
					w.emit()
				}
				continue
			}
			if !hasPrev || fp != prev {
				prev = fp
				hasPrev = true
				w.emit()
			}
		}
	}
}

// treeFingerprint quét nhanh cây thư mục với bộ lọc và băm toàn bộ metadata
// (path, size, mtime, mode, kind) để phát hiện thay đổi ở chế độ polling.
func treeFingerprint(ctx context.Context, root string, filter *Filter) (uint64, error) {
	m, err := ScanDir(ctx, root, ScanOptions{Filter: cloneFilter(filter)})
	if err != nil {
		return 0, err
	}
	h := xxhash.New()
	var buf [24]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(len(m.Entries)))
	if m.Complete {
		buf[8] = 1
	}
	_, _ = h.Write(buf[:9])

	for _, e := range m.Entries {
		binary.LittleEndian.PutUint64(buf[0:8], uint64(len(e.Path)))
		_, _ = h.Write(buf[:8])
		_, _ = h.WriteString(e.Path)

		var mtime int64
		if !e.IsDir() {
			// Bỏ qua mtime của thư mục (khớp diff.go) để tệp bị lọc/staging bên trong
			// không làm đổi dấu vân tay của thư mục cha.
			mtime = e.ModTime.UnixNano()
		}
		binary.LittleEndian.PutUint64(buf[0:8], uint64(e.Size))
		binary.LittleEndian.PutUint64(buf[8:16], uint64(mtime))
		binary.LittleEndian.PutUint32(buf[16:20], uint32(e.Mode))
		binary.LittleEndian.PutUint32(buf[20:24], uint32(e.Kind))
		_, _ = h.Write(buf[:24])
	}
	return h.Sum64(), nil
}

// cloneFilter tạo bản sao độc lập của Filter để tránh race khi Watcher và bên gọi cùng quét.
func cloneFilter(f *Filter) *Filter {
	if f == nil {
		return NewFilter(FilterSpec{})
	}
	cp := NewFilter(f.Spec())
	for k, layer := range f.gitLayers {
		cp.gitLayers[k] = append(gitLayer(nil), layer...)
	}
	return cp
}
