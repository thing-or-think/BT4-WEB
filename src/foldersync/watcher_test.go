package foldersync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func testWatchOptions(filter *Filter) WatchOptions {
	return WatchOptions{
		Filter:       filter,
		Quiet:        50 * time.Millisecond,
		MaxWait:      300 * time.Millisecond,
		PollInterval: 100 * time.Millisecond,
	}
}

func waitForBatch(t *testing.T, w *Watcher, timeout time.Duration) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case _, ok := <-w.Changes():
		if !ok {
			t.Fatal("watcher Changes channel closed unexpectedly")
		}
	case <-timer.C:
		t.Fatalf("timed out after %v waiting for watcher batch (mode=%s)", timeout, w.Mode())
	}
}

func expectNoBatch(t *testing.T, w *Watcher, d time.Duration) {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case _, ok := <-w.Changes():
		if ok {
			t.Fatalf("received unexpected watcher batch within %v (mode=%s)", d, w.Mode())
		}
	case <-timer.C:
	}
}

func TestWatcherBasicFileOps(t *testing.T) {
	root := t.TempDir()
	w, err := NewWatcher(context.Background(), root, testWatchOptions(nil))
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	defer w.Close()

	if w.Mode() != watchModeEvents {
		t.Fatalf("Mode() = %q, want %q", w.Mode(), watchModeEvents)
	}

	// Tạo tệp mới.
	p1 := filepath.Join(root, "alpha.txt")
	if err := os.WriteFile(p1, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForBatch(t, w, 5*time.Second)

	// Sửa nội dung tệp.
	if err := os.WriteFile(p1, []byte("hello-updated"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForBatch(t, w, 5*time.Second)

	// Đổi tên tệp.
	p2 := filepath.Join(root, "beta.txt")
	if err := os.Rename(p1, p2); err != nil {
		t.Fatal(err)
	}
	waitForBatch(t, w, 5*time.Second)

	// Xóa tệp.
	if err := os.Remove(p2); err != nil {
		t.Fatal(err)
	}
	waitForBatch(t, w, 5*time.Second)
}

func TestWatcherNewSubdirectory(t *testing.T) {
	root := t.TempDir()
	w, err := NewWatcher(context.Background(), root, testWatchOptions(nil))
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	defer w.Close()

	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	waitForBatch(t, w, 5*time.Second)

	// Chờ ngắn rồi ghi tệp bên trong thư mục vừa tạo: phải có batch mới cho thao tác ghi tệp.
	time.Sleep(100 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(sub, "note.txt"), []byte("inside"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForBatch(t, w, 5*time.Second)

	// Xóa thư mục đang theo dõi phải gỡ watch gọn gàng và phát batch.
	if err := os.RemoveAll(sub); err != nil {
		t.Fatal(err)
	}
	waitForBatch(t, w, 5*time.Second)
}

func TestWatcherNestedMkdirAll(t *testing.T) {
	root := t.TempDir()
	w, err := NewWatcher(context.Background(), root, testWatchOptions(nil))
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	defer w.Close()

	deep := filepath.Join(root, "a", "b", "c")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deep, "first.txt"), []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForBatch(t, w, 5*time.Second)

	// Đợi ổn định và xả tín hiệu dư (nếu first.txt rơi sang batch thứ hai) trước khi ghi tệp kế tiếp.
	time.Sleep(150 * time.Millisecond)
	select {
	case <-w.Changes():
	default:
	}

	if err := os.WriteFile(filepath.Join(deep, "second.txt"), []byte("two"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForBatch(t, w, 5*time.Second)
}

func TestWatcherIgnoresStagingAndFiltered(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}

	filter := NewFilter(FilterSpec{Exclude: []string{".log"}})
	w, err := NewWatcher(context.Background(), root, testWatchOptions(filter))
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	defer w.Close()

	// Thao tác trong thư mục staging nội bộ phải bị bỏ qua hoàn toàn.
	staging := filepath.Join(root, ".croc-sync-tmp-abc")
	if err := os.Mkdir(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "part.bin"), []byte("123"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "part.bin"), []byte("123456"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(staging); err != nil {
		t.Fatal(err)
	}

	// Thao tác trên tệp/thư mục khớp bộ lọc Exclude ".log" cũng phải bị bỏ qua.
	logFile := filepath.Join(root, "app.log")
	if err := os.WriteFile(logFile, []byte("line1"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logFile, []byte("line2"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(logFile); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "nested.LOG"), []byte("ignored"), 0o644); err != nil {
		t.Fatal(err)
	}
	logDir := filepath.Join(root, "archive.log")
	if err := os.Mkdir(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "inside.txt"), []byte("ignored"), 0o644); err != nil {
		t.Fatal(err)
	}

	expectNoBatch(t, w, 500*time.Millisecond)

	// Xác nhận Watcher vẫn hoạt động bình thường với tệp hợp lệ.
	if err := os.WriteFile(filepath.Join(root, "visible.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForBatch(t, w, 5*time.Second)
}

func TestWatcherDebounce(t *testing.T) {
	t.Run("ten_writes_collapse_to_one_batch", func(t *testing.T) {
		root := t.TempDir()
		p := writeFile(t, root, "data.txt", "init")

		opts := WatchOptions{
			Quiet:        200 * time.Millisecond,
			MaxWait:      2 * time.Second,
			PollInterval: 100 * time.Millisecond,
		}
		w, err := NewWatcher(context.Background(), root, opts)
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		defer w.Close()

		// 10 lần ghi cách nhau 20ms (~180ms tổng, nhỏ hơn MaxWait 2s, với Quiet 200ms) phải gộp thành đúng 1 batch.
		start := time.Now()
		for i := range 10 {
			if i > 0 {
				if wait := time.Until(start.Add(time.Duration(i) * 20 * time.Millisecond)); wait > 0 {
					time.Sleep(wait)
				}
			}
			if err := os.WriteFile(p, fmt.Appendf(nil, "write-%d", i), 0o644); err != nil {
				t.Fatal(err)
			}
		}

		waitForBatch(t, w, 5*time.Second)
		expectNoBatch(t, w, 400*time.Millisecond)
	})

	t.Run("continuous_writes_respect_max_wait", func(t *testing.T) {
		root := t.TempDir()
		p := writeFile(t, root, "stream.txt", "init")

		opts := testWatchOptions(nil)
		w, err := NewWatcher(context.Background(), root, opts)
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		defer w.Close()

		writeDone := make(chan error, 1)
		start := time.Now()
		go func() {
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			end := time.After(time.Second)
			seq := 0
			for {
				if err := os.WriteFile(p, fmt.Appendf(nil, "tick-%d", seq), 0o644); err != nil {
					writeDone <- err
					return
				}
				seq++
				select {
				case <-end:
					writeDone <- nil
					return
				case <-ticker.C:
				}
			}
		}()

		var (
			batches      int
			firstArrival time.Duration
		)
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()

		writing := true
		var settle <-chan time.Time
		var settleTimer *time.Timer
		for {
			select {
			case err := <-writeDone:
				if err != nil {
					t.Fatalf("continuous write failed: %v", err)
				}
				writing = false
				settleTimer = time.NewTimer(250 * time.Millisecond)
				settle = settleTimer.C
			case _, ok := <-w.Changes():
				if !ok {
					t.Fatal("watcher Changes channel closed unexpectedly")
				}
				if batches == 0 {
					firstArrival = time.Since(start)
				}
				batches++
			case <-settle:
				if settleTimer != nil {
					settleTimer.Stop()
				}
				goto done
			case <-deadline.C:
				if settleTimer != nil {
					settleTimer.Stop()
				}
				t.Fatalf("timed out waiting for continuous write batches (got %d, writing=%v)", batches, writing)
			}
		}

	done:
		if batches < 2 {
			t.Fatalf("batches = %d, want at least 2 during 1s continuous writes", batches)
		}
		maxAllowed := opts.MaxWait + 350*time.Millisecond
		if firstArrival > maxAllowed {
			t.Fatalf("first batch arrived after %v, want <= %v (MaxWait=%v)", firstArrival, maxAllowed, opts.MaxWait)
		}
	})
}

func TestWatcherEditorAtomicSave(t *testing.T) {
	root := t.TempDir()
	target := writeFile(t, root, "document.txt", "before")

	w, err := NewWatcher(context.Background(), root, testWatchOptions(nil))
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	defer w.Close()

	// Mô phỏng trình soạn thảo lưu an toàn: ghi tệp tạm rồi đổi tên ghi đè tệp đích.
	tmp := filepath.Join(root, "document.txt.tmp")
	if err := os.WriteFile(tmp, []byte("after-atomic-save"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, target); err != nil {
		t.Fatal(err)
	}

	waitForBatch(t, w, 5*time.Second)
	expectNoBatch(t, w, 250*time.Millisecond)
}

func TestWatcherBurst500Files(t *testing.T) {
	root := t.TempDir()
	w, err := NewWatcher(context.Background(), root, testWatchOptions(nil))
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	defer w.Close()

	const (
		totalFiles = 500
		workers    = 8
	)

	createDone := make(chan error, 1)
	go func() {
		jobs := make(chan int, totalFiles)
		for i := range totalFiles {
			jobs <- i
		}
		close(jobs)

		var (
			wg       sync.WaitGroup
			errMu    sync.Mutex
			firstErr error
		)
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					name := filepath.Join(root, fmt.Sprintf("burst-%03d.txt", i))
					if err := os.WriteFile(name, []byte("payload"), 0o644); err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						errMu.Unlock()
						return
					}
				}
			}()
		}
		wg.Wait()
		createDone <- firstErr
	}()

	var (
		batches     int
		settle      <-chan time.Time
		settleTimer *time.Timer
	)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()

	for {
		select {
		case err := <-createDone:
			if err != nil {
				t.Fatalf("burst create failed: %v", err)
			}
			settleTimer = time.NewTimer(300 * time.Millisecond)
			settle = settleTimer.C
		case _, ok := <-w.Changes():
			if !ok {
				t.Fatal("watcher Changes channel closed unexpectedly")
			}
			batches++
		case <-settle:
			if settleTimer != nil {
				settleTimer.Stop()
			}
			if batches < 1 || batches > 20 {
				t.Fatalf("batches = %d for %d files, want between 1 and 20", batches, totalFiles)
			}
			return
		case <-deadline.C:
			if settleTimer != nil {
				settleTimer.Stop()
			}
			t.Fatalf("timed out waiting for 500-file burst (batches=%d)", batches)
		}
	}
}

func TestWatcherErrorOverflowImmediateBatch(t *testing.T) {
	root := t.TempDir()
	// Dùng Quiet dài (2s) để chứng minh lỗi overflow bỏ qua debounce và phát batch ngay lập tức.
	opts := WatchOptions{
		Quiet:        2 * time.Second,
		MaxWait:      5 * time.Second,
		PollInterval: 100 * time.Millisecond,
	}
	w, err := NewWatcher(context.Background(), root, opts)
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	defer w.Close()

	start := time.Now()
	w.injectError(fsnotify.ErrEventOverflow)
	waitForBatch(t, w, 5*time.Second)

	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("overflow batch took %v, expected immediate delivery (< 1s, Quiet=%v)", elapsed, opts.Quiet)
	}
}

func TestWatcherPollingFallback(t *testing.T) {
	t.Run("constructor_error", func(t *testing.T) {
		origNew := fsWatcherNew
		fsWatcherNew = func() (*fsnotify.Watcher, error) {
			return nil, errors.New("simulated inotify instance limit")
		}
		t.Cleanup(func() { fsWatcherNew = origNew })

		root := t.TempDir()
		w, err := NewWatcher(context.Background(), root, testWatchOptions(nil))
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		defer w.Close()

		if w.Mode() != watchModePolling {
			t.Fatalf("Mode() = %q, want %q", w.Mode(), watchModePolling)
		}

		// Tạo tệp mới phải tạo batch ở chế độ polling.
		p := filepath.Join(root, "polled.txt")
		if err := os.WriteFile(p, []byte("v1"), 0o644); err != nil {
			t.Fatal(err)
		}
		waitForBatch(t, w, 5*time.Second)

		// Sửa nội dung tệp phải tạo batch tiếp theo.
		if err := os.WriteFile(p, []byte("v2-longer-content"), 0o644); err != nil {
			t.Fatal(err)
		}
		waitForBatch(t, w, 5*time.Second)
	})

	t.Run("add_watch_error", func(t *testing.T) {
		origAdd := watchAddHook
		watchAddHook = func(string) error {
			return errors.New("simulated ENOSPC watch limit")
		}
		t.Cleanup(func() { watchAddHook = origAdd })

		root := t.TempDir()
		w, err := NewWatcher(context.Background(), root, testWatchOptions(nil))
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		defer w.Close()

		if w.Mode() != watchModePolling {
			t.Fatalf("Mode() = %q, want %q", w.Mode(), watchModePolling)
		}

		if err := os.WriteFile(filepath.Join(root, "after-fallback.txt"), []byte("ok"), 0o644); err != nil {
			t.Fatal(err)
		}
		waitForBatch(t, w, 5*time.Second)
	})
}

func TestWatcherCloseAndCancelNoGoroutineLeak(t *testing.T) {
	waitGoroutinesNear := func(t *testing.T, baseline int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if runtime.NumGoroutine() <= baseline+2 {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("goroutine leak: got %d, baseline %d", runtime.NumGoroutine(), baseline)
	}

	t.Run("close_events_mode", func(t *testing.T) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		baseline := runtime.NumGoroutine()

		for range 3 {
			w, err := NewWatcher(context.Background(), t.TempDir(), testWatchOptions(nil))
			if err != nil {
				t.Fatalf("NewWatcher: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}
		waitGoroutinesNear(t, baseline)
	})

	t.Run("ctx_cancel_events_mode", func(t *testing.T) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		baseline := runtime.NumGoroutine()

		for range 3 {
			ctx, cancel := context.WithCancel(context.Background())
			w, err := NewWatcher(ctx, t.TempDir(), testWatchOptions(nil))
			if err != nil {
				cancel()
				t.Fatalf("NewWatcher: %v", err)
			}
			cancel()
			// Đợi kênh Changes đóng chứng tỏ goroutine đã dừng mà không cần gọi Close.
			select {
			case _, ok := <-w.Changes():
				if ok {
					t.Fatal("unexpected change signal on cancel")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("watcher did not stop after ctx cancel")
			}
		}
		waitGoroutinesNear(t, baseline)
	})

	t.Run("ctx_cancel_polling_mode", func(t *testing.T) {
		origNew := fsWatcherNew
		fsWatcherNew = func() (*fsnotify.Watcher, error) {
			return nil, errors.New("force polling")
		}
		t.Cleanup(func() { fsWatcherNew = origNew })

		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		baseline := runtime.NumGoroutine()

		for range 3 {
			ctx, cancel := context.WithCancel(context.Background())
			w, err := NewWatcher(ctx, t.TempDir(), testWatchOptions(nil))
			if err != nil {
				cancel()
				t.Fatalf("NewWatcher: %v", err)
			}
			cancel()
			select {
			case _, ok := <-w.Changes():
				if ok {
					t.Fatal("unexpected change signal on cancel")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("polling watcher did not stop after ctx cancel")
			}
		}
		waitGoroutinesNear(t, baseline)
	})
}
