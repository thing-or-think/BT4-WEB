package foldersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/croc"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/tcp"
)

// startRelayOnPorts chạy một relay croc mới trên đúng danh sách cổng đã cho (dùng cho test nối lại).
func startRelayOnPorts(t *testing.T, ports []string) testRelay {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	start := func(port string, banner ...string) {
		wg.Go(func() { _ = tcp.RunCtx(ctx, "warn", "127.0.0.1", port, "pass123", banner...) })
	}
	start(ports[0], strings.Join(ports[1:], ","))
	for _, p := range ports[1:] {
		start(p)
	}
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); wg.Wait() }) }
	t.Cleanup(stop)
	deadline := time.Now().Add(5 * time.Second)
	for _, p := range ports {
		for {
			c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", p), 50*time.Millisecond)
			if err == nil {
				_ = c.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("relay port %s not ready", p)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return testRelay{
		address: net.JoinHostPort("127.0.0.1", ports[0]),
		ports:   append([]string(nil), ports...),
		stop:    stop,
	}
}

func shortWatchOptions() WatchOptions {
	return WatchOptions{
		Quiet:        50 * time.Millisecond,
		MaxWait:      200 * time.Millisecond,
		PollInterval: 100 * time.Millisecond,
	}
}

func setWatchTimers(t *testing.T, keepalive, reconnectInit, reconnectMax time.Duration) {
	t.Helper()
	oldKeepalive := watchKeepaliveInterval
	oldInit := watchReconnectInitial
	oldMax := watchReconnectMax
	watchKeepaliveInterval = keepalive
	watchReconnectInitial = reconnectInit
	watchReconnectMax = reconnectMax
	t.Cleanup(func() {
		watchKeepaliveInterval = oldKeepalive
		watchReconnectInitial = oldInit
		watchReconnectMax = oldMax
	})
}

type eventLog struct {
	mu     sync.Mutex
	events []Event
	notify chan struct{}
}

func newEventLog() *eventLog {
	return &eventLog{notify: make(chan struct{}, 1)}
}

func (l *eventLog) Report(e Event) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
	select {
	case l.notify <- struct{}{}:
	default:
	}
}

func (l *eventLog) countKind(kind EventKind) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.events {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

func (l *eventLog) hasMatch(fn func(Event) bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.ContainsFunc(l.events, fn)
}

func (l *eventLog) snapshot() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Event(nil), l.events...)
}

type watchPair struct {
	ctx      context.Context
	cancel   context.CancelFunc
	srcDir   string
	dstDir   string
	hostLog  *eventLog
	joinLog  *eventLog
	hostDone chan error
	joinDone chan error
}

func startWatchPair(t *testing.T, relay testRelay, srcDir, dstDir string, filter FilterSpec, watchOpts WatchOptions, diff DiffOptions, decide func(*Plan) (Decision, error)) *watchPair {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	secret := fmt.Sprintf("%d-folder-sync-watch", 8000+roomCounter.Add(1))
	p := &watchPair{
		ctx:      ctx,
		cancel:   cancel,
		srcDir:   srcDir,
		dstDir:   dstDir,
		hostLog:  newEventLog(),
		joinLog:  newEventLog(),
		hostDone: make(chan error, 1),
		joinDone: make(chan error, 1),
	}

	go func() {
		_, err := RunSyncHost(ctx, HostOptions{
			Dir:          srcDir,
			Filter:       filter,
			Croc:         crocOptions(relay, secret, true),
			Reporter:     p.hostLog,
			Watch:        true,
			WatchOptions: watchOpts,
		})
		p.hostDone <- err
	}()
	go func() {
		time.Sleep(100 * time.Millisecond)
		_, err := RunSyncJoin(ctx, JoinOptions{
			Dir:      dstDir,
			Diff:     diff,
			Croc:     crocOptions(relay, secret, false),
			Decide:   decide,
			Reporter: p.joinLog,
		})
		p.joinDone <- err
	}()
	return p
}

func (p *watchPair) waitEvent(t *testing.T, label string, log *eventLog, timeout time.Duration, pred func(Event) bool) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if log.hasMatch(pred) {
			return
		}
		select {
		case <-log.notify:
		case err := <-p.hostDone:
			p.cancel()
			t.Fatalf("host exited early while waiting for %s: %v", label, err)
		case err := <-p.joinDone:
			p.cancel()
			t.Fatalf("join exited early while waiting for %s: %v", label, err)
		case <-timer.C:
			p.cancel()
			t.Fatalf("timed out after %s waiting for %s (events=%+v)", timeout, label, log.snapshot())
		}
	}
}

func (p *watchPair) waitInitialRound(t *testing.T) {
	t.Helper()
	p.waitEvent(t, "host round 1 result", p.hostLog, 15*time.Second, func(e Event) bool {
		return e.Kind == EventResult && e.Result != nil
	})
	p.waitEvent(t, "join round 1 commit", p.joinLog, 15*time.Second, func(e Event) bool {
		return e.Kind == EventCommitted && e.Result != nil
	})
}

func (p *watchPair) waitCond(t *testing.T, desc string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		select {
		case err := <-p.hostDone:
			p.cancel()
			t.Fatalf("host exited early while waiting for %s: %v", desc, err)
		case err := <-p.joinDone:
			p.cancel()
			t.Fatalf("join exited early while waiting for %s: %v", desc, err)
		default:
		}
		if time.Now().After(deadline) {
			p.cancel()
			t.Fatalf("timed out after %s waiting for %s", timeout, desc)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (p *watchPair) stopAndCheckNil(t *testing.T, timeout time.Duration) {
	t.Helper()
	p.cancel()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var gotHost, gotJoin bool
	var hostErr, joinErr error
	for !gotHost || !gotJoin {
		select {
		case hostErr = <-p.hostDone:
			gotHost = true
			p.hostDone = nil
		case joinErr = <-p.joinDone:
			gotJoin = true
			p.joinDone = nil
		case <-timer.C:
			t.Fatalf("watch did not exit within %s (gotHost=%v gotJoin=%v)", timeout, gotHost, gotJoin)
		}
	}
	if hostErr != nil || joinErr != nil {
		t.Fatalf("canceled watch errors: host=%v join=%v, want nil", hostErr, joinErr)
	}
	assertNoStagingLeft(t, p.dstDir)
}

func assertNoStagingLeft(t *testing.T, dstDir string) {
	t.Helper()
	entries, err := os.ReadDir(dstDir)
	if err != nil {
		t.Fatalf("readdir %s: %v", dstDir, err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), StagingDirPrefix) && e.Name() != LockFileName {
			t.Fatalf("staging directory left behind: %s", e.Name())
		}
	}
}

func fileHasContent(dir, rel, want string) bool {
	data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
	return err == nil && string(data) == want
}

func TestWatchBasicEditsAndBurst(t *testing.T) {
	setWatchTimers(t, 1*time.Second, 100*time.Millisecond, 500*time.Millisecond)
	relay := startRelay(t)
	srcDir, dstDir := t.TempDir(), t.TempDir()

	writeFile(t, srcDir, "hello.txt", "initial")
	writeFile(t, srcDir, "rename-me.txt", "rename payload")

	wp := startWatchPair(t, relay, srcDir, dstDir, FilterSpec{}, shortWatchOptions(), DiffOptions{Delete: true}, confirmAll)
	wp.waitInitialRound(t)
	assertSameTree(t, srcDir, dstDir)

	// 1. Sửa một tệp -> đích có nội dung mới trong vòng 3 giây.
	writeFile(t, srcDir, "hello.txt", "modified-content-v2")
	wp.waitCond(t, "hello.txt updated", 3*time.Second, func() bool {
		return fileHasContent(dstDir, "hello.txt", "modified-content-v2")
	})

	// 2. Tạo thư mục con mới kèm nhiều tệp -> tới đích đầy đủ.
	writeFile(t, srcDir, "sub/nested/a.txt", "alpha")
	writeFile(t, srcDir, "sub/nested/b.txt", "beta")
	wp.waitCond(t, "new subdirectory files", 5*time.Second, func() bool {
		return fileHasContent(dstDir, "sub/nested/a.txt", "alpha") &&
			fileHasContent(dstDir, "sub/nested/b.txt", "beta")
	})

	// 3. Lưu kiểu trình soạn thảo (ghi tệp tạm rồi đổi tên đè lên tệp đích).
	tmpPath := filepath.Join(srcDir, "hello.txt.tmp")
	finalPath := filepath.Join(srcDir, "hello.txt")
	if err := os.WriteFile(tmpPath, []byte("editor-atomic-save-v3"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		t.Fatal(err)
	}
	wp.waitCond(t, "editor-style atomic save", 5*time.Second, func() bool {
		return fileHasContent(dstDir, "hello.txt", "editor-atomic-save-v3") &&
			!exists(dstDir, "hello.txt.tmp")
	})

	// 4. Đổi tên tệp với Diff.Delete=true -> tên cũ biến mất, tên mới xuất hiện.
	if err := os.Rename(filepath.Join(srcDir, "rename-me.txt"), filepath.Join(srcDir, "renamed.txt")); err != nil {
		t.Fatal(err)
	}
	wp.waitCond(t, "renamed file synced and old removed", 5*time.Second, func() bool {
		return !exists(dstDir, "rename-me.txt") &&
			fileHasContent(dstDir, "renamed.txt", "rename payload")
	})

	// 5. Tạo 500 tệp cùng lúc -> tất cả tới đích và số lượt nhỏ (< 20).
	roundsBefore := wp.joinLog.countKind(EventRoundStart)
	if err := os.MkdirAll(filepath.Join(srcDir, "burst"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range 500 {
		name := filepath.Join(srcDir, "burst", fmt.Sprintf("f%03d.txt", i))
		if err := os.WriteFile(name, []byte(fmt.Sprintf("burst-%03d", i)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	wp.waitCond(t, "all 500 burst files arrived", 30*time.Second, func() bool {
		for i := range 500 {
			if !fileHasContent(dstDir, fmt.Sprintf("burst/f%03d.txt", i), fmt.Sprintf("burst-%03d", i)) {
				return false
			}
		}
		return true
	})
	burstRounds := wp.joinLog.countKind(EventRoundStart) - roundsBefore
	if burstRounds <= 0 || burstRounds >= 20 {
		t.Fatalf("500-file burst used %d rounds, want 1..19", burstRounds)
	}

	wp.stopAndCheckNil(t, 5*time.Second)
}

func TestWatchDeleteSafetyPerRound(t *testing.T) {
	setWatchTimers(t, 1*time.Second, 100*time.Millisecond, 500*time.Millisecond)
	relay := startRelay(t)
	srcDir, dstDir := t.TempDir(), t.TempDir()

	for i := range 6 {
		writeFile(t, srcDir, fmt.Sprintf("f%d.txt", i), fmt.Sprintf("body-%d", i))
	}

	// Decide=nil nghĩa là đồng ý tiếp tục nhưng KHÔNG xác nhận xóa >50%.
	wp := startWatchPair(t, relay, srcDir, dstDir, FilterSpec{}, shortWatchOptions(), DiffOptions{Delete: true}, nil)
	wp.waitInitialRound(t)
	assertSameTree(t, srcDir, dstDir)

	// Xóa 4/6 tệp (>50%) ở nguồn -> máy đích phải giữ lại các xóa đó trong Result.DeletesHeld.
	for i := range 4 {
		if err := os.Remove(filepath.Join(srcDir, fmt.Sprintf("f%d.txt", i))); err != nil {
			t.Fatal(err)
		}
	}
	wp.waitEvent(t, "deletes held event", wp.joinLog, 10*time.Second, func(e Event) bool {
		return e.Kind == EventCommitted && e.Result != nil && e.Result.DeletesHeld >= 4 && e.Result.Deleted == 0
	})
	for i := range 4 {
		if !exists(dstDir, fmt.Sprintf("f%d.txt", i)) {
			t.Fatalf("f%d.txt was deleted despite >50%% guard", i)
		}
	}

	// Chế độ watch vẫn phải tiếp tục chạy và đồng bộ các chỉnh sửa tiếp theo.
	writeFile(t, srcDir, "f5.txt", "updated-after-held-deletes")
	writeFile(t, srcDir, "added.txt", "new-file-after-held-deletes")
	wp.waitCond(t, "edits after held deletes", 5*time.Second, func() bool {
		return fileHasContent(dstDir, "f5.txt", "updated-after-held-deletes") &&
			fileHasContent(dstDir, "added.txt", "new-file-after-held-deletes")
	})
	for i := range 4 {
		if !exists(dstDir, fmt.Sprintf("f%d.txt", i)) {
			t.Fatalf("f%d.txt was deleted on a later round without confirmation", i)
		}
	}

	wp.stopAndCheckNil(t, 5*time.Second)
}

func TestWatchOneSessionTenEdits(t *testing.T) {
	setWatchTimers(t, 1*time.Second, 100*time.Millisecond, 500*time.Millisecond)

	var hostClients, joinClients atomic.Int32
	origNewClient := newClient
	newClient = func(ctx context.Context, ops croc.Options) (*croc.Client, error) {
		if ops.IsSender {
			hostClients.Add(1)
		} else {
			joinClients.Add(1)
		}
		return origNewClient(ctx, ops)
	}
	t.Cleanup(func() { newClient = origNewClient })

	relay := startRelay(t)
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "counter.txt", "init")

	wp := startWatchPair(t, relay, srcDir, dstDir, FilterSpec{}, shortWatchOptions(), DiffOptions{}, confirmAll)
	wp.waitInitialRound(t)

	for i := 1; i <= 10; i++ {
		want := fmt.Sprintf("edit-%02d-%s", i, strings.Repeat("x", i))
		writeFile(t, srcDir, "counter.txt", want)
		wp.waitCond(t, fmt.Sprintf("edit %d", i), 5*time.Second, func() bool {
			return fileHasContent(dstDir, "counter.txt", want)
		})
	}

	if gotH, gotJ := hostClients.Load(), joinClients.Load(); gotH != 1 || gotJ != 1 {
		t.Fatalf("newClient calls host=%d join=%d, want 1 each", gotH, gotJ)
	}
	wp.stopAndCheckNil(t, 5*time.Second)
}

func TestWatchReconnectSamePorts(t *testing.T) {
	// Đặt backoff đầu 500ms để relay mới kịp mở lại đúng cổng trước lần thử kết nối lại đầu tiên.
	setWatchTimers(t, 1*time.Second, 500*time.Millisecond, 1*time.Second)

	var (
		mu          sync.Mutex
		hostSecrets []string
		joinSecrets []string
	)
	origNewClient := newClient
	newClient = func(ctx context.Context, ops croc.Options) (*croc.Client, error) {
		mu.Lock()
		if ops.IsSender {
			hostSecrets = append(hostSecrets, ops.SharedSecret)
		} else {
			joinSecrets = append(joinSecrets, ops.SharedSecret)
		}
		mu.Unlock()
		return origNewClient(ctx, ops)
	}
	t.Cleanup(func() { newClient = origNewClient })

	relay := startRelay(t)
	ports := append([]string(nil), relay.ports...)
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "doc.txt", "before-drop")

	wp := startWatchPair(t, relay, srcDir, dstDir, FilterSpec{}, shortWatchOptions(), DiffOptions{}, confirmAll)
	wp.waitInitialRound(t)
	if !fileHasContent(dstDir, "doc.txt", "before-drop") {
		t.Fatal("initial file missing before relay stop")
	}

	// Dừng relay để cắt phiên hiện tại, rồi khởi động lại relay mới trên đúng các cổng đó.
	relay.stop()
	_ = startRelayOnPorts(t, ports)

	// Sửa tệp ở nguồn -> cả hai bên nối lại bằng RejoinCode và chỉnh sửa tới đích.
	writeFile(t, srcDir, "doc.txt", "after-reconnect-content")

	wp.waitEvent(t, "host reconnected", wp.hostLog, 15*time.Second, func(e Event) bool {
		return e.Kind == EventReconnected
	})
	wp.waitEvent(t, "join reconnected", wp.joinLog, 15*time.Second, func(e Event) bool {
		return e.Kind == EventReconnected
	})
	wp.waitCond(t, "edit after reconnect arrived", 10*time.Second, func() bool {
		return fileHasContent(dstDir, "doc.txt", "after-reconnect-content")
	})

	mu.Lock()
	hSec := append([]string(nil), hostSecrets...)
	jSec := append([]string(nil), joinSecrets...)
	mu.Unlock()

	if len(hSec) != 2 || len(jSec) != 2 {
		t.Fatalf("newClient calls host=%d join=%d, want 2 each", len(hSec), len(jSec))
	}
	if hSec[1] == hSec[0] || hSec[1] != jSec[1] {
		t.Fatalf("rejoin secrets invalid: host=%q join=%q", hSec, jSec)
	}
	wp.stopAndCheckNil(t, 5*time.Second)
}

func TestWatchCancelCleansUp(t *testing.T) {
	setWatchTimers(t, 200*time.Millisecond, 100*time.Millisecond, 500*time.Millisecond)
	relay := startRelay(t)
	before := runtime.NumGoroutine()

	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "note.txt", "hello watch")

	wp := startWatchPair(t, relay, srcDir, dstDir, FilterSpec{}, shortWatchOptions(), DiffOptions{}, confirmAll)
	wp.waitInitialRound(t)

	// Hủy ctx -> cả hai phía trả nil trong 5s, không còn staging và số goroutine trở lại gần mức ban đầu.
	wp.stopAndCheckNil(t, 5*time.Second)

	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before+3 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+3 {
		buf := make([]byte, 1<<20)
		t.Fatalf("goroutines %d -> %d\n%s", before, n, buf[:runtime.Stack(buf, true)])
	}
}

func TestWatchKeepalivePingPong(t *testing.T) {
	// Kiểm tra đơn vị Session xử lý ping/pong ở mọi trạng thái hợp lệ (và vẫn từ chối khi phiên đã hỏng).
	sm, scontent := srcManifest()
	s := NewSourceSession(sm, &fakeSource{content: scontent}, Hello{Round: 1, Watch: true})
	out, err := s.Handle(message.Message{Type: message.TypeSyncPing})
	if err != nil || len(out) != 1 || out[0].Type != message.TypeSyncPong {
		t.Fatalf("ping reply = (%v, %v), want sync-pong", out, err)
	}
	out, err = s.Handle(message.Message{Type: message.TypeSyncPong})
	if err != nil || len(out) != 0 {
		t.Fatalf("pong reply = (%v, %v), want nil", out, err)
	}
	if _, err := s.Handle(message.Message{Type: message.TypeSyncNeed}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("expected protocol error, got %v", err)
	}
	if _, err := s.Handle(message.Message{Type: message.TypeSyncPing}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("failed session accepted ping: %v", err)
	}

	// Kiểm tra tích hợp: nhiều nhịp keepalive giữa hai lượt không làm gián đoạn đồng bộ.
	setWatchTimers(t, 100*time.Millisecond, 100*time.Millisecond, 500*time.Millisecond)
	relay := startRelay(t)
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "keep.txt", "v1")

	wp := startWatchPair(t, relay, srcDir, dstDir, FilterSpec{}, shortWatchOptions(), DiffOptions{}, confirmAll)
	wp.waitInitialRound(t)

	time.Sleep(350 * time.Millisecond)
	writeFile(t, srcDir, "keep.txt", "v2-after-keepalive")
	wp.waitCond(t, "sync after keepalives", 5*time.Second, func() bool {
		return fileHasContent(dstDir, "keep.txt", "v2-after-keepalive")
	})

	wp.stopAndCheckNil(t, 5*time.Second)
}

func TestWatchKeepaliveAndPostReconnectRound(t *testing.T) {
	// Kiểm tra đơn vị: Session đã xong không báo Ready() lại, và hook chấp nhận ping/pong ở mọi trạng thái.
	sm, scontent := srcManifest()
	dstEnv := &fakeDest{dst: man(), decision: Decision{Proceed: true}}
	src := NewSourceSession(sm, &fakeSource{content: scontent}, Hello{Round: 1, Watch: true})
	dst := NewDestSession(dstEnv, DiffOptions{}, Hello{Watch: true})
	if _, err := exchange(t, src, dst); err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if ready, _ := src.Ready(); !ready {
		t.Fatal("source session must be ready while waiting for result")
	}
	for _, p := range dst.Need() {
		if err := dst.AllowReceive(p, int64(len(scontent[p])), false); err != nil {
			t.Fatalf("AllowReceive(%q): %v", p, err)
		}
	}
	finishMsgs, err := dst.Finish()
	if err != nil {
		t.Fatalf("dst.Finish: %v", err)
	}
	if _, err := src.Handle(finishMsgs[0]); err != nil {
		t.Fatalf("src.Handle(result): %v", err)
	}
	if ready, _ := src.Ready(); ready {
		t.Fatal("completed source session must not report Ready")
	}

	srcHook := &sourceWatchHook{
		ctx:         context.Background(),
		state:       &sourceWatchState{round: 1, established: true},
		wakeCh:      make(chan struct{}, 1),
		s:           src,
		roundNum:    1,
		roundActive: true,
	}
	if !srcHook.KeepOpen() {
		t.Fatal("sourceWatchHook.KeepOpen() = false, want true")
	}
	if ready, _ := srcHook.Ready(); ready {
		t.Fatal("sourceWatchHook.Ready() = true between rounds, want false")
	}
	var srcSent []message.Message
	if err := srcHook.Handle(message.Message{Type: message.TypeSyncPong}, func(m message.Message) error {
		srcSent = append(srcSent, m)
		return nil
	}); err != nil || len(srcSent) != 0 {
		t.Fatalf("srcHook pong = (%v, %v), want no replies", srcSent, err)
	}
	if err := srcHook.Handle(message.Message{Type: message.TypeSyncPing}, func(m message.Message) error {
		srcSent = append(srcSent, m)
		return nil
	}); err != nil || len(srcSent) != 1 || srcSent[0].Type != message.TypeSyncPong {
		t.Fatalf("srcHook ping = (%v, %v), want sync-pong", srcSent, err)
	}
	if !src.Done() {
		t.Fatal("ping/pong changed source session state")
	}

	dstHook := &destWatchHook{
		ctx:   context.Background(),
		state: &destWatchState{watch: true, round: 1},
		s:     dst,
	}
	var dstSent []message.Message
	if err := dstHook.Handle(message.Message{Type: message.TypeSyncPing}, func(m message.Message) error {
		dstSent = append(dstSent, m)
		return nil
	}); err != nil || len(dstSent) != 1 || dstSent[0].Type != message.TypeSyncPong {
		t.Fatalf("dstHook ping = (%v, %v), want sync-pong", dstSent, err)
	}
	if !dst.Done() {
		t.Fatal("ping changed destination session state")
	}

	// Kiểm tra tích hợp: keepalive 20ms qua nhiều chu kỳ không gây nối lại, và lượt đầu sau khi
	// ép nối lại phải nhận và commit tệp mới/đã sửa bình thường.
	setWatchTimers(t, 20*time.Millisecond, 500*time.Millisecond, 1*time.Second)

	var hostClients, joinClients atomic.Int32
	origNewClient := newClient
	newClient = func(ctx context.Context, ops croc.Options) (*croc.Client, error) {
		if ops.IsSender {
			hostClients.Add(1)
		} else {
			joinClients.Add(1)
		}
		return origNewClient(ctx, ops)
	}
	t.Cleanup(func() { newClient = origNewClient })

	relay := startRelay(t)
	ports := append([]string(nil), relay.ports...)
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "keep.txt", "v1")

	wp := startWatchPair(t, relay, srcDir, dstDir, FilterSpec{}, shortWatchOptions(), DiffOptions{Delete: true}, confirmAll)
	wp.waitInitialRound(t)
	if !fileHasContent(dstDir, "keep.txt", "v1") {
		t.Fatal("initial file missing")
	}

	// Chờ qua nhiều nhịp keepalive 20ms khi rảnh rồi chỉnh sửa: phải giữ đúng 1 phiên croc.
	time.Sleep(250 * time.Millisecond)
	writeFile(t, srcDir, "keep.txt", "v2-after-fast-keepalives")
	wp.waitCond(t, "sync after 20ms keepalives", 5*time.Second, func() bool {
		return fileHasContent(dstDir, "keep.txt", "v2-after-fast-keepalives")
	})
	time.Sleep(200 * time.Millisecond)
	if gotH, gotJ := hostClients.Load(), joinClients.Load(); gotH != 1 || gotJ != 1 {
		t.Fatalf("keepalives triggered reconnect: host=%d join=%d, want 1 each", gotH, gotJ)
	}
	if wp.hostLog.countKind(EventReconnecting) != 0 || wp.joinLog.countKind(EventReconnecting) != 0 {
		t.Fatalf("unexpected reconnecting event during keepalives: host=%d join=%d",
			wp.hostLog.countKind(EventReconnecting), wp.joinLog.countKind(EventReconnecting))
	}

	// Ép ngắt kết nối bằng cách dừng relay, sửa và thêm tệp rồi mở lại relay trên cùng các cổng.
	commitsBefore := wp.joinLog.countKind(EventCommitted)
	relay.stop()
	writeFile(t, srcDir, "keep.txt", "v3-first-round-after-reconnect")
	writeFile(t, srcDir, "after.txt", "post-reconnect-new")
	_ = startRelayOnPorts(t, ports)

	wp.waitEvent(t, "host reconnected", wp.hostLog, 15*time.Second, func(e Event) bool {
		return e.Kind == EventReconnected
	})
	wp.waitEvent(t, "join reconnected", wp.joinLog, 15*time.Second, func(e Event) bool {
		return e.Kind == EventReconnected
	})
	wp.waitCond(t, "first post-reconnect round committed", 10*time.Second, func() bool {
		return fileHasContent(dstDir, "keep.txt", "v3-first-round-after-reconnect") &&
			fileHasContent(dstDir, "after.txt", "post-reconnect-new") &&
			wp.joinLog.countKind(EventCommitted) > commitsBefore
	})
	if gotH, gotJ := hostClients.Load(), joinClients.Load(); gotH != 2 || gotJ != 2 {
		t.Fatalf("after forced reconnect: host=%d join=%d, want 2 each", gotH, gotJ)
	}

	// Tiếp tục nhiều nhịp keepalive 20ms trên phiên đã nối lại và đồng bộ thêm một lượt nữa.
	time.Sleep(250 * time.Millisecond)
	writeFile(t, srcDir, "after.txt", "v4-after-post-reconnect-keepalives")
	wp.waitCond(t, "sync after post-reconnect keepalives", 5*time.Second, func() bool {
		return fileHasContent(dstDir, "after.txt", "v4-after-post-reconnect-keepalives")
	})
	if gotH, gotJ := hostClients.Load(), joinClients.Load(); gotH != 2 || gotJ != 2 {
		t.Fatalf("post-reconnect keepalives triggered reconnect: host=%d join=%d, want 2 each", gotH, gotJ)
	}

	wp.stopAndCheckNil(t, 5*time.Second)
}

func TestWatchLatency(t *testing.T) {
	if os.Getenv("FOLDERSYNC_LATENCY") != "1" {
		t.Skip("set FOLDERSYNC_LATENCY=1 to run watch latency test")
	}
	relay := startRelay(t)
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "lat.txt", "init")

	// Dùng Quiet/MaxWait mặc định (WatchOptions rỗng).
	wp := startWatchPair(t, relay, srcDir, dstDir, FilterSpec{}, WatchOptions{}, DiffOptions{}, confirmAll)
	wp.waitInitialRound(t)

	const edits = 20
	samples := make([]time.Duration, 0, edits)
	for i := range edits {
		if i > 0 {
			time.Sleep(time.Second)
		}
		want := fmt.Sprintf("latency-%02d-%s", i, strings.Repeat("a", i%7+1))
		started := time.Now()
		writeFile(t, srcDir, "lat.txt", want)
		wp.waitCond(t, fmt.Sprintf("latency edit %d", i), 10*time.Second, func() bool {
			return fileHasContent(dstDir, "lat.txt", want)
		})
		samples = append(samples, time.Since(started))
	}

	slices.Sort(samples)
	p50 := samples[len(samples)/2]
	p95 := samples[(len(samples)*95+99)/100-1]
	t.Logf("watch latency (n=%d): p50=%s p95=%s max=%s", len(samples), p50, p95, samples[len(samples)-1])
	if p95 >= 2*time.Second {
		t.Fatalf("watch latency p95 = %s, want < 2s", p95)
	}

	wp.stopAndCheckNil(t, 5*time.Second)
}

// refusingDestHook mô phỏng máy đích hoàn tất bắt tay đồng bộ rồi gửi bản tin TypeError
// tường minh để từ chối phiên watch.
type refusingDestHook struct {
	reason string
}

var (
	_ croc.SyncHook     = (*refusingDestHook)(nil)
	_ croc.SyncFinisher = (*refusingDestHook)(nil)
)

func (h *refusingDestHook) Feature() string { return message.FeatureFolderSync }

func (h *refusingDestHook) Start(send func(message.Message) error) error {
	hello, err := encodeMsg(message.TypeSyncHello, Hello{
		Version: ProtocolVersion,
		Role:    RoleDestination,
	})
	if err != nil {
		return err
	}
	return send(hello)
}

func (h *refusingDestHook) Handle(m message.Message, send func(message.Message) error) error {
	if m.Type == message.TypeSyncManifestEnd {
		msgs, err := encodePathLists(message.TypeSyncNeed, nil)
		if err != nil {
			return err
		}
		for _, out := range msgs {
			if err := send(out); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *refusingDestHook) Ready() (bool, func(croc.FileInfo) bool) {
	return false, nil
}

func (h *refusingDestHook) BeforeFinish(send func(message.Message) error) error {
	_ = send(message.Message{
		Type:    message.TypeError,
		Message: h.reason,
	})
	return errors.New(h.reason)
}

func TestWatchPeerErrorRefusalStopsImmediately(t *testing.T) {
	// Kiểm tra đơn vị isFatalWatchError phân biệt lỗi tường minh từ đối phương với lỗi mạng.
	if !isFatalWatchError(fmt.Errorf("peer error: refusing files")) {
		t.Fatal("isFatalWatchError(peer error) = false, want true")
	}
	if !isFatalWatchError(fmt.Errorf("wrapped: %w", fmt.Errorf("peer error: unsupported version"))) {
		t.Fatal("isFatalWatchError(wrapped peer error) = false, want true")
	}
	if !isFatalWatchError(errors.Join(errors.New("cleanup"), fmt.Errorf("peer error: watch refused"))) {
		t.Fatal("isFatalWatchError(joined peer error) = false, want true")
	}
	if isFatalWatchError(errors.New("transfer disconnected: connection reset by peer")) {
		t.Fatal("isFatalWatchError(network disconnect) = true, want false")
	}

	setWatchTimers(t, 1*time.Second, 100*time.Millisecond, 500*time.Millisecond)

	var newClientCalls atomic.Int32
	origNewClient := newClient
	newClient = func(ctx context.Context, ops croc.Options) (*croc.Client, error) {
		newClientCalls.Add(1)
		return origNewClient(ctx, ops)
	}
	t.Cleanup(func() { newClient = origNewClient })

	relay := startRelay(t)
	srcDir, dstRoot := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "hello.txt", "initial")

	const refusalReason = "Watching (--watch) is not supported by web sync yet; run croc sync without --watch"
	secret := fmt.Sprintf("%d-folder-sync-refuse", 8500+roomCounter.Add(1))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	hostDone := make(chan error, 1)
	go func() {
		_, err := RunSyncHost(ctx, HostOptions{
			Dir:          srcDir,
			Croc:         crocOptions(relay, secret, true),
			Watch:        true,
			WatchOptions: shortWatchOptions(),
		})
		hostDone <- err
	}()

	recvDone := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		recvOps := crocOptions(relay, secret, false)
		recvOps.ReceiveRoot = dstRoot
		recvOps.SyncHook = &refusingDestHook{reason: refusalReason}
		client, err := origNewClient(ctx, recvOps)
		if err != nil {
			recvDone <- err
			return
		}
		recvDone <- client.Receive()
	}()

	select {
	case hostErr := <-hostDone:
		if hostErr == nil {
			t.Fatal("RunSyncHost returned nil, want peer refusal error")
		}
		if !strings.Contains(hostErr.Error(), peerErrorPrefix) || !strings.Contains(hostErr.Error(), refusalReason) {
			t.Fatalf("RunSyncHost error = %v, want peer error containing %q", hostErr, refusalReason)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatalf("RunSyncHost did not return within 5s after destination refusal (newClient calls=%d)", newClientCalls.Load())
	}

	if got := newClientCalls.Load(); got != 1 {
		t.Fatalf("newClient calls = %d, want 1", got)
	}

	select {
	case <-recvDone:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("refusing destination did not exit within 5s")
	}
}

type closeableTCPProxy struct {
	addr     string
	listener net.Listener
	mu       sync.Mutex
	closed   bool
	conns    []net.Conn
	wg       sync.WaitGroup
}

func startCloseableTCPProxy(t *testing.T, target string) *closeableTCPProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &closeableTCPProxy{
		addr:     ln.Addr().String(),
		listener: ln,
	}
	p.wg.Go(func() {
		for {
			clientConn, err := ln.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", target)
			if err != nil {
				_ = clientConn.Close()
				continue
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				_ = clientConn.Close()
				_ = upstream.Close()
				continue
			}
			p.conns = append(p.conns, clientConn, upstream)
			p.mu.Unlock()
			p.wg.Go(func() {
				_, _ = io.Copy(upstream, clientConn)
				_ = upstream.Close()
			})
			p.wg.Go(func() {
				_, _ = io.Copy(clientConn, upstream)
				_ = clientConn.Close()
			})
		}
	})
	t.Cleanup(func() {
		p.Close()
		p.wg.Wait()
	})
	return p
}

func (p *closeableTCPProxy) Close() {
	_ = p.listener.Close()
	p.mu.Lock()
	p.closed = true
	conns := append([]net.Conn(nil), p.conns...)
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// observeHelloHook bọc sourceWatchHook để báo hiệu ngay sau khi nguồn xử lý sync-hello của đích.
type observeHelloHook struct {
	croc.SyncHook
	rounds  croc.SyncRounds
	onHello func()
}

var (
	_ croc.SyncHook   = (*observeHelloHook)(nil)
	_ croc.SyncRounds = (*observeHelloHook)(nil)
)

func (h *observeHelloHook) Handle(m message.Message, send func(message.Message) error) error {
	err := h.SyncHook.Handle(m, send)
	if err == nil && m.Type == message.TypeSyncHello && h.onHello != nil {
		h.onHello()
	}
	return err
}

func (h *observeHelloHook) Wake() <-chan struct{} {
	return h.rounds.Wake()
}

func (h *observeHelloHook) OnWake(send func(message.Message) error) error {
	return h.rounds.OnWake(send)
}

func (h *observeHelloHook) KeepOpen() bool {
	return h.rounds.KeepOpen()
}

func (h *observeHelloHook) NextFiles() ([]croc.FileInfo, []croc.FileInfo, int) {
	return h.rounds.NextFiles()
}

// helloThenCloseDestHook mô phỏng trình duyệt gửi sync-hello ngay lúc bắt đầu rồi đóng kết nối
// sau khi đọc sync-hello (watch=true) của nguồn mà không gửi thêm bản tin nào.
type helloThenCloseDestHook struct {
	hostSawHello <-chan struct{}
	closeConn    func()
	closeOnce    sync.Once
}

var _ croc.SyncHook = (*helloThenCloseDestHook)(nil)

func (h *helloThenCloseDestHook) Feature() string { return message.FeatureFolderSync }

func (h *helloThenCloseDestHook) Start(send func(message.Message) error) error {
	hello, err := encodeMsg(message.TypeSyncHello, Hello{
		Version: ProtocolVersion,
		Role:    RoleDestination,
	})
	if err != nil {
		return err
	}
	return send(hello)
}

func (h *helloThenCloseDestHook) Handle(m message.Message, _ func(message.Message) error) error {
	if m.Type == message.TypeSyncHello {
		h.closeOnce.Do(func() {
			select {
			case <-h.hostSawHello:
			case <-time.After(3 * time.Second):
			}
			h.closeConn()
		})
	}
	return nil
}

func (h *helloThenCloseDestHook) Ready() (bool, func(croc.FileInfo) bool) {
	return false, nil
}

func TestWatchCloseAfterHelloStopsWithoutReconnect(t *testing.T) {
	// Kiểm tra đơn vị: sync-hello lượt 1 của đích KHÔNG đánh dấu established hay mở khóa rejoinCode;
	// chỉ khi đích chấp nhận lượt 1 (sync-need / sync-hash-request / sync-result) mới thiết lập watch.
	sm, scontent := srcManifest()
	state := &sourceWatchState{}
	src := NewSourceSession(sm, &fakeSource{content: scontent}, Hello{
		Round:      1,
		Watch:      true,
		RejoinCode: "rejoin-round-1",
	})
	hook := &sourceWatchHook{
		ctx:               context.Background(),
		state:             state,
		wakeCh:            make(chan struct{}, 1),
		s:                 src,
		roundNum:          1,
		roundActive:       true,
		pendingRejoinCode: "rejoin-round-1",
	}
	discardSend := func(message.Message) error { return nil }
	if err := hook.Start(discardSend); err != nil {
		t.Fatalf("hook.Start: %v", err)
	}
	destHello, err := encodeMsg(message.TypeSyncHello, Hello{
		Version: ProtocolVersion,
		Role:    RoleDestination,
	})
	if err != nil {
		t.Fatalf("encode dest hello: %v", err)
	}
	if err := hook.Handle(destHello, discardSend); err != nil {
		t.Fatalf("hook.Handle(hello): %v", err)
	}
	if state.established || state.rejoinCode != "" {
		t.Fatalf("after round-1 dest hello: established=%v rejoinCode=%q, want false and empty",
			state.established, state.rejoinCode)
	}
	needMsgs, err := encodePathLists(message.TypeSyncNeed, nil)
	if err != nil {
		t.Fatalf("encode sync-need: %v", err)
	}
	for _, nm := range needMsgs {
		if err := hook.Handle(nm, discardSend); err != nil {
			t.Fatalf("hook.Handle(sync-need): %v", err)
		}
	}
	if !state.established || state.rejoinCode != "rejoin-round-1" {
		t.Fatalf("after round-1 sync-need: established=%v rejoinCode=%q, want true and %q",
			state.established, state.rejoinCode, "rejoin-round-1")
	}

	// Kiểm tra tích hợp: máy đích gửi sync-hello rồi đóng kết nối mà không gửi thêm bản tin nào
	// (mô phỏng trình duyệt từ chối/đóng sớm) -> RunSyncHost thoát trong 5s với newClient gọi 1 lần.
	setWatchTimers(t, 1*time.Second, 100*time.Millisecond, 500*time.Millisecond)

	hostSawHello := make(chan struct{})
	var sawHelloOnce sync.Once
	var newClientCalls atomic.Int32
	origNewClient := newClient
	newClient = func(cctx context.Context, ops croc.Options) (*croc.Client, error) {
		if ops.IsSender {
			newClientCalls.Add(1)
			if rounds, ok := ops.SyncHook.(croc.SyncRounds); ok {
				ops.SyncHook = &observeHelloHook{
					SyncHook: ops.SyncHook,
					rounds:   rounds,
					onHello:  func() { sawHelloOnce.Do(func() { close(hostSawHello) }) },
				}
			}
		}
		return origNewClient(cctx, ops)
	}
	t.Cleanup(func() { newClient = origNewClient })

	relay := startRelay(t)
	proxy := startCloseableTCPProxy(t, relay.address)
	srcDir, dstRoot := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "hello.txt", "initial")

	secret := fmt.Sprintf("%d-folder-sync-hello-close", 8600+roomCounter.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	hostDone := make(chan error, 1)
	go func() {
		_, err := RunSyncHost(ctx, HostOptions{
			Dir:          srcDir,
			Croc:         crocOptions(relay, secret, true),
			Watch:        true,
			WatchOptions: shortWatchOptions(),
		})
		hostDone <- err
	}()

	recvDone := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		recvOps := crocOptions(relay, secret, false)
		recvOps.RelayAddress = proxy.addr
		recvOps.ReceiveRoot = dstRoot
		recvOps.SyncHook = &helloThenCloseDestHook{
			hostSawHello: hostSawHello,
			closeConn:    proxy.Close,
		}
		client, err := origNewClient(ctx, recvOps)
		if err != nil {
			recvDone <- err
			return
		}
		recvDone <- client.Receive()
	}()

	select {
	case hostErr := <-hostDone:
		if hostErr == nil {
			t.Fatal("RunSyncHost returned nil, want error when destination closes after hello")
		}
		if isPeerError(hostErr) {
			t.Fatalf("RunSyncHost error = %v, expected plain disconnect (no peer error frame sent)", hostErr)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatalf("RunSyncHost did not return within 5s after destination closed post-hello (newClient calls=%d)", newClientCalls.Load())
	}

	select {
	case <-hostSawHello:
	default:
		t.Fatal("host did not process destination sync-hello before disconnect")
	}

	if got := newClientCalls.Load(); got != 1 {
		t.Fatalf("newClient calls = %d, want 1", got)
	}

	select {
	case <-recvDone:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("destination did not exit within 5s")
	}
}
