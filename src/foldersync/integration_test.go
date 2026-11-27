package foldersync_test

// CLI <-> CLI integration tests for `croc sync`. They drive the public
// engine entry points (Contract C) against a local relay and only look at
// the filesystem and the ProgressReporter, so they double as acceptance
// tests for the engine. Until RunSyncHost/RunSyncJoin are implemented they
// skip with a clear message instead of failing.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/foldersync"
	"github.com/schollz/croc/v11/src/tcp"
)

const (
	relayPassword      = "pass123"
	integrationTimeout = 60 * time.Second
	watchDeadline      = 2 * time.Second
	syncTempPrefix     = ".croc-sync-tmp-"
)

// ---------------------------------------------------------------------
// Local relay
// ---------------------------------------------------------------------

func startRelay(t *testing.T) string {
	t.Helper()
	ports := freeConsecutivePorts(t, 5)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	run := func(port string, banner ...string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tcp.RunCtx(ctx, "warn", "127.0.0.1", port, relayPassword, banner...)
		}()
	}
	run(ports[0], strings.Join(ports[1:], ","))
	for _, port := range ports[1:] {
		run(port)
	}
	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("relay goroutines did not stop")
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for _, port := range ports {
		address := net.JoinHostPort("127.0.0.1", port)
		for {
			conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("relay port %s did not become ready: %v", address, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return net.JoinHostPort("127.0.0.1", ports[0])
}

func freeConsecutivePorts(t *testing.T, count int) []string {
	t.Helper()
	for attempt := 0; attempt < 100; attempt++ {
		var seed [2]byte
		_, _ = rand.Read(seed[:])
		base := 20000 + (int(seed[0])<<8|int(seed[1]))%20000
		listeners := make([]net.Listener, 0, count)
		ports := make([]string, 0, count)
		ok := true
		for i := 0; i < count; i++ {
			port := strconv.Itoa(base + i)
			listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", port))
			if err != nil {
				ok = false
				break
			}
			listeners = append(listeners, listener)
			ports = append(ports, port)
		}
		for _, listener := range listeners {
			_ = listener.Close()
		}
		if ok {
			return ports
		}
	}
	t.Fatalf("could not find %d consecutive free ports", count)
	return nil
}

func uniqueCode(t *testing.T) string {
	t.Helper()
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	return "8123-sync-it-" + hex.EncodeToString(suffix[:])
}

// ---------------------------------------------------------------------
// Reporter
// ---------------------------------------------------------------------

type recordingReporter struct {
	mu        sync.Mutex
	decide    func(foldersync.PlanView) bool
	codes     []string
	plans     []foldersync.PlanView
	summaries []foldersync.Summary
	events    []foldersync.WatchEvent
	failures  map[string]error
	progress  chan foldersync.Progress
	rounds    chan foldersync.Summary
}

func newReporter() *recordingReporter {
	return &recordingReporter{
		failures: map[string]error{},
		progress: make(chan foldersync.Progress, 1024),
		rounds:   make(chan foldersync.Summary, 64),
	}
}

func (r *recordingReporter) OnCode(code string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.codes = append(r.codes, code)
}

func (r *recordingReporter) OnStatus(string) {}

func (r *recordingReporter) OnPlan(plan foldersync.PlanView) bool {
	r.mu.Lock()
	r.plans = append(r.plans, plan)
	decide := r.decide
	r.mu.Unlock()
	if decide != nil {
		return decide(plan)
	}
	return true
}

func (r *recordingReporter) OnFileStart(string, int64) {}

func (r *recordingReporter) OnProgress(p foldersync.Progress) {
	select {
	case r.progress <- p:
	default:
	}
}

func (r *recordingReporter) OnFileDone(path string, err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures[path] = err
}

func (r *recordingReporter) OnWatchEvent(ev foldersync.WatchEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recordingReporter) OnRoundDone(s foldersync.Summary) {
	r.mu.Lock()
	r.summaries = append(r.summaries, s)
	r.mu.Unlock()
	select {
	case r.rounds <- s:
	default:
	}
}

func (r *recordingReporter) lastPlan(t *testing.T) foldersync.PlanView {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.plans) == 0 {
		t.Fatal("OnPlan was never called")
	}
	return r.plans[len(r.plans)-1]
}

func (r *recordingReporter) lastSummary(t *testing.T) foldersync.Summary {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.summaries) == 0 {
		t.Fatal("OnRoundDone was never called")
	}
	return r.summaries[len(r.summaries)-1]
}

var _ foldersync.ProgressReporter = (*recordingReporter)(nil)

// ---------------------------------------------------------------------
// Running both sides
// ---------------------------------------------------------------------

type pair struct {
	relay  string
	code   string
	host   foldersync.SyncConfig
	join   foldersync.SyncConfig
	hostR  *recordingReporter
	joinR  *recordingReporter
	result chan [2]error
	cancel context.CancelFunc
}

func newPair(t *testing.T, relay, source, target string) *pair {
	code := uniqueCode(t)
	return &pair{
		relay: relay,
		code:  code,
		host:  foldersync.SyncConfig{Dir: source, Code: code, Relay: relay},
		join:  foldersync.SyncConfig{Dir: target, Code: code, Relay: relay},
		hostR: newReporter(),
		joinR: newReporter(),
	}
}

// start launches Host and Join. Options set on p.host are mirrored to p.join
// where the CLI would pass the same flags on both machines.
func (p *pair) start(t *testing.T, parent context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, integrationTimeout)
	p.cancel = cancel
	p.result = make(chan [2]error, 1)
	p.join.Watch = p.host.Watch
	p.join.Checksum = p.host.Checksum
	go func() {
		var wg sync.WaitGroup
		var hostErr, joinErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			hostErr = foldersync.RunSyncHost(ctx, p.host, p.hostR)
			if errors.Is(hostErr, foldersync.ErrNotImplemented) {
				cancel()
			}
		}()
		go func() {
			defer wg.Done()
			joinErr = foldersync.RunSyncJoin(ctx, p.join, p.joinR)
			if errors.Is(joinErr, foldersync.ErrNotImplemented) {
				cancel()
			}
		}()
		wg.Wait()
		p.result <- [2]error{hostErr, joinErr}
	}()
}

func (p *pair) wait(t *testing.T) (hostErr, joinErr error) {
	t.Helper()
	defer p.cancel()
	select {
	case errs := <-p.result:
		skipIfNotImplemented(t, errs[0], errs[1])
		return errs[0], errs[1]
	case <-time.After(integrationTimeout + 10*time.Second):
		t.Fatal("sync did not finish")
		return nil, nil
	}
}

func (p *pair) run(t *testing.T) {
	t.Helper()
	p.start(t, context.Background())
	hostErr, joinErr := p.wait(t)
	if hostErr != nil || joinErr != nil {
		t.Fatalf("sync failed: host=%v join=%v", hostErr, joinErr)
	}
}

func skipIfNotImplemented(t *testing.T, errs ...error) {
	t.Helper()
	for _, err := range errs {
		if errors.Is(err, foldersync.ErrNotImplemented) {
			t.Skip("croc sync engine is not implemented yet (RunSyncHost/RunSyncJoin stubs)")
		}
	}
}

// ---------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------

// tree maps POSIX relative paths to content; a nil value means a folder.
type tree map[string]*string

func text(s string) *string { return &s }

func writeTree(t *testing.T, root string, spec tree) {
	t.Helper()
	for rel, content := range spec {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if content == nil {
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(*content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

type fileState struct {
	dir     bool
	content string
	mtime   time.Time
	mode    fs.FileMode
}

func readState(t *testing.T, root string) map[string]fileState {
	t.Helper()
	state := map[string]fileState{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		entry := fileState{dir: d.IsDir(), mtime: info.ModTime(), mode: info.Mode()}
		if !d.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			entry.content = string(data)
		}
		state[filepath.ToSlash(rel)] = entry
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func contents(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for rel, entry := range readState(t, root) {
		if entry.dir {
			out[rel] = "<dir>"
		} else {
			out[rel] = entry.content
		}
	}
	return out
}

func requireSameContents(t *testing.T, source, target string) {
	t.Helper()
	want, got := contents(t, source), contents(t, target)
	if fmt.Sprint(want) != fmt.Sprint(got) {
		t.Fatalf("target differs from source\nsource: %v\ntarget: %v", want, got)
	}
}

func requireNoTempFiles(t *testing.T, root string) {
	t.Helper()
	for rel := range readState(t, root) {
		if strings.HasPrefix(filepath.Base(rel), syncTempPrefix) {
			t.Fatalf("temporary file left behind: %s", rel)
		}
	}
}

func planPaths(items []foldersync.PlanItem) []string {
	paths := make([]string, 0, len(items))
	for _, item := range items {
		paths = append(paths, item.Path)
	}
	sort.Strings(paths)
	return paths
}

func requirePaths(t *testing.T, kind string, items []foldersync.PlanItem, want ...string) {
	t.Helper()
	got := planPaths(items)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%s: got %q, want %q", kind, got, want)
	}
}

func nestedSource(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeTree(t, root, tree{
		"top.txt":            text("top"),
		"docs/readme.md":     text("# hello"),
		"docs/empty.txt":     text(""),
		"empty-folder":       nil,
		"a/b/c/d/e/deep.bin": text(strings.Repeat("0123456789", 100)),
		"unicode/café.txt":   text("accent"),
	})
	return root
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

func TestSyncIntegrationMirrorsTreeAndSkipsSecondRun(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	relay := startRelay(t)
	source, target := nestedSource(t), t.TempDir()

	first := newPair(t, relay, source, target)
	first.run(t)
	requireSameContents(t, source, target)
	requireNoTempFiles(t, target)

	// ModTime of the source is restored on the target (§6).
	sourceState, targetState := readState(t, source), readState(t, target)
	for rel, entry := range sourceState {
		if entry.dir {
			continue
		}
		if diff := entry.mtime.Sub(targetState[rel].mtime); diff > 2*time.Second || diff < -2*time.Second {
			t.Errorf("%s: mtime not preserved (source %v, target %v)", rel, entry.mtime, targetState[rel].mtime)
		}
	}

	second := newPair(t, relay, source, target)
	second.host.Delete = true
	second.run(t)
	plan := second.hostR.lastPlan(t)
	if len(plan.Add)+len(plan.Update)+len(plan.Delete) != 0 {
		t.Fatalf("second run should skip everything, got add=%v update=%v delete=%v",
			planPaths(plan.Add), planPaths(plan.Update), planPaths(plan.Delete))
	}
	summary := second.hostR.lastSummary(t)
	if summary.Added+summary.Updated+summary.Deleted != 0 || summary.BytesSent != 0 {
		t.Fatalf("second run transferred data: %+v", summary)
	}
}

func TestSyncIntegrationEmptySourceFolder(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	relay := startRelay(t)
	source, target := t.TempDir(), t.TempDir()
	p := newPair(t, relay, source, target)
	p.run(t)
	if got := contents(t, target); len(got) != 0 {
		t.Fatalf("target should stay empty, got %v", got)
	}
}

func TestSyncIntegrationDryRunChangesNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	relay := startRelay(t)
	source, target := t.TempDir(), t.TempDir()
	writeTree(t, source, tree{"new.txt": text("new"), "changed.txt": text("v2-longer")})
	writeTree(t, target, tree{"changed.txt": text("v1"), "extra.txt": text("bye")})
	before := readState(t, target)

	p := newPair(t, relay, source, target)
	p.host.DryRun = true
	p.host.Delete = true
	p.run(t)

	if after := readState(t, target); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("dry run modified the target\nbefore: %v\nafter:  %v", before, after)
	}
	plan := p.hostR.lastPlan(t)
	if !plan.DryRun {
		t.Fatal("plan should be marked as dry run")
	}
	requirePaths(t, "add", plan.Add, "new.txt")
	requirePaths(t, "update", plan.Update, "changed.txt")
	requirePaths(t, "delete", plan.Delete, "extra.txt")
}

func TestSyncIntegrationDeleteStaysInsideTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	relay := startRelay(t)
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	target := filepath.Join(parent, "target")
	outside := filepath.Join(parent, "outside")
	writeTree(t, source, tree{"keep.txt": text("k")})
	writeTree(t, target, tree{"keep.txt": text("k"), "old.txt": text("old"), "old-dir/nested/file.txt": text("x")})
	writeTree(t, outside, tree{"precious.txt": text("do not touch")})

	p := newPair(t, relay, source, target)
	p.host.Delete = true
	p.run(t)

	requireSameContents(t, source, target)
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("target root must never be deleted: %v", err)
	}
	if got := contents(t, outside); got["precious.txt"] != "do not touch" {
		t.Fatalf("delete escaped the target: %v", got)
	}
	if s := p.hostR.lastSummary(t); s.Deleted == 0 {
		t.Fatalf("expected deletions, got %+v", s)
	}
}

func TestSyncIntegrationWithoutDeleteKeepsExtraFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	relay := startRelay(t)
	source, target := t.TempDir(), t.TempDir()
	writeTree(t, source, tree{"a.txt": text("a")})
	writeTree(t, target, tree{"extra.txt": text("x")})
	newPair(t, relay, source, target).run(t)
	got := contents(t, target)
	if got["a.txt"] != "a" || got["extra.txt"] != "x" {
		t.Fatalf("unexpected target %v", got)
	}
}

func TestSyncIntegrationChecksumDetectsSameSizeChange(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	relay := startRelay(t)
	source, target := t.TempDir(), t.TempDir()
	writeTree(t, source, tree{"data.bin": text("AAAA")})
	writeTree(t, target, tree{"data.bin": text("BBBB")})
	stamp := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, root := range []string{source, target} {
		if err := os.Chtimes(filepath.Join(root, "data.bin"), stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	p := newPair(t, relay, source, target)
	p.host.Checksum = true
	p.run(t)
	requirePaths(t, "update", p.hostR.lastPlan(t).Update, "data.bin")
	requireSameContents(t, source, target)
}

func TestSyncIntegrationDeclinedPlanAborts(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	relay := startRelay(t)
	source, target := t.TempDir(), t.TempDir()
	writeTree(t, source, tree{"a.txt": text("a")})
	writeTree(t, target, tree{"b.txt": text("b")})
	before := readState(t, target)

	p := newPair(t, relay, source, target)
	p.host.Delete = true
	p.hostR.decide = func(plan foldersync.PlanView) bool { return len(plan.Delete) == 0 }
	p.start(t, context.Background())
	hostErr, _ := p.wait(t)
	if !errors.Is(hostErr, foldersync.ErrAborted) {
		t.Fatalf("host error = %v, want ErrAborted", hostErr)
	}
	if after := readState(t, target); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("declined plan modified the target\nbefore: %v\nafter:  %v", before, after)
	}
}

func TestSyncIntegrationCancelThenResync(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	relay := startRelay(t)
	source, target := t.TempDir(), t.TempDir()
	big := make([]byte, 64<<20)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	writeTree(t, source, tree{"small.txt": text("small")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newPair(t, relay, source, target)
	p.start(t, ctx)
	select {
	case <-p.joinR.progress:
	case <-p.hostR.progress:
	case errs := <-p.result:
		skipIfNotImplemented(t, errs[0], errs[1])
		t.Fatalf("sync ended before any progress: host=%v join=%v", errs[0], errs[1])
	case <-time.After(integrationTimeout):
		t.Fatal("no progress reported")
	}
	cancel() // Ctrl+C mid-transfer
	hostErr, joinErr := p.wait(t)
	for side, err := range map[string]error{"host": hostErr, "join": joinErr} {
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("%s returned %v, want context.Canceled", side, err)
		}
	}
	requireNoTempFiles(t, target)

	newPair(t, relay, source, target).run(t)
	requireNoTempFiles(t, target)
	got, err := os.ReadFile(filepath.Join(target, "big.bin"))
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("big.bin not synced after resume: err=%v len=%d", err, len(got))
	}
}

func TestSyncIntegrationWatchSyncsWithinTwoSeconds(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	relay := startRelay(t)
	source, target := t.TempDir(), t.TempDir()
	writeTree(t, source, tree{"watched.txt": text("v1")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := newPair(t, relay, source, target)
	p.host.Watch = true
	p.start(t, ctx)

	select {
	case <-p.joinR.rounds:
	case <-p.hostR.rounds:
	case errs := <-p.result:
		skipIfNotImplemented(t, errs[0], errs[1])
		t.Fatalf("watch session ended early: host=%v join=%v", errs[0], errs[1])
	case <-time.After(integrationTimeout):
		t.Fatal("first round did not finish")
	}

	changedAt := time.Now()
	writeTree(t, source, tree{"watched.txt": text("v2 changed"), "created/new.txt": text("new")})
	deadline := changedAt.Add(watchDeadline)
	for {
		got := contents(t, target)
		if got["watched.txt"] == "v2 changed" && got["created/new.txt"] == "new" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("watch did not sync within %v: %v", watchDeadline, got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("watch latency: %v", time.Since(changedAt))

	cancel()
	hostErr, joinErr := p.wait(t)
	for side, err := range map[string]error{"host": hostErr, "join": joinErr} {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("%s returned %v after Ctrl+C, want context.Canceled", side, err)
		}
	}
	p.hostR.mu.Lock()
	events := len(p.hostR.events)
	p.hostR.mu.Unlock()
	if events == 0 {
		t.Fatal("OnWatchEvent was never called on the host")
	}
	requireNoTempFiles(t, target)
}

func TestSyncIntegrationDoesNotFollowSymlinkOutsideRoot(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	relay := startRelay(t)
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	target := filepath.Join(parent, "target")
	secret := filepath.Join(parent, "secret")
	writeTree(t, source, tree{"normal.txt": text("ok")})
	writeTree(t, secret, tree{"password.txt": text("hunter2")})
	writeTree(t, target, tree{})
	if err := os.Symlink(secret, filepath.Join(source, "escape")); err != nil {
		t.Skipf("cannot create symlinks here: %v", err)
	}
	newPair(t, relay, source, target).run(t)
	for rel, entry := range readState(t, target) {
		if entry.content == "hunter2" {
			t.Fatalf("content outside the source root leaked through symlink as %s", rel)
		}
	}
}
