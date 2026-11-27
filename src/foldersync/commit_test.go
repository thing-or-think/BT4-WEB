package foldersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// receiveInto mô phỏng croc: chép các tệp cần tải từ nguồn vào staging theo đường dẫn dây.
func receiveInto(t *testing.T, srcDir string, staging *Staging, plan *Plan) {
	t.Helper()
	for _, c := range transferChanges(plan) {
		data, err := os.ReadFile(filepath.Join(srcDir, filepath.FromSlash(c.Path)))
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, staging.Dir(), c.Path, string(data))
	}
}

type commitRun struct {
	plan   *Plan
	result *Result
	err    error
}

// syncOnce chạy đủ một lượt cục bộ: quét, diff, hash, nhận vào staging, commit.
func syncOnce(t *testing.T, srcDir, dstDir string, o DiffOptions, d Decision) commitRun {
	t.Helper()
	ctx := context.Background()
	src, _, plan := syncPlan(t, srcDir, dstDir, nil, o)
	var missing []string
	for _, c := range transferChanges(plan) {
		if len(c.Src.Hash) == 0 {
			missing = append(missing, c.Path)
		}
	}
	if err := HashEntries(ctx, srcDir, src, missing); err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireLock(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	staging, err := NewStaging(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	defer staging.Remove()
	receiveInto(t, srcDir, staging, plan)
	r, err := Commit(ctx, openRoot(t, dstDir), plan, CommitOptions{Staging: staging.Name, Decision: d})
	return commitRun{plan, r, err}
}

var proceed = Decision{Proceed: true, ConfirmDeletes: true}

// treeSnapshot đọc cây thư mục thành path -> nội dung ("<dir>" cho thư mục).
func treeSnapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	m := mustScan(t, dir, ScanOptions{})
	out := map[string]string{}
	for _, e := range m.Entries {
		if e.IsDir() {
			out[e.Path] = "<dir>"
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.DiskPath))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Path] = string(data)
	}
	return out
}

func assertSameTree(t *testing.T, srcDir, dstDir string) {
	t.Helper()
	want, got := treeSnapshot(t, srcDir), treeSnapshot(t, dstDir)
	if fmt.Sprint(want) != fmt.Sprint(got) {
		t.Fatalf("trees differ\nsrc: %v\ndst: %v", want, got)
	}
	src := mustScan(t, srcDir, ScanOptions{})
	dst := mustScan(t, dstDir, ScanOptions{})
	for i, e := range src.Entries {
		if e.IsFile() && !e.ModTime.Equal(dst.Entries[i].ModTime) {
			t.Fatalf("%s mtime %v, want %v", e.Path, dst.Entries[i].ModTime, e.ModTime)
		}
	}
}

// buildFixture tạo nguồn và đích có đủ loại thay đổi.
func buildFixture(t *testing.T) (srcDir, dstDir string) {
	srcDir, dstDir = t.TempDir(), t.TempDir()
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, n := range []string{"same.txt", "docs/keep.md", "k1", "k2", "k3", "k4"} {
		writeFile(t, srcDir, n, "same "+n)
		writeFile(t, dstDir, n, "same "+n)
	}
	writeFile(t, srcDir, "edited.txt", "new content, longer")
	writeFile(t, dstDir, "edited.txt", "old content")
	writeFile(t, srcDir, "samesize.txt", "AAAA")
	writeFile(t, dstDir, "samesize.txt", "BBBB")
	writeFile(t, srcDir, "added/deep/new.bin", "brand new")
	writeFile(t, srcDir, "Readme.md", "readme") // đích có README.md cùng nội dung
	writeFile(t, dstDir, "README.md", "readme")
	writeFile(t, srcDir, "wasfile", "") // thay bằng thư mục dưới đây
	_ = os.Remove(filepath.Join(srcDir, "wasfile"))
	writeFile(t, srcDir, "wasfile/inner.txt", "inner")
	writeFile(t, dstDir, "wasfile", "plain file")
	writeFile(t, srcDir, "wasdir", "now a file")
	writeFile(t, dstDir, "wasdir/child.txt", "child")
	writeFile(t, dstDir, "extra/stale.txt", "stale")
	if err := os.MkdirAll(filepath.Join(srcDir, "emptydir"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{srcDir, dstDir} {
		for _, n := range []string{"same.txt", "docs/keep.md", "k1", "k2", "k3", "k4", "Readme.md", "README.md"} {
			_ = os.Chtimes(filepath.Join(dir, filepath.FromSlash(n)), old, old)
		}
	}
	later := old.Add(time.Hour)
	_ = os.Chtimes(filepath.Join(srcDir, "samesize.txt"), later, later)
	return srcDir, dstDir
}

func TestCommitFullSyncConverges(t *testing.T) {
	srcDir, dstDir := buildFixture(t)
	run := syncOnce(t, srcDir, dstDir, DiffOptions{Delete: true}, proceed)
	if run.err != nil || run.result.Partial() {
		t.Fatalf("err=%v failures=%v", run.err, run.result.Failures)
	}
	assertSameTree(t, srcDir, dstDir)
	// Đổi tên chỉ khác hoa/thường giữ đúng tên nguồn.
	entries, _ := os.ReadDir(dstDir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if !slices.Contains(names, "Readme.md") || slices.Contains(names, "README.md") {
		t.Fatalf("case rename not applied: %v", names)
	}
	if run.result.Deleted == 0 || run.result.Added == 0 || run.result.Updated == 0 || run.result.Bytes == 0 {
		t.Fatalf("result = %+v", run.result)
	}
	if _, err := os.Stat(filepath.Join(dstDir, "extra")); !os.IsNotExist(err) {
		t.Fatal("extra directory not deleted")
	}
	// Không còn staging nào trong đích.
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), StagingDirPrefix) && e.Name() != LockFileName {
			t.Fatalf("staging left behind: %s", e.Name())
		}
	}

	// Lượt 2: không còn gì để làm.
	again := syncOnce(t, srcDir, dstDir, DiffOptions{Delete: true}, proceed)
	if again.plan.HasChanges() || again.result.Bytes != 0 {
		t.Fatalf("second round not idle: %+v", categorize(again.plan))
	}
}

func TestCommitDryRunWritesNothing(t *testing.T) {
	srcDir, dstDir := buildFixture(t)
	before := treeSnapshot(t, dstDir)
	run := syncOnce(t, srcDir, dstDir, DiffOptions{Delete: true}, Decision{})
	if run.err != nil || !run.result.DryRun {
		t.Fatalf("err=%v result=%+v", run.err, run.result)
	}
	if fmt.Sprint(treeSnapshot(t, dstDir)) != fmt.Sprint(before) {
		t.Fatal("dry run changed the destination")
	}
}

func TestCommitHoldsUnconfirmedDeletes(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "a", "a")
	writeFile(t, dstDir, "a", "a")
	for _, n := range []string{"x", "y", "z"} {
		writeFile(t, dstDir, n, n)
	}
	run := syncOnce(t, srcDir, dstDir, DiffOptions{Delete: true}, Decision{Proceed: true})
	if run.err != nil || run.result.DeletesHeld != 3 || run.result.Deleted != 0 {
		t.Fatalf("result = %+v", run.result)
	}
	for _, n := range []string{"x", "y", "z"} {
		if !exists(dstDir, n) {
			t.Fatalf("%s deleted without confirmation", n)
		}
	}
}

// TestCommitFaultInjection tiêm lỗi vào từng bước ghi; sau mỗi lần, mọi tệp đích
// phải là bản cũ hoặc bản mới hoàn chỉnh, và chạy lại thì hội tụ.
func TestCommitFaultInjection(t *testing.T) {
	stages := []string{"verify-staged", "verify-dest", "case-rename", "remove", "mkdir", "rename", "chmod", "chtimes"}
	for _, stage := range stages {
		for _, mode := range []string{"first", "all"} {
			t.Run(stage+"/"+mode, func(t *testing.T) {
				srcDir, dstDir := buildFixture(t)
				oldTree := treeSnapshot(t, dstDir)
				newTree := treeSnapshot(t, srcDir)
				injected := 0
				commitTestHook = func(s, p string) error {
					if s == stage && (mode == "all" || injected == 0) {
						injected++
						return fmt.Errorf("injected %s failure at %s", s, p)
					}
					return nil
				}
				run := syncOnce(t, srcDir, dstDir, DiffOptions{Delete: true}, proceed)
				commitTestHook = nil
				if run.err != nil {
					t.Fatal(run.err)
				}
				if injected == 0 {
					t.Skipf("stage %s not reached", stage)
				}
				if !run.result.Partial() {
					t.Fatal("injected failure not reported")
				}
				for p, content := range treeSnapshot(t, dstDir) {
					if content != oldTree[p] && content != newTree[p] {
						t.Fatalf("%s is neither old nor new: %q", p, content)
					}
				}
				// Chạy lại không tiêm lỗi: hội tụ.
				fix := syncOnce(t, srcDir, dstDir, DiffOptions{Delete: true}, proceed)
				if fix.err != nil || fix.result.Partial() {
					t.Fatalf("rerun: err=%v failures=%v", fix.err, fix.result.Failures)
				}
				assertSameTree(t, srcDir, dstDir)
			})
		}
	}
}

func TestCommitRejectsTamperedStaging(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "a.txt", "genuine")
	writeFile(t, srcDir, "b.txt", "genuine")
	writeFile(t, dstDir, "a.txt", "old")
	ctx := context.Background()
	src, _, plan := syncPlan(t, srcDir, dstDir, nil, DiffOptions{})
	if err := HashEntries(ctx, srcDir, src, []string{"a.txt", "b.txt"}); err != nil {
		t.Fatal(err)
	}
	staging, err := NewStaging(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	defer staging.Remove()
	writeFile(t, staging.Dir(), "a.txt", "evil!!!") // cùng size, khác nội dung
	writeFile(t, staging.Dir(), "b.txt", "short")   // sai size
	r, err := Commit(ctx, openRoot(t, dstDir), plan, CommitOptions{Staging: staging.Name, Decision: proceed})
	if err != nil || len(r.Failures) != 2 || !strings.Contains(strings.Join(r.Failures, "\n"), "does not match") {
		t.Fatalf("err=%v failures=%v", err, r.Failures)
	}
	if got, _ := os.ReadFile(filepath.Join(dstDir, "a.txt")); string(got) != "old" {
		t.Fatalf("a.txt = %q", got)
	}
	if exists(dstDir, "b.txt") {
		t.Fatal("unverified b.txt committed")
	}
	// Thiếu tệp trong staging và thiếu hash nguồn.
	plan.Add[0].Src.Hash = nil
	_ = os.Remove(filepath.Join(staging.Dir(), "a.txt"))
	r, _ = Commit(ctx, openRoot(t, dstDir), plan, CommitOptions{Staging: staging.Name, Decision: proceed})
	if len(r.Failures) != 2 {
		t.Fatalf("failures = %v", r.Failures)
	}
}

func TestCommitKeepsDestinationChangedAfterScan(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "upd.txt", "from source")
	writeFile(t, dstDir, "upd.txt", "old")
	writeFile(t, srcDir, "new.txt", "from source")
	ctx := context.Background()
	src, _, plan := syncPlan(t, srcDir, dstDir, nil, DiffOptions{})
	if err := HashEntries(ctx, srcDir, src, []string{"upd.txt", "new.txt"}); err != nil {
		t.Fatal(err)
	}
	staging, _ := NewStaging(dstDir)
	defer staging.Remove()
	receiveInto(t, srcDir, staging, plan)
	// Người dùng sửa đích sau khi quét, và tạo new.txt.
	writeFile(t, dstDir, "upd.txt", "user edit, keep me")
	writeFile(t, dstDir, "new.txt", "user created")
	r, err := Commit(ctx, openRoot(t, dstDir), plan, CommitOptions{Staging: staging.Name, Decision: proceed})
	if err != nil || len(r.Failures) != 2 {
		t.Fatalf("err=%v failures=%v", err, r.Failures)
	}
	for n, want := range map[string]string{"upd.txt": "user edit, keep me", "new.txt": "user created"} {
		if got, _ := os.ReadFile(filepath.Join(dstDir, n)); string(got) != want {
			t.Fatalf("%s = %q, user change lost", n, got)
		}
	}
}

func TestCommitConflictsAndCanceledContext(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "x", "file now")
	writeFile(t, dstDir, "x/child", "child")
	run := syncOnce(t, srcDir, dstDir, DiffOptions{}, proceed) // không --delete: xung đột
	if run.result.Conflicts != 1 || !run.result.Partial() || !exists(dstDir, "x/child") {
		t.Fatalf("result = %+v", run.result)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	plan := &Plan{Add: []Change{{Path: "a", Src: &Entry{Path: "a"}}}}
	if _, err := Commit(ctx, openRoot(t, dstDir), plan, CommitOptions{Decision: proceed}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestCommitTypeChangeNeedsDeleteConfirmation(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "x", "file now")
	writeFile(t, dstDir, "x/a", "a")
	writeFile(t, dstDir, "x/b", "b")
	run := syncOnce(t, srcDir, dstDir, DiffOptions{Delete: true}, Decision{Proceed: true})
	if !run.plan.RequireDeleteConfirm || !run.result.Partial() || !exists(dstDir, "x/a") {
		t.Fatalf("plan=%v result=%+v", categorize(run.plan), run.result)
	}
	run = syncOnce(t, srcDir, dstDir, DiffOptions{Delete: true}, proceed)
	if run.result.Partial() {
		t.Fatal(run.result.Failures)
	}
	assertSameTree(t, srcDir, dstDir)
}

func TestCommitWindowsLockedAndReadOnlyTargets(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows file sharing semantics")
	}
	srcDir, dstDir := t.TempDir(), t.TempDir()
	for _, n := range []string{"locked.txt", "readonly.txt", "other.txt", "gone-ro.txt"} {
		writeFile(t, srcDir, n, "new "+n)
		writeFile(t, dstDir, n, "old")
	}
	_ = os.Remove(filepath.Join(srcDir, "gone-ro.txt"))
	if err := os.Chmod(filepath.Join(dstDir, "readonly.txt"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(dstDir, "gone-ro.txt"), 0o444); err != nil {
		t.Fatal(err)
	}

	// Giữ tệp mở một lúc: commit phải thử lại rồi thành công.
	f, err := os.Open(filepath.Join(dstDir, "locked.txt"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { time.Sleep(250 * time.Millisecond); _ = f.Close() }()
	run := syncOnce(t, srcDir, dstDir, DiffOptions{Delete: true}, proceed)
	if run.result.Partial() {
		t.Fatalf("failures = %v", run.result.Failures)
	}
	assertSameTree(t, srcDir, dstDir)

	// Tệp bị giữ quá lâu: báo lỗi riêng tệp đó, các tệp khác vẫn xong.
	writeFile(t, srcDir, "locked.txt", "newer content")
	writeFile(t, srcDir, "other.txt", "newer other")
	held, err := os.Open(filepath.Join(dstDir, "locked.txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	oldRetries := fsRetries
	fsRetries = 2
	defer func() { fsRetries = oldRetries }()
	run = syncOnce(t, srcDir, dstDir, DiffOptions{}, proceed)
	if len(run.result.Failures) != 1 || !strings.HasPrefix(run.result.Failures[0], "locked.txt") {
		t.Fatalf("failures = %v", run.result.Failures)
	}
	if got, _ := os.ReadFile(filepath.Join(dstDir, "other.txt")); string(got) != "newer other" {
		t.Fatalf("other.txt = %q", got)
	}
	if got, _ := io.ReadAll(held); string(got) != "new locked.txt" {
		t.Fatalf("locked.txt changed while held: %q", got)
	}
}
