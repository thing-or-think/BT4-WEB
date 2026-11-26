package foldersync

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/receivefs"
)

// syncPlan quét hai thư mục, hash khi cần và trả kế hoạch.
func syncPlan(t *testing.T, srcDir, dstDir string, srcFilter *Filter, o DiffOptions) (*Manifest, *Manifest, *Plan) {
	t.Helper()
	src := mustScan(t, srcDir, ScanOptions{Filter: srcFilter})
	dst := mustScan(t, dstDir, ScanOptions{Filter: NewFilter(src.Filter)})
	return src, dst, compareWithHashes(t, srcDir, dstDir, src, dst, o)
}

// compareWithHashes chạy Compare, hash các tệp được yêu cầu ở cả hai phía rồi chạy lại.
func compareWithHashes(t *testing.T, srcDir, dstDir string, src, dst *Manifest, o DiffOptions) *Plan {
	t.Helper()
	plan, need, err := Compare(src, dst, o)
	if err != nil {
		t.Fatal(err)
	}
	if need != nil {
		ctx := context.Background()
		if err := HashEntries(ctx, srcDir, src, need); err != nil {
			t.Fatal(err)
		}
		if err := HashEntries(ctx, dstDir, dst, DstHashTargets(src, dst, need)); err != nil {
			t.Fatal(err)
		}
		if plan, need, err = Compare(src, dst, o); err != nil || need != nil {
			t.Fatalf("second compare: need=%q err=%v", need, err)
		}
	}
	return plan
}

func openRoot(t *testing.T, dir string) *receivefs.Root {
	t.Helper()
	r, err := receivefs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func exists(dir, rel string) bool {
	_, err := os.Lstat(filepath.Join(dir, filepath.FromSlash(rel)))
	return err == nil
}

func deletePaths(p *Plan) []string {
	var out []string
	for _, c := range p.Delete {
		out = append(out, c.Path)
	}
	return out
}

var committed = DeleteOptions{Committed: true}

func TestApplyDeletesRemovesExtraTree(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	for _, n := range []string{"a", "b", "c", "d"} {
		writeFile(t, srcDir, n, n)
		writeFile(t, dstDir, n, n)
	}
	writeFile(t, dstDir, "old/deep/x", "x")
	writeFile(t, dstDir, "z", "z")

	_, _, plan := syncPlan(t, srcDir, dstDir, nil, DiffOptions{Delete: true})
	if want := []string{"old/deep/x", "old/deep", "old", "z"}; !slices.Equal(deletePaths(plan), want) {
		t.Fatalf("deletes = %q, want %q (deepest first)", deletePaths(plan), want)
	}
	if plan.RequireDeleteConfirm {
		t.Fatal("2 of 6 files must not need confirmation")
	}
	// Một mục đã biến mất trước khi xóa vẫn được coi là xong.
	if err := os.Remove(filepath.Join(dstDir, "z")); err != nil {
		t.Fatal(err)
	}
	deleted, failed, err := ApplyDeletes(context.Background(), openRoot(t, dstDir), plan, committed)
	if err != nil || len(failed) != 0 {
		t.Fatalf("err=%v failed=%v", err, failed)
	}
	if len(deleted) != 4 || exists(dstDir, "old") || exists(dstDir, "z") || !exists(dstDir, "a") {
		t.Fatalf("deleted=%q", deleted)
	}
}

// Luật 1: quét nguồn có lỗi thì tắt xóa cả lượt.
func TestDeleteRuleIncompleteSourceBlocks(t *testing.T) {
	src := man(fe("a", 1, 0, ""))
	src.Complete = false
	src.Skipped = []SkippedPath{{Path: "unreadable", Reason: SkipReadError}}
	dst := man(fe("a", 1, 0, ""), fe("b", 1, 0, ""), fe("c", 1, 0, ""))
	plan, _, err := Compare(src, dst, DiffOptions{Delete: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Delete) != 0 || !strings.Contains(plan.DeleteBlockedReason, "incomplete") {
		t.Fatalf("delete=%v reason=%q", plan.Delete, plan.DeleteBlockedReason)
	}

	// ApplyDeletes cũng tự từ chối kế hoạch bị chặn dù có ai đó điền Delete.
	dir := t.TempDir()
	writeFile(t, dir, "b", "x")
	snap := fe("b", 1, 0, "")
	plan.Delete = []Change{{Path: "b", Dst: &snap}}
	if _, _, err := ApplyDeletes(context.Background(), openRoot(t, dir), plan, DeleteOptions{Committed: true, Confirmed: true}); !errors.Is(err, ErrDeleteBlocked) {
		t.Fatalf("err = %v", err)
	}
	if !exists(dir, "b") {
		t.Fatal("blocked plan deleted a file")
	}
}

// Luật 1 trên đĩa: một thư mục con của nguồn không đọc được giữa lúc quét.
func TestDeleteRuleIncompleteSourceOnDisk(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	for _, n := range []string{"a", "sub/a", "other/x"} {
		writeFile(t, srcDir, n, "x")
		writeFile(t, dstDir, n, "x")
	}
	writeFile(t, dstDir, "extra", "x")
	setScanHook(t, func(stage, rel string) {
		if stage == "readdir" && rel == "other" {
			_ = os.RemoveAll(filepath.Join(srcDir, "other"))
		}
	})
	src := mustScan(t, srcDir, ScanOptions{})
	scanTestHook = nil
	dst := mustScan(t, dstDir, ScanOptions{})
	if src.Complete {
		t.Fatal("source scan must be incomplete")
	}
	plan := compareWithHashes(t, srcDir, dstDir, src, dst, DiffOptions{Delete: true})
	if len(plan.Delete) != 0 || plan.DeleteBlockedReason == "" {
		t.Fatalf("plan = %+v", plan)
	}
}

// Luật 2: tệp bị lọc ở nguồn được bảo vệ ở đích.
func TestDeleteRuleFilteredPathsProtected(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, ".gitignore", "*.cache\n")
	for _, n := range []string{"a", "b", "c"} {
		writeFile(t, srcDir, n, n)
		writeFile(t, dstDir, n, n)
	}
	writeFile(t, dstDir, ".gitignore", "*.cache\n")
	writeFile(t, dstDir, "debug.log", "local log")
	writeFile(t, dstDir, "build.cache", "local cache")
	writeFile(t, dstDir, "secret/key", "local")
	writeFile(t, dstDir, "remove-me", "x")

	f := NewFilter(FilterSpec{Exclude: []string{".log"}, ExcludeExact: []string{"secret"}, Git: true})
	_, dst, plan := syncPlan(t, srcDir, dstDir, f, DiffOptions{Delete: true})
	if got := deletePaths(plan); !slices.Equal(got, []string{"remove-me"}) {
		t.Fatalf("deletes = %q", got)
	}
	if len(dst.Skipped) != 3 {
		t.Fatalf("destination must see filtered items as skipped: %v", dst.Skipped)
	}
	if _, _, err := ApplyDeletes(context.Background(), openRoot(t, dstDir), plan, committed); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"debug.log", "build.cache", "secret/key"} {
		if !exists(dstDir, n) {
			t.Fatalf("protected %s was deleted", n)
		}
	}
}

// Luật 3a: nguồn rỗng mà đích không rỗng.
func TestDeleteRuleEmptySource(t *testing.T) {
	dst := man(fe("a", 1, 0, ""), fe("b", 1, 0, ""))
	plan, _, err := Compare(man(), dst, DiffOptions{Delete: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Delete) != 0 || !strings.Contains(plan.DeleteBlockedReason, "allow-empty-source") {
		t.Fatalf("plan = %+v", plan)
	}
	plan, _, err = Compare(man(), dst, DiffOptions{Delete: true, AllowEmptySource: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Delete) != 2 || plan.DeleteBlockedReason != "" || !plan.RequireDeleteConfirm {
		t.Fatalf("allowed empty source plan = %+v", plan)
	}
	// Không có --delete thì không có gì bị chặn hay xóa.
	plan, _, _ = Compare(man(), dst, DiffOptions{})
	if len(plan.Delete) != 0 || plan.DeleteBlockedReason != "" {
		t.Fatalf("no-delete plan = %+v", plan)
	}
}

// Luật 3b: xóa quá 50% số tệp cần xác nhận riêng.
func TestDeleteRuleOverHalfNeedsConfirm(t *testing.T) {
	src := man(fe("a", 1, 0, ""))
	tests := []struct {
		dst  *Manifest
		want bool
	}{
		{man(fe("a", 1, 0, ""), fe("b", 1, 0, "")), false},                   // 1/2 = 50%
		{man(fe("a", 1, 0, ""), fe("b", 1, 0, ""), fe("c", 1, 0, "")), true}, // 2/3
		{man(fe("a", 1, 0, ""), de("d1"), de("d2"), de("d3")), false},        // chỉ thư mục
		{man(de("x"), fe("x/a", 1, 0, ""), fe("x/b", 1, 0, ""), fe("a", 1, 0, "")), true},
	}
	for i, tt := range tests {
		plan, _, err := Compare(src, tt.dst, DiffOptions{Delete: true})
		if err != nil {
			t.Fatal(err)
		}
		if plan.RequireDeleteConfirm != tt.want {
			t.Errorf("case %d: RequireDeleteConfirm = %v, want %v", i, plan.RequireDeleteConfirm, tt.want)
		}
	}

	dir := t.TempDir()
	writeFile(t, dir, "b", "x")
	snap := fe("b", 1, 0, "")
	plan := &Plan{Delete: []Change{{Path: "b", Dst: &snap}}, RequireDeleteConfirm: true}
	if _, _, err := ApplyDeletes(context.Background(), openRoot(t, dir), plan, committed); !errors.Is(err, ErrDeleteNeedsConfirm) {
		t.Fatalf("err = %v", err)
	}
	if !exists(dir, "b") {
		t.Fatal("unconfirmed plan deleted a file")
	}
}

// Luật 4: chỉ xóa sau commit, và kiểm lại size, ModTime, loại ngay trước khi xóa.
func TestDeleteRuleRecheckBeforeDelete(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	for _, n := range []string{"k1", "k2", "k3", "k4", "k5"} {
		writeFile(t, srcDir, n, n)
		writeFile(t, dstDir, n, n)
	}
	writeFile(t, dstDir, "grew", "x")
	writeFile(t, dstDir, "touched", "x")
	writeFile(t, dstDir, "flipped", "x")
	writeFile(t, dstDir, "dir/old", "x")
	writeFile(t, dstDir, "plain", "x")

	_, _, plan := syncPlan(t, srcDir, dstDir, nil, DiffOptions{Delete: true})
	if len(plan.Delete) != 6 {
		t.Fatalf("deletes = %q", deletePaths(plan))
	}
	root := openRoot(t, dstDir)
	if _, _, err := ApplyDeletes(context.Background(), root, plan, DeleteOptions{}); !errors.Is(err, ErrDeleteBeforeCommit) {
		t.Fatalf("err = %v", err)
	}

	// Đích đổi sau khi quét.
	writeFile(t, dstDir, "grew", "much bigger now")
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(filepath.Join(dstDir, "touched"), later, later); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dstDir, "flipped")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dstDir, "flipped"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dstDir, "dir/new-after-scan", "x") // thư mục có con mới

	deleted, failed, err := ApplyDeletes(context.Background(), root, plan, committed)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(deleted, []string{"dir/old", "plain"}) {
		t.Fatalf("deleted = %q", deleted)
	}
	var failedPaths []string
	for _, f := range failed {
		failedPaths = append(failedPaths, f.Path)
		if f.Path != "dir" && !errors.Is(f.Err, ErrFileChanged) {
			t.Errorf("%s: err = %v, want ErrFileChanged", f.Path, f.Err)
		}
	}
	if !slices.Equal(failedPaths, []string{"dir", "flipped", "grew", "touched"}) {
		t.Fatalf("failed = %q", failedPaths)
	}
	for _, n := range []string{"grew", "touched", "flipped", "dir/new-after-scan"} {
		if !exists(dstDir, n) {
			t.Fatalf("%s must survive", n)
		}
	}
}

// Luật 5: không đi qua symlink, không RemoveAll.
func TestDeleteRuleRejectsSymlinkComponent(t *testing.T) {
	dstDir, outside := t.TempDir(), t.TempDir()
	victim := writeFile(t, outside, "victim", "x")
	if err := os.Symlink(outside, filepath.Join(dstDir, "link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	info, _ := os.Stat(victim)
	snap := Entry{Path: "link/victim", Size: info.Size(), ModTime: info.ModTime(), Kind: receivefs.KindFile}
	plan := &Plan{Delete: []Change{{Path: "link/victim", Dst: &snap}}}
	_, failed, err := ApplyDeletes(context.Background(), openRoot(t, dstDir), plan, committed)
	if err != nil || len(failed) != 1 || !strings.Contains(failed[0].Err.Error(), "symlink") {
		t.Fatalf("err=%v failed=%v", err, failed)
	}
	if !exists(outside, "victim") {
		t.Fatal("deleted through a symlink")
	}
}

func TestDeleteRuleJunctionCannotEscape(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("junctions are Windows-only")
	}
	dstDir, outside := t.TempDir(), t.TempDir()
	victim := writeFile(t, outside, "victim", "x")
	link := filepath.Join(dstDir, "j")
	if out, err := exec.Command("cmd", "/c", "mklink", "/J", link, outside).CombinedOutput(); err != nil {
		t.Skipf("mklink /J unavailable: %v %s", err, out)
	}
	t.Cleanup(func() { _ = os.Remove(link) })
	info, _ := os.Stat(victim)
	snap := Entry{Path: "j/victim", Size: info.Size(), ModTime: info.ModTime(), Kind: receivefs.KindFile}
	plan := &Plan{Delete: []Change{{Path: "j/victim", Dst: &snap}}}
	_, failed, err := ApplyDeletes(context.Background(), openRoot(t, dstDir), plan, committed)
	if err != nil || len(failed) != 1 {
		t.Fatalf("err=%v failed=%v", err, failed)
	}
	if !exists(outside, "victim") {
		t.Fatal("deleted through a junction")
	}
}

func TestDeleteRuleNeverRemovesNonEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "d/child", "x")
	snap := de("d")
	plan := &Plan{Delete: []Change{{Path: "d", Dst: &snap}}}
	_, failed, err := ApplyDeletes(context.Background(), openRoot(t, dir), plan, committed)
	if err != nil || len(failed) != 1 || !exists(dir, "d/child") {
		t.Fatalf("err=%v failed=%v", err, failed)
	}
}

func TestApplyDeletesEdgeCases(t *testing.T) {
	dir := t.TempDir()
	root := openRoot(t, dir)
	if d, f, err := ApplyDeletes(context.Background(), root, nil, DeleteOptions{}); d != nil || f != nil || err != nil {
		t.Fatal("nil plan must be a no-op")
	}
	writeFile(t, dir, "a", "x")
	writeFile(t, dir, "b", "x")
	plan := &Plan{Delete: []Change{{Path: "a"}, {Path: "../escape", Dst: &Entry{Kind: receivefs.KindFile}}}}
	_, failed, err := ApplyDeletes(nil, root, plan, committed) //nolint:staticcheck // ctx nil được chấp nhận
	if err != nil || len(failed) != 2 {
		t.Fatalf("err=%v failed=%v", err, failed)
	}
	link := Entry{Path: "b", Kind: receivefs.KindSymlink}
	_, failed, _ = ApplyDeletes(context.Background(), root, &Plan{Delete: []Change{{Path: "b", Dst: &link}}}, committed)
	if len(failed) != 1 || !errors.Is(failed[0].Err, ErrUnsupportedKind) {
		t.Fatalf("failed = %v", failed)
	}
	snap := de("b")
	_, failed, _ = ApplyDeletes(context.Background(), root, &Plan{Delete: []Change{{Path: "b", Dst: &snap}}}, committed)
	if len(failed) != 1 || !errors.Is(failed[0].Err, ErrFileChanged) {
		t.Fatalf("file replacing a directory snapshot: failed = %v", failed)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fileSnap := fe("b", 1, 0, "")
	if _, _, err := ApplyDeletes(ctx, root, &Plan{Delete: []Change{{Path: "b", Dst: &fileSnap}}}, committed); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}
