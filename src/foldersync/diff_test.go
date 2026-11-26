package foldersync

import (
	"context"
	"encoding/binary"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/schollz/croc/v11/src/receivefs"
)

var baseTime = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

// fe tạo entry tệp: mtime = baseTime + sec giây, content rỗng nghĩa là chưa có hash.
func fe(p string, size int64, sec int, content string) Entry {
	e := Entry{Path: p, Size: size, ModTime: baseTime.Add(time.Duration(sec) * time.Second), Mode: 0o644, Kind: receivefs.KindFile}
	if content != "" {
		e.Hash = binary.BigEndian.AppendUint64(nil, xxhash.Sum64String(content))
	}
	return e
}

func de(p string) Entry {
	return Entry{Path: p, ModTime: baseTime, Mode: 0o755, Kind: receivefs.KindDirectory}
}

func withMode(e Entry, m os.FileMode) Entry { e.Mode = m; return e }

func man(entries ...Entry) *Manifest {
	m := &Manifest{Entries: entries, Complete: true}
	m.Sort()
	return m
}

// categorize trả về path -> nhóm ("add", "update", "touch", "skip", "delete", "conflict").
func categorize(p *Plan) map[string]string {
	out := map[string]string{}
	groups := []struct {
		name string
		cs   []Change
	}{{"add", p.Add}, {"update", p.Update}, {"touch", p.Touch}, {"skip", p.Skip}, {"delete", p.Delete}, {"conflict", p.Conflicts}}
	for _, g := range groups {
		for _, c := range g.cs {
			out[c.Path] = g.name
		}
	}
	return out
}

func TestCompareTable(t *testing.T) {
	tests := []struct {
		name     string
		src, dst *Manifest
		opts     DiffOptions
		want     map[string]string
		needHash []string
	}{
		{
			name: "identical",
			src:  man(fe("a", 3, 0, ""), de("d"), fe("d/b", 1, 0, "")),
			dst:  man(fe("a", 3, 0, ""), de("d"), fe("d/b", 1, 0, "")),
			want: map[string]string{"a": "skip", "d": "skip", "d/b": "skip"},
		},
		{
			name: "new file and directory",
			src:  man(fe("a", 3, 0, ""), de("d"), fe("d/b", 1, 0, "")),
			dst:  man(),
			want: map[string]string{"a": "add", "d": "add", "d/b": "add"},
		},
		{
			name: "size differs",
			src:  man(fe("a", 4, 0, "")),
			dst:  man(fe("a", 3, 0, "")),
			want: map[string]string{"a": "update"},
		},
		{
			name: "same size and mtime, different content: undetected without checksum",
			src:  man(fe("a", 3, 0, "abc")),
			dst:  man(fe("a", 3, 0, "xyz")),
			want: map[string]string{"a": "skip"},
		},
		{
			name: "same size and mtime, different content: detected with checksum",
			src:  man(fe("a", 3, 0, "abc")),
			dst:  man(fe("a", 3, 0, "xyz")),
			opts: DiffOptions{Checksum: true},
			want: map[string]string{"a": "update"},
		},
		{
			name: "checksum with equal hashes",
			src:  man(fe("a", 3, 0, "abc")),
			dst:  man(fe("a", 3, 0, "abc")),
			opts: DiffOptions{Checksum: true},
			want: map[string]string{"a": "skip"},
		},
		{
			name:     "checksum requests missing hashes",
			src:      man(fe("a", 3, 0, ""), fe("b", 2, 0, "")),
			dst:      man(fe("a", 3, 0, "abc"), fe("b", 5, 0, "")),
			opts:     DiffOptions{Checksum: true},
			needHash: []string{"a"},
		},
		{
			name:     "same size, mtime differs: needs hash",
			src:      man(fe("a", 3, 5, ""), fe("b", 3, 0, "")),
			dst:      man(fe("a", 3, 0, ""), fe("b", 3, 0, "")),
			needHash: []string{"a"},
		},
		{
			name: "same size, mtime differs, same content: touch",
			src:  man(fe("a", 3, 5, "abc")),
			dst:  man(fe("a", 3, 0, "abc")),
			want: map[string]string{"a": "touch"},
		},
		{
			name: "same size, mtime differs, different content: update",
			src:  man(fe("a", 3, 5, "abc")),
			dst:  man(fe("a", 3, 0, "xyz")),
			want: map[string]string{"a": "update"},
		},
		{
			name: "mode differs only",
			src:  man(withMode(fe("a", 3, 0, ""), 0o755), withMode(de("d"), 0o700)),
			dst:  man(fe("a", 3, 0, ""), de("d")),
			want: map[string]string{"a": "touch", "d": "touch"},
		},
		{
			name: "directory mtime is ignored",
			src:  man(func() Entry { e := de("d"); e.ModTime = baseTime.Add(time.Hour); return e }()),
			dst:  man(de("d")),
			want: map[string]string{"d": "skip"},
		},
		{
			name: "file becomes directory",
			src:  man(de("x"), fe("x/a", 1, 0, "")),
			dst:  man(fe("x", 1, 0, "")),
			want: map[string]string{"x": "update", "x/a": "add"},
		},
		{
			name: "empty directory becomes file",
			src:  man(fe("x", 1, 0, "")),
			dst:  man(de("x")),
			want: map[string]string{"x": "update"},
		},
		{
			name: "non-empty directory becomes file without delete: conflict",
			src:  man(fe("x", 1, 0, "")),
			dst:  man(de("x"), fe("x/a", 1, 0, "")),
			want: map[string]string{"x": "conflict"},
		},
		{
			name: "non-empty directory becomes file with delete",
			src:  man(fe("x", 1, 0, ""), fe("keep", 1, 0, ""), fe("keep2", 1, 0, "")),
			dst:  man(de("x"), fe("x/a", 1, 0, ""), fe("keep", 1, 0, ""), fe("keep2", 1, 0, "")),
			opts: DiffOptions{Delete: true},
			want: map[string]string{"x": "update", "x/a": "delete", "keep": "skip", "keep2": "skip"},
		},
		{
			name: "case-only rename is not add+delete",
			src:  man(fe("Readme.md", 3, 0, ""), de("Docs"), fe("Docs/a", 1, 0, "")),
			dst:  man(fe("README.md", 3, 0, ""), de("docs"), fe("docs/a", 1, 0, "")),
			opts: DiffOptions{Delete: true},
			want: map[string]string{"Readme.md": "touch", "Docs": "touch", "Docs/a": "touch"},
		},
		{
			name: "case-sensitive destination keeps exact match",
			src:  man(fe("a", 1, 0, ""), fe("b", 1, 0, "")),
			dst:  man(fe("A", 1, 0, ""), fe("a", 1, 0, ""), fe("b", 1, 0, "")),
			opts: DiffOptions{Delete: true},
			want: map[string]string{"a": "skip", "b": "skip", "A": "delete"},
		},
		{
			name: "extra destination files kept without delete",
			src:  man(fe("a", 1, 0, "")),
			dst:  man(fe("a", 1, 0, ""), fe("extra", 1, 0, "")),
			want: map[string]string{"a": "skip"},
		},
		{
			name: "extra destination tree deleted with delete",
			src:  man(fe("a", 1, 0, ""), fe("b", 1, 0, ""), fe("c", 1, 0, "")),
			dst:  man(fe("a", 1, 0, ""), fe("b", 1, 0, ""), fe("c", 1, 0, ""), de("old"), fe("old/f", 1, 0, "")),
			opts: DiffOptions{Delete: true},
			want: map[string]string{"a": "skip", "b": "skip", "c": "skip", "old": "delete", "old/f": "delete"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, need, err := Compare(tt.src, tt.dst, tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			if tt.needHash != nil {
				if plan != nil || !slices.Equal(need, tt.needHash) {
					t.Fatalf("plan=%v needHash=%q, want needHash %q", plan, need, tt.needHash)
				}
				return
			}
			if need != nil {
				t.Fatalf("unexpected needHash %q", need)
			}
			if got := categorize(plan); !maps.Equal(got, tt.want) {
				t.Fatalf("plan = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompareChangeDetails(t *testing.T) {
	src := man(fe("Readme.md", 3, 5, "abc"), de("x"), fe("x/a", 1, 0, ""), withMode(fe("m", 1, 0, ""), 0o600))
	dst := man(fe("README.md", 3, 0, "abc"), fe("x", 9, 0, ""), fe("m", 1, 0, ""))
	plan, _, err := Compare(src, dst, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Touch) != 2 {
		t.Fatalf("touch = %+v", plan.Touch)
	}
	for _, c := range plan.Touch {
		switch c.Path {
		case "Readme.md":
			if c.Dst.Path != "README.md" || c.Src.Path != "Readme.md" ||
				!strings.Contains(c.Reason, "rename case") || !strings.Contains(c.Reason, "mtime") {
				t.Fatalf("rename touch = %+v", c)
			}
		case "m":
			if c.Reason != "mode differs" {
				t.Fatalf("mode touch reason = %q", c.Reason)
			}
		}
	}
	if len(plan.Update) != 1 || !plan.Update[0].TypeChanged || plan.Update[0].Reason != "type changed" {
		t.Fatalf("update = %+v", plan.Update)
	}
	if len(plan.Add) != 1 || plan.Add[0].Dst != nil || plan.Add[0].Src.Path != "x/a" {
		t.Fatalf("add = %+v", plan.Add)
	}
	if !plan.HasChanges() {
		t.Fatal("plan must have changes")
	}
}

func TestCompareDeleteProtection(t *testing.T) {
	src := man(fe("a", 1, 0, ""), fe("b", 1, 0, ""), fe("c", 1, 0, ""), fe("d", 1, 0, ""))
	src.Skipped = []SkippedPath{
		{Path: "build", Reason: SkipFiltered},
		{Path: "logs/app.log", Reason: SkipFiltered},
		{Path: "link", Reason: SkipSymlink},
		{Path: "unreadable", Reason: SkipSpecial},
	}
	dst := man(
		fe("a", 1, 0, ""), fe("b", 1, 0, ""), fe("c", 1, 0, ""), fe("d", 1, 0, ""),
		de("build"), de("build/out"), fe("build/out/x", 1, 0, ""), // nằm dưới mục bị lọc
		de("logs"), fe("logs/app.log", 1, 0, ""), fe("logs/old.txt", 1, 0, ""),
		fe("link", 1, 0, ""), // nguồn là symlink, đích là tệp: không xóa
		fe("unreadable", 1, 0, ""),
		de("withlink"), fe("withlink/f", 1, 0, ""), // đích có symlink bên trong
		de("withdevice"), fe("withdevice/g", 1, 0, ""), // đích có tên không hợp lệ bên trong
		de("gone"), fe("gone/h", 1, 0, ""),
	)
	dst.Skipped = []SkippedPath{
		{Path: "withlink/sym", Reason: SkipSymlink},
		{DiskPath: filepath.Join("withdevice", "CON"), Reason: SkipInvalidName},
		{DiskPath: "AUX", Reason: SkipInvalidName}, // ở gốc: không có tổ tiên cần giữ
	}
	plan, _, err := Compare(src, dst, DiffOptions{Delete: true})
	if err != nil {
		t.Fatal(err)
	}
	var deleted []string
	for _, c := range plan.Delete {
		deleted = append(deleted, c.Path)
	}
	want := []string{"gone/h", "logs/old.txt", "withdevice/g", "withlink/f", "gone"}
	if !slices.Equal(deleted, want) {
		t.Fatalf("deletes = %q, want %q", deleted, want)
	}
	if plan.DeleteBlockedReason != "" {
		t.Fatalf("unexpected block: %s", plan.DeleteBlockedReason)
	}
}

func TestCompareErrors(t *testing.T) {
	bad := &Manifest{Entries: []Entry{fe("../x", 1, 0, "")}, Complete: true}
	if _, _, err := Compare(bad, man(), DiffOptions{}); err == nil {
		t.Fatal("invalid source manifest must fail")
	}
	if _, _, err := Compare(nil, man(), DiffOptions{}); err == nil {
		t.Fatal("nil source must fail")
	}
	if _, _, err := Compare(man(), nil, DiffOptions{}); err == nil {
		t.Fatal("nil destination must fail")
	}
}

func TestDstHashTargets(t *testing.T) {
	src := man(fe("Readme.md", 3, 5, ""), fe("a", 1, 5, ""), fe("new", 1, 0, ""))
	dst := man(fe("README.md", 3, 0, ""), fe("a", 1, 0, ""))
	_, need, err := Compare(src, dst, DiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(need, []string{"Readme.md", "a"}) {
		t.Fatalf("needHash = %q", need)
	}
	if got := DstHashTargets(src, dst, need); !slices.Equal(got, []string{"README.md", "a"}) {
		t.Fatalf("dst targets = %q", got)
	}
}

// TestCompareTwoTierOnDisk chạy đủ vòng quét -> diff -> hash -> diff trên đĩa thật.
func TestCompareTwoTierOnDisk(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	ctx := context.Background()
	for _, dir := range []string{srcDir, dstDir} {
		writeFile(t, dir, "same.txt", "unchanged")
		writeFile(t, dir, "touched.txt", "same-bytes")
		writeFile(t, dir, "edited.txt", "aaaa")
	}
	writeFile(t, srcDir, "edited.txt", "bbbb") // cùng size, khác nội dung
	writeFile(t, srcDir, "added/n.txt", "new")
	writeFile(t, dstDir, "extra.txt", "x")
	fixed := baseTime
	for _, dir := range []string{srcDir, dstDir} {
		for _, n := range []string{"same.txt", "touched.txt", "edited.txt"} {
			if err := os.Chtimes(filepath.Join(dir, n), fixed, fixed); err != nil {
				t.Fatal(err)
			}
		}
	}
	later := fixed.Add(10 * time.Second)
	for _, n := range []string{"touched.txt", "edited.txt"} {
		if err := os.Chtimes(filepath.Join(srcDir, n), later, later); err != nil {
			t.Fatal(err)
		}
	}

	src := mustScan(t, srcDir, ScanOptions{})
	dst := mustScan(t, dstDir, ScanOptions{Filter: NewFilter(src.Filter)})
	plan, need, err := Compare(src, dst, DiffOptions{Delete: true})
	if err != nil || plan != nil {
		t.Fatalf("first pass: plan=%v err=%v", plan, err)
	}
	if !slices.Equal(need, []string{"edited.txt", "touched.txt"}) {
		t.Fatalf("needHash = %q (same.txt must not be hashed)", need)
	}
	if err := HashEntries(ctx, srcDir, src, need); err != nil {
		t.Fatal(err)
	}
	if err := HashEntries(ctx, dstDir, dst, DstHashTargets(src, dst, need)); err != nil {
		t.Fatal(err)
	}
	plan, need, err = Compare(src, dst, DiffOptions{Delete: true})
	if err != nil || need != nil {
		t.Fatalf("second pass: need=%q err=%v", need, err)
	}
	want := map[string]string{
		"same.txt": "skip", "touched.txt": "touch", "edited.txt": "update",
		"added": "add", "added/n.txt": "add", "extra.txt": "delete",
	}
	if got := categorize(plan); !maps.Equal(got, want) {
		t.Fatalf("plan = %v, want %v", got, want)
	}
	for _, e := range src.Entries {
		if e.Path == "same.txt" && e.Hash != nil {
			t.Fatal("unchanged file must not be hashed (lazy hashing)")
		}
	}
}
