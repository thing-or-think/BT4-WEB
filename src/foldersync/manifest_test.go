package foldersync

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/receivefs"
)

func TestEntryKindHelpers(t *testing.T) {
	file := Entry{Kind: receivefs.KindFile}
	dir := Entry{Kind: receivefs.KindDirectory}
	link := Entry{Kind: receivefs.KindSymlink}
	if !file.IsFile() || file.IsDir() {
		t.Fatal("file entry misclassified")
	}
	if !dir.IsDir() || dir.IsFile() {
		t.Fatal("directory entry misclassified")
	}
	if link.IsDir() || link.IsFile() {
		t.Fatal("symlink entry must be neither file nor directory")
	}
}

func TestPlanHasChanges(t *testing.T) {
	change := []Change{{Path: "a"}}
	tests := []struct {
		name string
		plan Plan
		want bool
	}{
		{"empty", Plan{}, false},
		{"skip only", Plan{Skip: change}, false},
		{"blocked delete only", Plan{DeleteBlockedReason: "scan incomplete"}, false},
		{"add", Plan{Add: change}, true},
		{"update", Plan{Update: change}, true},
		{"touch", Plan{Touch: change}, true},
		{"delete", Plan{Delete: change}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.plan.HasChanges(); got != tc.want {
				t.Fatalf("HasChanges() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestManifestSort_SkippedAndStability(t *testing.T) {
	m := &Manifest{
		Entries: []Entry{
			{Path: "b", Size: 1},
			{Path: "a", Size: 1},
			{Path: "b", Size: 2}, // trùng Path: thứ tự ban đầu phải được giữ
		},
		Skipped: []SkippedPath{
			{Path: "z", Reason: SkipSymlink},
			{Path: "a", DiskPath: "y", Reason: SkipFiltered},
			{Path: "a", DiskPath: "x", Reason: SkipSymlink},
			{Path: "a", DiskPath: "x", Reason: SkipFiltered},
		},
	}
	m.Sort()

	gotEntries := fmt.Sprintf("%s%s%d%d", m.Entries[0].Path, m.Entries[1].Path, m.Entries[1].Size, m.Entries[2].Size)
	if gotEntries != "ab12" {
		t.Fatalf("entries order = %s, want a,b(1),b(2)", gotEntries)
	}
	want := []SkippedPath{
		{Path: "a", DiskPath: "x", Reason: SkipFiltered},
		{Path: "a", DiskPath: "x", Reason: SkipSymlink},
		{Path: "a", DiskPath: "y", Reason: SkipFiltered},
		{Path: "z", Reason: SkipSymlink},
	}
	for i := range want {
		if m.Skipped[i] != want[i] {
			t.Fatalf("Skipped[%d] = %+v, want %+v", i, m.Skipped[i], want[i])
		}
	}
}

// Chỉ thông tin trên dây được mã hóa; tên trên đĩa cục bộ không rời máy.
func TestManifestJSONOmitsLocalPaths(t *testing.T) {
	m := Manifest{
		Entries: []Entry{{Path: "café.txt", DiskPath: "/home/user/café.txt", Kind: receivefs.KindFile}},
		Skipped: []SkippedPath{{Path: "x", DiskPath: "/home/user/x", Reason: SkipSymlink}},
		Filter:  FilterSpec{Exclude: []string{"node_modules"}, Git: true},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if strings.Contains(s, "/home/user") {
		t.Fatalf("local disk path leaked into JSON: %s", s)
	}
	if strings.Contains(s, "mod_time") {
		t.Fatalf("zero ModTime should be omitted: %s", s)
	}
	var back Manifest
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Filter.Exclude[0] != "node_modules" || !back.Filter.Git {
		t.Fatalf("filter did not round-trip: %+v", back.Filter)
	}

	empty, _ := json.Marshal(Manifest{})
	if strings.Contains(string(empty), "filter") {
		t.Fatalf("empty filter should be omitted: %s", empty)
	}

	withTime, _ := json.Marshal(Entry{Path: "a", ModTime: time.Unix(1, 0).UTC()})
	if !strings.Contains(string(withTime), "mod_time") {
		t.Fatalf("non-zero ModTime must be encoded: %s", withTime)
	}
}

func TestNormalizeFromOS(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		sep     rune
		want    string
		wantErr bool
	}{
		{name: "windows separators", raw: `dir\sub\a.txt`, sep: '\\', want: "dir/sub/a.txt"},
		{name: "unix separators", raw: "dir/sub/a.txt", sep: '/', want: "dir/sub/a.txt"},
		{name: "unix NFD", raw: "café.txt", sep: '/', want: "café.txt"},
		// Trên Linux "a\b" là một tên tệp hợp lệ, không phải "a/b".
		{name: "unix backslash in name", raw: `a\b`, sep: '/', wantErr: true},
		{name: "unix traversal", raw: "../a", sep: '/', wantErr: true},
		{name: "windows traversal", raw: `..\a`, sep: '\\', wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeFromOS(tc.raw, tc.sep)
			if tc.wantErr {
				if !errors.Is(err, receivefs.ErrUnsafePath) {
					t.Fatalf("normalizeFromOS(%q) = (%q, %v), want ErrUnsafePath", tc.raw, got, err)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("normalizeFromOS(%q) = (%q, %v), want %q", tc.raw, got, err, tc.want)
			}
		})
	}

	// Hàm công khai dùng separator thật của hệ điều hành đang chạy.
	if got, err := NormalizeWirePathFromOS("dir/a.txt"); err != nil || got != "dir/a.txt" {
		t.Fatalf("NormalizeWirePathFromOS = (%q, %v)", got, err)
	}
}

func TestPathDepth(t *testing.T) {
	for p, want := range map[string]int{
		"":              0,
		".":             0,
		"a.txt":         1,
		"dir/a.txt":     2,
		"dir/sub/a.txt": 3,
	} {
		if got := PathDepth(p); got != want {
			t.Errorf("PathDepth(%q) = %d, want %d", p, got, want)
		}
	}
}

func TestIsStagingPath_UnicodeFold(t *testing.T) {
	// U+017F (ſ) gấp thành 's'; Windows/macOS có thể coi hai tên là cùng một mục.
	if !IsStagingPath(".croc-ſync-tmp-1/a") {
		t.Fatal("case-folded staging name must be detected")
	}
}

func TestValidateManifest_Metadata(t *testing.T) {
	dir := Entry{Path: "d", Kind: receivefs.KindDirectory}
	tests := []struct {
		name    string
		entry   Entry
		wantErr error
	}{
		{"negative size", Entry{Path: "d/a", Kind: receivefs.KindFile, Size: -1}, ErrInvalidEntry},
		{"directory with size", Entry{Path: "d/s", Kind: receivefs.KindDirectory, Size: 4096}, ErrInvalidEntry},
		{"mode type bits", Entry{Path: "d/a", Kind: receivefs.KindFile, Mode: 0o644 | 1<<31}, ErrInvalidEntry},
		{"setuid bit", Entry{Path: "d/a", Kind: receivefs.KindFile, Mode: 0o4755 | 1<<23}, ErrInvalidEntry},
		{"short hash", Entry{Path: "d/a", Kind: receivefs.KindFile, Hash: []byte{1, 2, 3}}, ErrInvalidEntry},
		{"directory hash", Entry{Path: "d/s", Kind: receivefs.KindDirectory, Hash: make([]byte, HashSize)}, ErrInvalidEntry},
		{"valid file", Entry{Path: "d/a", Kind: receivefs.KindFile, Size: 3, Mode: 0o755, Hash: make([]byte, HashSize)}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateManifest(&Manifest{Entries: []Entry{dir, tc.entry}})
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateManifest_ParentDirectories(t *testing.T) {
	t.Run("missing parent rejected", func(t *testing.T) {
		m := &Manifest{Entries: []Entry{{Path: "a/b.txt", Kind: receivefs.KindFile}}}
		if err := ValidateManifest(m); !errors.Is(err, ErrInvalidEntry) {
			t.Fatalf("got %v, want ErrInvalidEntry", err)
		}
	})
	t.Run("missing grandparent rejected", func(t *testing.T) {
		m := &Manifest{Entries: []Entry{
			{Path: "a/b", Kind: receivefs.KindDirectory},
			{Path: "a/b/c.txt", Kind: receivefs.KindFile},
		}}
		if err := ValidateManifest(m); !errors.Is(err, ErrInvalidEntry) {
			t.Fatalf("got %v, want ErrInvalidEntry", err)
		}
	})
	// Không có mục cha, "Docs/a" và "docs/b" vượt qua ValidateEntries nhưng
	// trên Windows sẽ ghi vào cùng một thư mục.
	t.Run("implicit parents differing only in case", func(t *testing.T) {
		m := &Manifest{Entries: []Entry{
			{Path: "Docs/a.txt", Kind: receivefs.KindFile},
			{Path: "docs/b.txt", Kind: receivefs.KindFile},
		}}
		if err := ValidateManifest(m); err == nil {
			t.Fatal("expected rejection")
		}
		m.Entries = append(m.Entries,
			Entry{Path: "Docs", Kind: receivefs.KindDirectory},
			Entry{Path: "docs", Kind: receivefs.KindDirectory})
		if err := ValidateManifest(m); !errors.Is(err, receivefs.ErrPathCollision) {
			t.Fatalf("got %v, want ErrPathCollision", err)
		}
	})
	t.Run("parent listed after child accepted", func(t *testing.T) {
		m := &Manifest{Entries: []Entry{
			{Path: "a/b.txt", Kind: receivefs.KindFile},
			{Path: "a", Kind: receivefs.KindDirectory},
		}}
		if err := ValidateManifest(m); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestValidateManifest_Skipped(t *testing.T) {
	tests := []struct {
		name    string
		skipped SkippedPath
		wantErr error
	}{
		{"filtered", SkippedPath{Path: "node_modules", Reason: SkipFiltered}, nil},
		{"symlink", SkippedPath{Path: "link", Reason: SkipSymlink}, nil},
		{"read error", SkippedPath{Path: "locked.db", Reason: SkipReadError}, nil},
		{"invalid name without path", SkippedPath{Reason: SkipInvalidName, Detail: `"a:b"`}, nil},
		{"unknown reason", SkippedPath{Path: "x", Reason: "nope"}, ErrInvalidEntry},
		{"empty path for filtered", SkippedPath{Reason: SkipFiltered}, receivefs.ErrUnsafePath},
		{"traversal", SkippedPath{Path: "../x", Reason: SkipSymlink}, receivefs.ErrUnsafePath},
		{"non-canonical", SkippedPath{Path: `a\b`, Reason: SkipFiltered}, receivefs.ErrUnsafePath},
		{"invalid name with bad path", SkippedPath{Path: "../x", Reason: SkipInvalidName}, receivefs.ErrUnsafePath},
		{"staging", SkippedPath{Path: ".croc-sync-tmp-1", Reason: SkipFiltered}, ErrStagingPath},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateManifest(&Manifest{Skipped: []SkippedPath{tc.skipped}})
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateManifest_LimitCountsSkipped(t *testing.T) {
	m := &Manifest{
		Entries: make([]Entry, 1),
		Skipped: make([]SkippedPath, MaxManifestEntries),
	}
	if err := ValidateManifest(m); !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("got %v, want ErrManifestTooLarge", err)
	}
	m = &Manifest{Entries: make([]Entry, MaxManifestEntries+1)}
	if err := ValidateManifest(m); !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("got %v, want ErrManifestTooLarge", err)
	}
}

func FuzzValidateManifest(f *testing.F) {
	f.Add("a", "a/b.txt", int64(1))
	f.Add("Docs", "docs/x", int64(0))
	f.Add("../x", "x", int64(-1))
	f.Fuzz(func(t *testing.T, dir, file string, size int64) {
		m := &Manifest{Entries: []Entry{
			{Path: dir, Kind: receivefs.KindDirectory},
			{Path: file, Kind: receivefs.KindFile, Size: size},
		}}
		if ValidateManifest(m) != nil {
			return
		}
		// Manifest đã chấp nhận thì mọi đường dẫn phải ở dạng chuẩn, không âm.
		for _, e := range m.Entries {
			if clean, err := NormalizeWirePath(e.Path); err != nil || clean != e.Path {
				t.Fatalf("accepted non-canonical path %q", e.Path)
			}
			if e.Size < 0 {
				t.Fatalf("accepted negative size %d", e.Size)
			}
		}
	})
}
