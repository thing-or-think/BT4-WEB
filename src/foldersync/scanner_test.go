package foldersync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/schollz/croc/v11/src/receivefs"
)

func entryPaths(m *Manifest) []string {
	out := make([]string, len(m.Entries))
	for i, e := range m.Entries {
		out[i] = e.Path
	}
	return out
}

func findSkipped(m *Manifest, reason SkipReason) []SkippedPath {
	var out []SkippedPath
	for _, s := range m.Skipped {
		if s.Reason == reason {
			out = append(out, s)
		}
	}
	return out
}

func mustScan(t *testing.T, root string, o ScanOptions) *Manifest {
	t.Helper()
	m, err := ScanDir(context.Background(), root, o)
	if err != nil {
		t.Fatalf("ScanDir: %v", err)
	}
	return m
}

func setScanHook(t *testing.T, hook func(stage, diskRel string)) {
	t.Helper()
	scanTestHook = hook
	t.Cleanup(func() { scanTestHook = nil })
}

func TestScanDirBasicTree(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "b.txt", "hello")
	writeFile(t, root, "a/z.txt", "")
	writeFile(t, root, "a/sub/deep.bin", "123456789")
	if err := os.MkdirAll(filepath.Join(root, "empty", "nested-empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, ".croc-sync-tmp-123", "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, ".croc-sync-tmp-123/x/partial", "junk")

	m := mustScan(t, root, ScanOptions{})
	want := []string{"a", "a/sub", "a/sub/deep.bin", "a/z.txt", "b.txt", "empty", "empty/nested-empty"}
	if got := entryPaths(m); !slices.Equal(got, want) {
		t.Fatalf("entries = %q, want %q", got, want)
	}
	if !m.Complete || len(m.Skipped) != 0 {
		t.Fatalf("Complete=%v Skipped=%v", m.Complete, m.Skipped)
	}
	if err := ValidateManifest(m); err != nil {
		t.Fatalf("scanned manifest must validate: %v", err)
	}

	byPath := map[string]Entry{}
	for _, e := range m.Entries {
		byPath[e.Path] = e
	}
	if e := byPath["a/sub/deep.bin"]; !e.IsFile() || e.Size != 9 || e.Hash != nil ||
		e.DiskPath != filepath.Join("a", "sub", "deep.bin") || e.ModTime.Location() != time.UTC || e.Mode == 0 {
		t.Fatalf("file entry = %+v", e)
	}
	if e := byPath["a/z.txt"]; e.Size != 0 || !e.IsFile() {
		t.Fatalf("empty file entry = %+v", e)
	}
	if e := byPath["empty"]; !e.IsDir() || e.Size != 0 {
		t.Fatalf("dir entry = %+v", e)
	}
}

func TestScanDirStableAcrossRuns(t *testing.T) {
	root := t.TempDir()
	for _, n := range []string{"z", "B", "a", "a/c", "a/b", "é"} {
		writeFile(t, root, n+"/f", n)
	}
	first := mustScan(t, root, ScanOptions{})
	for range 3 {
		again := mustScan(t, root, ScanOptions{})
		if !slices.Equal(entryPaths(first), entryPaths(again)) {
			t.Fatal("scan order is not stable")
		}
	}
	if !slices.IsSorted(entryPaths(first)) {
		t.Fatalf("entries not sorted: %q", entryPaths(first))
	}
}

func TestScanDirFilters(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, ".gitignore", "*.tmp\nbuild/\n")
	writeFile(t, root, "keep.go", "x")
	writeFile(t, root, "x.tmp", "x")
	writeFile(t, root, "build/out.bin", "x")
	writeFile(t, root, "logs/app.LOG", "x")
	writeFile(t, root, "secret.txt", "x")
	writeFile(t, root, "pkg/.gitignore", "!keep.tmp\nlocal/\n")
	writeFile(t, root, "pkg/keep.tmp", "x")
	writeFile(t, root, "pkg/drop.tmp", "x")
	writeFile(t, root, "pkg/local/a", "x")

	f := NewFilter(FilterSpec{Exclude: []string{".log"}, ExcludeExact: []string{"secret.txt"}, Git: true})
	m := mustScan(t, root, ScanOptions{Filter: f})
	want := []string{".gitignore", "keep.go", "logs", "pkg", "pkg/.gitignore", "pkg/keep.tmp"}
	if got := entryPaths(m); !slices.Equal(got, want) {
		t.Fatalf("entries = %q, want %q", got, want)
	}
	var filtered []string
	for _, s := range findSkipped(m, SkipFiltered) {
		filtered = append(filtered, s.Path)
	}
	wantFiltered := []string{"build", "logs/app.LOG", "pkg/drop.tmp", "pkg/local", "secret.txt", "x.tmp"}
	if !slices.Equal(filtered, wantFiltered) {
		t.Fatalf("filtered = %q, want %q", filtered, wantFiltered)
	}
	if !m.Complete {
		t.Fatal("filtering must not make a scan incomplete")
	}
	if m.Filter.Git != true || !slices.Equal(m.Filter.Exclude, []string{".log"}) {
		t.Fatalf("manifest filter = %+v", m.Filter)
	}
	if err := ValidateManifest(m); err != nil {
		t.Fatal(err)
	}
}

func TestScanDirInvalidAndCollidingNames(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, ".ssh/id_rsa", "secret")
	writeFile(t, root, "ok.txt", "x")

	nfc := "café"
	nfd := "café"
	writeFile(t, root, nfc, "1")
	nfdPath := filepath.Join(root, nfd)
	if err := os.WriteFile(nfdPath, []byte("2"), 0o644); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(root)
	distinct := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "caf") {
			distinct++
		}
	}

	m := mustScan(t, root, ScanOptions{})
	invalid := findSkipped(m, SkipInvalidName)
	var sawSSH bool
	for _, s := range invalid {
		if s.Path == "" && s.DiskPath == ".ssh" {
			sawSSH = true
		}
	}
	if !sawSSH {
		t.Fatalf(".ssh must be skipped as invalid name, skipped=%v", m.Skipped)
	}
	if slices.Contains(entryPaths(m), ".ssh/id_rsa") {
		t.Fatal("scanner descended into a rejected directory")
	}

	if distinct == 2 { // hệ tệp phân biệt NFC/NFD (NTFS, ext4)
		var collided []SkippedPath
		for _, s := range invalid {
			if s.Path == nfc {
				collided = append(collided, s)
			}
		}
		if len(collided) != 1 || !strings.Contains(collided[0].Detail, "collides") {
			t.Fatalf("NFC/NFD collision not reported: %v", invalid)
		}
		if n := strings.Count(strings.Join(entryPaths(m), "\n"), nfc); n != 1 {
			t.Fatalf("collision must keep exactly one entry, got %d", n)
		}
	}
	if !m.Complete {
		t.Fatal("invalid names must not make a scan incomplete")
	}
	if err := ValidateManifest(m); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(invalid[0].String(), "invalid_name") {
		t.Fatalf("String() = %q", invalid[0].String())
	}
}

func TestScanDirSymlinksAreNotFollowed(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "target/f.txt", "x")
	if err := os.Symlink(filepath.Join(root, "target"), filepath.Join(root, "link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	// Vòng symlink: loop/self -> loop.
	if err := os.MkdirAll(filepath.Join(root, "loop"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "loop"), filepath.Join(root, "loop", "self")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}

	m := mustScan(t, root, ScanOptions{})
	want := []string{"loop", "target", "target/f.txt"}
	if got := entryPaths(m); !slices.Equal(got, want) {
		t.Fatalf("entries = %q, want %q", got, want)
	}
	var links []string
	for _, s := range findSkipped(m, SkipSymlink) {
		links = append(links, s.Path)
	}
	if want := []string{"dangling", "link", "loop/self"}; !slices.Equal(links, want) {
		t.Fatalf("symlinks = %q, want %q", links, want)
	}
	if !m.Complete {
		t.Fatal("symlinks must not make a scan incomplete")
	}
}

func TestScanDirFileVanishesDuringScan(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "x")
	writeFile(t, root, "gone.txt", "x")
	setScanHook(t, func(stage, rel string) {
		if stage == "stat" && rel == "gone.txt" {
			_ = os.Remove(filepath.Join(root, rel))
		}
	})
	m := mustScan(t, root, ScanOptions{})
	if m.Complete {
		t.Fatal("vanished file must make the scan incomplete")
	}
	errs := findSkipped(m, SkipReadError)
	if len(errs) != 1 || errs[0].Path != "gone.txt" {
		t.Fatalf("read errors = %v", errs)
	}
	if got := entryPaths(m); !slices.Equal(got, []string{"a.txt"}) {
		t.Fatalf("entries = %q", got)
	}
}

func TestScanDirTypeChangesDuringScan(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "flip", "x")
	setScanHook(t, func(stage, rel string) {
		if stage == "stat" && rel == "flip" {
			_ = os.Remove(filepath.Join(root, rel))
			_ = os.Mkdir(filepath.Join(root, rel), 0o755)
		}
	})
	m := mustScan(t, root, ScanOptions{})
	errs := findSkipped(m, SkipReadError)
	if m.Complete || len(errs) != 1 || !strings.Contains(errs[0].Detail, "type changed") {
		t.Fatalf("Complete=%v errors=%v", m.Complete, errs)
	}
}

func TestScanDirUnreadableSubdirectory(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "sub/a.txt", "x")
	writeFile(t, root, "z.txt", "x")
	setScanHook(t, func(stage, rel string) {
		if stage == "readdir" && rel == "sub" {
			_ = os.RemoveAll(filepath.Join(root, "sub"))
		}
	})
	m := mustScan(t, root, ScanOptions{})
	errs := findSkipped(m, SkipReadError)
	if m.Complete || len(errs) != 1 || errs[0].Path != "sub" {
		t.Fatalf("Complete=%v errors=%v", m.Complete, errs)
	}
	if got := entryPaths(m); !slices.Equal(got, []string{"sub", "z.txt"}) {
		t.Fatalf("entries = %q", got)
	}
}

func TestScanDirRootErrors(t *testing.T) {
	root := t.TempDir()
	file := writeFile(t, root, "f", "x")
	if _, err := ScanDir(context.Background(), file, ScanOptions{}); !errors.Is(err, ErrNotDirectory) {
		t.Fatalf("file root error = %v", err)
	}
	if _, err := ScanDir(context.Background(), filepath.Join(root, "missing"), ScanOptions{}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing root error = %v", err)
	}
	setScanHook(t, func(stage, rel string) {
		if stage == "readdir" && rel == "" {
			_ = os.RemoveAll(root)
		}
	})
	if _, err := ScanDir(context.Background(), root, ScanOptions{}); err == nil {
		t.Fatal("unreadable root must fail")
	}
}

func TestScanDirContextCanceled(t *testing.T) {
	root := t.TempDir()
	for i := range 20 {
		writeFile(t, root, "d/"+string(rune('a'+i)), "x")
	}
	ctx, cancel := context.WithCancel(context.Background())
	setScanHook(t, func(stage, rel string) {
		if rel == filepath.Join("d", "c") {
			cancel()
		}
	})
	if _, err := ScanDir(ctx, root, ScanOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	//nolint:staticcheck // kiểm tra ctx nil được chấp nhận
	if _, err := ScanDir(nil, t.TempDir(), ScanOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestScanDirMaxEntries(t *testing.T) {
	root := t.TempDir()
	for _, n := range []string{"a", "b", "c", "d"} {
		writeFile(t, root, n, "x")
	}
	if _, err := ScanDir(context.Background(), root, ScanOptions{MaxEntries: 3}); !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("err = %v", err)
	}
	// Skipped cũng tính vào giới hạn.
	f := NewFilter(FilterSpec{Exclude: []string{"c", "d"}})
	if _, err := ScanDir(context.Background(), root, ScanOptions{MaxEntries: 3, Filter: f}); !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("skipped entries must count toward the limit, err = %v", err)
	}
	if m, err := ScanDir(context.Background(), root, ScanOptions{MaxEntries: 4}); err != nil || len(m.Entries) != 4 {
		t.Fatalf("exact limit: m=%v err=%v", m, err)
	}
}

func TestHashFileMatchesXXHash(t *testing.T) {
	root := t.TempDir()
	data := bytes.Repeat([]byte("croc-sync"), 300_000) // > 1 MiB để đi qua nhiều lần đọc
	p := filepath.Join(root, "big")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := HashFile(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	want := binary.BigEndian.AppendUint64(nil, xxhash.Sum64(data))
	if !bytes.Equal(got, want) || len(got) != HashSize {
		t.Fatalf("hash = %x, want %x", got, want)
	}
	if _, err := HashFile(context.Background(), filepath.Join(root, "missing")); err == nil {
		t.Fatal("missing file must fail")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := HashFile(ctx, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled hash err = %v", err)
	}
}

func TestHashEntries(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "alpha")
	writeFile(t, root, "dir/b.txt", "beta")
	writeFile(t, root, "changed.txt", "before")
	m := mustScan(t, root, ScanOptions{})

	// Đổi nội dung và kích thước sau khi quét.
	writeFile(t, root, "changed.txt", "after-change")

	err := HashEntries(context.Background(), root, m, []string{"a.txt", "dir/b.txt", "changed.txt", "dir", "missing"})
	if !errors.Is(err, ErrFileChanged) || !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("err = %v", err)
	}
	for _, e := range m.Entries {
		switch e.Path {
		case "a.txt":
			if want := binary.BigEndian.AppendUint64(nil, xxhash.Sum64String("alpha")); !bytes.Equal(e.Hash, want) {
				t.Fatalf("a.txt hash = %x", e.Hash)
			}
		case "dir/b.txt":
			if len(e.Hash) != HashSize {
				t.Fatal("dir/b.txt not hashed")
			}
		case "changed.txt", "dir":
			if e.Hash != nil {
				t.Fatalf("%s must keep an empty hash", e.Path)
			}
		}
	}
	if err := ValidateManifest(m); err != nil {
		t.Fatalf("hashed manifest must validate: %v", err)
	}
}

func TestHashEntriesDetectsSameSizeRewrite(t *testing.T) {
	root := t.TempDir()
	p := writeFile(t, root, "f", "aaaa")
	m := mustScan(t, root, ScanOptions{})
	if err := os.WriteFile(p, []byte("bbbb"), 0o644); err != nil {
		t.Fatal(err)
	}
	later := m.Entries[0].ModTime.Add(3 * time.Second)
	if err := os.Chtimes(p, later, later); err != nil {
		t.Fatal(err)
	}
	if err := HashEntries(context.Background(), root, m, []string{"f"}); !errors.Is(err, ErrFileChanged) {
		t.Fatalf("err = %v", err)
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if err := HashEntries(context.Background(), root, m, []string{"f"}); err == nil {
		t.Fatal("deleted file must fail")
	}
}

func TestHashEntriesErrors(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "f", "x")
	m := mustScan(t, root, ScanOptions{})
	if err := HashEntries(context.Background(), filepath.Join(root, "missing"), m, []string{"f"}); err == nil {
		t.Fatal("missing root must fail")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := HashEntries(ctx, root, m, []string{"f"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	// Entry không có DiskPath (ví dụ manifest nhận từ mạng) dùng Path.
	m.Entries[0].DiskPath = ""
	//nolint:staticcheck // kiểm tra ctx nil được chấp nhận
	if err := HashEntries(nil, root, m, []string{"f"}); err != nil || len(m.Entries[0].Hash) != HashSize {
		t.Fatalf("err=%v hash=%x", err, m.Entries[0].Hash)
	}
	if m.Entries[0].Kind != receivefs.KindFile {
		t.Fatal("unexpected kind")
	}
}
