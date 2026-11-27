package foldersync

import (
	"path"
	"strings"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/receivefs"
)

func FuzzNormalizeWirePathFromOS(f *testing.F) {
	for _, s := range []string{
		"a/b", `a\b`, "../x", "/abs", "C:/x", "CON", "a:b", "x.", "cafe\u0301",
		".ssh/id", ".croc-sync-tmp-1/x", "a//b/./c", "", ".", "SSH~1",
	} {
		f.Add(s, false)
		f.Add(s, true)
	}
	f.Fuzz(func(t *testing.T, raw string, windows bool) {
		sep := '/'
		if windows {
			sep = '\\'
		}
		wire, err := normalizeFromOS(raw, sep)
		if err != nil {
			return
		}
		again, err := NormalizeWirePath(wire)
		if err != nil || again != wire {
			t.Fatalf("not idempotent: %q -> %q -> %q (%v)", raw, wire, again, err)
		}
		if wire == "" || strings.HasPrefix(wire, "/") || strings.Contains(wire, `\`) {
			t.Fatalf("unsafe wire path %q from %q", wire, raw)
		}
		for _, part := range strings.Split(wire, "/") {
			if part == "" || part == "." || part == ".." {
				t.Fatalf("bad component in %q from %q", wire, raw)
			}
		}
		if PathDepth(wire) != len(strings.Split(wire, "/")) {
			t.Fatalf("PathDepth(%q) = %d", wire, PathDepth(wire))
		}
		m := &Manifest{Entries: []Entry{{Path: path.Base(wire), Kind: receivefs.KindFile}}, Complete: true}
		if err := ValidateManifest(m); err != nil && !IsStagingPath(wire) {
			t.Fatalf("normalized base %q rejected: %v", path.Base(wire), err)
		}
	})
}

func FuzzFilterMatch(f *testing.F) {
	f.Add("a/b.log", ".log", "a/b.log", "*.log\n!keep.log", true)
	f.Add(".croc-sync-tmp-x/y", "", "", "", false)
	f.Add(`win\path`, "PATH", "win", "[", true)
	f.Fuzz(func(t *testing.T, p, exclude, exact, gitignore string, isDir bool) {
		flt := NewFilter(FilterSpec{Exclude: []string{exclude}, ExcludeExact: []string{exact}, Git: true})
		flt.AddGitIgnoreLines("", strings.Split(gitignore, "\n")...)
		flt.AddGitIgnoreLines("a", strings.Split(gitignore, "\n")...)
		matched, reason := flt.MatchReason(p, isDir)
		if matched != (reason != FilterNone) {
			t.Fatalf("matched=%v reason=%q", matched, reason)
		}
		if IsStagingPath(normalizeFilterPath(p)) && reason != FilterStaging {
			t.Fatalf("staging path %q not filtered", p)
		}
	})
}

// fuzzPaths là bảng chữ cái nhỏ có quan hệ cha/con, hoa/thường để fuzz Compare.
var fuzzPaths = []string{"a", "A", "b", "a/x", "a/y", "A/x", "b/z", "b/z/q", "c"}

// manifestFromBytes dựng manifest từ dữ liệu fuzz; mỗi mục dùng 3 byte.
func manifestFromBytes(data []byte) *Manifest {
	m := &Manifest{Complete: true}
	seen := map[string]bool{}
	for i := 0; i+2 < len(data); i += 3 {
		p := fuzzPaths[int(data[i])%len(fuzzPaths)]
		if seen[p] {
			continue
		}
		seen[p] = true
		flags := data[i+1]
		e := Entry{Path: p, Mode: 0o644, ModTime: baseTime.Add(time.Duration(flags&3) * time.Second)}
		if flags&4 != 0 {
			e.Kind = receivefs.KindDirectory
			e.Mode = 0o755
		} else {
			e.Kind = receivefs.KindFile
			e.Size = int64(data[i+2] % 3)
			if flags&8 != 0 {
				e.Hash = []byte{0, 0, 0, 0, 0, 0, 0, data[i+2] % 2}
			}
		}
		if flags&16 != 0 {
			e.Mode = 0o600
		}
		m.Entries = append(m.Entries, e)
	}
	// Thêm thư mục cha còn thiếu để manifest nguồn có cơ hội hợp lệ.
	for _, e := range m.Entries {
		for dir := path.Dir(e.Path); dir != "."; dir = path.Dir(dir) {
			if !seen[dir] {
				seen[dir] = true
				m.Entries = append(m.Entries, Entry{Path: dir, Kind: receivefs.KindDirectory, Mode: 0o755, ModTime: baseTime})
			}
		}
	}
	m.Sort()
	return m
}

func FuzzCompare(f *testing.F) {
	f.Add([]byte{0, 0, 1, 3, 0, 2}, []byte{1, 0, 1, 2, 4, 0, 7, 0, 1}, uint8(0))
	f.Add([]byte{3, 8, 1}, []byte{3, 9, 2, 6, 0, 1}, uint8(3))
	f.Add([]byte{}, []byte{0, 0, 0}, uint8(7))
	f.Fuzz(func(t *testing.T, srcData, dstData []byte, opts uint8) {
		src, dst := manifestFromBytes(srcData), manifestFromBytes(dstData)
		if ValidateManifest(src) != nil {
			return
		}
		if opts&8 != 0 {
			src.Complete = false
		}
		o := DiffOptions{Checksum: opts&1 != 0, Delete: opts&2 != 0, AllowEmptySource: opts&4 != 0}
		plan, need, err := Compare(src, dst, o)
		if err != nil {
			t.Fatalf("valid source rejected: %v", err)
		}
		srcPaths := map[string]bool{}
		for _, e := range src.Entries {
			srcPaths[e.Path] = true
		}
		if plan == nil {
			if len(need) == 0 {
				t.Fatal("nil plan without hash requests")
			}
			for _, p := range need {
				if !srcPaths[p] {
					t.Fatalf("hash requested for unknown path %q", p)
				}
			}
			return
		}
		if need != nil {
			t.Fatal("plan and hash requests together")
		}
		count := map[string]int{}
		for _, g := range [][]Change{plan.Add, plan.Update, plan.Touch, plan.Skip, plan.Conflicts} {
			for _, c := range g {
				count[c.Path]++
			}
		}
		for p := range srcPaths {
			if count[p] != 1 {
				t.Fatalf("source path %q appears %d times in plan", p, count[p])
			}
		}
		if !o.Delete && len(plan.Delete) != 0 {
			t.Fatal("deletes without --delete")
		}
		if !src.Complete && len(plan.Delete) != 0 {
			t.Fatal("deletes with an incomplete source")
		}
		for i, c := range plan.Delete {
			if srcPaths[c.Path] || c.Dst == nil || c.Dst.Path != c.Path {
				t.Fatalf("bad delete %+v", c)
			}
			if i > 0 && PathDepth(plan.Delete[i-1].Path) < PathDepth(c.Path) {
				t.Fatalf("deletes not deepest-first: %q before %q", plan.Delete[i-1].Path, c.Path)
			}
		}
	})
}
