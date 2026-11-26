package foldersync

import (
	"context"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/schollz/croc/v11/src/croc"
)

func TestNewFilterNormalizesAndDeduplicates(t *testing.T) {
	f := NewFilter(FilterSpec{
		Exclude:      []string{" .LOG ", "", ".log", "Tmp"},
		ExcludeExact: []string{"./build/", `dist\app`, "", "build"},
		Git:          true,
	})
	got := f.Spec()
	if want := []string{".log", "tmp"}; !slices.Equal(got.Exclude, want) {
		t.Fatalf("Exclude = %q, want %q", got.Exclude, want)
	}
	if want := []string{"build", "dist/app"}; !slices.Equal(got.ExcludeExact, want) {
		t.Fatalf("ExcludeExact = %q, want %q", got.ExcludeExact, want)
	}
	if !got.Git || !f.GitEnabled() {
		t.Fatal("git flag lost")
	}

	// Spec trả về bản sao: sửa kết quả không ảnh hưởng bộ lọc.
	got.Exclude[0] = "changed"
	if f.Spec().Exclude[0] != ".log" {
		t.Fatal("Spec must return a copy")
	}
}

func TestNilFilter(t *testing.T) {
	var f *Filter
	if f.GitEnabled() {
		t.Fatal("nil filter must not enable git")
	}
	if spec := f.Spec(); len(spec.Exclude) != 0 || spec.Git {
		t.Fatalf("nil filter spec = %+v", spec)
	}
	if f.Match("a/b.txt", false) {
		t.Fatal("nil filter matched an ordinary path")
	}
	if matched, reason := f.MatchReason(".croc-sync-tmp-1/x", false); !matched || reason != FilterStaging {
		t.Fatalf("nil filter staging = %v %q", matched, reason)
	}
}

func TestFilterMatchReason(t *testing.T) {
	f := NewFilter(FilterSpec{
		Exclude:      []string{".LOG", "cache"},
		ExcludeExact: []string{"build", "docs/secret.txt"},
		Git:          true,
	})
	f.AddGitIgnoreLines("", "*.tmp", "node_modules/", "/rootonly.txt")

	tests := []struct {
		path   string
		isDir  bool
		reason FilterReason
	}{
		{"", true, FilterNone},
		{".", true, FilterNone},
		{"src/main.go", false, FilterNone},
		{".croc-sync-tmp-abc", true, FilterStaging},
		{"a/.CROC-SYNC-TMP-abc/f", false, FilterStaging},
		{"build", true, FilterExcludeFile},
		{"build/app.exe", false, FilterExcludeFile},
		{"build2/app.exe", false, FilterNone},
		{"docs/secret.txt", false, FilterExcludeFile},
		{"Docs/secret.txt", false, FilterNone}, // --exclude-file phân biệt hoa/thường như croc send
		{"logs/App.Log", false, FilterExclude},
		{"x/CACHE/y", false, FilterExclude},
		{"a.tmp", false, FilterGitIgnore},
		{"deep/b.tmp", false, FilterGitIgnore},
		{"node_modules", true, FilterGitIgnore},
		{"node_modules", false, FilterNone}, // mẫu "dir/" chỉ khớp thư mục
		{"pkg/node_modules/x.js", false, FilterGitIgnore},
		{"rootonly.txt", false, FilterGitIgnore},
		{"sub/rootonly.txt", false, FilterNone},
		{`win\style\a.tmp`, false, FilterGitIgnore},
	}
	for _, tt := range tests {
		matched, reason := f.MatchReason(tt.path, tt.isDir)
		if reason != tt.reason || matched != (tt.reason != FilterNone) {
			t.Errorf("MatchReason(%q, dir=%v) = %v %q, want %q", tt.path, tt.isDir, matched, reason, tt.reason)
		}
	}
}

func TestFilterGitDisabledIgnoresGitRules(t *testing.T) {
	f := NewFilter(FilterSpec{})
	f.AddGitIgnoreLines("", "*.tmp")
	if f.Match("a.tmp", false) {
		t.Fatal("gitignore applied while --git is off")
	}
	if err := f.LoadGitIgnoreDir(t.TempDir(), ""); err != nil {
		t.Fatal(err)
	}
	if err := f.LoadGitIgnoreTree(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestFilterNestedGitIgnore(t *testing.T) {
	f := NewFilter(FilterSpec{Git: true})
	f.AddGitIgnoreLines("", "*.log", "ignored/")
	f.AddGitIgnoreLines("keep", "!*.log", "local.txt")
	f.AddGitIgnoreLines("ignored", "!*") // không thể phủ định khi thư mục cha đã bị ignore

	tests := []struct {
		path  string
		isDir bool
		want  bool
	}{
		{"a.log", false, true},
		{"keep/a.log", false, false}, // lớp sâu hơn phủ định lớp gốc
		{"keep/sub/a.log", false, false},
		{"keep/local.txt", false, true},
		{"local.txt", false, false}, // quy tắc lớp con không ảnh hưởng thư mục cha
		{"ignored", true, true},
		{"ignored/x.txt", false, true},
		{"other/a.log", false, true},
	}
	for _, tt := range tests {
		if got := f.Match(tt.path, tt.isDir); got != tt.want {
			t.Errorf("Match(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestFilterLoadGitIgnoreFromDisk(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, ".gitignore", "*.o\nskip/\n")
	writeFile(t, root, "a/.gitignore", "!keep.o\r\nlocal\r\n") // CRLF vẫn hợp lệ
	writeFile(t, root, "skip/.gitignore", "!*\n")
	writeFile(t, root, "b/.gitignore/x", "") // ".gitignore" là thư mục: bỏ qua

	f := NewFilter(FilterSpec{Git: true})
	if err := f.LoadGitIgnoreTree(root); err != nil {
		t.Fatal(err)
	}
	checks := map[string]bool{
		"x.o":      true,
		"a/keep.o": false,
		"a/y.o":    true,
		"a/local":  true,
		"local":    false,
		"skip/z":   true,
		"b/c.o":    true,
	}
	for p, want := range checks {
		if got := f.Match(p, false); got != want {
			t.Errorf("Match(%q) = %v, want %v", p, got, want)
		}
	}
	if _, ok := f.gitLayers["skip"]; ok {
		t.Error("loaded .gitignore inside an ignored directory")
	}
	if _, ok := f.gitLayers["b"]; ok {
		t.Error("loaded a .gitignore directory as rules")
	}

	if err := f.LoadGitIgnoreDir(root, "missing"); err != nil {
		t.Fatalf("missing directory: %v", err)
	}
	if err := f.LoadGitIgnoreTree(filepath.Join(root, "missing")); err == nil {
		t.Fatal("LoadGitIgnoreTree on a missing root must fail")
	}
}

func TestNormalizeFilterPath(t *testing.T) {
	tests := map[string]string{
		"":          "",
		" . ":       "",
		"./a/b/":    "a/b",
		`a\\b\c`:    "a/b/c",
		"/abs/x":    "abs/x",
		"a//b/./c/": "a/b/c",
	}
	for in, want := range tests {
		if got := normalizeFilterPath(in); got != want {
			t.Errorf("normalizeFilterPath(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestFilterMatchesCrocSend kiểm tra bộ lọc cho cùng kết quả với croc send
// (GetFilesInfoWithExactExclusionsContext + lọc --exclude ở CLI) trên một fixture
// chỉ có .gitignore gốc, vì croc send không hỗ trợ .gitignore lồng nhau.
func TestFilterMatchesCrocSend(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{
		".gitignore", "main.go", "README.md", "debug.LOG", "notes.txt",
		"build/app.bin", "build/sub/x.bin", "docs/guide.md", "docs/draft.tmp",
		"node_modules/pkg/index.js", "src/cache/data.bin", "src/util.go",
		"src/gen/out.txt", "dist/bundle.js", "deep/a/b/c.txt",
	} {
		content := "x"
		if name == ".gitignore" {
			content = "*.tmp\nnode_modules/\n/dist\nsrc/gen/\n"
		}
		writeFile(t, root, name, content)
	}
	spec := FilterSpec{
		Exclude:      []string{".log", "cache"},
		ExcludeExact: []string{"build", "notes.txt"},
		Git:          true,
	}

	// Kết quả croc send: danh sách tệp tương đối so với thư mục gốc.
	files, _, _, err := croc.GetFilesInfoWithExactExclusionsContext(context.Background(),
		[]string{root}, false, true, nil, spec.ExcludeExact)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Base(root)
	var want []string
	for _, fi := range files {
		full := path.Join(strings.ToLower(fi.FolderRemote), strings.ToLower(fi.Name))
		excluded := false
		for _, ex := range spec.Exclude {
			if strings.Contains(full, ex) {
				excluded = true
			}
		}
		if excluded {
			continue
		}
		rel := strings.TrimPrefix(path.Join(fi.FolderRemote, fi.Name), base+"/")
		want = append(want, rel)
	}

	got := walkWithFilter(t, root, NewFilter(spec))
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("filter mismatch with croc send\n got: %q\nwant: %q", got, want)
	}
}

// walkWithFilter liệt kê tệp còn lại sau khi áp bộ lọc, nạp .gitignore theo từng thư mục.
func walkWithFilter(t *testing.T, root string, f *Filter) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return f.LoadGitIgnoreDir(root, "")
		}
		if f.Match(rel, d.IsDir()) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return f.LoadGitIgnoreDir(root, rel)
		}
		out = append(out, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

// writeFile tạo tệp rel (dạng '/') dưới root cùng các thư mục cha.
func writeFile(t testing.TB, root, rel, content string) string {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return full
}
