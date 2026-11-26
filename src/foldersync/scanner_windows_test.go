//go:build windows

package foldersync

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
)

// Junction (mount point) tạo được mà không cần quyền symlink. Go báo junction là
// ModeIrregular, nên scanner phải bỏ qua nó (SkipSpecial) và không đi theo vòng lặp.
func TestScanDirJunctionLoopIsNotFollowed(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "loop/f.txt", "x")
	link := filepath.Join(root, "loop", "self")
	out, err := exec.Command("cmd", "/c", "mklink", "/J", link, filepath.Join(root, "loop")).CombinedOutput()
	if err != nil {
		t.Skipf("mklink /J unavailable: %v %s", err, out)
	}
	t.Cleanup(func() { _ = os.Remove(link) })

	m := mustScan(t, root, ScanOptions{})
	if got := entryPaths(m); !slices.Equal(got, []string{"loop", "loop/f.txt"}) {
		t.Fatalf("entries = %q", got)
	}
	var skipped []string
	for _, s := range m.Skipped {
		if s.Reason == SkipSpecial || s.Reason == SkipSymlink {
			skipped = append(skipped, s.Path)
		}
	}
	if !slices.Equal(skipped, []string{"loop/self"}) {
		t.Fatalf("skipped = %v", m.Skipped)
	}
	if !m.Complete {
		t.Fatal("junction must not make the scan incomplete")
	}
}
