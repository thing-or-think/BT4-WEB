package foldersync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/receivefs"
)

// syntheticManifest tạo manifest gồm dirs thư mục, mỗi thư mục perDir tệp.
func syntheticManifest(dirs, perDir int) *Manifest {
	m := &Manifest{Complete: true, Entries: make([]Entry, 0, dirs*(perDir+1))}
	for d := range dirs {
		dir := fmt.Sprintf("dir%04d", d)
		m.Entries = append(m.Entries, Entry{Path: dir, Kind: receivefs.KindDirectory, Mode: 0o755, ModTime: baseTime})
		for f := range perDir {
			m.Entries = append(m.Entries, Entry{
				Path: fmt.Sprintf("%s/file%04d.txt", dir, f), Kind: receivefs.KindFile,
				Size: int64(f), Mode: 0o644, ModTime: baseTime,
			})
		}
	}
	m.Sort()
	return m
}

// BenchmarkCompare100k đo diff tầng 1 trên 100.000 tệp với 5% tệp đổi size.
func BenchmarkCompare100k(b *testing.B) {
	src := syntheticManifest(1000, 100)
	dst := syntheticManifest(1000, 100)
	for i := range dst.Entries {
		if i%20 == 0 && dst.Entries[i].IsFile() {
			dst.Entries[i].Size++
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		plan, need, err := Compare(src, dst, DiffOptions{Delete: true})
		if err != nil || need != nil || len(plan.Update) == 0 {
			b.Fatalf("err=%v need=%d", err, len(need))
		}
	}
}

// BenchmarkScanDir10k đo quét thật trên đĩa với 10.000 tệp.
func BenchmarkScanDir10k(b *testing.B) {
	root := b.TempDir()
	makeTree(b, root, 100, 100)
	b.ResetTimer()
	for b.Loop() {
		if _, err := ScanDir(context.Background(), root, ScanOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func makeTree(tb testing.TB, root string, dirs, perDir int) {
	tb.Helper()
	for d := range dirs {
		dir := filepath.Join(root, fmt.Sprintf("dir%04d", d))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			tb.Fatal(err)
		}
		for f := range perDir {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("file%04d.txt", f)), []byte("x"), 0o644); err != nil {
				tb.Fatal(err)
			}
		}
	}
}

// TestScanDiff100kBudget kiểm mục tiêu 1.6: quét + diff 100.000 tệp < 10 s, RAM < 200 MB.
// Tạo 100.000 tệp tốn thời gian nên chỉ chạy khi đặt FOLDERSYNC_BENCH_100K=1.
func TestScanDiff100kBudget(t *testing.T) {
	if os.Getenv("FOLDERSYNC_BENCH_100K") == "" {
		t.Skip("set FOLDERSYNC_BENCH_100K=1 to run the 100k-file budget test")
	}
	srcDir, dstDir := t.TempDir(), t.TempDir()
	makeTree(t, srcDir, 1000, 100)
	makeTree(t, dstDir, 1000, 100)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	peak := before.HeapInuse
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { // lấy mẫu heap để ước lượng đỉnh RAM
		defer close(done)
		var ms runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				runtime.ReadMemStats(&ms)
				peak = max(peak, ms.HeapInuse)
			}
		}
	}()

	start := time.Now()
	src := mustScan(t, srcDir, ScanOptions{})
	dst := mustScan(t, dstDir, ScanOptions{Filter: NewFilter(src.Filter)})
	scanned := time.Since(start)
	plan, need, err := Compare(src, dst, DiffOptions{Delete: true})
	elapsed := time.Since(start)
	close(stop)
	<-done
	if err != nil {
		t.Fatal(err)
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	peak = max(peak, after.HeapInuse)
	peakMB := float64(peak-before.HeapInuse) / (1 << 20)
	hashReq := len(need)
	files := 0
	if plan != nil {
		files = len(plan.Skip)
	}
	t.Logf("entries=%d+%d scan=%v total=%v peakHeap=%.1f MiB needHash=%d skip=%d",
		len(src.Entries), len(dst.Entries), scanned, elapsed, peakMB, hashReq, files)
	if elapsed > 10*time.Second {
		t.Errorf("scan+diff took %v, target < 10s", elapsed)
	}
	if peakMB > 200 {
		t.Errorf("peak heap %.1f MiB, target < 200 MiB", peakMB)
	}
}
