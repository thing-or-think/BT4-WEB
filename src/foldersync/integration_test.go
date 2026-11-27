package foldersync

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/croc"
	"github.com/schollz/croc/v11/src/tcp"
	"golang.org/x/text/unicode/norm"
)

type testRelay struct {
	address string
	ports   []string
	stop    func() // dừng relay giữa chừng (mô phỏng rớt mạng)
}

// startRelay chạy relay croc trên loopback (cổng điều khiển + 4 cổng dữ liệu).
func startRelay(t *testing.T) testRelay {
	t.Helper()
	var ports []string
	for len(ports) < 5 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		_, port, _ := net.SplitHostPort(l.Addr().String())
		_ = l.Close()
		ports = append(ports, port)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	start := func(port string, banner ...string) {
		wg.Go(func() { _ = tcp.RunCtx(ctx, "warn", "127.0.0.1", port, "pass123", banner...) })
	}
	start(ports[0], strings.Join(ports[1:], ","))
	for _, p := range ports[1:] {
		start(p)
	}
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); wg.Wait() }) }
	t.Cleanup(stop)
	deadline := time.Now().Add(5 * time.Second)
	for _, p := range ports {
		for {
			c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", p), 50*time.Millisecond)
			if err == nil {
				_ = c.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("relay port %s not ready", p)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return testRelay{address: net.JoinHostPort("127.0.0.1", ports[0]), ports: ports, stop: stop}
}

var roomCounter atomic.Int32

func crocOptions(r testRelay, secret string, sender bool) croc.Options {
	o := croc.Options{
		SharedSecret:     secret,
		RelayAddress:     r.address,
		RelayPassword:    "pass123",
		NoPrompt:         true,
		DisableLocal:     true,
		Curve:            "siec",
		DisableClipboard: true,
	}
	if sender {
		o.RelayPorts = append([]string(nil), r.ports...)
		o.Transport = croc.TransportRelay
	}
	return o
}

type roundResult struct {
	host, join       *Result
	hostErr, joinErr error
	plan             *Plan
}

// runRound chạy một lượt nguồn -> đích qua relay, có thời hạn chống treo.
func runRound(t *testing.T, r testRelay, srcDir, dstDir string, filter FilterSpec, diff DiffOptions, decide func(*Plan) (Decision, error)) roundResult {
	t.Helper()
	secret := fmt.Sprintf("%d-folder-sync-round", 7000+roomCounter.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var out roundResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		out.host, out.hostErr = RunSyncHost(ctx, HostOptions{Dir: srcDir, Filter: filter, Croc: crocOptions(r, secret, true)})
	}()
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		out.join, out.joinErr = RunSyncJoin(ctx, JoinOptions{
			Dir: dstDir, Diff: diff, Croc: crocOptions(r, secret, false), Decide: decide,
			Reporter: ReporterFunc(func(e Event) {
				if e.Kind == EventPlan {
					out.plan = e.Plan
				}
			}),
		})
	}()
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatalf("round timed out: host=%v join=%v", out.hostErr, out.joinErr)
	}
	return out
}

func mustRound(t *testing.T, rr roundResult) roundResult {
	t.Helper()
	if rr.hostErr != nil || rr.joinErr != nil {
		t.Fatalf("host=%v join=%v", rr.hostErr, rr.joinErr)
	}
	if rr.join.Partial() || rr.host.Partial() {
		t.Fatalf("partial: %v", rr.join.Failures)
	}
	return rr
}

func confirmAll(*Plan) (Decision, error) { return Decision{Proceed: true, ConfirmDeletes: true}, nil }

func TestIntegrationRoundsOverRelay(t *testing.T) {
	relay := startRelay(t)
	srcDir, dstDir := t.TempDir(), t.TempDir()
	big := strings.Repeat("0123456789abcdef", 200_000) // 3,2 MB: đi qua pipeline chunk
	writeFile(t, srcDir, "big.bin", big)
	for i := range 30 { // nhiều tệp nhỏ: pipeline bundle
		writeFile(t, srcDir, fmt.Sprintf("small/f%02d.txt", i), fmt.Sprintf("small file %d", i))
	}
	writeFile(t, srcDir, "nested/deep/a.txt", "deep")
	if err := os.MkdirAll(filepath.Join(srcDir, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, srcDir, "skip.log", "excluded")
	filter := FilterSpec{Exclude: []string{".log"}}

	// Lượt 1: đủ.
	r1 := mustRound(t, runRound(t, relay, srcDir, dstDir, filter, DiffOptions{Delete: true}, confirmAll))
	if r1.host.Added != r1.join.Added || r1.join.Bytes != int64(len(big))+int64(len("deep"))+sumSmall() {
		t.Fatalf("round 1 result = %+v", r1.join)
	}
	if exists(dstDir, "skip.log") {
		t.Fatal("filtered file transferred")
	}
	_ = os.Remove(filepath.Join(srcDir, "skip.log"))
	assertSameTree(t, srcDir, dstDir)

	// Lượt 2: không đổi gì -> 0 byte dữ liệu tệp.
	r2 := mustRound(t, runRound(t, relay, srcDir, dstDir, filter, DiffOptions{Delete: true}, confirmAll))
	if r2.join.Bytes != 0 || r2.plan.HasChanges() || r2.join.Added+r2.join.Updated != 0 {
		t.Fatalf("round 2 not idle: %+v", r2.join)
	}

	// Sửa đúng một tệp: chỉ gửi tệp đó.
	writeFile(t, srcDir, "small/f07.txt", "changed content for seven")
	r3 := mustRound(t, runRound(t, relay, srcDir, dstDir, filter, DiffOptions{Delete: true}, confirmAll))
	if r3.join.Updated != 1 || r3.join.Bytes != int64(len("changed content for seven")) {
		t.Fatalf("round 3 = %+v", r3.join)
	}
	assertSameTree(t, srcDir, dstDir)

	// Xóa ở nguồn: đích xóa theo.
	_ = os.Remove(filepath.Join(srcDir, "nested", "deep", "a.txt"))
	r4 := mustRound(t, runRound(t, relay, srcDir, dstDir, filter, DiffOptions{Delete: true}, confirmAll))
	if r4.join.Deleted != 1 || exists(dstDir, "nested/deep/a.txt") {
		t.Fatalf("round 4 = %+v", r4.join)
	}

	// Sửa giữ nguyên size và mtime: chỉ --checksum phát hiện.
	p := filepath.Join(srcDir, "small", "f03.txt")
	info, _ := os.Stat(p)
	writeFile(t, srcDir, "small/f03.txt", "SMALL FILE 3")
	_ = os.Chtimes(p, info.ModTime(), info.ModTime())
	r5 := mustRound(t, runRound(t, relay, srcDir, dstDir, filter, DiffOptions{}, confirmAll))
	if r5.join.Updated != 0 {
		t.Fatalf("without checksum the edit must be invisible: %+v", r5.join)
	}
	r6 := mustRound(t, runRound(t, relay, srcDir, dstDir, filter, DiffOptions{Checksum: true}, confirmAll))
	if r6.join.Updated != 1 {
		t.Fatalf("checksum round = %+v", r6.join)
	}
	assertSameTree(t, srcDir, dstDir)

	// Dry-run: kế hoạch có thay đổi nhưng đích không đổi.
	writeFile(t, srcDir, "dry.txt", "dry")
	before := treeSnapshot(t, dstDir)
	r7 := mustRound(t, runRound(t, relay, srcDir, dstDir, filter, DiffOptions{}, func(*Plan) (Decision, error) { return Decision{}, nil }))
	if !r7.join.DryRun || !r7.host.DryRun || len(r7.plan.Add) != 1 || fmt.Sprint(treeSnapshot(t, dstDir)) != fmt.Sprint(before) {
		t.Fatalf("dry run = %+v", r7.join)
	}
}

func sumSmall() int64 {
	var n int64
	for i := range 30 {
		n += int64(len(fmt.Sprintf("small file %d", i)))
	}
	return n
}

// TestIntegrationCancelStopsGoroutines: hủy ctx khi chờ đối phương thì cả hai API
// trả về, không rò goroutine.
func TestIntegrationCancelStopsGoroutines(t *testing.T) {
	relay := startRelay(t)
	before := runtime.NumGoroutine()
	for _, role := range []string{"host", "join"} {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			secret := fmt.Sprintf("%d-folder-sync-alone", 7500+roomCounter.Add(1))
			var err error
			if role == "host" {
				_, err = RunSyncHost(ctx, HostOptions{Dir: t.TempDir(), Croc: crocOptions(relay, secret, true)})
			} else {
				_, err = RunSyncJoin(ctx, JoinOptions{Dir: t.TempDir(), Croc: crocOptions(relay, secret, false)})
			}
			done <- err
		}()
		time.Sleep(500 * time.Millisecond)
		cancel()
		select {
		case err := <-done:
			if err == nil {
				t.Fatalf("%s: canceled run returned nil", role)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("%s did not return after cancel", role)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+2 {
		buf := make([]byte, 1<<20)
		t.Fatalf("goroutines %d -> %d\n%s", before, n, buf[:runtime.Stack(buf, true)])
	}
}

// TestIntegrationNFDFileName kiểm tra đồng bộ tệp có tên dạng NFD trên đĩa nguồn:
// croc gửi tên NFC trên dây nhưng mở đúng tệp NFD cục bộ.
func TestIntegrationNFDFileName(t *testing.T) {
	srcDir, dstDir := t.TempDir(), t.TempDir()
	nfdName := "cafe\u0301.txt"
	nfcName := norm.NFC.String(nfdName)
	const content = "nfd file content"

	if err := os.WriteFile(filepath.Join(srcDir, nfdName), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	srcEntries, err := os.ReadDir(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	foundNFD := false
	for _, e := range srcEntries {
		if e.Name() == nfdName {
			foundNFD = true
			break
		}
	}
	if !foundNFD {
		t.Skip("filesystem normalized NFD filename on disk")
	}

	relay := startRelay(t)

	// Lượt 1: đồng bộ tệp NFD sang đích.
	r1 := mustRound(t, runRound(t, relay, srcDir, dstDir, FilterSpec{}, DiffOptions{}, confirmAll))
	if r1.join.Added != 1 || r1.join.Bytes != int64(len(content)) {
		t.Fatalf("round 1 result = %+v", r1.join)
	}

	dstEntries, err := os.ReadDir(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	var matchedName string
	for _, e := range dstEntries {
		if norm.NFC.String(e.Name()) == nfcName {
			matchedName = e.Name()
			break
		}
	}
	if matchedName == "" {
		t.Fatalf("destination missing %q (entries: %v)", nfcName, dstEntries)
	}
	got, err := os.ReadFile(filepath.Join(dstDir, matchedName))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Fatalf("destination content = %q; want %q", string(got), content)
	}

	// Lượt 2: không đổi gì -> 0 byte dữ liệu tệp và kế hoạch không có thay đổi.
	r2 := mustRound(t, runRound(t, relay, srcDir, dstDir, FilterSpec{}, DiffOptions{}, confirmAll))
	if r2.join.Bytes != 0 || r2.plan == nil || r2.plan.HasChanges() || r2.join.Added+r2.join.Updated != 0 {
		t.Fatalf("round 2 not idle: %+v", r2.join)
	}
}
