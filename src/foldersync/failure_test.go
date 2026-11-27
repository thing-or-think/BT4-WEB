package foldersync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/croc"
	"github.com/schollz/croc/v11/src/receivefs"
)

// stagingHasData báo đã có dữ liệu tệp nào đó được ghi vào staging của dstDir.
func stagingHasData(dstDir string) bool {
	found := false
	entries, _ := os.ReadDir(dstDir)
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), StagingDirPrefix) {
			continue
		}
		_ = filepath.WalkDir(filepath.Join(dstDir, e.Name()), func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && d.Name() != stagingLockName {
				if info, ierr := d.Info(); ierr == nil && info.Size() > 0 {
					found = true
				}
			}
			return nil
		})
	}
	return found
}

func noStagingLeft(t *testing.T, dstDir string) {
	t.Helper()
	entries, _ := os.ReadDir(dstDir)
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), StagingDirPrefix) {
			t.Fatalf("staging left behind: %s", e.Name())
		}
	}
}

// interruptedRound chạy một lượt có giới hạn tốc độ và gọi interrupt khi dữ liệu đã
// bắt đầu vào staging. Trả lỗi của hai bên.
func interruptedRound(t *testing.T, relay testRelay, srcDir, dstDir string, interrupt func(cancelJoin func())) (error, error) {
	t.Helper()
	secret := fmt.Sprintf("%d-folder-sync-cut", 7700+roomCounter.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	joinCtx, cancelJoin := context.WithCancel(ctx)
	defer cancelJoin()
	var hostErr, joinErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ops := crocOptions(relay, secret, true)
		ops.ThrottleUpload = "1M"
		_, hostErr = RunSyncHost(ctx, HostOptions{Dir: srcDir, Croc: ops})
	}()
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		_, joinErr = RunSyncJoin(joinCtx, JoinOptions{Dir: dstDir, Croc: crocOptions(relay, secret, false), Decide: confirmAll})
	}()
	deadline := time.Now().Add(30 * time.Second)
	for !stagingHasData(dstDir) {
		if time.Now().After(deadline) {
			t.Fatal("transfer never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	interrupt(cancelJoin)
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatal("interrupted round hung")
	}
	return hostErr, joinErr
}

func interruptFixture(t *testing.T) (srcDir, dstDir string, oldBig []byte) {
	srcDir, dstDir = t.TempDir(), t.TempDir()
	oldBig = bytes.Repeat([]byte("old!"), 1<<20)                               // 4 MiB
	writeFile(t, srcDir, "big.bin", strings.Repeat("new-content-1234", 1<<18)) // 4 MiB, khác nội dung
	writeFile(t, dstDir, "big.bin", string(oldBig))
	writeFile(t, srcDir, "a.txt", "a")
	return srcDir, dstDir, oldBig
}

// 2C.3: rớt mạng giữa lúc truyền.
func TestFailureNetworkDropKeepsDestination(t *testing.T) {
	srcDir, dstDir, oldBig := interruptFixture(t)
	relay := startRelay(t)
	hostErr, joinErr := interruptedRound(t, relay, srcDir, dstDir, func(func()) { relay.stop() })
	if hostErr == nil && joinErr == nil {
		t.Fatal("a dropped relay must fail the round")
	}
	if got, _ := os.ReadFile(filepath.Join(dstDir, "big.bin")); !bytes.Equal(got, oldBig) {
		t.Fatal("destination file damaged by an interrupted transfer")
	}
	noStagingLeft(t, dstDir)
	mustRound(t, runRound(t, startRelay(t), srcDir, dstDir, FilterSpec{}, DiffOptions{}, confirmAll))
	assertSameTree(t, srcDir, dstDir)
}

// 2C.3: Ctrl+C (hủy ctx) ở máy đích giữa lúc truyền.
func TestFailureInterruptKeepsDestination(t *testing.T) {
	srcDir, dstDir, oldBig := interruptFixture(t)
	relay := startRelay(t)
	_, joinErr := interruptedRound(t, relay, srcDir, dstDir, func(cancelJoin func()) { cancelJoin() })
	if joinErr == nil {
		t.Fatal("canceled join returned nil")
	}
	if got, _ := os.ReadFile(filepath.Join(dstDir, "big.bin")); !bytes.Equal(got, oldBig) {
		t.Fatal("destination file damaged by an interrupted transfer")
	}
	noStagingLeft(t, dstDir)
	mustRound(t, runRound(t, relay, srcDir, dstDir, FilterSpec{}, DiffOptions{}, confirmAll))
	assertSameTree(t, srcDir, dstDir)
}

// 2C.3: tệp nguồn đổi sau khi quét (khác size): đích dừng lượt, không ghi gì.
func TestFailureSourceChangedBeforeHash(t *testing.T) {
	relay := startRelay(t)
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "a.txt", "original")
	writeFile(t, dstDir, "a.txt", "dest old, longer") // khác size: luôn phải tải a.txt
	hostTest.afterScan = func(*Manifest) { writeFile(t, srcDir, "a.txt", "edited while syncing") }
	rr := runRound(t, relay, srcDir, dstDir, FilterSpec{}, DiffOptions{}, confirmAll)
	hostTest.afterScan = nil
	if !errors.Is(rr.joinErr, ErrSourceChanged) {
		t.Fatalf("join err = %v, want ErrSourceChanged", rr.joinErr)
	}
	if rr.hostErr == nil {
		t.Fatal("host must not report success")
	}
	if got, _ := os.ReadFile(filepath.Join(dstDir, "a.txt")); string(got) != "dest old, longer" {
		t.Fatalf("a.txt = %q", got)
	}
	mustRound(t, runRound(t, relay, srcDir, dstDir, FilterSpec{}, DiffOptions{}, confirmAll))
	assertSameTree(t, srcDir, dstDir)
}

// sourceEnvFunc bọc SourceEnv để chạy thêm việc sau khi hash.
type sourceEnvFunc struct {
	inner SourceEnv
	after func([]PathHash) []PathHash
}

func (s sourceEnvFunc) HashSource(paths []string) []PathHash {
	return s.after(s.inner.HashSource(paths))
}

// 2C.3: tệp nguồn đổi (cùng size) sau khi đã hash: dữ liệu nhận không khớp hash,
// đích từ chối tệp đó và giữ bản cũ.
func TestFailureSourceChangedAfterHash(t *testing.T) {
	relay := startRelay(t)
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "a.txt", "AAAAAAAA")
	writeFile(t, srcDir, "b.txt", "fine")
	writeFile(t, dstDir, "a.txt", "old")
	hostTest.env = func(inner SourceEnv) SourceEnv {
		return sourceEnvFunc{inner, func(h []PathHash) []PathHash {
			writeFile(t, srcDir, "a.txt", "BBBBBBBB")
			return h
		}}
	}
	rr := runRound(t, relay, srcDir, dstDir, FilterSpec{}, DiffOptions{}, confirmAll)
	hostTest.env = nil
	// Hai cách dừng an toàn: croc bên gửi thấy mtime khác FileInfo và hủy lượt
	// (đích nhận lỗi kết nối), hoặc dữ liệu tới nơi và commit từ chối vì sai hash.
	switch {
	case rr.joinErr != nil:
		if rr.hostErr == nil {
			t.Fatal("host reported success for an aborted round")
		}
	case !rr.join.Partial() || !strings.Contains(strings.Join(rr.join.Failures, "\n"), "does not match"):
		t.Fatalf("result = %+v", rr.join)
	default:
		if got, _ := os.ReadFile(filepath.Join(dstDir, "b.txt")); string(got) != "fine" {
			t.Fatal("other files must still be committed")
		}
	}
	if got, _ := os.ReadFile(filepath.Join(dstDir, "a.txt")); string(got) != "old" {
		t.Fatalf("a.txt = %q", got)
	}
	noStagingLeft(t, dstDir)
	mustRound(t, runRound(t, relay, srcDir, dstDir, FilterSpec{}, DiffOptions{}, confirmAll))
	assertSameTree(t, srcDir, dstDir)
}

// 2C.3: đĩa đầy khi commit (mô phỏng ENOSPC ở mọi lần rename).
func TestFailureDiskFullDuringCommit(t *testing.T) {
	relay := startRelay(t)
	srcDir, dstDir := t.TempDir(), t.TempDir()
	writeFile(t, srcDir, "a.txt", "new a")
	writeFile(t, srcDir, "b.txt", "new b")
	writeFile(t, dstDir, "a.txt", "old")
	commitTestHook = func(stage, p string) error {
		if stage == "rename" {
			return errors.New("no space left on device")
		}
		return nil
	}
	rr := runRound(t, relay, srcDir, dstDir, FilterSpec{}, DiffOptions{}, confirmAll)
	commitTestHook = nil
	if rr.joinErr != nil || len(rr.join.Failures) != 2 || !rr.host.Partial() {
		t.Fatalf("err=%v join=%+v host=%+v", rr.joinErr, rr.join, rr.host)
	}
	if got, _ := os.ReadFile(filepath.Join(dstDir, "a.txt")); string(got) != "old" || exists(dstDir, "b.txt") {
		t.Fatal("destination changed despite failed commit")
	}
	noStagingLeft(t, dstDir)
	mustRound(t, runRound(t, relay, srcDir, dstDir, FilterSpec{}, DiffOptions{}, confirmAll))
	assertSameTree(t, srcDir, dstDir)
}

// greedyHook giả nguồn độc hại gửi mọi tệp, không chỉ tệp trong need.
type greedyHook struct{ *crocHook }

func (g greedyHook) Ready() (bool, func(croc.FileInfo) bool) {
	ready, _ := g.crocHook.Ready()
	if !ready {
		return false, nil
	}
	return true, func(croc.FileInfo) bool { return true }
}

// 2C.4: nguồn độc hại.
func TestMaliciousSourceIsBlocked(t *testing.T) {
	relay := startRelay(t)
	cases := []struct {
		name  string
		setup func(srcDir string)
		check func(t *testing.T, rr roundResult)
	}{
		{
			name: "path traversal in manifest",
			setup: func(string) {
				hostTest.afterScan = func(m *Manifest) {
					m.Entries = append(m.Entries, Entry{Path: "../evil.txt", Size: 4, Kind: receivefs.KindFile, ModTime: baseTime})
				}
			},
		},
		{
			name: "case collision in manifest",
			setup: func(string) {
				hostTest.afterScan = func(m *Manifest) {
					m.Entries = append(m.Entries, Entry{Path: "A.TXT", Size: 1, Kind: receivefs.KindFile, ModTime: baseTime})
					m.Sort()
				}
			},
		},
		{
			name: "staging path in manifest",
			setup: func(string) {
				hostTest.afterScan = func(m *Manifest) {
					m.Entries = append(m.Entries, Entry{Path: StagingDirPrefix + "x", Kind: receivefs.KindDirectory, ModTime: baseTime})
					m.Sort()
				}
			},
		},
		{
			name: "sensitive directory in manifest",
			setup: func(string) {
				hostTest.afterScan = func(m *Manifest) {
					m.Entries = append(m.Entries, Entry{Path: ".ssh", Kind: receivefs.KindDirectory, ModTime: baseTime},
						Entry{Path: ".ssh/authorized_keys", Size: 1, Kind: receivefs.KindFile, ModTime: baseTime})
					m.Sort()
				}
			},
		},
		{
			name:  "file outside need",
			setup: func(string) { hostTest.hook = func(h *crocHook) croc.SyncHook { return greedyHook{h} } },
		},
		{
			name: "wrong hash",
			setup: func(string) {
				hostTest.env = func(inner SourceEnv) SourceEnv {
					return sourceEnvFunc{inner, func(h []PathHash) []PathHash {
						for i := range h {
							h[i].Hash = []byte{1, 2, 3, 4, 5, 6, 7, 8}
						}
						return h
					}}
				}
			},
			check: func(t *testing.T, rr roundResult) {
				if rr.joinErr != nil || !rr.join.Partial() || rr.join.Added+rr.join.Updated != 0 {
					t.Fatalf("err=%v join=%+v", rr.joinErr, rr.join)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parent := t.TempDir()
			srcDir, dstDir := filepath.Join(parent, "src"), filepath.Join(parent, "dst")
			writeFile(t, srcDir, "a.txt", "a")
			writeFile(t, srcDir, "same.txt", "same")
			writeFile(t, srcDir, "new.txt", "new")
			writeFile(t, dstDir, "same.txt", "same")
			writeFile(t, dstDir, "a.txt", "old a")
			_ = os.Chtimes(filepath.Join(srcDir, "same.txt"), baseTime, baseTime)
			_ = os.Chtimes(filepath.Join(dstDir, "same.txt"), baseTime, baseTime)
			before := treeSnapshot(t, dstDir)
			tc.setup(srcDir)
			rr := runRound(t, relay, srcDir, dstDir, FilterSpec{}, DiffOptions{Delete: true}, confirmAll)
			hostTest.afterScan, hostTest.env, hostTest.hook = nil, nil, nil
			if tc.check != nil {
				tc.check(t, rr)
			} else if rr.joinErr == nil {
				t.Fatalf("malicious round accepted: %+v", rr.join)
			}
			if fmt.Sprint(treeSnapshot(t, dstDir)) != fmt.Sprint(before) {
				t.Fatalf("destination changed:\nbefore %v\nafter  %v", before, treeSnapshot(t, dstDir))
			}
			for _, outside := range []string{"evil.txt", filepath.Join("src", "evil.txt")} {
				if _, err := os.Stat(filepath.Join(parent, outside)); err == nil {
					t.Fatalf("wrote outside the destination: %s", outside)
				}
			}
			noStagingLeft(t, dstDir)
		})
	}
}
