package croc

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Spike A (folder sync): a receiver given Options.ReceiveRoot writes below that
// directory instead of the process working directory, so two receivers can run
// concurrently in one process without os.Chdir. The payload mixes a large file
// (chunked pipeline) with many small files (bundle pipeline).
func TestReceiveRootIsolatesConcurrentReceivers(t *testing.T) {
	relay := startTestRelay(t, 4)
	source := t.TempDir()
	payload := filepath.Join(source, "payload")
	want := writeReceiveRootFixture(t, payload)

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	before := listNames(t, cwd)

	roots := []string{t.TempDir(), t.TempDir()}
	var wg sync.WaitGroup
	for i, root := range roots {
		secret := fmt.Sprintf("%d-receive-root-spike", 8100+i)
		sender, err := newTestClient(receiveRootTestOptions(relay, secret, true, ""))
		if err != nil {
			t.Fatal(err)
		}
		receiver, err := newTestClient(receiveRootTestOptions(relay, secret, false, root))
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(2)
		go func() {
			defer wg.Done()
			files, empty, folders, err := GetFilesInfo([]string{payload}, false, false, nil)
			if err != nil {
				t.Errorf("GetFilesInfo: %v", err)
				return
			}
			if err := sender.Send(files, empty, folders); err != nil {
				t.Errorf("send %d: %v", i, err)
			}
		}()
		go func() {
			defer wg.Done()
			time.Sleep(100 * time.Millisecond)
			if err := receiver.Receive(); err != nil {
				t.Errorf("receive %d: %v", i, err)
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	for _, root := range roots {
		for rel, content := range want {
			got, err := os.ReadFile(filepath.Join(root, "payload", filepath.FromSlash(rel)))
			if err != nil {
				t.Fatalf("read %s in %s: %v", rel, root, err)
			}
			if !bytes.Equal(got, content) {
				t.Fatalf("%s in %s differs from source", rel, root)
			}
		}
	}
	after := listNames(t, cwd)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("working directory changed:\nbefore %v\nafter  %v", before, after)
	}
}

func receiveRootTestOptions(relay testRelay, secret string, sender bool, root string) Options {
	ops := Options{
		IsSender:      sender,
		SharedSecret:  secret,
		RelayAddress:  relay.address,
		RelayPassword: "pass123",
		NoPrompt:      true,
		DisableLocal:  true,
		Curve:         "siec",
		Overwrite:     true,
		ReceiveRoot:   root,
	}
	if sender {
		ops.RelayPorts = append([]string(nil), relay.allPorts...)
	}
	return ops
}

func writeReceiveRootFixture(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	want := map[string][]byte{"large.bin": make([]byte, 3<<20)}
	if _, err := rand.Read(want["large.bin"]); err != nil {
		t.Fatal(err)
	}
	for i := range 24 {
		want[fmt.Sprintf("small/file-%02d.txt", i)] = []byte(fmt.Sprintf("small file %d\n", i))
	}
	for rel, content := range want {
		name := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return want
}

func listNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}
