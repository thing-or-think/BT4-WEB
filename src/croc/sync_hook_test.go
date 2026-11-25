package croc

import (
	"encoding/json"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/message"
)

// spikeSyncHook is the smallest hook that exercises Spike B: the sender
// offers the names it has, the receiver answers with the names it lacks, and
// only those are sent. src/foldersync will replace it with the real protocol.
type spikeSyncHook struct {
	sender bool
	names  []string // sender: what it offers
	root   string   // receiver: destination to compare against

	need    map[string]bool
	gotNeed bool
	trace   []message.Type
}

const spikeFeature = "folder-sync-spike-v0"

func (h *spikeSyncHook) Feature() string { return spikeFeature }

func (h *spikeSyncHook) Start(send func(message.Message) error) error {
	if !h.sender {
		return nil
	}
	payload, err := json.Marshal(h.names)
	if err != nil {
		return err
	}
	return send(message.Message{Type: "sync-manifest", Bytes: payload})
}

func (h *spikeSyncHook) Handle(m message.Message, send func(message.Message) error) error {
	h.trace = append(h.trace, m.Type)
	switch {
	case !h.sender && m.Type == "sync-manifest":
		var offered []string
		if err := json.Unmarshal(m.Bytes, &offered); err != nil {
			return err
		}
		var need []string
		for _, name := range offered {
			if _, err := os.Stat(filepath.Join(h.root, filepath.FromSlash(name))); err != nil {
				need = append(need, name)
			}
		}
		payload, err := json.Marshal(need)
		if err != nil {
			return err
		}
		return send(message.Message{Type: "sync-need", Bytes: payload})
	case h.sender && m.Type == "sync-need":
		var need []string
		if err := json.Unmarshal(m.Bytes, &need); err != nil {
			return err
		}
		h.need = make(map[string]bool, len(need))
		for _, name := range need {
			h.need[name] = true
		}
		h.gotNeed = true
		return nil
	}
	return errors.New("unexpected sync message " + string(m.Type))
}

func (h *spikeSyncHook) Ready() (bool, func(FileInfo) bool) {
	if !h.gotNeed {
		return false, nil
	}
	return true, func(info FileInfo) bool {
		return h.need[path.Join(info.FolderRemote, info.Name)]
	}
}

type spikeRun struct {
	sender, receiver       *Client
	sendErr, receiveErr    error
	senderHook, receiverHk *spikeSyncHook
}

func runSpikeSync(t *testing.T, payload, root string, senderHook, receiverHook *spikeSyncHook) spikeRun {
	t.Helper()
	relay := startTestRelay(t, 4)
	secret := "8301-sync-hook-spike"
	senderOps := receiveRootTestOptions(relay, secret, true, "")
	receiverOps := receiveRootTestOptions(relay, secret, false, root)
	if senderHook != nil {
		senderOps.SyncHook = senderHook
	}
	if receiverHook != nil {
		receiverOps.SyncHook = receiverHook
	}
	sender, err := newTestClient(senderOps)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := newTestClient(receiverOps)
	if err != nil {
		t.Fatal(err)
	}
	run := spikeRun{sender: sender, receiver: receiver, senderHook: senderHook, receiverHk: receiverHook}
	files, empty, folders, err := GetFilesInfo([]string{payload}, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if senderHook != nil {
		for _, f := range files {
			senderHook.names = append(senderHook.names, path.Join(f.FolderRemote, f.Name))
		}
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); run.sendErr = sender.Send(files, empty, folders) }()
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		run.receiveErr = receiver.Receive()
	}()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		sender.stop.Cancel()
		receiver.stop.Cancel()
		<-done
		t.Fatal("transfer hung")
	}
	return run
}

func TestSyncHookSendsOnlyRequestedFiles(t *testing.T) {
	source := t.TempDir()
	payload := filepath.Join(source, "payload")
	want := writeReceiveRootFixture(t, payload)
	root := t.TempDir()

	// The destination already has two files, one with different content. The
	// spike compares names only, so both must be left untouched.
	existing := map[string]string{"payload/small/file-00.txt": "keep me\n", "payload/large.bin": "old"}
	for rel, content := range existing {
		name := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	senderHook := &spikeSyncHook{sender: true}
	receiverHook := &spikeSyncHook{root: root}
	run := runSpikeSync(t, payload, root, senderHook, receiverHook)
	if run.sendErr != nil || run.receiveErr != nil {
		t.Fatalf("send=%v receive=%v", run.sendErr, run.receiveErr)
	}

	for rel, content := range existing {
		got, _ := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if string(got) != content {
			t.Fatalf("%s was overwritten", rel)
		}
	}
	for rel, content := range want {
		full := "payload/" + rel
		if _, ok := existing[full]; ok {
			continue
		}
		got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(full)))
		if err != nil || string(got) != string(content) {
			t.Fatalf("%s not received: %v", full, err)
		}
	}
	if n := len(run.sender.FilesToTransfer); n != len(want)-len(existing) {
		t.Fatalf("sender prepared %d files, want %d", n, len(want)-len(existing))
	}
	// Transport selection still sees the full upper bound, not zero.
	var total int64
	for _, content := range want {
		total += int64(len(content))
	}
	if got := run.sender.tailcat.transferBytes.Load(); got != total {
		t.Fatalf("transferBytes = %d, want upper bound %d", got, total)
	}
	if strings.Join(typesOf(receiverHook.trace), ",") != "sync-manifest" ||
		strings.Join(typesOf(senderHook.trace), ",") != "sync-need" {
		t.Fatalf("unexpected message order: receiver=%v sender=%v", receiverHook.trace, senderHook.trace)
	}
}

func TestSyncHookNothingToSend(t *testing.T) {
	source := t.TempDir()
	payload := filepath.Join(source, "payload")
	writeReceiveRootFixture(t, payload)
	root := t.TempDir()
	if err := os.CopyFS(filepath.Join(root, "payload"), os.DirFS(payload)); err != nil {
		t.Fatal(err)
	}
	run := runSpikeSync(t, payload, root, &spikeSyncHook{sender: true}, &spikeSyncHook{root: root})
	if run.sendErr != nil || run.receiveErr != nil {
		t.Fatalf("send=%v receive=%v", run.sendErr, run.receiveErr)
	}
	if n := len(run.sender.FilesToTransfer); n != 0 {
		t.Fatalf("sender prepared %d files, want 0", n)
	}
}

// A peer without the feature must fail fast with a clear message, not hang.
func TestSyncHookRequiresPeerFeature(t *testing.T) {
	for _, tc := range []struct {
		name               string
		senderHook, recvHk bool
	}{
		{"receiver without hook", true, false},
		{"sender without hook", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := t.TempDir()
			payload := filepath.Join(source, "payload")
			writeReceiveRootFixture(t, payload)
			root := t.TempDir()
			var sh, rh *spikeSyncHook
			if tc.senderHook {
				sh = &spikeSyncHook{sender: true}
			}
			if tc.recvHk {
				rh = &spikeSyncHook{root: root}
			}
			run := runSpikeSync(t, payload, root, sh, rh)
			hooked := run.sendErr
			if !tc.senderHook {
				hooked = run.receiveErr
			}
			if !errors.Is(hooked, ErrPeerLacksSync) && !strings.Contains(errString(hooked), "does not support folder sync") {
				t.Fatalf("hooked side error = %v, want ErrPeerLacksSync", hooked)
			}
			entries, _ := os.ReadDir(root)
			if len(entries) != 0 {
				t.Fatalf("files written despite failed negotiation: %v", entries)
			}
		})
	}
}

func typesOf(trace []message.Type) []string {
	out := make([]string, len(trace))
	for i, t := range trace {
		out[i] = string(t)
	}
	return out
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
