package croc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/message"
)

type roundsPayload struct {
	files        []FileInfo
	emptyFolders []FileInfo
	totalFolders int
	names        []string
	last         bool
}

type roundsManifest struct {
	Names []string `json:"names"`
	Last  bool     `json:"last,omitempty"`
}

// roundsSpikeHook extends the spike sync protocol to run several rounds over
// one session using SyncRounds and SyncFinisher.
type roundsSpikeHook struct {
	sender bool
	root   string

	mu     sync.Mutex
	queue  []roundsPayload
	wakeCh chan struct{}

	// Accessed only on the control goroutine while the session is running.
	current         roundsPayload
	need            map[string]bool
	gotNeed         bool
	last            bool
	roundsCompleted int
	finishCalls     int
	resultCount     int
	roundDone       chan int
}

var (
	_ SyncHook     = (*roundsSpikeHook)(nil)
	_ SyncFinisher = (*roundsSpikeHook)(nil)
	_ SyncRounds   = (*roundsSpikeHook)(nil)
)

func newSenderRoundsHook(first roundsPayload) *roundsSpikeHook {
	return &roundsSpikeHook{
		sender:    true,
		wakeCh:    make(chan struct{}, 1),
		current:   first,
		last:      first.last,
		roundDone: make(chan int, 64),
	}
}

func newReceiverRoundsHook(root string) *roundsSpikeHook {
	return &roundsSpikeHook{
		root:      root,
		roundDone: make(chan int, 64),
	}
}

func (h *roundsSpikeHook) enqueue(p roundsPayload) {
	h.mu.Lock()
	h.queue = append(h.queue, p)
	h.mu.Unlock()
	select {
	case h.wakeCh <- struct{}{}:
	default:
	}
}

func (h *roundsSpikeHook) Feature() string { return spikeFeature }

func (h *roundsSpikeHook) Start(send func(message.Message) error) error {
	if !h.sender {
		return nil
	}
	return h.sendManifest(send)
}

func (h *roundsSpikeHook) sendManifest(send func(message.Message) error) error {
	payload, err := json.Marshal(roundsManifest{
		Names: h.current.names,
		Last:  h.current.last,
	})
	if err != nil {
		return err
	}
	return send(message.Message{Type: "sync-manifest", Bytes: payload})
}

func (h *roundsSpikeHook) Handle(m message.Message, send func(message.Message) error) error {
	switch {
	case !h.sender && m.Type == "sync-manifest":
		var manifest roundsManifest
		if err := json.Unmarshal(m.Bytes, &manifest); err != nil {
			return err
		}
		h.last = manifest.Last
		var need []string
		for _, name := range manifest.Names {
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
	case h.sender && m.Type == message.TypeSyncResult:
		h.resultCount++
		return nil
	}
	return errors.New("unexpected sync message " + string(m.Type))
}

func (h *roundsSpikeHook) Ready() (bool, func(FileInfo) bool) {
	if !h.gotNeed {
		return false, nil
	}
	return true, func(info FileInfo) bool {
		return h.need[path.Join(info.FolderRemote, info.Name)]
	}
}

func (h *roundsSpikeHook) BeforeFinish(send func(message.Message) error) error {
	h.finishCalls++
	return send(message.Message{Type: message.TypeSyncResult, Bytes: []byte(`{}`)})
}

func (h *roundsSpikeHook) Wake() <-chan struct{} {
	return h.wakeCh
}

func (h *roundsSpikeHook) OnWake(send func(message.Message) error) error {
	if !h.sender {
		return nil
	}
	h.mu.Lock()
	if len(h.queue) == 0 {
		h.mu.Unlock()
		return nil
	}
	next := h.queue[0]
	h.queue = h.queue[1:]
	more := len(h.queue) > 0
	h.mu.Unlock()
	if more {
		select {
		case h.wakeCh <- struct{}{}:
		default:
		}
	}
	h.current = next
	h.last = next.last
	h.gotNeed = false
	h.need = nil
	return h.sendManifest(send)
}

func (h *roundsSpikeHook) KeepOpen() bool {
	h.roundsCompleted++
	h.gotNeed = false
	h.need = nil
	h.roundDone <- h.roundsCompleted
	return !h.last
}

func (h *roundsSpikeHook) NextFiles() ([]FileInfo, []FileInfo, int) {
	return h.current.files, h.current.emptyFolders, h.current.totalFolders
}

func buildRoundsPayload(t *testing.T, payloadDir string, last bool) roundsPayload {
	t.Helper()
	files, empty, folders, err := GetFilesInfo([]string{payloadDir}, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, path.Join(f.FolderRemote, f.Name))
	}
	return roundsPayload{
		files:        files,
		emptyFolders: empty,
		totalFolders: folders,
		names:        names,
		last:         last,
	}
}

func writeRoundsFile(t *testing.T, baseDir, rel string, content []byte) {
	t.Helper()
	name := filepath.Join(baseDir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

func assertReceivedContent(t *testing.T, root string, want map[string][]byte) {
	t.Helper()
	for rel, expected := range want {
		got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("%s content mismatch: got %d bytes, want %d bytes", rel, len(got), len(expected))
		}
	}
}

func waitRoundComplete(t *testing.T, label string, roundCh <-chan int, wantRound int, sendDone, recvDone <-chan error, clients ...*Client) {
	t.Helper()
	select {
	case got := <-roundCh:
		if got != wantRound {
			cancelClients(clients...)
			t.Fatalf("%s completed round %d, want %d", label, got, wantRound)
		}
	case err := <-sendDone:
		cancelClients(clients...)
		t.Fatalf("sender exited early before %s round %d: %v", label, wantRound, err)
	case err := <-recvDone:
		cancelClients(clients...)
		t.Fatalf("receiver exited early before %s round %d: %v", label, wantRound, err)
	case <-time.After(20 * time.Second):
		cancelClients(clients...)
		t.Fatalf("timed out waiting for %s round %d", label, wantRound)
	}
}

func assertStillOpen(t *testing.T, round int, sendDone, recvDone <-chan error) {
	t.Helper()
	select {
	case err := <-sendDone:
		t.Fatalf("Send returned after round %d before KeepOpen returned false: %v", round, err)
	case err := <-recvDone:
		t.Fatalf("Receive returned after round %d before KeepOpen returned false: %v", round, err)
	default:
	}
}

func waitSessionEnd(t *testing.T, timeout time.Duration, sendDone, recvDone <-chan error, clients ...*Client) (sendErr, recvErr error) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var gotSend, gotRecv bool
	for !gotSend || !gotRecv {
		select {
		case sendErr = <-sendDone:
			gotSend = true
			sendDone = nil
		case recvErr = <-recvDone:
			gotRecv = true
			recvDone = nil
		case <-timer.C:
			cancelClients(clients...)
			t.Fatalf("session did not finish within %s (gotSend=%v gotRecv=%v)", timeout, gotSend, gotRecv)
		}
	}
	return sendErr, recvErr
}

func TestSyncRoundsSeveralRoundsOneSession(t *testing.T) {
	relay := startTestRelay(t, 4)
	source := t.TempDir()
	payloadDir := filepath.Join(source, "payload")
	root := t.TempDir()

	want := make(map[string][]byte)
	want["payload/large-round1.bin"] = randomBytes(t, 3<<20)
	for i := range 20 {
		rel := fmt.Sprintf("payload/small/file-%02d.txt", i)
		want[rel] = []byte(fmt.Sprintf("round 1 small file %d\n", i))
	}
	for rel, content := range want {
		writeRoundsFile(t, source, rel, content)
	}

	round1 := buildRoundsPayload(t, payloadDir, false)
	senderHook := newSenderRoundsHook(round1)
	receiverHook := newReceiverRoundsHook(root)

	secret := "8501-sync-rounds-multi"
	senderOps := receiveRootTestOptions(relay, secret, true, "")
	senderOps.SyncHook = senderHook
	receiverOps := receiveRootTestOptions(relay, secret, false, root)
	receiverOps.SyncHook = receiverHook

	sender, err := newTestClient(senderOps)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := newTestClient(receiverOps)
	if err != nil {
		t.Fatal(err)
	}

	var sendCalls, receiveCalls atomic.Int32
	sendDone := make(chan error, 1)
	recvDone := make(chan error, 1)

	go func() {
		sendCalls.Add(1)
		sendDone <- sender.Send(round1.files, round1.emptyFolders, round1.totalFolders)
	}()
	go func() {
		time.Sleep(100 * time.Millisecond)
		receiveCalls.Add(1)
		recvDone <- receiver.Receive()
	}()

	// Round 1: 3 MiB file + 20 small files.
	waitRoundComplete(t, "sender", senderHook.roundDone, 1, sendDone, recvDone, sender, receiver)
	waitRoundComplete(t, "receiver", receiverHook.roundDone, 1, sendDone, recvDone, sender, receiver)
	assertStillOpen(t, 1, sendDone, recvDone)
	assertReceivedContent(t, root, want)

	// Round 2: add 5 new small files and one new 2 MiB file.
	want["payload/large-round2.bin"] = randomBytes(t, 2<<20)
	writeRoundsFile(t, source, "payload/large-round2.bin", want["payload/large-round2.bin"])
	for i := 20; i < 25; i++ {
		rel := fmt.Sprintf("payload/small/file-%02d.txt", i)
		want[rel] = []byte(fmt.Sprintf("round 2 small file %d\n", i))
		writeRoundsFile(t, source, rel, want[rel])
	}
	senderHook.enqueue(buildRoundsPayload(t, payloadDir, false))

	waitRoundComplete(t, "sender", senderHook.roundDone, 2, sendDone, recvDone, sender, receiver)
	waitRoundComplete(t, "receiver", receiverHook.roundDone, 2, sendDone, recvDone, sender, receiver)
	assertStillOpen(t, 2, sendDone, recvDone)
	assertReceivedContent(t, root, want)

	// Round 3: no new files (empty need), then close session.
	senderHook.enqueue(buildRoundsPayload(t, payloadDir, true))

	sendErr, recvErr := waitSessionEnd(t, 20*time.Second, sendDone, recvDone, sender, receiver)
	if sendErr != nil || recvErr != nil {
		t.Fatalf("send=%v receive=%v", sendErr, recvErr)
	}
	assertReceivedContent(t, root, want)

	if sendCalls.Load() != 1 || receiveCalls.Load() != 1 {
		t.Fatalf("calls send=%d receive=%d, want 1 each", sendCalls.Load(), receiveCalls.Load())
	}
	if senderHook.roundsCompleted != 3 || receiverHook.roundsCompleted != 3 {
		t.Fatalf("roundsCompleted sender=%d receiver=%d, want 3 each", senderHook.roundsCompleted, receiverHook.roundsCompleted)
	}
	if receiverHook.finishCalls != 3 || senderHook.resultCount != 3 {
		t.Fatalf("finishCalls=%d resultCount=%d, want 3 each", receiverHook.finishCalls, senderHook.resultCount)
	}
}

func TestSyncRoundsChunkedAndBundledPipelines(t *testing.T) {
	relay := startTestRelay(t, 4)
	source := t.TempDir()
	payloadDir := filepath.Join(source, "payload")
	root := t.TempDir()

	// Round 1: 6 small files (bundle pipeline).
	want := make(map[string][]byte)
	for i := range 6 {
		rel := fmt.Sprintf("payload/r1-%02d.txt", i)
		want[rel] = []byte(fmt.Sprintf("bundle round 1 file %d\n", i))
		writeRoundsFile(t, source, rel, want[rel])
	}
	round1 := buildRoundsPayload(t, payloadDir, false)
	senderHook := newSenderRoundsHook(round1)
	receiverHook := newReceiverRoundsHook(root)

	secret := "8502-sync-rounds-pipelines"
	senderOps := receiveRootTestOptions(relay, secret, true, "")
	senderOps.SyncHook = senderHook
	receiverOps := receiveRootTestOptions(relay, secret, false, root)
	receiverOps.SyncHook = receiverHook

	sender, err := newTestClient(senderOps)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := newTestClient(receiverOps)
	if err != nil {
		t.Fatal(err)
	}

	sendDone := make(chan error, 1)
	recvDone := make(chan error, 1)
	go func() { sendDone <- sender.Send(round1.files, round1.emptyFolders, round1.totalFolders) }()
	go func() { time.Sleep(100 * time.Millisecond); recvDone <- receiver.Receive() }()

	waitRoundComplete(t, "sender", senderHook.roundDone, 1, sendDone, recvDone, sender, receiver)
	waitRoundComplete(t, "receiver", receiverHook.roundDone, 1, sendDone, recvDone, sender, receiver)
	assertReceivedContent(t, root, want)

	// Round 2: single 2 MiB file (chunked pipeline, below bundle.MinFiles).
	want["payload/solo-large.bin"] = randomBytes(t, 2<<20)
	writeRoundsFile(t, source, "payload/solo-large.bin", want["payload/solo-large.bin"])
	senderHook.enqueue(buildRoundsPayload(t, payloadDir, false))

	waitRoundComplete(t, "sender", senderHook.roundDone, 2, sendDone, recvDone, sender, receiver)
	waitRoundComplete(t, "receiver", receiverHook.roundDone, 2, sendDone, recvDone, sender, receiver)
	assertReceivedContent(t, root, want)

	// Round 3: 6 more small files (bundle pipeline again), then close.
	for i := range 6 {
		rel := fmt.Sprintf("payload/r3-%02d.txt", i)
		want[rel] = []byte(fmt.Sprintf("bundle round 3 file %d\n", i))
		writeRoundsFile(t, source, rel, want[rel])
	}
	senderHook.enqueue(buildRoundsPayload(t, payloadDir, true))

	sendErr, recvErr := waitSessionEnd(t, 20*time.Second, sendDone, recvDone, sender, receiver)
	if sendErr != nil || recvErr != nil {
		t.Fatalf("send=%v receive=%v", sendErr, recvErr)
	}
	assertReceivedContent(t, root, want)
}

func TestSyncRoundsManyRoundsStayUnderRelayLimits(t *testing.T) {
	const totalRounds = 25
	started := time.Now()

	relay := startTestRelay(t, 4)
	source := t.TempDir()
	payloadDir := filepath.Join(source, "payload")
	root := t.TempDir()

	want := make(map[string][]byte, totalRounds)
	firstRel := "payload/file-00.txt"
	want[firstRel] = []byte("round 0\n")
	writeRoundsFile(t, source, firstRel, want[firstRel])

	round1 := buildRoundsPayload(t, payloadDir, false)
	senderHook := newSenderRoundsHook(round1)
	receiverHook := newReceiverRoundsHook(root)

	secret := "8503-sync-rounds-many"
	senderOps := receiveRootTestOptions(relay, secret, true, "")
	senderOps.SyncHook = senderHook
	receiverOps := receiveRootTestOptions(relay, secret, false, root)
	receiverOps.SyncHook = receiverHook

	sender, err := newTestClient(senderOps)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := newTestClient(receiverOps)
	if err != nil {
		t.Fatal(err)
	}

	sendDone := make(chan error, 1)
	recvDone := make(chan error, 1)
	go func() { sendDone <- sender.Send(round1.files, round1.emptyFolders, round1.totalFolders) }()
	go func() { time.Sleep(100 * time.Millisecond); recvDone <- receiver.Receive() }()

	for round := 1; round < totalRounds; round++ {
		waitRoundComplete(t, "sender", senderHook.roundDone, round, sendDone, recvDone, sender, receiver)
		waitRoundComplete(t, "receiver", receiverHook.roundDone, round, sendDone, recvDone, sender, receiver)

		rel := fmt.Sprintf("payload/file-%02d.txt", round)
		want[rel] = []byte(fmt.Sprintf("round %d\n", round))
		writeRoundsFile(t, source, rel, want[rel])
		senderHook.enqueue(buildRoundsPayload(t, payloadDir, round == totalRounds-1))
	}

	sendErr, recvErr := waitSessionEnd(t, 30*time.Second, sendDone, recvDone, sender, receiver)
	if sendErr != nil || recvErr != nil {
		t.Fatalf("send=%v receive=%v", sendErr, recvErr)
	}
	if elapsed := time.Since(started); elapsed > 30*time.Second {
		t.Fatalf("25 rounds took %s, want under 30s", elapsed)
	}
	assertReceivedContent(t, root, want)
	if senderHook.roundsCompleted != totalRounds || receiverHook.roundsCompleted != totalRounds {
		t.Fatalf("roundsCompleted sender=%d receiver=%d, want %d each", senderHook.roundsCompleted, receiverHook.roundsCompleted, totalRounds)
	}
}

func TestSyncRoundsCancelBetweenRounds(t *testing.T) {
	relay := startTestRelay(t, 4)
	source := t.TempDir()
	payloadDir := filepath.Join(source, "payload")
	root := t.TempDir()

	want := map[string][]byte{
		"payload/initial.txt": []byte("hello round 1\n"),
	}
	writeRoundsFile(t, source, "payload/initial.txt", want["payload/initial.txt"])

	round1 := buildRoundsPayload(t, payloadDir, false)
	senderHook := newSenderRoundsHook(round1)
	receiverHook := newReceiverRoundsHook(root)

	secret := "8504-sync-rounds-cancel"
	senderOps := receiveRootTestOptions(relay, secret, true, "")
	senderOps.SyncHook = senderHook
	receiverOps := receiveRootTestOptions(relay, secret, false, root)
	receiverOps.SyncHook = receiverHook

	sender, err := newTestClient(senderOps)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := newTestClient(receiverOps)
	if err != nil {
		t.Fatal(err)
	}

	sendDone := make(chan error, 1)
	recvDone := make(chan error, 1)
	go func() { sendDone <- sender.Send(round1.files, round1.emptyFolders, round1.totalFolders) }()
	go func() { time.Sleep(100 * time.Millisecond); recvDone <- receiver.Receive() }()

	waitRoundComplete(t, "sender", senderHook.roundDone, 1, sendDone, recvDone, sender, receiver)
	waitRoundComplete(t, "receiver", receiverHook.roundDone, 1, sendDone, recvDone, sender, receiver)
	assertReceivedContent(t, root, want)
	assertStillOpen(t, 1, sendDone, recvDone)

	receiver.stop.Cancel()

	sendErr, recvErr := waitSessionEnd(t, 10*time.Second, sendDone, recvDone, sender, receiver)
	if !errors.Is(recvErr, context.Canceled) {
		t.Fatalf("receive error = %v, want context.Canceled", recvErr)
	}
	if sendErr == nil {
		t.Fatal("sender returned nil after receiver canceled between rounds")
	}
}

func TestSyncRoundsPeerDisconnectBetweenRounds(t *testing.T) {
	relay := startTestRelay(t, 4)
	source := t.TempDir()
	payloadDir := filepath.Join(source, "payload")
	root := t.TempDir()

	writeRoundsFile(t, source, "payload/initial.txt", []byte("hello round 1\n"))
	round1 := buildRoundsPayload(t, payloadDir, false)
	senderHook := newSenderRoundsHook(round1)
	receiverHook := newReceiverRoundsHook(root)

	secret := "8505-sync-rounds-disconnect"
	senderOps := receiveRootTestOptions(relay, secret, true, "")
	senderOps.SyncHook = senderHook
	receiverOps := receiveRootTestOptions(relay, secret, false, root)
	receiverOps.SyncHook = receiverHook

	sender, err := newTestClient(senderOps)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := newTestClient(receiverOps)
	if err != nil {
		t.Fatal(err)
	}

	sendDone := make(chan error, 1)
	recvDone := make(chan error, 1)
	go func() { sendDone <- sender.Send(round1.files, round1.emptyFolders, round1.totalFolders) }()
	go func() { time.Sleep(100 * time.Millisecond); recvDone <- receiver.Receive() }()

	waitRoundComplete(t, "sender", senderHook.roundDone, 1, sendDone, recvDone, sender, receiver)
	waitRoundComplete(t, "receiver", receiverHook.roundDone, 1, sendDone, recvDone, sender, receiver)
	assertStillOpen(t, 1, sendDone, recvDone)

	// Simulate an abrupt peer drop between rounds.
	receiver.connection(0).Close()

	sendErr, recvErr := waitSessionEnd(t, 10*time.Second, sendDone, recvDone, sender, receiver)
	if !isTransferDisconnectError(sendErr) {
		t.Fatalf("sender error = %v, want transferDisconnectError", sendErr)
	}
	if !isTransferDisconnectError(recvErr) {
		t.Fatalf("receiver error = %v, want transferDisconnectError", recvErr)
	}
}
