package croc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/message"
)

// Spike C (folder sync --watch): compares opening a new croc session per sync
// round with one control-message round trip on an already open session. Both
// run against a loopback relay, so the numbers are a lower bound; public relay
// latency adds network RTTs to every PAKE and relay hop.
//
// Run with: CROC_SPIKE=1 go test -run TestSpikeSessionReopenLatency -v ./src/croc
func TestSpikeSessionReopenLatency(t *testing.T) {
	if os.Getenv("CROC_SPIKE") == "" {
		t.Skip("set CROC_SPIKE=1 to measure")
	}
	const rounds = 20
	relay := startTestRelay(t, 4)
	source := filepath.Join(t.TempDir(), "edit.txt")
	if err := os.WriteFile(source, []byte("one small edit\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	reopen := make([]time.Duration, 0, rounds)
	for i := range rounds {
		root := t.TempDir()
		secret := fmt.Sprintf("%d-spike-reopen", 8400+i)
		sender, err := newTestClient(receiveRootTestOptions(relay, secret, true, ""))
		if err != nil {
			t.Fatal(err)
		}
		receiver, err := newTestClient(receiveRootTestOptions(relay, secret, false, root))
		if err != nil {
			t.Fatal(err)
		}
		files, empty, folders, err := GetFilesInfo([]string{source}, false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		var wg sync.WaitGroup
		var sendErr, recvErr error
		wg.Add(2)
		go func() { defer wg.Done(); sendErr = sender.Send(files, empty, folders) }()
		go func() { defer wg.Done(); recvErr = receiver.Receive() }()
		wg.Wait()
		if sendErr != nil || recvErr != nil {
			if errors.Is(sendErr, ErrRelayConnection) && strings.Contains(sendErr.Error(), "rate limited") {
				// Each session admits a control and data connections per peer;
				// the relay limits admissions per source address.
				t.Logf("relay admission limit reached after %d sessions: %v", i, sendErr)
				break
			}
			t.Fatalf("round %d: send=%v receive=%v", i, sendErr, recvErr)
		}
		reopen = append(reopen, time.Since(start))
	}
	// Let the admission window recover before the keepalive measurement.
	if len(reopen) < rounds {
		time.Sleep(time.Minute)
	}

	// Round trips on a live session, timed inside the hook.
	senderHook := &pingHook{sender: true, rounds: rounds}
	receiverHook := &pingHook{}
	root := t.TempDir()
	secret := "8499-spike-keepalive"
	senderOps := receiveRootTestOptions(relay, secret, true, "")
	senderOps.SyncHook = senderHook
	receiverOps := receiveRootTestOptions(relay, secret, false, root)
	receiverOps.SyncHook = receiverHook
	sender, _ := newTestClient(senderOps)
	receiver, _ := newTestClient(receiverOps)
	files, empty, folders, _ := GetFilesInfo([]string{source}, false, false, nil)
	var wg sync.WaitGroup
	var sendErr, recvErr error
	wg.Add(2)
	go func() { defer wg.Done(); sendErr = sender.Send(files, empty, folders) }()
	go func() { defer wg.Done(); recvErr = receiver.Receive() }()
	wg.Wait()
	if sendErr != nil || recvErr != nil {
		t.Fatalf("keepalive: send=%v receive=%v", sendErr, recvErr)
	}

	t.Logf("new session per round (send+receive 1 small file): %s", summarize(reopen))
	t.Logf("control round trip on open session:               %s", summarize(senderHook.rtts))
}

type pingHook struct {
	sender bool
	rounds int
	sentAt time.Time
	rtts   []time.Duration
	done   bool
}

func (h *pingHook) Feature() string { return spikeFeature }

func (h *pingHook) Start(send func(message.Message) error) error {
	if !h.sender {
		return nil
	}
	h.sentAt = time.Now()
	return send(message.Message{Type: "sync-ping"})
}

func (h *pingHook) Handle(m message.Message, send func(message.Message) error) error {
	if !h.sender {
		return send(message.Message{Type: "sync-pong"})
	}
	h.rtts = append(h.rtts, time.Since(h.sentAt))
	if len(h.rtts) == h.rounds {
		h.done = true
		return nil
	}
	h.sentAt = time.Now()
	return send(message.Message{Type: "sync-ping"})
}

func (h *pingHook) Ready() (bool, func(FileInfo) bool) { return h.done, nil }

func summarize(samples []time.Duration) string {
	if len(samples) == 0 {
		return "no samples"
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	pick := func(p float64) time.Duration { return sorted[int(p*float64(len(sorted)-1))] }
	return fmt.Sprintf("n=%d p50=%s p95=%s max=%s",
		len(sorted), pick(0.50).Round(time.Microsecond), pick(0.95).Round(time.Microsecond), sorted[len(sorted)-1].Round(time.Microsecond))
}
