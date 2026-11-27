package foldersync

import (
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/schollz/croc/v11/src/message"
)

// fakeSource trả hash từ nội dung giả lập.
type fakeSource struct {
	content map[string]string
	changed map[string]bool
	calls   [][]string
}

func (f *fakeSource) HashSource(paths []string) []PathHash {
	f.calls = append(f.calls, slices.Clone(paths))
	out := make([]PathHash, len(paths))
	for i, p := range paths {
		out[i] = PathHash{Path: p}
		if f.changed[p] {
			out[i].Error = "file changed"
			continue
		}
		out[i].Hash = binary.BigEndian.AppendUint64(nil, xxhash.Sum64String(f.content[p]))
	}
	return out
}

type fakeDest struct {
	dst       *Manifest
	content   map[string]string
	decision  Decision
	scanErr   error
	decideErr error
	commitErr error
	plan      *Plan
	committed bool
	filter    FilterSpec
}

func (f *fakeDest) ScanDestination(filter FilterSpec) (*Manifest, error) {
	f.filter = filter
	return f.dst, f.scanErr
}

func (f *fakeDest) HashDestination(dst *Manifest, paths []string) error {
	for _, p := range paths {
		for i := range dst.Entries {
			if dst.Entries[i].Path == p {
				dst.Entries[i].Hash = binary.BigEndian.AppendUint64(nil, xxhash.Sum64String(f.content[p]))
			}
		}
	}
	return nil
}

func (f *fakeDest) Decide(p *Plan) (Decision, error) {
	f.plan = p
	return f.decision, f.decideErr
}

func (f *fakeDest) Commit(p *Plan, _ *Manifest, d Decision) (*Result, error) {
	f.committed = true
	if f.commitErr != nil {
		return nil, f.commitErr
	}
	r := &Result{DryRun: !d.Proceed}
	if d.Proceed {
		r.Added, r.Updated, r.Touched, r.Deleted = len(p.Add), len(p.Update), len(p.Touch), len(p.Delete)
	}
	return r, nil
}

// exchange chuyển bản tin qua lại cho đến khi không còn gì, ghi lại thứ tự loại bản tin.
func exchange(t *testing.T, src, dst *Session) (trace []string, err error) {
	t.Helper()
	type pending struct {
		to *Session
		m  message.Message
	}
	var queue []pending
	push := func(to *Session, msgs []message.Message) {
		for _, m := range msgs {
			queue = append(queue, pending{to, m})
		}
	}
	out, err := src.Start()
	if err != nil {
		return nil, err
	}
	push(dst, out)
	out, err = dst.Start()
	if err != nil {
		return nil, err
	}
	// Đích gửi hello trước manifest của nguồn để mô phỏng thứ tự thực tế.
	queue = append([]pending{{src, out[0]}}, queue...)
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		trace = append(trace, string(p.m.Type))
		other := src
		if p.to == src {
			other = dst
		}
		out, err := p.to.Handle(p.m)
		if err != nil {
			return trace, err
		}
		push(other, out)
	}
	return trace, nil
}

func srcManifest() (*Manifest, map[string]string) {
	content := map[string]string{"same": "s", "edited": "new!", "touched": "t", "new": "n", "d/x": "x"}
	return man(fe("same", 1, 0, ""), fe("edited", 4, 5, ""), fe("touched", 1, 5, ""), fe("new", 1, 0, ""), de("d"), fe("d/x", 1, 0, "")), content
}

func TestSessionFullRound(t *testing.T) {
	sm, scontent := srcManifest()
	sm.Filter = FilterSpec{Exclude: []string{".tmp"}}
	srcEnv := &fakeSource{content: scontent}
	dstEnv := &fakeDest{
		dst:      man(fe("same", 1, 0, ""), fe("edited", 4, 0, ""), fe("touched", 1, 0, ""), fe("old", 1, 0, "")),
		content:  map[string]string{"same": "s", "edited": "old!", "touched": "t", "old": "o"},
		decision: Decision{Proceed: true},
	}
	src := NewSourceSession(sm, srcEnv, Hello{})
	dst := NewDestSession(dstEnv, DiffOptions{Delete: true}, Hello{})
	if src.Role() != RoleSource || dst.Role() != RoleDestination {
		t.Fatal("roles")
	}
	trace, err := exchange(t, src, dst)
	if err != nil {
		t.Fatalf("exchange: %v (trace %v)", err, trace)
	}
	if dstEnv.filter.Exclude[0] != ".tmp" {
		t.Fatal("destination must scan with the source filter")
	}
	// Tầng 2 chỉ hỏi edited/touched; sau đó hỏi hash cho tệp sẽ tải (new, d/x, edited đã có).
	if len(srcEnv.calls) != 2 || !slices.Equal(srcEnv.calls[0], []string{"edited", "touched"}) ||
		!slices.Equal(srcEnv.calls[1], []string{"d/x", "new"}) {
		t.Fatalf("hash calls = %q", srcEnv.calls)
	}
	ready, need := src.Ready()
	if !ready || len(need) != 3 || !need["edited"] || !need["new"] || !need["d/x"] || need["touched"] {
		t.Fatalf("need = %v", need)
	}
	if got := dst.Need(); !slices.Equal(got, []string{"d/x", "new", "edited"}) {
		t.Fatalf("dst need = %q", got)
	}

	// croc chỉ cho nhận tệp trong need với đúng kích thước.
	if err := dst.AllowReceive("new", 1, false); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []struct {
		p     string
		size  int64
		isDir bool
	}{{"same", 1, false}, {"new", 2, false}, {"d", 0, true}, {"../x", 1, false}} {
		if err := dst.AllowReceive(bad.p, bad.size, bad.isDir); !errors.Is(err, ErrProtocol) {
			t.Fatalf("AllowReceive(%q) = %v", bad.p, err)
		}
	}

	finish, err := dst.Finish()
	if err != nil || len(finish) != 1 || finish[0].Type != message.TypeSyncResult {
		t.Fatalf("finish: %v %v", finish, err)
	}
	if _, err := src.Handle(finish[0]); err != nil {
		t.Fatal(err)
	}
	if !src.Done() || !dst.Done() {
		t.Fatal("both sessions must be done")
	}
	r := src.Result()
	if r.Added != 3 || r.Updated != 1 || r.Touched != 1 || r.Deleted != 1 || dst.Result() == nil || dst.Plan() == nil {
		t.Fatalf("result = %+v", r)
	}
	// Mọi bản tin sau khi xong đều bị từ chối.
	if _, err := src.Handle(finish[0]); !errors.Is(err, ErrProtocol) {
		t.Fatalf("message after done: %v", err)
	}
	if _, err := src.Handle(finish[0]); !errors.Is(err, ErrProtocol) {
		t.Fatalf("message after failure: %v", err)
	}
	wantTrace := []string{"sync-hello", "sync-hello", "sync-manifest-start", "sync-manifest-batch", "sync-manifest-end",
		"sync-hash-request", "sync-hash-result", "sync-hash-request", "sync-hash-result", "sync-need"}
	if !slices.Equal(trace, wantTrace) {
		t.Fatalf("trace = %v", trace)
	}
}

func TestSessionDryRunRequestsNothing(t *testing.T) {
	sm, scontent := srcManifest()
	dstEnv := &fakeDest{dst: man(), decision: Decision{Proceed: false}}
	src := NewSourceSession(sm, &fakeSource{content: scontent}, Hello{})
	dst := NewDestSession(dstEnv, DiffOptions{}, Hello{})
	if _, err := exchange(t, src, dst); err != nil {
		t.Fatal(err)
	}
	ready, need := src.Ready()
	if !ready || len(need) != 0 || len(dst.Need()) != 0 {
		t.Fatalf("dry run need = %v", need)
	}
	if err := dst.AllowReceive("new", 1, false); err == nil {
		t.Fatal("dry run must refuse every file")
	}
	finish, err := dst.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Handle(finish[0]); err != nil || !src.Result().DryRun {
		t.Fatalf("result = %+v, %v", src.Result(), err)
	}
}

func TestSessionSourceChangedAborts(t *testing.T) {
	sm, scontent := srcManifest()
	srcEnv := &fakeSource{content: scontent, changed: map[string]bool{"new": true}}
	dst := NewDestSession(&fakeDest{dst: man(), decision: Decision{Proceed: true}}, DiffOptions{}, Hello{})
	_, err := exchange(t, NewSourceSession(sm, srcEnv, Hello{}), dst)
	if !errors.Is(err, ErrSourceChanged) || !strings.Contains(err.Error(), "new") {
		t.Fatalf("err = %v", err)
	}
}

func TestSessionEnvErrorsPropagate(t *testing.T) {
	sm, scontent := srcManifest()
	boom := errors.New("boom")
	for name, env := range map[string]*fakeDest{
		"scan":   {dst: man(), scanErr: boom},
		"decide": {dst: man(), decideErr: boom},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := exchange(t, NewSourceSession(sm, &fakeSource{content: scontent}, Hello{}), NewDestSession(env, DiffOptions{}, Hello{}))
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	env := &fakeDest{dst: man(), decision: Decision{Proceed: true}, commitErr: boom}
	dst := NewDestSession(env, DiffOptions{}, Hello{})
	if _, err := exchange(t, NewSourceSession(sm, &fakeSource{content: scontent}, Hello{}), dst); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Finish(); !errors.Is(err, boom) {
		t.Fatalf("commit err = %v", err)
	}
}

func mustEncode(t *testing.T, typ message.Type, v any) message.Message {
	t.Helper()
	m, err := encodeMsg(typ, v)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// TestSessionRejectsInvalidTransitions đưa từng bản tin sai vào một phiên ở trạng thái
// xác định; mọi trường hợp phải trả ErrProtocol và làm hỏng phiên.
func TestSessionRejectsInvalidTransitions(t *testing.T) {
	sm, scontent := srcManifest()
	destHello := mustEncode(t, message.TypeSyncHello, Hello{Version: ProtocolVersion, Role: RoleDestination})
	srcHello := mustEncode(t, message.TypeSyncHello, Hello{Version: ProtocolVersion, Role: RoleSource})
	manifestMsgs, _ := EncodeManifest(sm)

	newSource := func(t *testing.T, prefix ...message.Message) *Session {
		s := NewSourceSession(sm, &fakeSource{content: scontent}, Hello{})
		if _, err := s.Start(); err != nil {
			t.Fatal(err)
		}
		for _, m := range prefix {
			if _, err := s.Handle(m); err != nil {
				t.Fatalf("prefix %s: %v", m.Type, err)
			}
		}
		return s
	}
	newDest := func(t *testing.T, prefix ...message.Message) *Session {
		s := NewDestSession(&fakeDest{dst: man(), decision: Decision{Proceed: true}}, DiffOptions{}, Hello{})
		if _, err := s.Start(); err != nil {
			t.Fatal(err)
		}
		for _, m := range prefix {
			if _, err := s.Handle(m); err != nil {
				t.Fatalf("prefix %s: %v", m.Type, err)
			}
		}
		return s
	}
	need := func(seq int, last bool, paths ...string) message.Message {
		return mustEncode(t, message.TypeSyncNeed, PathList{Seq: seq, Paths: paths, Last: last})
	}
	hashReq := func(seq int, last bool, paths ...string) message.Message {
		return mustEncode(t, message.TypeSyncHashRequest, PathList{Seq: seq, Paths: paths, Last: last})
	}
	result := mustEncode(t, message.TypeSyncResult, Result{})

	cases := []struct {
		name string
		s    func(t *testing.T) *Session
		m    message.Message
	}{
		{"source: need before hello", func(t *testing.T) *Session { return newSource(t) }, need(0, true, "new")},
		{"source: wrong peer role", func(t *testing.T) *Session { return newSource(t) }, srcHello},
		{"source: wrong version", func(t *testing.T) *Session { return newSource(t) },
			mustEncode(t, message.TypeSyncHello, Hello{Version: 99, Role: RoleDestination})},
		{"source: duplicate hello", func(t *testing.T) *Session { return newSource(t, destHello) }, destHello},
		{"source: manifest from destination", func(t *testing.T) *Session { return newSource(t, destHello) }, manifestMsgs[0]},
		{"source: need unknown path", func(t *testing.T) *Session { return newSource(t, destHello) }, need(0, true, "nope")},
		{"source: need directory", func(t *testing.T) *Session { return newSource(t, destHello) }, need(0, true, "d")},
		{"source: need traversal", func(t *testing.T) *Session { return newSource(t, destHello) }, need(0, true, "../etc/passwd")},
		{"source: need duplicate", func(t *testing.T) *Session { return newSource(t, destHello) }, need(0, true, "new", "new")},
		{"source: need out of order", func(t *testing.T) *Session { return newSource(t, destHello) }, need(1, true, "new")},
		{"source: need duplicate batch", func(t *testing.T) *Session { return newSource(t, destHello, need(0, false, "new")) }, need(0, true, "same")},
		{"source: need after need", func(t *testing.T) *Session { return newSource(t, destHello, need(0, true, "new")) }, need(0, true, "same")},
		{"source: hash after need", func(t *testing.T) *Session { return newSource(t, destHello, need(0, true)) }, hashReq(0, true, "new")},
		{"source: need during hash request", func(t *testing.T) *Session { return newSource(t, destHello, hashReq(0, false, "new")) }, need(0, true)},
		{"source: hash unknown path", func(t *testing.T) *Session { return newSource(t, destHello) }, hashReq(0, true, "nope")},
		{"source: hash duplicate", func(t *testing.T) *Session { return newSource(t, destHello) }, hashReq(0, true, "new", "new")},
		{"source: result before need", func(t *testing.T) *Session { return newSource(t, destHello) }, result},
		{"source: garbage need", func(t *testing.T) *Session { return newSource(t, destHello) },
			message.Message{Type: message.TypeSyncNeed, Bytes: []byte("{")}},
		{"source: garbage result", func(t *testing.T) *Session { return newSource(t, destHello, need(0, true)) },
			message.Message{Type: message.TypeSyncResult, Bytes: []byte("[]")}},
		{"source: unknown sync type", func(t *testing.T) *Session { return newSource(t, destHello) }, message.Message{Type: "sync-evil"}},
		{"dest: manifest before hello", func(t *testing.T) *Session { return newDest(t) }, manifestMsgs[0]},
		{"dest: wrong peer role", func(t *testing.T) *Session { return newDest(t) }, destHello},
		{"dest: garbage hello", func(t *testing.T) *Session { return newDest(t) }, message.Message{Type: message.TypeSyncHello, Bytes: []byte("1")}},
		{"dest: batch before start", func(t *testing.T) *Session { return newDest(t, srcHello) }, manifestMsgs[1]},
		{"dest: need from source", func(t *testing.T) *Session { return newDest(t, srcHello) }, need(0, true)},
		{"dest: duplicate start", func(t *testing.T) *Session { return newDest(t, srcHello, manifestMsgs[0]) }, manifestMsgs[0]},
		{"dest: hash result unrequested", func(t *testing.T) *Session { return newDest(t, srcHello, manifestMsgs[0]) },
			mustEncode(t, message.TypeSyncHashResult, HashList{Last: true})},
		{"dest: result from source", func(t *testing.T) *Session { return newDest(t, srcHello) }, result},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.s(t)
			if _, err := s.Handle(tc.m); !errors.Is(err, ErrProtocol) {
				t.Fatalf("err = %v, want ErrProtocol", err)
			}
			// Phiên đã hỏng: bản tin hợp lệ sau đó cũng bị từ chối.
			if _, err := s.Handle(destHello); !errors.Is(err, ErrProtocol) {
				t.Fatalf("failed session accepted another message: %v", err)
			}
		})
	}
}

// TestSessionRejectsBadHashResults kiểm đích từ chối kết quả hash không khớp yêu cầu.
func TestSessionRejectsBadHashResults(t *testing.T) {
	src := man(fe("a", 1, 5, ""))
	dstEnv := func() *fakeDest {
		return &fakeDest{dst: man(fe("a", 1, 0, "")), content: map[string]string{"a": "x"}, decision: Decision{Proceed: true}}
	}
	srcHello := mustEncode(t, message.TypeSyncHello, Hello{Version: ProtocolVersion, Role: RoleSource})
	manifestMsgs, _ := EncodeManifest(src)
	good := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	cases := map[string]HashList{
		"unrequested path": {Hashes: []PathHash{{Path: "a", Hash: good}, {Path: "b", Hash: good}}, Last: true},
		"duplicate path":   {Hashes: []PathHash{{Path: "a", Hash: good}, {Path: "a", Hash: good}}, Last: true},
		"missing path":     {Last: true},
		"short hash":       {Hashes: []PathHash{{Path: "a", Hash: []byte{1}}}, Last: true},
		"wrong seq":        {Seq: 1, Hashes: []PathHash{{Path: "a", Hash: good}}, Last: true},
	}
	for name, hl := range cases {
		t.Run(name, func(t *testing.T) {
			dst := NewDestSession(dstEnv(), DiffOptions{}, Hello{})
			_, _ = dst.Start()
			var out []message.Message
			for _, m := range append([]message.Message{srcHello}, manifestMsgs...) {
				var err error
				if out, err = dst.Handle(m); err != nil {
					t.Fatal(err)
				}
			}
			if len(out) != 1 || out[0].Type != message.TypeSyncHashRequest {
				t.Fatalf("expected a hash request, got %v", out)
			}
			if _, err := dst.Handle(mustEncode(t, message.TypeSyncHashResult, hl)); !errors.Is(err, ErrProtocol) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestSessionStartTwiceAndFinishMisuse(t *testing.T) {
	sm, scontent := srcManifest()
	s := NewSourceSession(sm, &fakeSource{content: scontent}, Hello{})
	if _, err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Start(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("second start: %v", err)
	}
	if _, err := s.Finish(); !errors.Is(err, ErrProtocol) {
		t.Fatal("source must not finish")
	}
	if err := s.AllowReceive("new", 1, false); !errors.Is(err, ErrProtocol) {
		t.Fatal("source must not receive")
	}
	d := NewDestSession(&fakeDest{}, DiffOptions{}, Hello{})
	if ready, _ := d.Ready(); ready {
		t.Fatal("destination is never ready to send")
	}
	if _, err := d.Finish(); !errors.Is(err, ErrProtocol) {
		t.Fatal("finish before transfer phase accepted")
	}
	bad := NewSourceSession(&Manifest{Entries: make([]Entry, MaxManifestEntries+1)}, &fakeSource{}, Hello{})
	if _, err := bad.Start(); err == nil {
		t.Fatal("oversized manifest start accepted")
	}
}
