package foldersync

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/schollz/croc/v11/src/message"
)

func bigManifest(n int) *Manifest {
	m := &Manifest{Complete: true, Filter: FilterSpec{Exclude: []string{".log"}, Git: true}}
	m.Entries = append(m.Entries, de("d"))
	for i := range n {
		m.Entries = append(m.Entries, fe(fmt.Sprintf("d/file-%05d.txt", i), int64(i), i, "x"))
	}
	m.Skipped = []SkippedPath{{Path: "d/link", Reason: SkipSymlink}, {Reason: SkipInvalidName, Detail: "CON"}}
	m.Sort()
	return m
}

func assemble(t *testing.T, msgs []message.Message) (*Manifest, error) {
	t.Helper()
	var a manifestAssembler
	for _, m := range msgs {
		switch m.Type {
		case message.TypeSyncManifestStart:
			if err := a.Start(m); err != nil {
				return nil, err
			}
		case message.TypeSyncManifestBatch:
			if err := a.Batch(m); err != nil {
				return nil, err
			}
		case message.TypeSyncManifestEnd:
			return a.End(m)
		default:
			t.Fatalf("unexpected %s", m.Type)
		}
	}
	return nil, errors.New("no end")
}

func TestManifestWireRoundTrip(t *testing.T) {
	src := bigManifest(500)
	msgs, err := encodeManifestWithLimit(src, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) < 10 {
		t.Fatalf("expected many batches, got %d messages", len(msgs))
	}
	for _, m := range msgs {
		if len(m.Bytes) > 4096 {
			t.Fatalf("%s is %d bytes", m.Type, len(m.Bytes))
		}
	}
	got, err := assemble(t, msgs)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Entries) != len(src.Entries) || len(got.Skipped) != 2 || !got.Complete || !got.Filter.Git {
		t.Fatalf("round trip lost data: %d entries %d skipped", len(got.Entries), len(got.Skipped))
	}
	for i := range src.Entries {
		a, b := src.Entries[i], got.Entries[i]
		if a.Path != b.Path || a.Size != b.Size || !a.ModTime.Equal(b.ModTime) || a.Mode != b.Mode || string(a.Hash) != string(b.Hash) || a.DiskPath != "" && b.DiskPath != "" {
			t.Fatalf("entry %d differs: %+v vs %+v", i, a, b)
		}
	}
	// Kích thước mặc định: 500 mục vừa một lô.
	def, err := EncodeManifest(src)
	if err != nil || len(def) != 3 {
		t.Fatalf("default encoding: %d messages, %v", len(def), err)
	}
	// Manifest rỗng: start + end.
	empty, err := EncodeManifest(&Manifest{Complete: true})
	if err != nil || len(empty) != 2 {
		t.Fatalf("empty manifest: %d messages, %v", len(empty), err)
	}
	if m, err := assemble(t, empty); err != nil || len(m.Entries) != 0 {
		t.Fatalf("empty assemble: %v", err)
	}
}

func TestManifestWireDiskPathNotSent(t *testing.T) {
	m := &Manifest{Entries: []Entry{{Path: "a", DiskPath: `secret\local\a`, Kind: 0}}, Complete: true}
	msgs, err := EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range msgs {
		if strings.Contains(string(msg.Bytes), "secret") {
			t.Fatalf("disk path leaked: %s", msg.Bytes)
		}
	}
}

func TestManifestWireRejectsBrokenStreams(t *testing.T) {
	msgs, err := encodeManifestWithLimit(bigManifest(200), 2048)
	if err != nil {
		t.Fatal(err)
	}
	start, batches, end := msgs[0], msgs[1:len(msgs)-1], msgs[len(msgs)-1]
	if len(batches) < 3 {
		t.Fatalf("need at least 3 batches, got %d", len(batches))
	}
	join := func(parts ...[]message.Message) []message.Message { return slices.Concat(parts...) }
	one := func(m message.Message) []message.Message { return []message.Message{m} }

	tooBig := message.Message{Type: message.TypeSyncManifestBatch, Bytes: make([]byte, MaxBatchBytes+1)}
	tamperedEnd, _ := encodeMsg(message.TypeSyncManifestEnd, ManifestEnd{Batches: len(batches) + 1})
	overDeclared, _ := encodeMsg(message.TypeSyncManifestStart, ManifestStart{Entries: MaxManifestEntries, Skipped: 1})
	negative, _ := encodeMsg(message.TypeSyncManifestStart, ManifestStart{Entries: -1})
	emptyBatch, _ := encodeMsg(message.TypeSyncManifestBatch, ManifestBatch{Seq: 0})
	unknownField := message.Message{Type: message.TypeSyncManifestStart, Bytes: []byte(`{"entries":0,"skipped":0,"complete":true,"evil":1}`)}
	trailing := message.Message{Type: message.TypeSyncManifestStart, Bytes: []byte(`{"entries":0,"skipped":0} {}`)}
	smallStart, _ := encodeMsg(message.TypeSyncManifestStart, ManifestStart{Entries: 1, Skipped: 0, Complete: true})

	invalidEntries := &Manifest{Entries: []Entry{fe("../escape", 1, 0, "")}, Complete: true}
	invalidMsgs, _ := EncodeManifest(invalidEntries)

	tests := map[string][]message.Message{
		"batch out of order": join(one(start), one(batches[1]), one(batches[0])),
		"duplicate batch":    join(one(start), one(batches[0]), one(batches[0])),
		"missing batch":      join(one(start), one(batches[0]), batches[2:], one(end)),
		"missing entries":    join(one(start), batches[:len(batches)-1], one(tamperedEnd)),
		"end count mismatch": join(one(start), batches, one(tamperedEnd)),
		"oversized batch":    join(one(start), one(tooBig)),
		"batch before start": join(one(batches[0])),
		"duplicate start":    join(one(start), one(start)),
		"declared too large": join(one(overDeclared)),
		"negative count":     join(one(negative)),
		"empty batch":        join(one(start), one(emptyBatch)),
		"unknown field":      join(one(unknownField)),
		"trailing data":      join(one(trailing)),
		"more than declared": join(one(smallStart), one(batches[0])),
		"invalid manifest":   invalidMsgs,
		"end before start":   join(one(end)),
		"batch after end":    join(msgs, one(batches[0])),
		"garbage json":       join(one(message.Message{Type: message.TypeSyncManifestStart, Bytes: []byte("{")})),
	}
	for name, stream := range tests {
		t.Run(name, func(t *testing.T) {
			var a manifestAssembler
			var err error
			for _, m := range stream {
				switch m.Type {
				case message.TypeSyncManifestStart:
					err = a.Start(m)
				case message.TypeSyncManifestBatch:
					err = a.Batch(m)
				case message.TypeSyncManifestEnd:
					_, err = a.End(m)
				}
				if err != nil {
					break
				}
			}
			if err == nil {
				t.Fatal("broken stream accepted")
			}
		})
	}
}

func TestManifestWireEncodeLimits(t *testing.T) {
	huge := &Manifest{Entries: []Entry{{Path: strings.Repeat("a", 5000)}}}
	if _, err := encodeManifestWithLimit(huge, 1024); !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v", err)
	}
	hugeSkip := &Manifest{Skipped: []SkippedPath{{Path: strings.Repeat("a", 5000), Reason: SkipFiltered}}}
	if _, err := encodeManifestWithLimit(hugeSkip, 1024); !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v", err)
	}
	tooMany := &Manifest{Entries: make([]Entry, MaxManifestEntries+1)}
	if _, err := EncodeManifest(tooMany); !errors.Is(err, ErrManifestTooLarge) {
		t.Fatalf("err = %v", err)
	}
}

func TestPathListRoundTripAndErrors(t *testing.T) {
	var paths []string
	for i := range 300 {
		paths = append(paths, fmt.Sprintf("dir/file-%04d", i))
	}
	msgs, err := encodePathListsWithLimit(message.TypeSyncNeed, paths, 1024)
	if err != nil || len(msgs) < 5 {
		t.Fatalf("%d messages, %v", len(msgs), err)
	}
	var a listAssembler[string]
	for i, m := range msgs {
		var l PathList
		if err := decodeMsg(m, &l); err != nil {
			t.Fatal(err)
		}
		done, err := a.add(m.Type, l.Seq, l.Paths, l.Last)
		if err != nil || done != (i == len(msgs)-1) {
			t.Fatalf("batch %d: done=%v err=%v", i, done, err)
		}
	}
	if !slices.Equal(a.items, paths) {
		t.Fatal("path list round trip mismatch")
	}
	if _, err := a.add(message.TypeSyncNeed, a.nextSeq, nil, true); !errors.Is(err, ErrProtocol) {
		t.Fatal("batch after last accepted")
	}

	// Danh sách rỗng vẫn là một lô Last.
	empty, err := encodePathLists(message.TypeSyncNeed, nil)
	if err != nil || len(empty) != 1 {
		t.Fatalf("empty: %d %v", len(empty), err)
	}

	var b listAssembler[string]
	if _, err := b.add("x", 1, []string{"a"}, false); !errors.Is(err, ErrProtocol) {
		t.Fatal("out-of-order seq accepted")
	}
	if _, err := b.add("x", 0, nil, false); !errors.Is(err, ErrProtocol) {
		t.Fatal("empty non-final batch accepted")
	}
	b.reset(2)
	if _, err := b.add("x", 0, []string{"a", "b", "c"}, true); !errors.Is(err, ErrProtocol) {
		t.Fatal("over-limit list accepted")
	}
	if _, err := encodePathListsWithLimit(message.TypeSyncNeed, []string{strings.Repeat("a", 2000)}, 1024); !errors.Is(err, ErrProtocol) {
		t.Fatal("oversized path accepted")
	}
}

func TestHashListRoundTrip(t *testing.T) {
	var hashes []PathHash
	for i := range 200 {
		hashes = append(hashes, PathHash{Path: fmt.Sprintf("f%03d", i), Hash: []byte{1, 2, 3, 4, 5, 6, 7, byte(i)}})
	}
	hashes = append(hashes, PathHash{Path: "changed", Error: "file changed"})
	msgs, err := encodeHashListsWithLimit(hashes, 1024)
	if err != nil || len(msgs) < 3 {
		t.Fatalf("%d messages, %v", len(msgs), err)
	}
	var a listAssembler[PathHash]
	for _, m := range msgs {
		var l HashList
		if err := decodeMsg(m, &l); err != nil {
			t.Fatal(err)
		}
		if _, err := a.add(m.Type, l.Seq, l.Hashes, l.Last); err != nil {
			t.Fatal(err)
		}
	}
	if len(a.items) != len(hashes) || a.items[200].Error == "" || !a.done {
		t.Fatal("hash list round trip mismatch")
	}
	if _, err := encodeHashListsWithLimit([]PathHash{{Path: strings.Repeat("a", 2000)}}, 1024); !errors.Is(err, ErrProtocol) {
		t.Fatal("oversized hash entry accepted")
	}
}

func TestEncodeMsgLimit(t *testing.T) {
	if _, err := encodeMsg(message.TypeSyncNeed, PathList{Paths: []string{strings.Repeat("a", MaxBatchBytes)}}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v", err)
	}
	if _, err := encodeMsg(message.TypeSyncNeed, func() {}); err == nil {
		t.Fatal("unencodable payload accepted")
	}
	if (&Result{}).Partial() || !(&Result{Failures: []string{"x"}}).Partial() {
		t.Fatal("Partial wrong")
	}
}

// FuzzWireDecoders cho dữ liệu tùy ý vào mọi bộ giải mã và bộ ghép: không được panic,
// và manifest được chấp nhận luôn hợp lệ.
func FuzzWireDecoders(f *testing.F) {
	good, _ := encodeManifestWithLimit(bigManifest(5), 512)
	for _, m := range good {
		f.Add(string(m.Type), m.Bytes)
	}
	f.Add(string(message.TypeSyncNeed), []byte(`{"seq":0,"paths":["a"],"last":true}`))
	f.Add(string(message.TypeSyncHashResult), []byte(`{"seq":0,"hashes":[{"path":"a","hash":"AAAAAAAAAAA="}],"last":true}`))
	f.Fuzz(func(t *testing.T, typ string, payload []byte) {
		m := message.Message{Type: message.Type(typ), Bytes: payload}
		var a manifestAssembler
		_ = a.Start(m)
		_ = a.Batch(m)
		if man, err := a.End(m); err == nil {
			if verr := ValidateManifest(man); verr != nil {
				t.Fatalf("accepted invalid manifest: %v", verr)
			}
		}
		// Ghép theo luồng hợp lệ: start chuẩn rồi một lô tùy ý.
		var b manifestAssembler
		if b.Start(good[0]) == nil && b.Batch(message.Message{Type: message.TypeSyncManifestBatch, Bytes: payload}) == nil {
			for _, rest := range good[2:] {
				if rest.Type == message.TypeSyncManifestBatch {
					_ = b.Batch(rest)
				} else if man, err := b.End(rest); err == nil {
					if verr := ValidateManifest(man); verr != nil {
						t.Fatalf("accepted invalid manifest: %v", verr)
					}
				}
			}
		}
		var l PathList
		if decodeMsg(m, &l) == nil {
			var la listAssembler[string]
			_, _ = la.add(m.Type, l.Seq, l.Paths, l.Last)
		}
		var h HashList
		_ = decodeMsg(m, &h)
		var r Result
		_ = decodeMsg(m, &r)
		var hello Hello
		_ = decodeMsg(m, &hello)
		_ = json.Valid(payload)
	})
}
