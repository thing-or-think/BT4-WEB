package foldersync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/schollz/croc/v11/src/message"
)

const (
	// ProtocolVersion là phiên bản giao thức sync-* hiện tại.
	ProtocolVersion = 1

	// MaxBatchBytes giới hạn JSON của một bản tin theo lô (để Web xử lý được).
	MaxBatchBytes = 1 << 20

	// MaxListItems giới hạn tổng số đường dẫn trong một luồng hash/need.
	MaxListItems = MaxManifestEntries
)

// ErrProtocol báo bản tin sai vai trò, sai thứ tự, trùng, thiếu hoặc quá lớn.
// Phiên phải đóng khi gặp lỗi này.
var ErrProtocol = errors.New("foldersync: protocol violation")

func protoErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, args...))
}

// Role là vai trò của một bên trong phiên sync.
type Role string

const (
	RoleSource      Role = "source"
	RoleDestination Role = "destination"
)

// Hello là bản tin đầu tiên mỗi bên gửi sau khi kênh đã mã hóa.
type Hello struct {
	Version    int    `json:"version"`
	Role       Role   `json:"role"`
	Round      int    `json:"round,omitempty"` // Lượt watch (Giai đoạn 4); 0 cho lượt đơn
	Watch      bool   `json:"watch,omitempty"`
	RejoinCode string `json:"rejoin_code,omitempty"` // Mã kết nối lại cho phiên watch khi rớt mạng
}

// ManifestStart mở luồng manifest và khai báo trước kích thước.
type ManifestStart struct {
	Entries  int        `json:"entries"`
	Skipped  int        `json:"skipped"`
	Filter   FilterSpec `json:"filter,omitzero"`
	Complete bool       `json:"complete"`
}

// ManifestBatch là một lô manifest.
type ManifestBatch struct {
	Seq     int           `json:"seq"`
	Entries []Entry       `json:"entries,omitempty"`
	Skipped []SkippedPath `json:"skipped,omitempty"`
}

// ManifestEnd đóng luồng manifest.
type ManifestEnd struct {
	Batches int `json:"batches"`
}

// PathList là một lô của luồng đường dẫn (sync-hash-request, sync-need).
type PathList struct {
	Seq   int      `json:"seq"`
	Paths []string `json:"paths,omitempty"`
	Last  bool     `json:"last,omitempty"`
}

// PathHash là hash của một tệp nguồn, hoặc lỗi nếu không hash được.
type PathHash struct {
	Path  string `json:"path"`
	Hash  []byte `json:"hash,omitempty"`
	Error string `json:"error,omitempty"`
}

// HashList là một lô của luồng sync-hash-result.
type HashList struct {
	Seq    int        `json:"seq"`
	Hashes []PathHash `json:"hashes,omitempty"`
	Last   bool       `json:"last,omitempty"`
}

// Result là bản tổng kết máy đích gửi về sau khi commit.
type Result struct {
	DryRun        bool     `json:"dry_run,omitempty"`
	Added         int      `json:"added"`
	Updated       int      `json:"updated"`
	Touched       int      `json:"touched"`
	Deleted       int      `json:"deleted"`
	Unchanged     int      `json:"unchanged"`
	Conflicts     int      `json:"conflicts"`
	Bytes         int64    `json:"bytes"`
	Failures      []string `json:"failures,omitempty"`
	DeleteBlocked string   `json:"delete_blocked,omitempty"`
	DeletesHeld   int      `json:"deletes_held,omitempty"` // Xóa không được xác nhận nên bị giữ lại
}

// Partial cho biết lượt có mục thất bại (mã thoát 2 ở CLI).
func (r *Result) Partial() bool { return r != nil && len(r.Failures) > 0 }

// encodeMsg đóng gói payload JSON vào một bản tin sync-*.
func encodeMsg(t message.Type, v any) (message.Message, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return message.Message{}, err
	}
	if len(b) > MaxBatchBytes {
		return message.Message{}, fmt.Errorf("%w: %s payload %d bytes > %d", ErrProtocol, t, len(b), MaxBatchBytes)
	}
	return message.Message{Type: t, Bytes: b}, nil
}

// decodeMsg giải payload với giới hạn kích thước và từ chối trường lạ.
func decodeMsg(m message.Message, v any) error {
	if len(m.Bytes) > MaxBatchBytes {
		return protoErr("%s payload %d bytes > %d", m.Type, len(m.Bytes), MaxBatchBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(m.Bytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return protoErr("decode %s: %v", m.Type, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return protoErr("decode %s: trailing data", m.Type)
	}
	return nil
}

// batcher gom phần tử vào lô sao cho JSON của mỗi lô không vượt MaxBatchBytes.
type batcher struct {
	limit    int
	overhead int // ước lượng phần khung JSON của một lô
	size     int
	count    int
}

// fits kiểm phần tử có kích thước n còn vừa lô hiện tại không (n đã gồm dấu phẩy).
func (b *batcher) fits(n int) bool { return b.count == 0 || b.size+n+b.overhead <= b.limit }
func (b *batcher) add(n int)       { b.size += n + 1; b.count++ }
func (b *batcher) reset()          { b.size, b.count = 0, 0 }

const batchOverhead = 256

// EncodeManifest chia manifest thành start, các lô và end.
func EncodeManifest(m *Manifest) ([]message.Message, error) {
	return encodeManifestWithLimit(m, MaxBatchBytes)
}

func encodeManifestWithLimit(m *Manifest, limit int) ([]message.Message, error) {
	if len(m.Entries)+len(m.Skipped) > MaxManifestEntries {
		return nil, fmt.Errorf("%w: %d items", ErrManifestTooLarge, len(m.Entries)+len(m.Skipped))
	}
	start, err := encodeMsg(message.TypeSyncManifestStart, ManifestStart{
		Entries: len(m.Entries), Skipped: len(m.Skipped), Filter: m.Filter, Complete: m.Complete,
	})
	if err != nil {
		return nil, err
	}
	out := []message.Message{start}
	b := batcher{limit: limit, overhead: batchOverhead}
	cur := ManifestBatch{}
	flush := func() error {
		if b.count == 0 {
			return nil
		}
		msg, err := encodeMsg(message.TypeSyncManifestBatch, cur)
		if err != nil {
			return err
		}
		out = append(out, msg)
		cur = ManifestBatch{Seq: cur.Seq + 1}
		b.reset()
		return nil
	}
	total := 0
	for _, e := range m.Entries {
		raw, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		if len(raw)+batchOverhead > limit {
			return nil, fmt.Errorf("%w: entry %q too large", ErrProtocol, e.Path)
		}
		if !b.fits(len(raw)) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		b.add(len(raw))
		total += len(raw)
		cur.Entries = append(cur.Entries, e)
	}
	for _, s := range m.Skipped {
		raw, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		if len(raw)+batchOverhead > limit {
			return nil, fmt.Errorf("%w: skipped %q too large", ErrProtocol, s.Path)
		}
		if !b.fits(len(raw)) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		b.add(len(raw))
		total += len(raw)
		cur.Skipped = append(cur.Skipped, s)
	}
	if total > MaxManifestJSONBytes {
		return nil, fmt.Errorf("%w: %d JSON bytes", ErrManifestTooLarge, total)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	end, err := encodeMsg(message.TypeSyncManifestEnd, ManifestEnd{Batches: cur.Seq})
	if err != nil {
		return nil, err
	}
	return append(out, end), nil
}

// manifestAssembler ghép manifest từ các lô nhận được và kiểm tính toàn vẹn luồng.
type manifestAssembler struct {
	started, ended bool
	start          ManifestStart
	nextSeq        int
	jsonBytes      int
	m              Manifest
}

func (a *manifestAssembler) Start(m message.Message) error {
	if a.started {
		return protoErr("duplicate manifest start")
	}
	if err := decodeMsg(m, &a.start); err != nil {
		return err
	}
	s := a.start
	if s.Entries < 0 || s.Skipped < 0 || s.Entries > MaxManifestEntries || s.Skipped > MaxManifestEntries-s.Entries {
		return protoErr("manifest declares %d entries + %d skipped", s.Entries, s.Skipped)
	}
	a.started = true
	a.m = Manifest{Filter: s.Filter, Complete: s.Complete}
	return nil
}

func (a *manifestAssembler) Batch(m message.Message) error {
	if !a.started || a.ended {
		return protoErr("manifest batch outside a manifest stream")
	}
	var b ManifestBatch
	if err := decodeMsg(m, &b); err != nil {
		return err
	}
	if b.Seq != a.nextSeq {
		return protoErr("manifest batch seq %d, want %d", b.Seq, a.nextSeq)
	}
	if len(b.Entries)+len(b.Skipped) == 0 {
		return protoErr("empty manifest batch %d", b.Seq)
	}
	if len(a.m.Entries)+len(b.Entries) > a.start.Entries || len(a.m.Skipped)+len(b.Skipped) > a.start.Skipped {
		return protoErr("manifest batch %d exceeds declared size", b.Seq)
	}
	a.jsonBytes += len(m.Bytes)
	if a.jsonBytes > MaxManifestJSONBytes {
		return protoErr("manifest exceeds %d JSON bytes", MaxManifestJSONBytes)
	}
	a.nextSeq++
	a.m.Entries = append(a.m.Entries, b.Entries...)
	a.m.Skipped = append(a.m.Skipped, b.Skipped...)
	return nil
}

// End đóng luồng, kiểm đủ số lô và số mục, rồi xác thực toàn bộ manifest.
func (a *manifestAssembler) End(m message.Message) (*Manifest, error) {
	if !a.started || a.ended {
		return nil, protoErr("manifest end outside a manifest stream")
	}
	var e ManifestEnd
	if err := decodeMsg(m, &e); err != nil {
		return nil, err
	}
	if e.Batches != a.nextSeq {
		return nil, protoErr("manifest end declares %d batches, got %d", e.Batches, a.nextSeq)
	}
	if len(a.m.Entries) != a.start.Entries || len(a.m.Skipped) != a.start.Skipped {
		return nil, protoErr("manifest incomplete: %d/%d entries, %d/%d skipped",
			len(a.m.Entries), a.start.Entries, len(a.m.Skipped), a.start.Skipped)
	}
	a.ended = true
	if err := ValidateManifest(&a.m); err != nil {
		return nil, err
	}
	return &a.m, nil
}

// encodePathLists chia danh sách đường dẫn thành các lô; luôn có ít nhất một lô (Last).
func encodePathLists(t message.Type, paths []string) ([]message.Message, error) {
	return encodePathListsWithLimit(t, paths, MaxBatchBytes)
}

func encodePathListsWithLimit(t message.Type, paths []string, limit int) ([]message.Message, error) {
	var out []message.Message
	b := batcher{limit: limit, overhead: batchOverhead}
	cur := PathList{}
	for _, p := range paths {
		raw, _ := json.Marshal(p)
		if len(raw)+batchOverhead > limit {
			return nil, fmt.Errorf("%w: path %q too large", ErrProtocol, p)
		}
		if !b.fits(len(raw)) {
			msg, err := encodeMsg(t, cur)
			if err != nil {
				return nil, err
			}
			out = append(out, msg)
			cur = PathList{Seq: cur.Seq + 1}
			b.reset()
		}
		b.add(len(raw))
		cur.Paths = append(cur.Paths, p)
	}
	cur.Last = true
	msg, err := encodeMsg(t, cur)
	if err != nil {
		return nil, err
	}
	return append(out, msg), nil
}

func encodeHashLists(hashes []PathHash) ([]message.Message, error) {
	return encodeHashListsWithLimit(hashes, MaxBatchBytes)
}

func encodeHashListsWithLimit(hashes []PathHash, limit int) ([]message.Message, error) {
	var out []message.Message
	b := batcher{limit: limit, overhead: batchOverhead}
	cur := HashList{}
	for _, h := range hashes {
		raw, _ := json.Marshal(h)
		if len(raw)+batchOverhead > limit {
			return nil, fmt.Errorf("%w: hash entry %q too large", ErrProtocol, h.Path)
		}
		if !b.fits(len(raw)) {
			msg, err := encodeMsg(message.TypeSyncHashResult, cur)
			if err != nil {
				return nil, err
			}
			out = append(out, msg)
			cur = HashList{Seq: cur.Seq + 1}
			b.reset()
		}
		b.add(len(raw))
		cur.Hashes = append(cur.Hashes, h)
	}
	cur.Last = true
	msg, err := encodeMsg(message.TypeSyncHashResult, cur)
	if err != nil {
		return nil, err
	}
	return append(out, msg), nil
}

// listAssembler ghép một luồng lô có Seq tăng dần và kết thúc bằng Last.
type listAssembler[T any] struct {
	nextSeq int
	done    bool
	items   []T
	limit   int
}

// add nhận lô seq; trả true khi luồng đã kết thúc.
func (a *listAssembler[T]) add(kind message.Type, seq int, items []T, last bool) (bool, error) {
	if a.done {
		return false, protoErr("%s after the stream ended", kind)
	}
	if seq != a.nextSeq {
		return false, protoErr("%s seq %d, want %d", kind, seq, a.nextSeq)
	}
	limit := a.limit
	if limit <= 0 {
		limit = MaxListItems
	}
	if len(a.items)+len(items) > limit {
		return false, protoErr("%s exceeds %d items", kind, limit)
	}
	if len(items) == 0 && !last {
		return false, protoErr("empty non-final %s batch", kind)
	}
	a.nextSeq++
	a.items = append(a.items, items...)
	a.done = last
	return last, nil
}

func (a *listAssembler[T]) reset(limit int) {
	*a = listAssembler[T]{limit: limit}
}
