package foldersync

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/schollz/croc/v11/src/message"
)

// ErrSourceChanged báo tệp nguồn đổi trong lúc đồng bộ; chạy lại lượt sẽ hội tụ.
var ErrSourceChanged = errors.New("foldersync: source files changed during sync")

// SourceEnv là phần I/O của máy nguồn mà Session cần.
type SourceEnv interface {
	// HashSource tính hash cho các đường dẫn đã có trong manifest nguồn.
	HashSource(paths []string) []PathHash
}

// Decision là quyết định của máy đích sau khi xem kế hoạch.
type Decision struct {
	Proceed        bool // false: dry-run hoặc người dùng từ chối; không tải và không ghi gì
	ConfirmDeletes bool // xác nhận riêng cho kế hoạch RequireDeleteConfirm
}

// DestEnv là phần I/O của máy đích mà Session cần.
type DestEnv interface {
	// ScanDestination quét thư mục đích với bộ lọc của nguồn.
	ScanDestination(filter FilterSpec) (*Manifest, error)
	// HashDestination điền Hash cho các đường dẫn đích được yêu cầu.
	HashDestination(dst *Manifest, paths []string) error
	// Decide hiển thị kế hoạch (dry-run, xác nhận) và trả quyết định.
	Decide(plan *Plan) (Decision, error)
	// Commit đưa các tệp đã nhận vào đích và áp dụng kế hoạch.
	Commit(plan *Plan, src *Manifest, d Decision) (*Result, error)
}

type sessionState int

const (
	stWaitHello sessionState = iota
	stWaitManifestStart
	stManifest
	stWaitHashes
	stServing   // nguồn: phục vụ yêu cầu hash và nhận need
	stWaitFiles // đích: đang chờ croc tải các tệp trong need
	stWaitResult
	stDone
	stFailed
)

// Session là state machine của giao thức sync-* cho một lượt. Nó không làm I/O mạng:
// Start/Handle/Finish trả về các bản tin cần gửi. Mọi lỗi giao thức đưa phiên vào
// trạng thái hỏng và mọi bản tin sau đó đều bị từ chối.
type Session struct {
	role    Role
	state   sessionState
	hello   Hello
	started bool

	// Nguồn
	local    *Manifest
	srcIndex map[string]*Entry
	srcEnv   SourceEnv
	hashReq  listAssembler[string]
	needList listAssembler[string]
	need     map[string]bool

	// Đích
	dstEnv     DestEnv
	opts       DiffOptions
	asm        manifestAssembler
	src, dst   *Manifest
	requested  map[string]bool
	hashResp   listAssembler[PathHash]
	afterHash  func() ([]message.Message, error)
	plan       *Plan
	decision   Decision
	needSizes  map[string]int64
	needOrder  []string
	accepted   map[string]bool
	result     *Result
	srcChanged []string
	compares   int
}

// NewSourceSession tạo phiên cho máy nguồn với manifest đã quét.
func NewSourceSession(local *Manifest, env SourceEnv, hello Hello) *Session {
	hello.Version, hello.Role = ProtocolVersion, RoleSource
	idx := make(map[string]*Entry, len(local.Entries))
	for i := range local.Entries {
		idx[local.Entries[i].Path] = &local.Entries[i]
	}
	return &Session{role: RoleSource, hello: hello, local: local, srcIndex: idx, srcEnv: env}
}

// NewDestSession tạo phiên cho máy đích.
func NewDestSession(env DestEnv, opts DiffOptions, hello Hello) *Session {
	hello.Version, hello.Role = ProtocolVersion, RoleDestination
	return &Session{role: RoleDestination, hello: hello, dstEnv: env, opts: opts}
}

// Role trả vai trò của phiên.
func (s *Session) Role() Role { return s.role }

// Start trả các bản tin gửi ngay khi kênh đã mã hóa.
func (s *Session) Start() ([]message.Message, error) {
	if s.started {
		return nil, s.fail(protoErr("session already started"))
	}
	s.started = true
	hello, err := encodeMsg(message.TypeSyncHello, s.hello)
	if err != nil {
		return nil, s.fail(err)
	}
	out := []message.Message{hello}
	if s.role == RoleSource {
		msgs, err := EncodeManifest(s.local)
		if err != nil {
			return nil, s.fail(err)
		}
		out = append(out, msgs...)
	}
	return out, nil
}

func (s *Session) fail(err error) error {
	s.state = stFailed
	return err
}

// Handle xử lý một bản tin sync-* từ phía bên kia.
func (s *Session) Handle(m message.Message) ([]message.Message, error) {
	if s.state == stFailed {
		return nil, protoErr("%s after the session failed", m.Type)
	}
	if s.state == stDone {
		return nil, s.fail(protoErr("%s after the session finished", m.Type))
	}
	if s.state == stWaitHello {
		if m.Type != message.TypeSyncHello {
			return nil, s.fail(protoErr("%s before sync-hello", m.Type))
		}
		var h Hello
		if err := decodeMsg(m, &h); err != nil {
			return nil, s.fail(err)
		}
		if h.Version != ProtocolVersion {
			return nil, s.fail(protoErr("peer speaks sync protocol %d, this croc speaks %d; update croc on both computers", h.Version, ProtocolVersion))
		}
		want := RoleDestination
		if s.role == RoleDestination {
			want = RoleSource
		}
		if h.Role != want {
			return nil, s.fail(protoErr("peer role %q, want %q (run croc sync <dir> on one computer and join from the other)", h.Role, want))
		}
		if s.role == RoleSource {
			s.state = stServing
		} else {
			s.state = stWaitManifestStart
		}
		return nil, nil
	}
	var out []message.Message
	var err error
	if s.role == RoleSource {
		out, err = s.handleSource(m)
	} else {
		out, err = s.handleDest(m)
	}
	if err != nil {
		return nil, s.fail(err)
	}
	return out, nil
}

func (s *Session) handleSource(m message.Message) ([]message.Message, error) {
	switch {
	case m.Type == message.TypeSyncHashRequest && s.state == stServing:
		var l PathList
		if err := decodeMsg(m, &l); err != nil {
			return nil, err
		}
		for _, p := range l.Paths {
			if e := s.srcIndex[p]; e == nil || !e.IsFile() {
				return nil, protoErr("hash requested for %q, which is not a file in the manifest", p)
			}
		}
		done, err := s.hashReq.add(m.Type, l.Seq, l.Paths, l.Last)
		if err != nil || !done {
			return nil, err
		}
		paths := s.hashReq.items
		if dup := firstDuplicate(paths); dup != "" {
			return nil, protoErr("hash requested twice for %q", dup)
		}
		s.hashReq.reset(0)
		hashes := s.srcEnv.HashSource(paths)
		return encodeHashLists(hashes)

	case m.Type == message.TypeSyncNeed && s.state == stServing:
		if s.hashReq.nextSeq != 0 {
			return nil, protoErr("sync-need while a hash request is open")
		}
		var l PathList
		if err := decodeMsg(m, &l); err != nil {
			return nil, err
		}
		for _, p := range l.Paths {
			if e := s.srcIndex[p]; e == nil || !e.IsFile() {
				return nil, protoErr("destination asked for %q, which is not a file in the manifest", p)
			}
		}
		done, err := s.needList.add(m.Type, l.Seq, l.Paths, l.Last)
		if err != nil || !done {
			return nil, err
		}
		if dup := firstDuplicate(s.needList.items); dup != "" {
			return nil, protoErr("destination asked twice for %q", dup)
		}
		s.need = make(map[string]bool, len(s.needList.items))
		for _, p := range s.needList.items {
			s.need[p] = true
		}
		s.state = stWaitResult
		return nil, nil

	case m.Type == message.TypeSyncResult && s.state == stWaitResult:
		var r Result
		if err := decodeMsg(m, &r); err != nil {
			return nil, err
		}
		s.result = &r
		s.state = stDone
		return nil, nil
	}
	return nil, protoErr("unexpected %s for the source in state %d", m.Type, s.state)
}

// Ready (nguồn) cho biết máy đích đã gửi xong danh sách need.
func (s *Session) Ready() (bool, map[string]bool) {
	if s.role != RoleSource || s.need == nil {
		return false, nil
	}
	return true, s.need
}

// Result trả kết quả lượt (nguồn: do đích gửi về; đích: kết quả commit).
func (s *Session) Result() *Result { return s.result }

// Plan (đích) trả kế hoạch đã tính, nil nếu chưa có.
func (s *Session) Plan() *Plan { return s.plan }

// Done cho biết phiên đã kết thúc bình thường.
func (s *Session) Done() bool { return s.state == stDone }

func (s *Session) handleDest(m message.Message) ([]message.Message, error) {
	switch {
	case m.Type == message.TypeSyncManifestStart && s.state == stWaitManifestStart:
		if err := s.asm.Start(m); err != nil {
			return nil, err
		}
		s.state = stManifest
		return nil, nil

	case m.Type == message.TypeSyncManifestBatch && s.state == stManifest:
		return nil, s.asm.Batch(m)

	case m.Type == message.TypeSyncManifestEnd && s.state == stManifest:
		src, err := s.asm.End(m)
		if err != nil {
			return nil, err
		}
		src.Sort()
		s.src = src
		dst, err := s.dstEnv.ScanDestination(src.Filter)
		if err != nil {
			return nil, err
		}
		s.dst = dst
		return s.compare()

	case m.Type == message.TypeSyncHashResult && s.state == stWaitHashes:
		var l HashList
		if err := decodeMsg(m, &l); err != nil {
			return nil, err
		}
		done, err := s.hashResp.add(m.Type, l.Seq, l.Hashes, l.Last)
		if err != nil || !done {
			return nil, err
		}
		if err := s.applyHashes(s.hashResp.items); err != nil {
			return nil, err
		}
		if len(s.srcChanged) > 0 {
			return nil, fmt.Errorf("%w: %s", ErrSourceChanged, strings.Join(s.srcChanged, ", "))
		}
		next := s.afterHash
		s.afterHash = nil
		return next()
	}
	return nil, protoErr("unexpected %s for the destination in state %d", m.Type, s.state)
}

// applyHashes kiểm luồng kết quả hash khớp đúng tập đã yêu cầu rồi điền vào manifest nguồn.
func (s *Session) applyHashes(items []PathHash) error {
	idx := make(map[string]*Entry, len(s.src.Entries))
	for i := range s.src.Entries {
		idx[s.src.Entries[i].Path] = &s.src.Entries[i]
	}
	seen := make(map[string]bool, len(items))
	for _, h := range items {
		if !s.requested[h.Path] {
			return protoErr("unrequested hash for %q", h.Path)
		}
		if seen[h.Path] {
			return protoErr("duplicate hash for %q", h.Path)
		}
		seen[h.Path] = true
		if h.Error != "" {
			s.srcChanged = append(s.srcChanged, h.Path)
			continue
		}
		if len(h.Hash) != HashSize {
			return protoErr("hash for %q has %d bytes", h.Path, len(h.Hash))
		}
		idx[h.Path].Hash = h.Hash
	}
	if len(seen) != len(s.requested) {
		return protoErr("got %d hashes, requested %d", len(seen), len(s.requested))
	}
	return nil
}

// requestHashes gửi yêu cầu hash cho paths và hẹn bước tiếp theo khi có kết quả.
func (s *Session) requestHashes(paths []string, next func() ([]message.Message, error)) ([]message.Message, error) {
	s.requested = make(map[string]bool, len(paths))
	for _, p := range paths {
		s.requested[p] = true
	}
	s.hashResp.reset(len(paths))
	s.afterHash = next
	s.state = stWaitHashes
	return encodePathLists(message.TypeSyncHashRequest, paths)
}

// compare chạy diff; nếu cần hash tầng 2 thì hash đích tại chỗ và hỏi nguồn.
func (s *Session) compare() ([]message.Message, error) {
	s.compares++
	if s.compares > 3 {
		return nil, errors.New("foldersync: hashes still missing after hashing; destination changed during sync")
	}
	plan, needHash, err := Compare(s.src, s.dst, s.opts)
	if err != nil {
		return nil, err
	}
	if len(needHash) > 0 {
		if err := s.dstEnv.HashDestination(s.dst, DstHashTargets(s.src, s.dst, needHash)); err != nil {
			return nil, err
		}
		var missing []string
		for _, p := range needHash {
			if e := findEntry(s.src, p); e != nil && len(e.Hash) == 0 {
				missing = append(missing, p)
			}
		}
		if len(missing) == 0 {
			return s.compare()
		}
		return s.requestHashes(missing, s.compare)
	}
	s.plan = plan
	return s.verifyTransfers()
}

// verifyTransfers bảo đảm mọi tệp sẽ tải đều có hash nguồn, để commit kiểm được nội dung.
func (s *Session) verifyTransfers() ([]message.Message, error) {
	var missing []string
	for _, c := range transferChanges(s.plan) {
		if len(c.Src.Hash) == 0 {
			missing = append(missing, c.Path)
		}
	}
	if len(missing) > 0 {
		return s.requestHashes(missing, s.verifyTransfers)
	}
	return s.decide()
}

// transferChanges trả các thay đổi cần tải dữ liệu tệp (Add/Update có nguồn là tệp).
func transferChanges(p *Plan) []Change {
	var out []Change
	for _, group := range [][]Change{p.Add, p.Update} {
		for _, c := range group {
			if c.Src != nil && c.Src.IsFile() {
				out = append(out, c)
			}
		}
	}
	return out
}

func (s *Session) decide() ([]message.Message, error) {
	d, err := s.dstEnv.Decide(s.plan)
	if err != nil {
		return nil, err
	}
	s.decision = d
	s.needSizes = map[string]int64{}
	s.needOrder = nil
	s.accepted = map[string]bool{}
	if d.Proceed {
		for _, c := range transferChanges(s.plan) {
			s.needSizes[c.Path] = c.Src.Size
			s.needOrder = append(s.needOrder, c.Path)
		}
	}
	s.state = stWaitFiles
	return encodePathLists(message.TypeSyncNeed, s.needOrder)
}

// AllowReceive (đích) chỉ chấp nhận mỗi tệp trong need đúng một lần với đúng kích thước.
func (s *Session) AllowReceive(path string, size int64, isDir bool) error {
	if s.role != RoleDestination || s.state != stWaitFiles {
		return protoErr("file %q offered outside the transfer phase", path)
	}
	want, ok := s.needSizes[path]
	switch {
	case isDir:
		return protoErr("source offered directory %q; directories are created by the destination", path)
	case !ok:
		return protoErr("source offered %q, which the destination did not request", path)
	case want != size:
		return protoErr("source offered %q with %d bytes, manifest says %d", path, size, want)
	case s.accepted[path]:
		return protoErr("source offered %q more than once", path)
	}
	s.accepted[path] = true
	return nil
}

// Need (đích) trả danh sách tệp đã yêu cầu.
func (s *Session) Need() []string { return slices.Clone(s.needOrder) }

// Finish (đích) chạy khi croc đã nhận xong mọi tệp: kiểm đủ tệp đã yêu cầu, commit và gửi sync-result.
func (s *Session) Finish() ([]message.Message, error) {
	if s.role != RoleDestination || s.state != stWaitFiles {
		return nil, s.fail(protoErr("finish in state %d", s.state))
	}
	for _, p := range s.needOrder {
		if !s.accepted[p] {
			return nil, s.fail(protoErr("source did not offer requested file %q", p))
		}
	}
	if len(s.accepted) != len(s.needSizes) {
		return nil, s.fail(protoErr("source offered %d files, requested %d", len(s.accepted), len(s.needSizes)))
	}
	r, err := s.dstEnv.Commit(s.plan, s.src, s.decision)
	if err != nil {
		return nil, s.fail(err)
	}
	s.result = r
	msg, err := encodeMsg(message.TypeSyncResult, r)
	if err != nil {
		return nil, s.fail(err)
	}
	s.state = stDone
	return []message.Message{msg}, nil
}

func findEntry(m *Manifest, p string) *Entry {
	i, ok := slices.BinarySearchFunc(m.Entries, p, func(e Entry, t string) int { return strings.Compare(e.Path, t) })
	if !ok {
		return nil
	}
	return &m.Entries[i]
}

func firstDuplicate(items []string) string {
	seen := make(map[string]bool, len(items))
	for _, p := range items {
		if seen[p] {
			return p
		}
		seen[p] = true
	}
	return ""
}
