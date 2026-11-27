package foldersync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/schollz/croc/v11/src/croc"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/receivefs"
)

// EventKind phân loại sự kiện gửi cho Reporter.
type EventKind string

const (
	EventScanned   EventKind = "scanned"   // đã quét xong thư mục cục bộ
	EventPlan      EventKind = "plan"      // đích đã có kế hoạch (trước Decide)
	EventCommitted EventKind = "committed" // đích đã commit xong
	EventResult    EventKind = "result"    // nguồn nhận được sync-result
	EventWarning   EventKind = "warning"   // mục bị bỏ qua, staging mồ côi đã dọn...
)

// Event là một mốc tiến trình của lượt sync.
type Event struct {
	Kind     EventKind
	Message  string
	Manifest *Manifest
	Plan     *Plan
	Result   *Result
}

// Reporter nhận sự kiện tiến trình (CLI in ra màn hình, Web cập nhật giao diện).
type Reporter interface{ Report(Event) }

// ReporterFunc biến một hàm thành Reporter.
type ReporterFunc func(Event)

// Report gọi f(e).
func (f ReporterFunc) Report(e Event) { f(e) }

func report(r Reporter, e Event) {
	if r != nil {
		r.Report(e)
	}
}

// HostOptions cấu hình máy nguồn (croc sync <dir>).
type HostOptions struct {
	Dir      string
	Filter   FilterSpec
	Croc     croc.Options // relay, mã, transport...; IsSender và SyncHook do RunSyncHost đặt
	Reporter Reporter
}

// JoinOptions cấu hình máy đích.
type JoinOptions struct {
	Dir  string
	Diff DiffOptions
	Croc croc.Options // relay, mã...; IsSender, ReceiveRoot, SyncHook do RunSyncJoin đặt
	// Decide xem kế hoạch và trả quyết định; nil nghĩa là tiến hành, không xác nhận xóa
	// quá 50% (các xóa đó bị giữ lại và báo trong Result.DeletesHeld).
	Decide   func(*Plan) (Decision, error)
	Reporter Reporter
}

// newClient là điểm tạo croc client.
var newClient = croc.NewCtx

// hostTest cho phép test đóng vai nguồn độc hại hoặc tiêm thay đổi giữa các bước.
var hostTest struct {
	afterScan func(m *Manifest)
	env       func(SourceEnv) SourceEnv
	hook      func(*crocHook) croc.SyncHook
}

// RunSyncHost quét Dir, mở phiên croc với vai trò nguồn và phục vụ một lượt đồng bộ.
// Trả kết quả máy đích gửi về. Hủy ctx sẽ dừng phiên và mọi goroutine của nó.
func RunSyncHost(ctx context.Context, o HostOptions) (*Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	dir, err := filepath.Abs(o.Dir)
	if err != nil {
		return nil, err
	}
	filter := NewFilter(o.Filter)
	m, err := ScanDir(ctx, dir, ScanOptions{Filter: filter})
	if err != nil {
		return nil, err
	}
	if hostTest.afterScan != nil {
		hostTest.afterScan(m)
	}
	report(o.Reporter, Event{Kind: EventScanned, Manifest: m})
	for _, s := range m.Skipped {
		if s.Reason != SkipFiltered {
			report(o.Reporter, Event{Kind: EventWarning, Message: "skipped " + s.String()})
		}
	}

	var env SourceEnv = &hostEnv{ctx: ctx, dir: dir, m: m}
	if hostTest.env != nil {
		env = hostTest.env(env)
	}
	session := NewSourceSession(m, env, Hello{})
	ops := o.Croc
	ops.IsSender = true
	var hook croc.SyncHook = &crocHook{s: session}
	if hostTest.hook != nil {
		hook = hostTest.hook(&crocHook{s: session})
	}
	ops.SyncHook = hook
	client, err := newClient(ctx, ops)
	if err != nil {
		return nil, err
	}
	files, folders := sourceFileInfos(dir, m)
	sendErr := client.Send(files, nil, folders)
	if sendErr != nil {
		return session.Result(), sendErr
	}
	r := session.Result()
	if r == nil {
		return nil, errors.New("foldersync: destination finished without reporting a result")
	}
	report(o.Reporter, Event{Kind: EventResult, Result: r})
	return r, nil
}

// sourceFileInfos đổi manifest thành danh sách croc gửi. Hook chỉ giữ tệp đích cần.
func sourceFileInfos(dir string, m *Manifest) ([]croc.FileInfo, int) {
	var files []croc.FileInfo
	folders := 0
	for _, e := range m.Entries {
		if e.IsDir() {
			folders++
			continue
		}
		remote := path.Dir(e.Path)
		if remote == "." {
			remote = "./"
		} else {
			remote += "/"
		}
		wireBase := path.Base(e.Path)
		sourceName := ""
		if diskBase := filepath.Base(e.DiskPath); diskBase != wireBase {
			sourceName = diskBase
		}
		files = append(files, croc.FileInfo{
			Name:         wireBase,
			SourceName:   sourceName,
			FolderRemote: remote,
			FolderSource: filepath.Join(dir, filepath.Dir(e.DiskPath)),
			Size:         e.Size,
			ModTime:      e.ModTime,
			Mode:         e.Mode,
		})
	}
	return files, folders
}

type hostEnv struct {
	ctx context.Context
	dir string
	m   *Manifest
}

// HashSource hash từng tệp riêng để báo lỗi theo đường dẫn (tệp đổi sau khi quét).
func (h *hostEnv) HashSource(paths []string) []PathHash {
	out := make([]PathHash, len(paths))
	for i, p := range paths {
		out[i].Path = p
		if err := HashEntries(h.ctx, h.dir, h.m, []string{p}); err != nil {
			out[i].Error = err.Error()
			continue
		}
		out[i].Hash = findEntry(h.m, p).Hash
	}
	return out
}

// RunSyncJoin khóa Dir, nhận một lượt đồng bộ vào staging và commit theo kế hoạch.
func RunSyncJoin(ctx context.Context, o JoinOptions) (*Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	dir, err := filepath.Abs(o.Dir)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("%w: %s", ErrNotDirectory, dir)
	}
	lock, err := AcquireLock(dir)
	if err != nil {
		return nil, err
	}
	defer lock.Release()
	if removed, err := CleanOrphanStaging(dir); err != nil {
		report(o.Reporter, Event{Kind: EventWarning, Message: "could not remove old staging: " + err.Error()})
	} else if len(removed) > 0 {
		report(o.Reporter, Event{Kind: EventWarning, Message: fmt.Sprintf("removed %d interrupted staging folder(s)", len(removed))})
	}
	staging, err := NewStaging(dir)
	if err != nil {
		return nil, err
	}
	defer staging.Remove()
	root, err := receivefs.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()

	env := &joinEnv{ctx: ctx, dir: dir, root: root, staging: staging, o: o}
	session := NewDestSession(env, o.Diff, Hello{})
	ops := o.Croc
	ops.IsSender = false
	ops.ReceiveRoot = staging.Dir()
	ops.NoPrompt = true
	ops.Overwrite = true
	ops.Stdout = false
	ops.SyncHook = &crocHook{s: session}
	client, err := newClient(ctx, ops)
	if err != nil {
		return nil, err
	}
	if err := client.Receive(); err != nil {
		return session.Result(), err
	}
	r := session.Result()
	if r == nil {
		return nil, errors.New("foldersync: transfer ended before the sync round finished")
	}
	return r, nil
}

type joinEnv struct {
	ctx     context.Context
	dir     string
	root    *receivefs.Root
	staging *Staging
	o       JoinOptions
}

func (j *joinEnv) ScanDestination(filter FilterSpec) (*Manifest, error) {
	return ScanDir(j.ctx, j.dir, ScanOptions{Filter: NewFilter(filter)})
}

func (j *joinEnv) HashDestination(dst *Manifest, paths []string) error {
	return HashEntries(j.ctx, j.dir, dst, paths)
}

func (j *joinEnv) Decide(p *Plan) (Decision, error) {
	report(j.o.Reporter, Event{Kind: EventPlan, Plan: p})
	if j.o.Decide == nil {
		return Decision{Proceed: true}, nil
	}
	return j.o.Decide(p)
}

func (j *joinEnv) Commit(p *Plan, _ *Manifest, d Decision) (*Result, error) {
	r, err := Commit(j.ctx, j.root, p, CommitOptions{Staging: j.staging.Name, Decision: d})
	if err != nil {
		return nil, err
	}
	report(j.o.Reporter, Event{Kind: EventCommitted, Result: r, Plan: p})
	return r, nil
}

// crocHook nối Session vào croc.SyncHook (và SyncReceiveGuard, SyncFinisher ở đích).
type crocHook struct{ s *Session }

var (
	_ croc.SyncHook         = (*crocHook)(nil)
	_ croc.SyncReceiveGuard = (*crocHook)(nil)
	_ croc.SyncFinisher     = (*crocHook)(nil)
)

func (h *crocHook) Feature() string { return message.FeatureFolderSync }

func sendAll(send func(message.Message) error, msgs []message.Message, err error) error {
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if err := send(m); err != nil {
			return err
		}
	}
	return nil
}

func (h *crocHook) Start(send func(message.Message) error) error {
	msgs, err := h.s.Start()
	return sendAll(send, msgs, err)
}

func (h *crocHook) Handle(m message.Message, send func(message.Message) error) error {
	msgs, err := h.s.Handle(m)
	return sendAll(send, msgs, err)
}

func (h *crocHook) Ready() (bool, func(croc.FileInfo) bool) {
	ready, need := h.s.Ready()
	if !ready {
		return false, nil
	}
	return true, func(fi croc.FileInfo) bool {
		if fi.Name == "" {
			return false // thư mục rỗng: đích tự tạo
		}
		return need[path.Join(fi.FolderRemote, fi.Name)]
	}
}

func (h *crocHook) AllowReceive(p string, size int64, isDir bool) error {
	return h.s.AllowReceive(p, size, isDir)
}

func (h *crocHook) BeforeFinish(send func(message.Message) error) error {
	msgs, err := h.s.Finish()
	return sendAll(send, msgs, err)
}
