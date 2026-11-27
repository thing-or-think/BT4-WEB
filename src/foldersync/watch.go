package foldersync

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/schollz/croc/v11/src/codephrase"
	"github.com/schollz/croc/v11/src/croc"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/receivefs"
)

// Các khoảng thời gian của chế độ watch; là biến gói để test rút ngắn.
var (
	watchKeepaliveInterval = 20 * time.Second
	watchReconnectInitial  = 1 * time.Second
	watchReconnectMax      = 30 * time.Second
)

// nextWatchBackoff tính thời gian chờ thử kết nối lại theo cấp số nhân (1s, 2s, 4s... tối đa 30s).
func nextWatchBackoff(cur time.Duration) time.Duration {
	initial := watchReconnectInitial
	if initial <= 0 {
		initial = time.Second
	}
	maxDelay := watchReconnectMax
	if maxDelay < initial {
		maxDelay = initial
	}
	if cur <= 0 {
		return initial
	}
	if cur >= maxDelay {
		return maxDelay
	}
	next := cur * 2
	if next <= 0 || next > maxDelay {
		return maxDelay
	}
	return next
}

// waitWatchBackoff ngủ d hoặc dừng ngay nếu ctx bị hủy; trả false nếu ctx đã hủy.
func waitWatchBackoff(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// peerErrorPrefix là tiền tố mà src/croc/croc.go gắn khi đối phương gửi bản tin TypeError.
const peerErrorPrefix = "peer error:"

// isPeerError kiểm tra lỗi có xuất phát từ bản tin lỗi tường minh (TypeError) của đối phương hay không.
func isPeerError(err error) bool {
	if err == nil {
		return false
	}
	if strings.HasPrefix(err.Error(), peerErrorPrefix) {
		return true
	}
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		for _, sub := range multi.Unwrap() {
			if isPeerError(sub) {
				return true
			}
		}
		return false
	}
	if inner := errors.Unwrap(err); inner != nil {
		return isPeerError(inner)
	}
	return false
}

// isFatalWatchError phân biệt lỗi không phải mạng (vi phạm giao thức, lỗi từ chối từ đối phương,
// khóa đích, thư mục không hợp lệ, thiếu hỗ trợ sync) cần kết thúc watch ngay thay vì nối lại.
func isFatalWatchError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrProtocol) ||
		errors.Is(err, ErrDestinationLocked) ||
		errors.Is(err, ErrNotDirectory) ||
		errors.Is(err, ErrManifestTooLarge) ||
		errors.Is(err, ErrInvalidEntry) ||
		errors.Is(err, ErrUnsupportedKind) ||
		errors.Is(err, receivefs.ErrUnsafePath) ||
		errors.Is(err, croc.ErrPeerLacksSync) ||
		isPeerError(err)
}

// clearStagingContents dọn sạch tệp và thư mục con trong staging trước mỗi lượt mới,
// nhưng giữ lại chính thư mục staging và tệp khóa .lock vì croc giữ mở ReceiveRoot.
func clearStagingContents(s *Staging) error {
	if s == nil {
		return nil
	}
	r, err := os.OpenRoot(s.Dir())
	if err != nil {
		return err
	}
	defer r.Close()
	entries, err := os.ReadDir(s.Dir())
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		if e.Name() == stagingLockName {
			continue
		}
		if err := r.RemoveAll(e.Name()); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// scanHostDir quét thư mục nguồn với bản sao bộ lọc mới và báo cáo sự kiện quét/cảnh báo.
func scanHostDir(ctx context.Context, dir string, filterSpec FilterSpec, reporter Reporter) (*Manifest, error) {
	m, err := ScanDir(ctx, dir, ScanOptions{Filter: NewFilter(filterSpec)})
	if err != nil {
		return nil, err
	}
	report(reporter, Event{Kind: EventScanned, Manifest: m})
	for _, s := range m.Skipped {
		if s.Reason != SkipFiltered {
			report(reporter, Event{Kind: EventWarning, Message: "skipped " + s.String()})
		}
	}
	return m, nil
}

// sourceWatchState giữ trạng thái của máy nguồn xuyên suốt các lần nối lại phiên croc.
type sourceWatchState struct {
	round       int
	established bool
	rejoinCode  string
	lastResult  *Result
}

// runSyncHostWatch chạy chế độ theo dõi liên tục trên máy nguồn qua một phiên croc dài,
// tự kết nối lại bằng RejoinCode khi rớt mạng cho tới khi ctx bị hủy hoặc gặp lỗi nghiêm trọng.
func runSyncHostWatch(ctx context.Context, o HostOptions) (*Result, error) {
	dir, err := filepath.Abs(o.Dir)
	if err != nil {
		return nil, err
	}
	// Khởi tạo Watcher trước lượt quét đầu để không bỏ sót chỉnh sửa xảy ra lúc đang quét.
	watchOpts := o.WatchOptions
	if watchOpts.Filter == nil {
		watchOpts.Filter = NewFilter(o.Filter)
	}
	watcher, err := NewWatcher(ctx, dir, watchOpts)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, err
	}
	defer watcher.Close()

	initialManifest, err := scanHostDir(ctx, dir, o.Filter, o.Reporter)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil
		}
		return nil, err
	}

	var state sourceWatchState
	var backoff time.Duration
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return state.lastResult, nil
		}
		var m *Manifest
		if attempt == 0 {
			m = initialManifest
		} else {
			// Xả tín hiệu watcher đã đệm trước khi quét lại toàn bộ để tránh chạy dư một lượt rỗng.
			select {
			case <-watcher.Changes():
			default:
			}
			m, err = scanHostDir(ctx, dir, o.Filter, o.Reporter)
			if err != nil {
				if ctx.Err() != nil {
					return state.lastResult, nil
				}
				return state.lastResult, err
			}
		}

		rejoinCode, err := codephrase.Generate()
		if err != nil {
			return state.lastResult, err
		}
		roundNum := state.round + 1
		env := &hostEnv{ctx: ctx, dir: dir, m: m}
		session := NewSourceSession(m, env, Hello{
			Round:      roundNum,
			Watch:      true,
			RejoinCode: rejoinCode,
		})
		hook := &sourceWatchHook{
			ctx:               ctx,
			dir:               dir,
			filter:            o.Filter,
			reporter:          o.Reporter,
			reconnecting:      attempt > 0,
			state:             &state,
			wakeCh:            make(chan struct{}, 1),
			pumpDone:          make(chan struct{}),
			s:                 session,
			manifest:          m,
			roundNum:          roundNum,
			roundActive:       true,
			pendingRejoinCode: rejoinCode,
		}

		ops := o.Croc
		ops.RelayPorts = slices.Clone(o.Croc.RelayPorts)
		ops.IsSender = true
		ops.SyncHook = hook
		if attempt > 0 {
			// Không bao giờ dùng lại mã ban đầu để kết nối lại (tránh bị đoán mã trực tuyến).
			ops.SharedSecret = state.rejoinCode
			ops.RoomName = ""
		}

		attemptCtx, cancelAttempt := context.WithCancel(ctx)
		client, err := newClient(attemptCtx, ops)
		if err != nil {
			cancelAttempt()
			if ctx.Err() != nil {
				return state.lastResult, nil
			}
			return state.lastResult, err
		}

		pumpCtx, cancelPump := context.WithCancel(attemptCtx)
		go hook.runWakePump(pumpCtx, watcher.Changes())

		files, folders := sourceFileInfos(dir, m)
		sendErr := client.Send(files, nil, folders)
		cancelPump()
		<-hook.pumpDone
		cancelAttempt()

		if ctx.Err() != nil {
			return state.lastResult, nil
		}
		if hook.fatalErr != nil {
			return state.lastResult, hook.fatalErr
		}
		if sendErr == nil {
			return state.lastResult, nil
		}
		// Khi croc nhận TypeError trước pha truyền tệp, c.stop.Cancel() có thể khiến goroutine
		// chuẩn bị tệp trả context.Canceled trước errchan dù ctx của người gọi chưa bị hủy.
		if isFatalWatchError(sendErr) || errors.Is(sendErr, context.Canceled) || !state.established || state.rejoinCode == "" {
			return state.lastResult, sendErr
		}
		if hook.reconnectedSent {
			backoff = 0
		}
		backoff = nextWatchBackoff(backoff)
		report(o.Reporter, Event{
			Kind:    EventReconnecting,
			Message: fmt.Sprintf("reconnecting in %s after error: %v", backoff, sendErr),
		})
		if !waitWatchBackoff(ctx, backoff) {
			return state.lastResult, nil
		}
	}
}

// sourceWatchHook nối vòng đời nhiều lượt watch của máy nguồn vào croc.SyncRounds.
type sourceWatchHook struct {
	ctx          context.Context
	dir          string
	filter       FilterSpec
	reporter     Reporter
	reconnecting bool

	state *sourceWatchState

	// Trạng thái chung giữa goroutine bơm sự kiện và goroutine điều khiển croc.
	mu            sync.Mutex
	changePending bool
	pingPending   bool
	wakeCh        chan struct{}
	pumpDone      chan struct{}

	// Chỉ truy cập trên goroutine điều khiển của croc trong suốt phiên.
	s                 *Session
	manifest          *Manifest
	roundNum          int
	roundActive       bool
	pendingRejoinCode string
	reconnectedSent   bool
	fatalErr          error
}

var (
	_ croc.SyncHook   = (*sourceWatchHook)(nil)
	_ croc.SyncRounds = (*sourceWatchHook)(nil)
)

func (h *sourceWatchHook) signalWake() {
	select {
	case h.wakeCh <- struct{}{}:
	default:
	}
}

// runWakePump chuyển tiếp tín hiệu thay đổi từ Watcher và nhịp keepalive vào Wake().
func (h *sourceWatchHook) runWakePump(sessionCtx context.Context, changes <-chan struct{}) {
	defer close(h.pumpDone)
	var ticker *time.Ticker
	var tickCh <-chan time.Time
	if interval := watchKeepaliveInterval; interval > 0 {
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
		tickCh = ticker.C
	}
	for {
		select {
		case <-sessionCtx.Done():
			return
		case _, ok := <-changes:
			if !ok {
				changes = nil
				continue
			}
			h.mu.Lock()
			h.changePending = true
			h.mu.Unlock()
			h.signalWake()
		case <-tickCh:
			h.mu.Lock()
			h.pingPending = true
			h.mu.Unlock()
			h.signalWake()
		}
	}
}

func (h *sourceWatchHook) Feature() string { return message.FeatureFolderSync }

func (h *sourceWatchHook) Start(send func(message.Message) error) error {
	h.state.round = h.roundNum
	if h.reconnecting && !h.reconnectedSent {
		h.reconnectedSent = true
		report(h.reporter, Event{Kind: EventReconnected})
	}
	report(h.reporter, Event{Kind: EventRoundStart, Message: fmt.Sprintf("round %d", h.roundNum)})
	msgs, err := h.s.Start()
	if err != nil && isFatalWatchError(err) {
		h.fatalErr = err
	}
	return sendAll(send, msgs, err)
}

// handleWatchKeepalive xử lý sync-ping/sync-pong độc lập với trạng thái Session hiện tại
// để nhịp giữ kết nối luôn được chấp nhận ở mọi trạng thái và không chạm vào state machine.
func handleWatchKeepalive(m message.Message, send func(message.Message) error) (bool, error) {
	switch m.Type {
	case message.TypeSyncPing:
		if len(m.Bytes) > MaxBatchBytes {
			return true, protoErr("%s payload %d bytes > %d", m.Type, len(m.Bytes), MaxBatchBytes)
		}
		return true, send(message.Message{Type: message.TypeSyncPong})
	case message.TypeSyncPong:
		if len(m.Bytes) > MaxBatchBytes {
			return true, protoErr("%s payload %d bytes > %d", m.Type, len(m.Bytes), MaxBatchBytes)
		}
		return true, nil
	default:
		return false, nil
	}
}

func (h *sourceWatchHook) Handle(m message.Message, send func(message.Message) error) error {
	if handled, err := handleWatchKeepalive(m, send); handled {
		if err != nil && isFatalWatchError(err) {
			h.fatalErr = err
		}
		return err
	}
	if h.s == nil {
		err := protoErr("unexpected %s with no active sync session", m.Type)
		h.fatalErr = err
		return err
	}
	msgs, err := h.s.Handle(m)
	if err != nil {
		if isFatalWatchError(err) {
			h.fatalErr = err
		}
		return err
	}
	switch m.Type {
	case message.TypeSyncHello:
		if !h.state.established {
			// Lượt đầu đã qua PAKE và trao đổi Hello: bật trạng thái nối lại với RejoinCode lượt 1.
			h.state.established = true
			h.state.rejoinCode = h.pendingRejoinCode
		} else if h.roundNum > 1 && h.s.PeerHello().Round == h.roundNum {
			// Ở lượt kế tiếp trên cùng phiên, đích chỉ gửi Hello mang số lượt này sau khi đã nhận Hello (và RejoinCode) mới.
			h.state.rejoinCode = h.pendingRejoinCode
		}
	case message.TypeSyncHashRequest, message.TypeSyncNeed:
		// Đích đã xử lý xong Hello + manifest của lượt này, chắc chắn đã lưu pendingRejoinCode.
		h.state.established = true
		h.state.rejoinCode = h.pendingRejoinCode
	case message.TypeSyncResult:
		h.state.established = true
		h.state.rejoinCode = h.pendingRejoinCode
		r := h.s.Result()
		h.state.lastResult = r
		report(h.reporter, Event{Kind: EventResult, Result: r})
	}
	return sendAll(send, msgs, nil)
}

func (h *sourceWatchHook) Ready() (bool, func(croc.FileInfo) bool) {
	if !h.roundActive || h.s == nil {
		return false, nil
	}
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

func (h *sourceWatchHook) Wake() <-chan struct{} {
	return h.wakeCh
}

func (h *sourceWatchHook) OnWake(send func(message.Message) error) error {
	if h.ctx.Err() != nil {
		return h.ctx.Err()
	}
	h.mu.Lock()
	wantChange := h.changePending
	wantPing := h.pingPending
	h.pingPending = false
	if !h.roundActive {
		h.changePending = false
	}
	h.mu.Unlock()

	if wantPing {
		if err := send(message.Message{Type: message.TypeSyncPing}); err != nil {
			return err
		}
	}
	if !wantChange || h.roundActive {
		// Lượt đang chạy: giữ changePending=true (gộp thay đổi) để chạy ngay sau khi lượt xong.
		return nil
	}
	return h.startNextRound(send)
}

func (h *sourceWatchHook) startNextRound(send func(message.Message) error) error {
	m, err := scanHostDir(h.ctx, h.dir, h.filter, h.reporter)
	if err != nil {
		if h.ctx.Err() == nil {
			h.fatalErr = err
		}
		return err
	}
	rejoinCode, err := codephrase.Generate()
	if err != nil {
		return err
	}
	h.state.round++
	h.roundNum = h.state.round
	h.manifest = m
	h.pendingRejoinCode = rejoinCode
	h.roundActive = true
	env := &hostEnv{ctx: h.ctx, dir: h.dir, m: m}
	h.s = NewSourceSession(m, env, Hello{
		Round:      h.roundNum,
		Watch:      true,
		RejoinCode: rejoinCode,
	})
	report(h.reporter, Event{Kind: EventRoundStart, Message: fmt.Sprintf("round %d", h.roundNum)})
	msgs, err := h.s.Start()
	if err != nil && isFatalWatchError(err) {
		h.fatalErr = err
	}
	return sendAll(send, msgs, err)
}

func (h *sourceWatchHook) KeepOpen() bool {
	if h.s == nil || !h.s.Done() {
		h.fatalErr = protoErr("destination finished round %d without reporting a result", h.roundNum)
		return false
	}
	h.roundActive = false
	if h.ctx.Err() != nil {
		return false
	}
	h.mu.Lock()
	pending := h.changePending
	h.mu.Unlock()
	if pending {
		h.signalWake()
	}
	return true
}

func (h *sourceWatchHook) NextFiles() ([]croc.FileInfo, []croc.FileInfo, int) {
	if h.manifest == nil {
		return nil, nil, 0
	}
	files, folders := sourceFileInfos(h.dir, h.manifest)
	return files, nil, folders
}

// destWatchState giữ trạng thái chế độ watch của máy đích xuyên suốt các lần nối lại.
type destWatchState struct {
	watch      bool
	round      int
	rejoinCode string
	lastResult *Result
}

// runSyncJoinLoop chạy máy đích: tự theo Hello.Watch của nguồn. Nếu nguồn không bật watch,
// hành vi và giá trị trả về giống hệt lượt đơn; nếu nguồn bật watch, giữ phiên mở qua nhiều
// lượt và tự nối lại bằng RejoinCode mới nhất (giữ nguyên khóa đích và staging).
func runSyncJoinLoop(ctx context.Context, dir string, root *receivefs.Root, staging *Staging, o JoinOptions) (*Result, error) {
	var state destWatchState
	var backoff time.Duration
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if ctx.Err() != nil {
				return state.lastResult, nil
			}
			if err := clearStagingContents(staging); err != nil {
				return state.lastResult, err
			}
		}
		env := &joinEnv{ctx: ctx, dir: dir, root: root, staging: staging, o: o}
		session := NewDestSession(env, o.Diff, Hello{Watch: attempt > 0})
		hook := &destWatchHook{
			ctx:          ctx,
			env:          env,
			diff:         o.Diff,
			reporter:     o.Reporter,
			reconnecting: attempt > 0,
			state:        &state,
			s:            session,
		}

		ops := o.Croc
		ops.RelayPorts = slices.Clone(o.Croc.RelayPorts)
		ops.IsSender = false
		ops.ReceiveRoot = staging.Dir()
		ops.NoPrompt = true
		ops.Overwrite = true
		ops.Stdout = false
		ops.SyncHook = hook
		if attempt > 0 {
			ops.SharedSecret = state.rejoinCode
			ops.RoomName = ""
		}

		attemptCtx, cancelAttempt := context.WithCancel(ctx)
		client, err := newClient(attemptCtx, ops)
		if err != nil {
			cancelAttempt()
			if state.watch && ctx.Err() != nil {
				return state.lastResult, nil
			}
			return state.lastResult, err
		}
		recvErr := client.Receive()
		cancelAttempt()

		if !state.watch {
			if recvErr != nil {
				return hook.s.Result(), recvErr
			}
			r := hook.s.Result()
			if r == nil {
				return nil, errors.New("foldersync: transfer ended before the sync round finished")
			}
			return r, nil
		}

		if ctx.Err() != nil {
			return state.lastResult, nil
		}
		if hook.fatalErr != nil {
			return state.lastResult, hook.fatalErr
		}
		if env.fatalErr != nil {
			return state.lastResult, env.fatalErr
		}
		if recvErr == nil {
			// Trong chế độ watch, phiên croc không tự đóng khi ctx còn sống; trường hợp này xảy ra
			// nếu kết nối đứt đúng khoảng giữa lúc đích gửi finished và lúc nhận finished từ nguồn.
			recvErr = errors.New("foldersync: watch session closed unexpectedly")
		}
		if state.rejoinCode == "" || isFatalWatchError(recvErr) {
			return state.lastResult, recvErr
		}
		if hook.reconnectedSent {
			backoff = 0
		}
		backoff = nextWatchBackoff(backoff)
		report(o.Reporter, Event{
			Kind:    EventReconnecting,
			Message: fmt.Sprintf("reconnecting in %s after error: %v", backoff, recvErr),
		})
		if !waitWatchBackoff(ctx, backoff) {
			return state.lastResult, nil
		}
	}
}

// destWatchHook nối máy đích vào croc.SyncRounds để chạy nhiều lượt trên cùng một phiên croc.
type destWatchHook struct {
	ctx          context.Context
	env          *joinEnv
	diff         DiffOptions
	reporter     Reporter
	reconnecting bool

	state           *destWatchState
	s               *Session
	reconnectedSent bool
	fatalErr        error
}

var (
	_ croc.SyncHook         = (*destWatchHook)(nil)
	_ croc.SyncReceiveGuard = (*destWatchHook)(nil)
	_ croc.SyncFinisher     = (*destWatchHook)(nil)
	_ croc.SyncRounds       = (*destWatchHook)(nil)
)

func (h *destWatchHook) Feature() string { return message.FeatureFolderSync }

func (h *destWatchHook) Start(send func(message.Message) error) error {
	msgs, err := h.s.Start()
	if err != nil && isFatalWatchError(err) {
		h.fatalErr = err
	}
	return sendAll(send, msgs, err)
}

func (h *destWatchHook) Handle(m message.Message, send func(message.Message) error) error {
	if handled, err := handleWatchKeepalive(m, send); handled {
		if err != nil && isFatalWatchError(err) {
			h.fatalErr = err
		}
		return err
	}
	if m.Type == message.TypeSyncHello {
		// Lượt kế tiếp trên cùng một phiên croc watch sau khi lượt trước đã xong.
		if h.state.watch && h.s != nil && h.s.Done() {
			return h.handleNextRoundHello(m, send)
		}
		// Lượt đầu tiên của phiên croc này (lượt 1 ban đầu hoặc lượt đầu sau khi nối lại).
		if h.s != nil && h.s.state == stWaitHello {
			return h.handleInitialHello(m, send)
		}
	}
	if h.s == nil {
		err := protoErr("unexpected %s with no active sync session", m.Type)
		h.fatalErr = err
		return err
	}

	msgs, err := h.s.Handle(m)
	if err != nil {
		if h.ctx.Err() == nil && (isFatalWatchError(err) || h.env.fatalErr != nil) {
			h.fatalErr = err
		}
		return err
	}
	return sendAll(send, msgs, nil)
}

func (h *destWatchHook) handleInitialHello(m message.Message, send func(message.Message) error) error {
	msgs, err := h.s.Handle(m)
	if err != nil {
		if isFatalWatchError(err) {
			h.fatalErr = err
		}
		return err
	}
	peer := h.s.PeerHello()
	if h.reconnecting {
		if !peer.Watch {
			err := protoErr("source disabled watch mode after reconnect")
			h.fatalErr = err
			return err
		}
		if peer.Round <= h.state.round {
			err := protoErr("round %d is not greater than current round %d", peer.Round, h.state.round)
			h.fatalErr = err
			return err
		}
		h.state.round = peer.Round
		if peer.RejoinCode != "" {
			h.state.rejoinCode = peer.RejoinCode
		}
		if !h.reconnectedSent {
			h.reconnectedSent = true
			report(h.reporter, Event{Kind: EventReconnected})
		}
		report(h.reporter, Event{Kind: EventRoundStart, Message: fmt.Sprintf("round %d", peer.Round)})
	} else if peer.Watch {
		round := peer.Round
		if round <= 0 {
			round = 1
		}
		h.state.watch = true
		h.state.round = round
		if peer.RejoinCode != "" {
			h.state.rejoinCode = peer.RejoinCode
		}
		report(h.reporter, Event{Kind: EventRoundStart, Message: fmt.Sprintf("round %d", round)})
	}
	return sendAll(send, msgs, nil)
}

func (h *destWatchHook) handleNextRoundHello(m message.Message, send func(message.Message) error) error {
	var peer Hello
	if err := decodeMsg(m, &peer); err != nil {
		h.fatalErr = err
		return err
	}
	if peer.Round <= h.state.round {
		err := protoErr("round %d is not greater than current round %d", peer.Round, h.state.round)
		h.fatalErr = err
		return err
	}
	if err := clearStagingContents(h.env.staging); err != nil {
		h.fatalErr = err
		return err
	}
	next := NewDestSession(h.env, h.diff, Hello{Round: peer.Round, Watch: peer.Watch})
	handleMsgs, err := next.Handle(m)
	if err != nil {
		if isFatalWatchError(err) {
			h.fatalErr = err
		}
		return err
	}
	h.state.round = peer.Round
	if peer.RejoinCode != "" {
		h.state.rejoinCode = peer.RejoinCode
	}
	if !peer.Watch {
		h.state.watch = false
	}
	report(h.reporter, Event{Kind: EventRoundStart, Message: fmt.Sprintf("round %d", peer.Round)})
	startMsgs, err := next.Start()
	if err != nil {
		if isFatalWatchError(err) {
			h.fatalErr = err
		}
		return err
	}
	if err := sendAll(send, startMsgs, nil); err != nil {
		return err
	}
	h.s = next
	return sendAll(send, handleMsgs, nil)
}

func (h *destWatchHook) Ready() (bool, func(croc.FileInfo) bool) {
	return false, nil
}

func (h *destWatchHook) AllowReceive(p string, size int64, isDir bool) error {
	err := h.s.AllowReceive(p, size, isDir)
	if err != nil && isFatalWatchError(err) {
		h.fatalErr = err
	}
	return err
}

func (h *destWatchHook) BeforeFinish(send func(message.Message) error) error {
	msgs, err := h.s.Finish()
	if err != nil {
		if h.ctx.Err() == nil && (isFatalWatchError(err) || h.env.fatalErr != nil) {
			h.fatalErr = err
		}
		return err
	}
	h.state.lastResult = h.s.Result()
	return sendAll(send, msgs, nil)
}

func (h *destWatchHook) Wake() <-chan struct{} {
	return nil
}

func (h *destWatchHook) OnWake(func(message.Message) error) error {
	return nil
}

func (h *destWatchHook) KeepOpen() bool {
	if !h.state.watch {
		return false
	}
	if h.ctx.Err() != nil {
		return false
	}
	if h.s == nil || !h.s.Done() {
		return false
	}
	return true
}

func (h *destWatchHook) NextFiles() ([]croc.FileInfo, []croc.FileInfo, int) {
	return nil, nil, 0
}
