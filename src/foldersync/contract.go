// Package foldersync — Contract C (CLI ↔ Engine), version 1.
//
// This file only declares types and function signatures shared between
// the CLI (Member 3, src/cli/sync.go) and the sync engine (Member 2).
// It deliberately does NOT depend on Contract A types (SyncEntry,
// Manifest, SyncPlan) so the CLI can compile and be tested before the
// core engine exists. The engine converts its internal SyncPlan into
// PlanView before calling ProgressReporter.OnPlan.
//
// Changing anything in this file requires agreement of Member 2 and
// Member 3.
package foldersync

import (
	"context"
	"errors"
	"time"
)

// ---------------------------------------------------------------------
// 1. Input: CLI -> Engine
// ---------------------------------------------------------------------

// SyncConfig holds only what the engine needs. Purely presentational
// flags (--quiet, --qr, --yes) stay in the CLI and are NOT part of it.
type SyncConfig struct {
	// Dir is the local folder: the source on Host, the target on Join.
	Dir string

	// Code is the PAKE code phrase. On Host it may be empty: the engine
	// generates one and reports it via OnCode. On Join it is required
	// (read from CROC_SECRET by the CLI).
	Code string

	Relay     string // --relay, empty = croc default
	Transport string // --transport, passed through unchanged

	Delete   bool // --delete
	DryRun   bool // --dry-run
	Watch    bool // --watch
	Checksum bool // --checksum

	Exclude      []string // --exclude (substring match)
	ExcludeFiles []string // --exclude-file (exact relative path)
	UseGitignore bool     // --git
}

// ---------------------------------------------------------------------
// 2. Entry points: CLI calls one of these
// ---------------------------------------------------------------------

// RunSyncHost runs on the source machine (`croc sync <folder>`).
// RunSyncJoin runs on the target machine (`CROC_SECRET=... croc sync <folder>`).
//
// Both block until the sync finishes. With cfg.Watch they block until
// ctx is cancelled. When ctx is cancelled (Ctrl+C) they must stop
// transfers, remove .croc-sync-tmp-* files and return ctx.Err().
//
// Return values:
//   - nil                    success (or dry-run completed)
//   - ErrPeerUnsupported     peer lacks the folder-sync-v1 feature
//   - ErrAborted             the user declined in OnPlan
//   - context.Canceled       user pressed Ctrl+C
//   - other error            fatal failure (network, PAKE, I/O on root)
//
// A failure on a single file is NOT fatal: it is reported through
// OnFileDone and counted in Summary.Errors.
//
// The bodies below are stubs. Member 2 replaces ONLY the bodies (e.g.
// by delegating to code in executor.go); the signatures are frozen.
// The CLI keeps its own swappable variables pointing at these
// functions so it can run against a fake engine in tests and demos.
func RunSyncHost(ctx context.Context, cfg SyncConfig, r ProgressReporter) error {
	return ErrNotImplemented
}

func RunSyncJoin(ctx context.Context, cfg SyncConfig, r ProgressReporter) error {
	return ErrNotImplemented
}

// ErrNotImplemented is returned by the stubs until the engine lands.
var ErrNotImplemented = errors.New("foldersync: engine not implemented yet")

// Sentinel errors. The CLI checks them with errors.Is.
var (
	ErrPeerUnsupported = errors.New("foldersync: peer does not support folder-sync-v1")
	ErrAborted         = errors.New("foldersync: aborted by user")
)

// ---------------------------------------------------------------------
// 3. Output: Engine -> CLI
// ---------------------------------------------------------------------

// ProgressReporter is implemented by the CLI (and by tests).
// The engine never writes to stdout/stderr directly.
//
// Concurrency: the engine may call methods from several goroutines
// (multi-stream transfer). Implementations must be safe for concurrent
// use. OnPlan is the exception: it is called exactly once per sync
// round, before any file is transferred, and blocks until it returns.
type ProgressReporter interface {
	// OnCode is called on Host once the code phrase is known, before
	// waiting for the peer. The CLI prints it (and a QR with --qr).
	OnCode(code string)

	// OnStatus reports human-readable connection steps, e.g.
	// "connecting to relay", "peer connected (P2P)".
	OnStatus(msg string)

	// OnPlan shows the computed plan.
	//   Host: return true to proceed, false to abort (engine returns
	//         ErrAborted). With plan.DryRun the return value is ignored
	//         and nothing is written. The CLI must ask for
	//         confirmation when len(plan.Delete) > 0 unless --yes.
	//   Join: informational only, return value is ignored.
	OnPlan(plan PlanView) bool

	OnFileStart(path string, size int64)
	OnProgress(p Progress)
	OnFileDone(path string, err error) // err == nil means success

	// OnWatchEvent is called in --watch mode for each debounced change.
	OnWatchEvent(ev WatchEvent)

	// OnRoundDone is called after each completed sync round: once for a
	// normal sync, once per batch of changes in --watch mode.
	OnRoundDone(s Summary)
}

// PlanView is the CLI-facing view of a SyncPlan. Paths are POSIX
// relative paths. Lists are sorted by path.
type PlanView struct {
	Add    []PlanItem
	Update []PlanItem
	Delete []PlanItem
	Skip   []PlanItem

	DryRun        bool
	TransferBytes int64 // total size of Add + Update
	SavedBytes    int64 // total size of Skip (not re-sent)
}

type PlanItem struct {
	Path  string
	Size  int64
	IsDir bool
}

// Progress carries both per-file and overall progress so the CLI can
// draw one or two bars. Speed is computed by the CLI.
type Progress struct {
	Path         string
	FileDone     int64
	FileTotal    int64
	OverallDone  int64
	OverallTotal int64
}

type WatchEventKind string

const (
	WatchCreate WatchEventKind = "create"
	WatchModify WatchEventKind = "modify"
	WatchDelete WatchEventKind = "delete"
	WatchRename WatchEventKind = "rename"
)

type WatchEvent struct {
	Kind    WatchEventKind
	Path    string
	OldPath string // only for WatchRename
	Time    time.Time
}

type Summary struct {
	Added, Updated, Deleted, Skipped int
	BytesSent                        int64
	Duration                         time.Duration
	Errors                           []FileError
}

type FileError struct {
	Path string
	Err  error
}

// NopReporter ignores everything and always proceeds. Useful for
// tests and as an embeddable base for partial implementations.
type NopReporter struct{}

func (NopReporter) OnCode(string)             {}
func (NopReporter) OnStatus(string)           {}
func (NopReporter) OnPlan(PlanView) bool      { return true }
func (NopReporter) OnFileStart(string, int64) {}
func (NopReporter) OnProgress(Progress)       {}
func (NopReporter) OnFileDone(string, error)  {}
func (NopReporter) OnWatchEvent(WatchEvent)   {}
func (NopReporter) OnRoundDone(Summary)       {}

var _ ProgressReporter = NopReporter{}
