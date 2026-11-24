package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/schollz/croc/v11/internal/cli"
	"github.com/schollz/croc/v11/src/foldersync"
	"github.com/stretchr/testify/require"
)

// lockedBuffer is a goroutine-safe output sink for reporter tests.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type syncEngineFunc func(ctx context.Context, cfg foldersync.SyncConfig, r foldersync.ProgressReporter) error

// fakeSyncEngine replaces the engine entry points for one test and records
// which side was started and with which configuration.
type fakeSyncEngine struct {
	mu     sync.Mutex
	calls  int
	join   bool
	cfg    foldersync.SyncConfig
	output *lockedBuffer
}

func installFakeSyncEngine(t *testing.T, run syncEngineFunc) *fakeSyncEngine {
	t.Helper()
	t.Setenv("CROC_CONFIG_DIR", t.TempDir())
	unsetEnv(t, "CROC_SECRET")
	unsetEnv(t, "CROC_RELAY")
	unsetEnv(t, "CROC_RELAY6")
	unsetEnv(t, "CROC_PASS")

	fake := &fakeSyncEngine{output: &lockedBuffer{}}
	record := func(join bool) syncEngineFunc {
		return func(ctx context.Context, cfg foldersync.SyncConfig, r foldersync.ProgressReporter) error {
			fake.mu.Lock()
			fake.calls++
			fake.join = join
			fake.cfg = cfg
			fake.mu.Unlock()
			if run == nil {
				return nil
			}
			return run(ctx, cfg, r)
		}
	}

	oldHost, oldJoin := runSyncHost, runSyncJoin
	oldTerminal, oldInput, oldQR := syncTerminal, syncReadInput, syncShowQRCode
	runSyncHost = record(false)
	runSyncJoin = record(true)
	syncTerminal = func() (io.Writer, bool) { return fake.output, false }
	syncReadInput = func(context.Context, string) (string, error) {
		t.Error("unexpected confirmation prompt")
		return "", errors.New("unexpected prompt")
	}
	syncShowQRCode = func(string) {}
	t.Cleanup(func() {
		runSyncHost, runSyncJoin = oldHost, oldJoin
		syncTerminal, syncReadInput, syncShowQRCode = oldTerminal, oldInput, oldQR
	})
	return fake
}

func (f *fakeSyncEngine) snapshot() (int, bool, foldersync.SyncConfig) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.join, f.cfg
}

func runSyncCommand(t *testing.T, args ...string) error {
	t.Helper()
	return newApp().RunContext(t.Context(), append([]string{"croc"}, args...))
}

func samplePlan() foldersync.PlanView {
	return foldersync.PlanView{
		Add:           []foldersync.PlanItem{{Path: "docs", IsDir: true}, {Path: "docs/new.txt", Size: 1200}},
		Update:        []foldersync.PlanItem{{Path: "src/main.go", Size: 3400}},
		Delete:        []foldersync.PlanItem{{Path: "old.log", Size: 512}},
		Skip:          []foldersync.PlanItem{{Path: "keep.txt", Size: 10}, {Path: "keep2.txt", Size: 20}},
		TransferBytes: 4600,
		SavedBytes:    30,
	}
}

// ---------------------------------------------------------------------
// Command wiring and flag parsing
// ---------------------------------------------------------------------

func TestSyncCommandIsRegistered(t *testing.T) {
	for _, command := range newApp().Commands {
		if command.Name == "sync" {
			require.Equal(t, "croc sync", command.HelpName)
			return
		}
	}
	t.Fatal("sync command not found")
}

func TestSyncHostDefaults(t *testing.T) {
	fake := installFakeSyncEngine(t, nil)
	dir := t.TempDir()

	require.NoError(t, runSyncCommand(t, "sync", dir))

	calls, join, cfg := fake.snapshot()
	require.Equal(t, 1, calls)
	require.False(t, join)
	absDir, err := filepath.Abs(dir)
	require.NoError(t, err)
	require.Equal(t, foldersync.SyncConfig{Dir: absDir, Transport: "auto"}, cfg)
	require.Contains(t, fake.output.String(), "Hosting sync from source")
}

func TestSyncHostPassesEngineFlags(t *testing.T) {
	fake := installFakeSyncEngine(t, nil)
	dir := t.TempDir()

	require.NoError(t, runSyncCommand(t, "sync",
		"--code", "test-sync-code",
		"--delete", "--checksum", "--git",
		"--exclude", " node_modules , .cache ,,",
		"--exclude-file", "./build/out.bin, notes.txt,.",
		"--transport", "RELAY",
		dir))

	_, join, cfg := fake.snapshot()
	require.False(t, join)
	require.Equal(t, "test-sync-code", cfg.Code)
	require.True(t, cfg.Delete)
	require.True(t, cfg.Checksum)
	require.True(t, cfg.UseGitignore)
	require.False(t, cfg.DryRun)
	require.False(t, cfg.Watch)
	require.Equal(t, []string{"node_modules", ".cache"}, cfg.Exclude)
	require.Equal(t, []string{"build/out.bin", "notes.txt"}, cfg.ExcludeFiles)
	require.Equal(t, "relay", cfg.Transport)
}

func TestSyncJoinReadsCodeFromEnvironment(t *testing.T) {
	fake := installFakeSyncEngine(t, nil)
	t.Setenv("CROC_SECRET", "  join-secret-code ")
	target := filepath.Join(t.TempDir(), "not-created-yet")

	require.NoError(t, runSyncCommand(t, "sync", target))

	calls, join, cfg := fake.snapshot()
	require.Equal(t, 1, calls)
	require.True(t, join)
	require.Equal(t, "join-secret-code", cfg.Code)
	require.Equal(t, target, cfg.Dir)
	// The CLI must not create the target folder itself (dry-run safety).
	_, err := os.Stat(target)
	require.True(t, errors.Is(err, os.ErrNotExist))
	require.Contains(t, fake.output.String(), "Joining sync as the target")
}

func TestSyncRelayFlagPositions(t *testing.T) {
	for _, args := range [][]string{
		{"sync", "--relay", "localhost:9009"},
		{"--relay", "localhost:9009", "sync"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			fake := installFakeSyncEngine(t, nil)
			require.NoError(t, runSyncCommand(t, append(args, t.TempDir())...))
			_, _, cfg := fake.snapshot()
			require.Equal(t, "localhost:9009", cfg.Relay)
		})
	}
}

func TestSyncRelayFromEnvironment(t *testing.T) {
	fake := installFakeSyncEngine(t, nil)
	t.Setenv("CROC_RELAY", "relay.example.com:9009")
	require.NoError(t, runSyncCommand(t, "sync", t.TempDir()))
	_, _, cfg := fake.snapshot()
	require.Equal(t, "relay.example.com:9009", cfg.Relay)
}

func TestSyncYesFlagPositionsSkipConfirmation(t *testing.T) {
	for _, args := range [][]string{
		{"sync", "--delete", "--yes"},
		{"--yes", "sync", "--delete"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var proceed bool
			installFakeSyncEngine(t, func(_ context.Context, _ foldersync.SyncConfig, r foldersync.ProgressReporter) error {
				proceed = r.OnPlan(samplePlan())
				return nil
			})
			require.NoError(t, runSyncCommand(t, append(args, t.TempDir())...))
			require.True(t, proceed)
		})
	}
}

func TestSyncCommandRejectsInvalidInput(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file.txt")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o644))

	tests := []struct {
		name   string
		secret string
		args   []string
		want   string
	}{
		{name: "no folder", args: []string{"sync"}, want: "must specify a folder"},
		{name: "two folders", args: []string{"sync", dir, dir}, want: "exactly one folder"},
		{name: "missing source", args: []string{"sync", filepath.Join(dir, "missing")}, want: "does not exist"},
		{name: "source is a file", args: []string{"sync", file}, want: "not a folder"},
		{name: "target is a file", secret: "join-code", args: []string{"sync", file}, want: "not a folder"},
		{name: "dry-run with watch", args: []string{"sync", "--dry-run", "--watch", dir}, want: "--dry-run cannot be combined with --watch"},
		{name: "bad transport", args: []string{"sync", "--transport", "carrier-pigeon", dir}, want: "invalid transport"},
		{name: "code and secret", secret: "join-code", args: []string{"sync", "--code", "host-code", dir}, want: "not both"},
		{name: "two relays", args: []string{"--relay6", "[::1]:9009", "sync", "--relay", "localhost:9009", dir}, want: "only one explicit relay"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := installFakeSyncEngine(t, nil)
			if tt.secret != "" {
				t.Setenv("CROC_SECRET", tt.secret)
			}
			err := runSyncCommand(t, tt.args...)
			require.ErrorContains(t, err, tt.want)
			calls, _, _ := fake.snapshot()
			require.Zero(t, calls, "engine must not start on invalid input")
		})
	}
}

func TestSyncRefusesDangerousTargets(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)

	_, err := resolveSyncDir(home, true)
	require.ErrorContains(t, err, "home folder")

	root := filepath.VolumeName(home) + string(filepath.Separator)
	_, err = resolveSyncDir(root, true)
	require.ErrorContains(t, err, "filesystem root")

	// A sub-folder of home is fine, and the source side has no such limit.
	_, err = resolveSyncDir(filepath.Join(home, "backup"), true)
	require.NoError(t, err)
	_, err = resolveSyncDir(home, false)
	require.NoError(t, err)
}

func TestSyncPassWarning(t *testing.T) {
	fake := installFakeSyncEngine(t, nil)
	require.NoError(t, runSyncCommand(t, "--pass", "custom-pass", "sync", t.TempDir()))
	require.Contains(t, fake.output.String(), "--pass is not supported by croc sync yet")
}

func TestSyncCodeWarning(t *testing.T) {
	unsetEnv(t, "CROC_PASS")
	dir := t.TempDir()
	parse := func(goos string, classic bool) []string {
		t.Helper()
		app := newApp()
		var warnings []string
		for _, command := range app.Commands {
			if command.Name == "sync" {
				command.Action = func(c *cli.Context) error {
					var err error
					_, warnings, err = parseSyncInvocation(c, func(string) string { return "" }, goos, classic)
					return err
				}
			}
		}
		require.NoError(t, app.Run([]string{"croc", "sync", "--code", "abc-def-ghi", dir}))
		return warnings
	}

	unix := parse("linux", false)
	require.Len(t, unix, 1)
	require.Contains(t, unix[0], "process list")
	require.Empty(t, parse("windows", false))
	require.Empty(t, parse("linux", true), "classic mode accepts codes on the command line")
}

func TestSyncEngineErrors(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		is       error
		contains string
	}{
		{name: "peer unsupported", err: fmt.Errorf("handshake: %w", foldersync.ErrPeerUnsupported), is: foldersync.ErrPeerUnsupported, contains: "does not support croc sync"},
		{name: "aborted", err: foldersync.ErrAborted, is: foldersync.ErrAborted, contains: "no files were transferred or deleted"},
		{name: "not implemented", err: foldersync.ErrNotImplemented, is: foldersync.ErrNotImplemented, contains: "not available in this build"},
		{name: "fatal", err: errors.New("relay unreachable"), contains: "croc sync: relay unreachable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installFakeSyncEngine(t, func(context.Context, foldersync.SyncConfig, foldersync.ProgressReporter) error {
				return tt.err
			})
			err := runSyncCommand(t, "sync", t.TempDir())
			require.ErrorContains(t, err, tt.contains)
			if tt.is != nil {
				require.ErrorIs(t, err, tt.is)
			}
		})
	}
}

func TestSyncWithRealStubEngineReportsNotImplemented(t *testing.T) {
	installFakeSyncEngine(t, nil)
	runSyncHost = foldersync.RunSyncHost
	err := runSyncCommand(t, "sync", t.TempDir())
	require.ErrorIs(t, err, foldersync.ErrNotImplemented)
}

func TestSyncCtrlCReturnsCanceled(t *testing.T) {
	started := make(chan struct{})
	fake := installFakeSyncEngine(t, func(ctx context.Context, _ foldersync.SyncConfig, r foldersync.ProgressReporter) error {
		r.OnProgress(foldersync.Progress{Path: "big.bin", FileDone: 10, FileTotal: 100, OverallDone: 10, OverallTotal: 100})
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})

	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		result <- newApp().RunContext(ctx, []string{"croc", "sync", "--watch", dir})
	}()
	<-started
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("sync did not stop after cancellation")
	}
	require.Contains(t, fake.output.String(), "Stopped watching.")
}

func TestSyncFileErrorsFailTheCommand(t *testing.T) {
	installFakeSyncEngine(t, func(_ context.Context, _ foldersync.SyncConfig, r foldersync.ProgressReporter) error {
		r.OnPlan(foldersync.PlanView{Add: []foldersync.PlanItem{{Path: "a.txt", Size: 1}}})
		r.OnFileDone("a.txt", errors.New("file is locked"))
		r.OnRoundDone(foldersync.Summary{Errors: []foldersync.FileError{{Path: "a.txt", Err: errors.New("file is locked")}}})
		return nil
	})
	err := runSyncCommand(t, "sync", t.TempDir())
	require.ErrorContains(t, err, "1 item could not be synced")
}

// ---------------------------------------------------------------------
// Reporter behaviour
// ---------------------------------------------------------------------

// newTestReporter returns a reporter whose answers are scripted. Each entry
// in prompts is the output written just before an answer was read.
func newTestReporter(inv syncInvocation, answers ...string) (*syncReporter, *lockedBuffer, *[]string) {
	out := &lockedBuffer{}
	r := newSyncReporter(context.Background(), out, false, inv)
	prompts := &[]string{}
	r.readInput = func(_ context.Context, prompt string) (string, error) {
		if prompt != "" {
			panic("the reporter must print prompts itself, not through readInput")
		}
		text := out.String()
		if i := strings.LastIndex(strings.TrimSuffix(text, "\n"), "\n"); i >= 0 {
			text = text[i+1:]
		}
		*prompts = append(*prompts, text)
		if len(answers) == 0 {
			return "", io.EOF
		}
		answer := answers[0]
		answers = answers[1:]
		return answer, nil
	}
	r.now = func() time.Time { return time.Date(2026, 11, 23, 15, 4, 5, 0, time.Local) }
	return r, out, prompts
}

func TestSyncReporterConfirmsDeletes(t *testing.T) {
	tests := []struct {
		answer string
		want   bool
	}{
		{"y", true}, {"YES", true}, {"n", false}, {"", false}, {"a", false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("answer %q", tt.answer), func(t *testing.T) {
			r, out, prompts := newTestReporter(syncInvocation{}, tt.answer)
			require.Equal(t, tt.want, r.OnPlan(samplePlan()))
			require.Len(t, *prompts, 1)
			require.Contains(t, (*prompts)[0], "Delete 1 item from the target folder? (y/N)")
			require.Contains(t, out.String(), "- delete old.log")
			if !tt.want {
				require.Contains(t, out.String(), "Deletion not confirmed.")
			}
		})
	}
}

func TestSyncReporterUnreadableAnswerAborts(t *testing.T) {
	r, out, _ := newTestReporter(syncInvocation{})
	require.False(t, r.OnPlan(samplePlan()))
	require.Contains(t, out.String(), "--yes")
}

func TestSyncReporterSkipsConfirmation(t *testing.T) {
	plan := samplePlan()
	cases := map[string]syncInvocation{
		"yes":  {Yes: true},
		"join": {Join: true},
	}
	for name, inv := range cases {
		t.Run(name, func(t *testing.T) {
			r, _, prompts := newTestReporter(inv)
			require.True(t, r.OnPlan(plan))
			require.Empty(t, *prompts)
		})
	}
	t.Run("no deletes", func(t *testing.T) {
		r, _, prompts := newTestReporter(syncInvocation{})
		noDelete := plan
		noDelete.Delete = nil
		require.True(t, r.OnPlan(noDelete))
		require.Empty(t, *prompts)
	})
	t.Run("dry run", func(t *testing.T) {
		r, out, prompts := newTestReporter(syncInvocation{Config: foldersync.SyncConfig{DryRun: true}})
		dry := plan
		dry.DryRun = true
		require.True(t, r.OnPlan(dry))
		require.Empty(t, *prompts)
		require.Contains(t, out.String(), "dry run: nothing will be changed")
	})
}

func TestSyncReporterWatchAlwaysDelete(t *testing.T) {
	r, out, prompts := newTestReporter(syncInvocation{Config: foldersync.SyncConfig{Watch: true}}, "a")
	require.True(t, r.OnPlan(samplePlan()))
	require.Contains(t, (*prompts)[0], "a = yes for the rest of this session")

	// Second round: compact output, deletions listed, no new prompt.
	second := foldersync.PlanView{Delete: []foldersync.PlanItem{{Path: "gone.txt", Size: 3}}}
	require.True(t, r.OnPlan(second))
	require.Len(t, *prompts, 1)
	require.Contains(t, out.String(), "[15:04:05] changes: 0 to add, 0 to update, 1 to delete")
}

func TestSyncReporterWatchListsDeletesBeforeAsking(t *testing.T) {
	r, out, prompts := newTestReporter(syncInvocation{Config: foldersync.SyncConfig{Watch: true}}, "n")
	require.True(t, r.OnPlan(foldersync.PlanView{Add: []foldersync.PlanItem{{Path: "a.txt"}}}))
	require.Empty(t, *prompts)
	require.False(t, r.OnPlan(foldersync.PlanView{Delete: []foldersync.PlanItem{{Path: "gone.txt", Size: 3}}}))
	require.Len(t, *prompts, 1)
	require.Contains(t, out.String(), "- delete gone.txt")
}

func TestSyncReporterQuiet(t *testing.T) {
	r, out, prompts := newTestReporter(syncInvocation{Quiet: true}, "y")
	r.OnStatus("connecting to relay")
	r.OnWatchEvent(foldersync.WatchEvent{Kind: foldersync.WatchModify, Path: "a.txt"})
	r.OnFileStart("a.txt", 10)
	r.OnProgress(foldersync.Progress{Path: "a.txt", FileDone: 5, FileTotal: 10, OverallDone: 5, OverallTotal: 10})
	require.Empty(t, out.String())

	// Deletions are still listed before the prompt.
	require.True(t, r.OnPlan(samplePlan()))
	require.Len(t, *prompts, 1)
	require.Contains(t, out.String(), "- delete old.log")
	require.NotContains(t, out.String(), "src/main.go")

	r.OnCode("abc-def-ghi")
	r.OnFileDone("a.txt", errors.New("permission denied"))
	r.OnRoundDone(foldersync.Summary{Added: 1})
	text := out.String()
	require.Contains(t, text, "Code is: abc-def-ghi")
	require.Contains(t, text, "error: a.txt: permission denied")
	require.Contains(t, text, "Sync complete")
}

func TestSyncReporterOnCode(t *testing.T) {
	var shown []string
	r, out, _ := newTestReporter(syncInvocation{QRCode: true, Config: foldersync.SyncConfig{Relay: "localhost:9009"}})
	r.showQR = func(code string) { shown = append(shown, code) }
	r.OnCode("acid-acorn-acre")
	text := out.String()
	require.Contains(t, text, "Code is: acid-acorn-acre")
	require.Contains(t, text, "CROC_SECRET='acid-acorn-acre' croc sync --relay localhost:9009 <target-folder>")
	require.Contains(t, text, "PowerShell: $env:CROC_SECRET='acid-acorn-acre'; croc sync --relay 'localhost:9009' <target-folder>")
	require.Equal(t, []string{"acid-acorn-acre"}, shown)
}

func TestSyncReporterSummaries(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		r, out, _ := newTestReporter(syncInvocation{})
		r.OnRoundDone(foldersync.Summary{Added: 2, Updated: 1, Deleted: 1, Skipped: 7, BytesSent: 2048, Duration: 1234 * time.Millisecond})
		require.Contains(t, out.String(), "Sync complete in 1.2s: 2 added, 1 updated, 1 deleted, 7 unchanged · 2.0 kB sent")
	})
	t.Run("dry run", func(t *testing.T) {
		r, out, _ := newTestReporter(syncInvocation{Config: foldersync.SyncConfig{DryRun: true}})
		r.OnRoundDone(foldersync.Summary{})
		require.Contains(t, out.String(), "Dry run complete: no files were transferred, written or deleted.")
		require.NotContains(t, out.String(), "Sync complete")
	})
	t.Run("watch", func(t *testing.T) {
		r, out, _ := newTestReporter(syncInvocation{Config: foldersync.SyncConfig{Watch: true}})
		r.OnRoundDone(foldersync.Summary{Skipped: 3})
		r.OnRoundDone(foldersync.Summary{Updated: 1, BytesSent: 10, Duration: 85 * time.Millisecond})
		text := out.String()
		require.Equal(t, 1, strings.Count(text, "Watching for changes (Ctrl+C to stop)"))
		require.Contains(t, text, "[15:04:05] synced: 0 added, 1 updated, 0 deleted, 0 unchanged · 10 B in 85ms")
	})
	t.Run("errors", func(t *testing.T) {
		r, out, _ := newTestReporter(syncInvocation{})
		r.OnRoundDone(foldersync.Summary{Errors: []foldersync.FileError{{Path: "a", Err: errors.New("x")}, {Path: "b", Err: errors.New("y")}}})
		require.Contains(t, out.String(), "Sync finished with errors")
		require.Contains(t, out.String(), "2 failed items")
		require.Equal(t, 2, r.failedCount())
	})
}

func TestSyncReporterWatchEvents(t *testing.T) {
	r, out, _ := newTestReporter(syncInvocation{Config: foldersync.SyncConfig{Watch: true}})
	at := time.Date(2026, 1, 2, 9, 8, 7, 0, time.Local)
	r.OnWatchEvent(foldersync.WatchEvent{Kind: foldersync.WatchCreate, Path: "new.txt", Time: at})
	r.OnWatchEvent(foldersync.WatchEvent{Kind: foldersync.WatchRename, OldPath: "a.txt", Path: "b.txt", Time: at})
	r.OnFileDone("new.txt", nil)
	text := out.String()
	require.Contains(t, text, "[09:08:07] create new.txt")
	require.Contains(t, text, "[09:08:07] rename a.txt → b.txt")
	require.Contains(t, text, "synced new.txt")
}

func TestSyncReporterProgressBar(t *testing.T) {
	r, out, _ := newTestReporter(syncInvocation{})
	r.OnFileStart("photos/a.jpg", 600)
	r.OnProgress(foldersync.Progress{Path: "photos/a.jpg", FileDone: 300, FileTotal: 600, OverallDone: 300, OverallTotal: 1000})
	r.OnStatus("peer connected (P2P)")
	r.OnProgress(foldersync.Progress{Path: "photos/b.jpg", FileDone: 400, FileTotal: 400, OverallDone: 1000, OverallTotal: 1000})
	r.OnRoundDone(foldersync.Summary{Added: 2, BytesSent: 1000})
	text := out.String()
	require.Contains(t, text, "Sending ")
	require.Contains(t, text, "peer connected (P2P)")
	require.Contains(t, text, "Sync complete")

	// Progress without any total must not create a bar or panic.
	r.OnProgress(foldersync.Progress{Path: "x"})
}

func TestSyncReporterCloseOnCancel(t *testing.T) {
	r, out, _ := newTestReporter(syncInvocation{})
	r.OnProgress(foldersync.Progress{Path: "a", OverallDone: 1, OverallTotal: 10})
	r.close(context.Canceled)
	require.Contains(t, out.String(), "Sync stopped.")

	quiet, quietOut, _ := newTestReporter(syncInvocation{Quiet: true})
	quiet.close(context.Canceled)
	require.Empty(t, quietOut.String())
}

// Run with -race: the engine may report from several goroutines at once.
func TestSyncReporterConcurrentUse(t *testing.T) {
	r, _, _ := newTestReporter(syncInvocation{Config: foldersync.SyncConfig{Watch: true}})
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				path := fmt.Sprintf("dir%d/file%d.txt", worker, i)
				r.OnFileStart(path, 100)
				r.OnProgress(foldersync.Progress{Path: path, FileDone: 50, FileTotal: 100, OverallDone: int64(worker*50 + i), OverallTotal: 1000})
				r.OnStatus("status")
				r.OnWatchEvent(foldersync.WatchEvent{Kind: foldersync.WatchModify, Path: path})
				if i%10 == 0 {
					r.OnFileDone(path, errors.New("boom"))
				} else {
					r.OnFileDone(path, nil)
				}
			}
		}()
	}
	wg.Wait()
	r.OnRoundDone(foldersync.Summary{})
	r.close(nil)
}

// ---------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------

func TestFormatSyncPlan(t *testing.T) {
	text := formatSyncPlan(samplePlan(), false, syncPlanListLimit, syncDeleteListLimit, false)
	for _, want := range []string{
		"Sync plan\n",
		"+ add    docs/\n",
		"+ add    docs/new.txt  1.2 kB",
		"~ update src/main.go   3.3 kB",
		"- delete old.log       512 B",
		"= skip   2 unchanged items",
		// Directories count as items too.
		"Total: 2 to add, 1 to update, 1 to delete, 2 unchanged · 4.5 kB to send · 30 B not re-sent",
	} {
		require.Contains(t, text, want)
	}

	join := formatSyncPlan(samplePlan(), true, 0, 0, false)
	require.Contains(t, join, "Incoming changes from the source")

	empty := formatSyncPlan(foldersync.PlanView{Skip: samplePlan().Skip}, false, 0, 0, false)
	require.Contains(t, empty, "Already in sync: 2 items unchanged.")
}

func TestFormatSyncPlanAlignsSizeColumn(t *testing.T) {
	text := formatSyncPlan(samplePlan(), false, 0, 0, false)
	column := -1
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasSuffix(line, " kB") && !strings.HasSuffix(line, " B") {
			continue
		}
		if strings.Contains(line, "Total:") {
			continue
		}
		fields := strings.Fields(line)
		size := fields[len(fields)-2] + " " + fields[len(fields)-1]
		at := strings.LastIndex(line, size)
		if column == -1 {
			column = at
		}
		require.Equal(t, column, at, "size column misaligned in %q", line)
	}
	require.NotEqual(t, -1, column)
}

func TestFormatSyncPlanTruncatesOutsideDryRun(t *testing.T) {
	var plan foldersync.PlanView
	for i := range syncPlanListLimit + 5 {
		plan.Add = append(plan.Add, foldersync.PlanItem{Path: fmt.Sprintf("f%02d.txt", i), Size: 1})
	}
	text := formatSyncPlan(plan, false, syncPlanListLimit, syncDeleteListLimit, false)
	require.Contains(t, text, "… and 5 more to add (use --dry-run to list everything)")
	require.NotContains(t, text, "f24.txt")

	plan.DryRun = true
	full := formatSyncPlan(plan, false, 0, 0, false)
	require.Contains(t, full, "f24.txt")
	require.NotContains(t, full, "more to add")
}

func TestFormatSyncPlanLabelsWorkWithoutColor(t *testing.T) {
	colored := formatSyncPlan(samplePlan(), false, 0, 0, true)
	require.Contains(t, colored, "\x1b[")
	plain := formatSyncPlan(samplePlan(), false, 0, 0, false)
	require.NotContains(t, plain, "\x1b[")
	for _, word := range []string{"add", "update", "delete", "skip"} {
		require.Contains(t, plain, word)
	}
}

func TestSyncQuoting(t *testing.T) {
	require.Equal(t, "localhost:9009", posixQuote("localhost:9009"))
	require.Equal(t, "[::1]:9009", posixQuote("[::1]:9009"))
	require.Equal(t, `'it'\''s here'`, posixQuote("it's here"))
	require.Equal(t, "''", posixQuote(""))
	require.Equal(t, "'it''s'", powershellQuote("it's"))
}

func TestShortenSyncPath(t *testing.T) {
	require.Equal(t, "short.txt", shortenSyncPath("short.txt", 20))
	require.Equal(t, "…/c/file.txt", shortenSyncPath("a/b/c/file.txt", 12))
	require.Equal(t, 12, len([]rune(shortenSyncPath("a/b/c/file.txt", 12))))
}

func TestPluralize(t *testing.T) {
	require.Equal(t, "1 item", pluralize(1, "item", "items"))
	require.Equal(t, "0 items", pluralize(0, "item", "items"))
	require.Equal(t, "3 items", pluralize(3, "item", "items"))
}

// ---------------------------------------------------------------------
// Manual demo against a fake engine
// ---------------------------------------------------------------------

// TestSyncDemo plays a scripted host session on the real terminal so the CLI
// can be shown before the engine exists. Build the test binary and run it
// directly to keep colors and the progress bar:
//
//	go test -c -o croc-sync-demo.exe ./src/cli
//	CROC_SYNC_DEMO=1 ./croc-sync-demo.exe -test.run TestSyncDemo
func TestSyncDemo(t *testing.T) {
	if os.Getenv("CROC_SYNC_DEMO") == "" {
		t.Skip("set CROC_SYNC_DEMO=1 to run the interactive sync demo")
	}
	oldHost := runSyncHost
	t.Cleanup(func() { runSyncHost = oldHost })
	runSyncHost = func(ctx context.Context, cfg foldersync.SyncConfig, r foldersync.ProgressReporter) error {
		r.OnCode("demo-acid-acorn-acre")
		r.OnStatus("connecting to relay…")
		time.Sleep(500 * time.Millisecond)
		r.OnStatus("peer connected (P2P)")
		plan := foldersync.PlanView{
			Add:           []foldersync.PlanItem{{Path: "photos/2026", IsDir: true}, {Path: "photos/2026/beach.jpg", Size: 4 << 20}, {Path: "notes/todo.md", Size: 2 << 10}},
			Update:        []foldersync.PlanItem{{Path: "notes/ideas.md", Size: 8 << 10}},
			Delete:        []foldersync.PlanItem{{Path: "tmp/old.log", Size: 1 << 10}},
			Skip:          make([]foldersync.PlanItem, 120),
			DryRun:        cfg.DryRun,
			TransferBytes: 4<<20 + 10<<10,
			SavedBytes:    300 << 20,
		}
		if !r.OnPlan(plan) {
			return foldersync.ErrAborted
		}
		if cfg.DryRun {
			r.OnRoundDone(foldersync.Summary{})
			return nil
		}
		files := plan.Add[1:]
		files = append(files, plan.Update...)
		var done int64
		started := time.Now()
		for _, file := range files {
			r.OnFileStart(file.Path, file.Size)
			for sent := int64(0); sent < file.Size; {
				if err := ctx.Err(); err != nil {
					return err
				}
				step := min(file.Size-sent, 256<<10)
				sent += step
				done += step
				r.OnProgress(foldersync.Progress{Path: file.Path, FileDone: sent, FileTotal: file.Size, OverallDone: done, OverallTotal: plan.TransferBytes})
				time.Sleep(60 * time.Millisecond)
			}
			r.OnFileDone(file.Path, nil)
		}
		r.OnRoundDone(foldersync.Summary{Added: 2, Updated: 1, Deleted: 1, Skipped: 120, BytesSent: done, Duration: time.Since(started)})
		return nil
	}
	args := []string{"croc", "sync", "--delete"}
	if os.Getenv("CROC_SYNC_DEMO_DRY_RUN") != "" {
		args = append(args, "--dry-run")
	}
	err := newApp().RunContext(t.Context(), append(args, t.TempDir()))
	if err != nil && !errors.Is(err, foldersync.ErrAborted) {
		t.Fatal(err)
	}
}
