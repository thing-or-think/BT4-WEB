package cli

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/foldersync"
	"github.com/schollz/croc/v11/src/tcp"
	"github.com/stretchr/testify/require"
)

func startCLITestRelay(t *testing.T) string {
	t.Helper()

	listeners := make([]net.Listener, 0, 5)
	ports := make([]string, 0, 5)
	for range 5 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners = append(listeners, l)
		_, port, err := net.SplitHostPort(l.Addr().String())
		require.NoError(t, err)
		ports = append(ports, port)
	}
	for _, l := range listeners {
		_ = l.Close()
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	start := func(port string, banner ...string) {
		wg.Go(func() {
			_ = tcp.RunCtx(ctx, "warn", "127.0.0.1", port, "pass123", banner...)
		})
	}
	start(ports[0], strings.Join(ports[1:], ","))
	for _, p := range ports[1:] {
		start(p)
	}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	deadline := time.Now().Add(5 * time.Second)
	for _, p := range ports {
		for {
			conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", p), 50*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("relay port %s not ready", p)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	return net.JoinHostPort("127.0.0.1", ports[0])
}

type cliSyncRoundResult struct {
	hostRes *foldersync.Result
	joinRes *foldersync.Result
	hostErr error
	joinErr error
	stderr  string
}

// runCLISyncRound runs source and destination through the real CLI app against
// a local relay. To let CROC_SECRET decide roles without racing across the two
// in-process goroutines, the source pauses at runSyncHost until CROC_SECRET is
// set and the destination has entered runSyncJoin.
func runCLISyncRound(t *testing.T, addr, code, srcDir, dstDir string, dstGlobalFlags, dstSyncFlags []string) cliSyncRoundResult {
	t.Helper()

	unsetEnv(t, "CROC_SECRET")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rPipe, wPipe, err := os.Pipe()
	require.NoError(t, err)
	origStderr := os.Stderr
	os.Stderr = wPipe
	var stderrBuf bytes.Buffer
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		_, _ = io.Copy(&stderrBuf, rPipe)
	}()
	restoreStderr := func() {
		if os.Stderr == wPipe {
			os.Stderr = origStderr
			_ = wPipe.Close()
			<-stderrDone
			_ = rPipe.Close()
		}
	}
	defer restoreStderr()

	var res cliSyncRoundResult
	hostEntered := make(chan struct{})
	destEntered := make(chan struct{})
	var hostOnce, destOnce sync.Once
	var destReachedJoin atomic.Bool

	origHost, origJoin := runSyncHost, runSyncJoin
	defer func() {
		runSyncHost = origHost
		runSyncJoin = origJoin
	}()

	runSyncHost = func(runCtx context.Context, o foldersync.HostOptions) (*foldersync.Result, error) {
		hostOnce.Do(func() { close(hostEntered) })
		select {
		case <-destEntered:
		case <-runCtx.Done():
			return nil, runCtx.Err()
		}
		r, runErr := origHost(runCtx, o)
		res.hostRes = r
		return r, runErr
	}

	runSyncJoin = func(runCtx context.Context, o foldersync.JoinOptions) (*foldersync.Result, error) {
		destReachedJoin.Store(true)
		destOnce.Do(func() { close(destEntered) })
		select {
		case <-time.After(100 * time.Millisecond):
		case <-runCtx.Done():
			return nil, runCtx.Err()
		}
		r, runErr := origJoin(runCtx, o)
		res.joinRes = r
		return r, runErr
	}

	srcArgs := []string{
		"croc", "--relay", addr, "--pass", "pass123",
		"--no-update-check", "--disable-clipboard",
		"sync", "--code", code, "--no-local", "--transport", "relay",
		srcDir,
	}
	dstArgs := []string{
		"croc", "--relay", addr, "--pass", "pass123",
		"--no-update-check", "--disable-clipboard",
	}
	dstArgs = append(dstArgs, dstGlobalFlags...)
	dstArgs = append(dstArgs, "sync")
	dstArgs = append(dstArgs, dstSyncFlags...)
	dstArgs = append(dstArgs, dstDir)

	hostDone := make(chan error, 1)
	go func() {
		hostDone <- newApp().RunContext(ctx, srcArgs)
	}()

	select {
	case <-hostEntered:
	case hostErr := <-hostDone:
		restoreStderr()
		t.Fatalf("source exited before reaching RunSyncHost: %v\nstderr:\n%s", hostErr, stderrBuf.String())
	case <-ctx.Done():
		cancel()
		<-hostDone
		restoreStderr()
		t.Fatalf("timed out waiting for source to reach RunSyncHost\nstderr:\n%s", stderrBuf.String())
	}

	require.NoError(t, os.Setenv("CROC_SECRET", code))

	dstDone := make(chan error, 1)
	go func() {
		defer func() {
			if !destReachedJoin.Load() {
				cancel()
				destOnce.Do(func() { close(destEntered) })
			}
		}()
		dstDone <- newApp().RunContext(ctx, dstArgs)
	}()

	res.hostErr = <-hostDone
	res.joinErr = <-dstDone
	restoreStderr()
	res.stderr = stderrBuf.String()

	require.NoError(t, ctx.Err(), "CLI sync round timed out: host=%v join=%v\nstderr:\n%s", res.hostErr, res.joinErr, res.stderr)
	return res
}

func writeSyncTestFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(rel))
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(t, os.WriteFile(full, []byte(content), 0o600))
}

func snapshotSyncTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if p == dir {
			return nil
		}
		if strings.HasPrefix(d.Name(), foldersync.StagingDirPrefix) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if d.IsDir() {
			out[key] = "<dir>"
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		out[key] = "file:" + string(data)
		return nil
	})
	require.NoError(t, err)
	return out
}

func TestCLISyncIntegration(t *testing.T) {
	relayAddr := startCLITestRelay(t)

	cfgDir := t.TempDir()
	t.Setenv("CROC_CONFIG_DIR", cfgDir)
	require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "classic_enabled"), []byte("enabled"), 0o600))

	origIsTerm := syncStdinIsTerminal
	syncStdinIsTerminal = func() bool { return false }
	t.Cleanup(func() { syncStdinIsTerminal = origIsTerm })

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeSyncTestFile(t, srcDir, "hello.txt", "hello croc sync\n")
	writeSyncTestFile(t, srcDir, "nested/sub.txt", "nested content")
	writeSyncTestFile(t, srcDir, "nested/deep/data.bin", strings.Repeat("abcdef0123456789", 1024))
	require.NoError(t, os.MkdirAll(filepath.Join(srcDir, "empty"), 0o755))

	t.Run("full copy then trees equal", func(t *testing.T) {
		rr := runCLISyncRound(t, relayAddr, "8901-cli-sync-full", srcDir, dstDir, nil, nil)
		require.NoError(t, rr.hostErr)
		require.NoError(t, rr.joinErr)
		require.NotNil(t, rr.joinRes)
		require.Equal(t, snapshotSyncTree(t, srcDir), snapshotSyncTree(t, dstDir))
		require.Contains(t, rr.stderr, "\r\nPlan: ")
	})

	t.Run("second run prints nothing changed and copies zero files", func(t *testing.T) {
		rr := runCLISyncRound(t, relayAddr, "8902-cli-sync-noop", srcDir, dstDir, nil, nil)
		require.NoError(t, rr.hostErr)
		require.NoError(t, rr.joinErr)
		require.NotNil(t, rr.joinRes)
		require.Equal(t, 0, rr.joinRes.Added)
		require.Equal(t, 0, rr.joinRes.Updated)
		require.Equal(t, 0, rr.joinRes.Touched)
		require.Equal(t, 0, rr.joinRes.Deleted)
		require.Equal(t, int64(0), rr.joinRes.Bytes)
		require.Contains(t, rr.stderr, "Already in sync.")
	})

	writeSyncTestFile(t, dstDir, "extra.txt", "only on destination\n")

	t.Run("dry-run with delete leaves destination byte-identical", func(t *testing.T) {
		before := snapshotSyncTree(t, dstDir)
		rr := runCLISyncRound(t, relayAddr, "8903-cli-sync-dry", srcDir, dstDir, nil, []string{"--dry-run", "--delete"})
		require.NoError(t, rr.hostErr)
		require.NoError(t, rr.joinErr)
		require.NotNil(t, rr.joinRes)
		require.True(t, rr.joinRes.DryRun)
		require.Contains(t, rr.stderr, "\r\nPlan: ")
		require.Contains(t, rr.stderr, "Dry run: nothing was changed.")
		require.Equal(t, before, snapshotSyncTree(t, dstDir))
	})

	t.Run("delete without yes on non-terminal refuses with exit code 1", func(t *testing.T) {
		before := snapshotSyncTree(t, dstDir)
		rr := runCLISyncRound(t, relayAddr, "8904-cli-sync-refuse", srcDir, dstDir, nil, []string{"--delete"})
		assertExitCode(t, rr.joinErr, 1)
		require.Contains(t, rr.joinErr.Error(), "without --yes")
		require.Equal(t, before, snapshotSyncTree(t, dstDir))
	})

	t.Run("global yes with delete removes extra file", func(t *testing.T) {
		rr := runCLISyncRound(t, relayAddr, "8905-cli-sync-delete", srcDir, dstDir, []string{"--yes"}, []string{"--delete"})
		require.NoError(t, rr.hostErr)
		require.NoError(t, rr.joinErr)
		require.NotNil(t, rr.joinRes)
		require.Equal(t, 1, rr.joinRes.Deleted)
		_, statErr := os.Stat(filepath.Join(dstDir, "extra.txt"))
		require.True(t, os.IsNotExist(statErr))
		require.Equal(t, snapshotSyncTree(t, srcDir), snapshotSyncTree(t, dstDir))
	})
}

func TestCLISyncWatchIntegration(t *testing.T) {
	relayAddr := startCLITestRelay(t)

	cfgDir := t.TempDir()
	t.Setenv("CROC_CONFIG_DIR", cfgDir)
	require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "classic_enabled"), []byte("enabled"), 0o600))
	unsetEnv(t, "CROC_SECRET")

	origIsTerm := syncStdinIsTerminal
	syncStdinIsTerminal = func() bool { return false }
	t.Cleanup(func() { syncStdinIsTerminal = origIsTerm })

	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeSyncTestFile(t, srcDir, "hello.txt", "initial watch content\n")

	overallCtx, overallCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer overallCancel()
	srcCtx, srcCancel := context.WithCancel(overallCtx)
	defer srcCancel()
	dstCtx, dstCancel := context.WithCancel(overallCtx)
	defer dstCancel()

	rPipe, wPipe, err := os.Pipe()
	require.NoError(t, err)
	origStderr := os.Stderr
	os.Stderr = wPipe
	var stderrBuf bytes.Buffer
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		_, _ = io.Copy(&stderrBuf, rPipe)
	}()
	restoreStderr := func() {
		if os.Stderr == wPipe {
			os.Stderr = origStderr
			_ = wPipe.Close()
			<-stderrDone
			_ = rPipe.Close()
		}
	}
	defer restoreStderr()

	hostEntered := make(chan struct{})
	destEntered := make(chan struct{})
	hostRound1Done := make(chan struct{})
	joinRound1Done := make(chan struct{})
	var hostOnce, destOnce, hostRound1Once, joinRound1Once sync.Once
	var destReachedJoin atomic.Bool
	var joinCommitted atomic.Int64

	origHost, origJoin := runSyncHost, runSyncJoin
	defer func() {
		runSyncHost = origHost
		runSyncJoin = origJoin
	}()

	runSyncHost = func(runCtx context.Context, o foldersync.HostOptions) (*foldersync.Result, error) {
		origRep := o.Reporter
		o.Reporter = foldersync.ReporterFunc(func(e foldersync.Event) {
			if origRep != nil {
				origRep.Report(e)
			}
			if e.Kind == foldersync.EventResult && e.Result != nil {
				hostRound1Once.Do(func() { close(hostRound1Done) })
			}
		})
		hostOnce.Do(func() { close(hostEntered) })
		select {
		case <-destEntered:
		case <-runCtx.Done():
			return nil, runCtx.Err()
		}
		return origHost(runCtx, o)
	}

	runSyncJoin = func(runCtx context.Context, o foldersync.JoinOptions) (*foldersync.Result, error) {
		origRep := o.Reporter
		o.Reporter = foldersync.ReporterFunc(func(e foldersync.Event) {
			if origRep != nil {
				origRep.Report(e)
			}
			if e.Kind == foldersync.EventCommitted && e.Result != nil {
				joinCommitted.Add(1)
				joinRound1Once.Do(func() { close(joinRound1Done) })
			}
		})
		destReachedJoin.Store(true)
		destOnce.Do(func() { close(destEntered) })
		select {
		case <-time.After(100 * time.Millisecond):
		case <-runCtx.Done():
			return nil, runCtx.Err()
		}
		return origJoin(runCtx, o)
	}

	const code = "8910-cli-sync-watch"
	srcArgs := []string{
		"croc", "--relay", relayAddr, "--pass", "pass123",
		"--no-update-check", "--disable-clipboard",
		"sync", "--code", code, "--no-local", "--transport", "relay", "--watch",
		srcDir,
	}
	dstArgs := []string{
		"croc", "--relay", relayAddr, "--pass", "pass123",
		"--no-update-check", "--disable-clipboard",
		"sync", dstDir,
	}

	hostDone := make(chan error, 1)
	go func() {
		hostDone <- newApp().RunContext(srcCtx, srcArgs)
	}()

	select {
	case <-hostEntered:
	case hostErr := <-hostDone:
		restoreStderr()
		t.Fatalf("source exited before reaching RunSyncHost: %v\nstderr:\n%s", hostErr, stderrBuf.String())
	case <-overallCtx.Done():
		srcCancel()
		<-hostDone
		restoreStderr()
		t.Fatalf("timed out waiting for source to reach RunSyncHost\nstderr:\n%s", stderrBuf.String())
	}

	require.NoError(t, os.Setenv("CROC_SECRET", code))

	dstDone := make(chan error, 1)
	go func() {
		defer func() {
			if !destReachedJoin.Load() {
				srcCancel()
				destOnce.Do(func() { close(destEntered) })
			}
		}()
		dstDone <- newApp().RunContext(dstCtx, dstArgs)
	}()

	stopAndDrain := func() (error, error) {
		srcCancel()
		dstCancel()
		var hostErr, joinErr error
		var gotHost, gotJoin bool
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		for !gotHost || !gotJoin {
			select {
			case hostErr = <-hostDone:
				gotHost = true
				hostDone = nil
			case joinErr = <-dstDone:
				gotJoin = true
				dstDone = nil
			case <-timer.C:
				restoreStderr()
				t.Fatalf("watch processes did not exit after cancel (gotHost=%v gotJoin=%v)\nstderr:\n%s", gotHost, gotJoin, stderrBuf.String())
			}
		}
		restoreStderr()
		return hostErr, joinErr
	}

	waitCond := func(desc string, timeout time.Duration, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for {
			if cond() {
				return
			}
			select {
			case err := <-hostDone:
				hostDone = nil
				srcCancel()
				dstCancel()
				if dstDone != nil {
					<-dstDone
					dstDone = nil
				}
				restoreStderr()
				t.Fatalf("source exited early while waiting for %s: %v\nstderr:\n%s", desc, err, stderrBuf.String())
			case err := <-dstDone:
				dstDone = nil
				srcCancel()
				dstCancel()
				if hostDone != nil {
					<-hostDone
					hostDone = nil
				}
				restoreStderr()
				t.Fatalf("destination exited early while waiting for %s: %v\nstderr:\n%s", desc, err, stderrBuf.String())
			case <-overallCtx.Done():
				hostErr, joinErr := stopAndDrain()
				t.Fatalf("overall timeout while waiting for %s (host=%v join=%v)\nstderr:\n%s", desc, hostErr, joinErr, stderrBuf.String())
			default:
			}
			if time.Now().After(deadline) {
				hostErr, joinErr := stopAndDrain()
				t.Fatalf("timed out after %s waiting for %s (host=%v join=%v)\nstderr:\n%s", timeout, desc, hostErr, joinErr, stderrBuf.String())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	waitCond("initial watch sync", 15*time.Second, func() bool {
		select {
		case <-hostRound1Done:
		default:
			return false
		}
		select {
		case <-joinRound1Done:
		default:
			return false
		}
		data, readErr := os.ReadFile(filepath.Join(dstDir, "hello.txt"))
		return readErr == nil && string(data) == "initial watch content\n"
	})

	writeSyncTestFile(t, srcDir, "hello.txt", "modified in watch mode\n")
	writeSyncTestFile(t, srcDir, "sub/added.txt", "brand new file\n")

	waitCond("modified and created files synced", 5*time.Second, func() bool {
		if joinCommitted.Load() < 2 {
			return false
		}
		d1, err1 := os.ReadFile(filepath.Join(dstDir, "hello.txt"))
		d2, err2 := os.ReadFile(filepath.Join(dstDir, "sub", "added.txt"))
		return err1 == nil && string(d1) == "modified in watch mode\n" &&
			err2 == nil && string(d2) == "brand new file\n"
	})

	hostErr, joinErr := stopAndDrain()
	stderr := stderrBuf.String()

	require.NoError(t, overallCtx.Err(), "watch integration test timed out\nstderr:\n%s", stderr)
	require.NoError(t, hostErr, "source watch returned error\nstderr:\n%s", stderr)
	require.NoError(t, joinErr, "destination watch returned error\nstderr:\n%s", stderr)
	require.Equal(t, snapshotSyncTree(t, srcDir), snapshotSyncTree(t, dstDir))
	require.Contains(t, stderr, "Watching "+srcDir+" for changes (Ctrl+C to stop).")
	require.Contains(t, stderr, "Round 1:")
	require.Contains(t, stderr, "Round 2:")
	require.Contains(t, stderr, "Stopped watching.")
}
