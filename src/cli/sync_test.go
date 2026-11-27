package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/schollz/croc/v11/src/foldersync"
	"github.com/schollz/croc/v11/src/termui"
	"github.com/stretchr/testify/require"
)

func stubSyncRunners(
	t *testing.T,
	host func(context.Context, foldersync.HostOptions) (*foldersync.Result, error),
	join func(context.Context, foldersync.JoinOptions) (*foldersync.Result, error),
) {
	t.Helper()
	origHost, origJoin := runSyncHost, runSyncJoin
	runSyncHost = host
	runSyncJoin = join
	t.Cleanup(func() {
		runSyncHost = origHost
		runSyncJoin = origJoin
	})
}

func assertExitCode(t *testing.T, err error, want int) {
	t.Helper()
	require.Error(t, err)
	var coded interface{ ExitCode() int }
	require.True(t, errors.As(err, &coded), "expected ExitError, got %T (%v)", err, err)
	require.Equal(t, want, coded.ExitCode())
}

func TestSyncRoleDetection(t *testing.T) {
	dir := t.TempDir()

	t.Run("source when CROC_SECRET is unset", func(t *testing.T) {
		unsetEnv(t, "CROC_SECRET")
		var calledHost, calledJoin bool
		var gotHost foldersync.HostOptions
		stubSyncRunners(t,
			func(_ context.Context, o foldersync.HostOptions) (*foldersync.Result, error) {
				calledHost = true
				gotHost = o
				return &foldersync.Result{}, nil
			},
			func(_ context.Context, _ foldersync.JoinOptions) (*foldersync.Result, error) {
				calledJoin = true
				return &foldersync.Result{}, nil
			},
		)

		err := newApp().Run([]string{
			"croc", "--relay", "127.0.0.1:9009", "--disable-clipboard",
			"sync", "--exclude", "TMP,*.bak", "--exclude-file", "build/out.bin", "--git", "--no-local", dir,
		})
		require.NoError(t, err)
		require.True(t, calledHost)
		require.False(t, calledJoin)
		require.Equal(t, dir, gotHost.Dir)
		require.Equal(t, []string{"tmp", "*.bak"}, gotHost.Filter.Exclude)
		require.Equal(t, []string{"build/out.bin"}, gotHost.Filter.ExcludeExact)
		require.True(t, gotHost.Filter.Git)
		require.True(t, gotHost.Croc.DisableLocal)
		require.NotEmpty(t, gotHost.Croc.SharedSecret)
		require.NotNil(t, gotHost.Croc.SendInstructions)
	})

	t.Run("destination when CROC_SECRET is set", func(t *testing.T) {
		t.Setenv("CROC_SECRET", "acid-acorn-acre")
		var calledHost, calledJoin bool
		var gotJoin foldersync.JoinOptions
		stubSyncRunners(t,
			func(_ context.Context, _ foldersync.HostOptions) (*foldersync.Result, error) {
				calledHost = true
				return &foldersync.Result{}, nil
			},
			func(_ context.Context, o foldersync.JoinOptions) (*foldersync.Result, error) {
				calledJoin = true
				gotJoin = o
				return &foldersync.Result{}, nil
			},
		)

		err := newApp().Run([]string{
			"croc", "--relay", "127.0.0.1:9009",
			"sync", "--delete", "--checksum", "--allow-empty-source", dir,
		})
		require.NoError(t, err)
		require.False(t, calledHost)
		require.True(t, calledJoin)
		require.Equal(t, dir, gotJoin.Dir)
		require.Equal(t, "acid-acorn-acre", gotJoin.Croc.SharedSecret)
		require.True(t, gotJoin.Diff.Delete)
		require.True(t, gotJoin.Diff.Checksum)
		require.True(t, gotJoin.Diff.AllowEmptySource)
		decision, decErr := gotJoin.Decide(&foldersync.Plan{})
		require.NoError(t, decErr)
		require.Equal(t, foldersync.Decision{Proceed: true, ConfirmDeletes: false}, decision)
	})

	t.Run("destination dry-run sets Proceed false", func(t *testing.T) {
		t.Setenv("CROC_SECRET", "acid-acorn-acre")
		var gotJoin foldersync.JoinOptions
		stubSyncRunners(t,
			func(_ context.Context, _ foldersync.HostOptions) (*foldersync.Result, error) {
				t.Fatal("host runner should not be called")
				return nil, nil
			},
			func(_ context.Context, o foldersync.JoinOptions) (*foldersync.Result, error) {
				gotJoin = o
				return &foldersync.Result{DryRun: true}, nil
			},
		)

		err := newApp().Run([]string{"croc", "--relay", "127.0.0.1:9009", "sync", "--dry-run", dir})
		require.NoError(t, err)
		decision, decErr := gotJoin.Decide(&foldersync.Plan{})
		require.NoError(t, decErr)
		require.Equal(t, foldersync.Decision{Proceed: false, ConfirmDeletes: false}, decision)
	})
}

func TestSyncWrongRoleFlags(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name      string
		envSecret string
		flagArgs  []string
		wantSub   string
	}{
		// Destination-only flags used on source (CROC_SECRET unset)
		{
			name:     "source rejects --delete",
			flagArgs: []string{"--delete"},
			wantSub:  "--delete only applies on the destination",
		},
		{
			name:     "source rejects --dry-run",
			flagArgs: []string{"--dry-run"},
			wantSub:  "--dry-run only applies on the destination",
		},
		{
			name:     "source rejects --checksum",
			flagArgs: []string{"--checksum"},
			wantSub:  "--checksum only applies on the destination",
		},
		{
			name:     "source rejects --allow-empty-source",
			flagArgs: []string{"--allow-empty-source"},
			wantSub:  "--allow-empty-source only applies on the destination",
		},
		// Source-only flags used on destination (CROC_SECRET set)
		{
			name:      "destination rejects --code",
			envSecret: "acid-acorn-acre",
			flagArgs:  []string{"--code", "other-secret"},
			wantSub:   "--code",
		},
		{
			name:      "destination rejects -c alias",
			envSecret: "acid-acorn-acre",
			flagArgs:  []string{"-c", "other-secret"},
			wantSub:   "--code",
		},
		{
			name:      "destination rejects --transport",
			envSecret: "acid-acorn-acre",
			flagArgs:  []string{"--transport", "relay"},
			wantSub:   "--transport only applies on the source",
		},
		{
			name:      "destination rejects --exclude",
			envSecret: "acid-acorn-acre",
			flagArgs:  []string{"--exclude", "*.tmp"},
			wantSub:   "--exclude only applies on the source",
		},
		{
			name:      "destination rejects --exclude-file",
			envSecret: "acid-acorn-acre",
			flagArgs:  []string{"--exclude-file", "a.txt"},
			wantSub:   "--exclude-file only applies on the source",
		},
		{
			name:      "destination rejects --git",
			envSecret: "acid-acorn-acre",
			flagArgs:  []string{"--git"},
			wantSub:   "--git only applies on the source",
		},
		{
			name:      "destination rejects --qrcode",
			envSecret: "acid-acorn-acre",
			flagArgs:  []string{"--qrcode"},
			wantSub:   "--qrcode only applies on the source",
		},
		{
			name:      "destination rejects --qr alias",
			envSecret: "acid-acorn-acre",
			flagArgs:  []string{"--qr"},
			wantSub:   "--qrcode only applies on the source",
		},
		{
			name:      "destination rejects --watch",
			envSecret: "acid-acorn-acre",
			flagArgs:  []string{"--watch"},
			wantSub:   "--watch only applies on the source",
		},
		{
			name:      "destination rejects --no-local",
			envSecret: "acid-acorn-acre",
			flagArgs:  []string{"--no-local"},
			wantSub:   "--no-local only applies on the source",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envSecret == "" {
				unsetEnv(t, "CROC_SECRET")
			} else {
				t.Setenv("CROC_SECRET", tt.envSecret)
			}
			stubSyncRunners(t,
				func(_ context.Context, _ foldersync.HostOptions) (*foldersync.Result, error) {
					t.Fatal("host runner must not be called on invalid flags")
					return nil, nil
				},
				func(_ context.Context, _ foldersync.JoinOptions) (*foldersync.Result, error) {
					t.Fatal("join runner must not be called on invalid flags")
					return nil, nil
				},
			)

			args := append([]string{"croc", "sync"}, tt.flagArgs...)
			args = append(args, dir)
			err := newApp().Run(args)
			assertExitCode(t, err, 1)
			require.Contains(t, err.Error(), tt.wantSub)
		})
	}
}

func TestSyncUnavailableFlagsOnSource(t *testing.T) {
	dir := t.TempDir()
	unsetEnv(t, "CROC_SECRET")
	stubSyncRunners(t,
		func(_ context.Context, _ foldersync.HostOptions) (*foldersync.Result, error) {
			t.Fatal("host runner must not be called")
			return nil, nil
		},
		func(_ context.Context, _ foldersync.JoinOptions) (*foldersync.Result, error) {
			t.Fatal("join runner must not be called")
			return nil, nil
		},
	)

	tests := []struct {
		name string
		flag string
		want string
	}{
		{name: "watch", flag: "--watch", want: "--watch is not available yet"},
		{name: "qrcode", flag: "--qrcode", want: "--qrcode needs web sync, which is not available yet"},
		{name: "qr alias", flag: "--qr", want: "--qrcode needs web sync, which is not available yet"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newApp().Run([]string{"croc", "sync", tt.flag, dir})
			assertExitCode(t, err, 1)
			require.EqualError(t, err, tt.want)
		})
	}
}

func TestSyncFolderArguments(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "plain.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("hello"), 0o600))
	missingDir := filepath.Join(dir, "does-not-exist")

	for _, role := range []struct {
		name      string
		envSecret string
	}{
		{name: "source", envSecret: ""},
		{name: "destination", envSecret: "acid-acorn-acre"},
	} {
		t.Run(role.name, func(t *testing.T) {
			if role.envSecret == "" {
				unsetEnv(t, "CROC_SECRET")
			} else {
				t.Setenv("CROC_SECRET", role.envSecret)
			}
			stubSyncRunners(t,
				func(_ context.Context, _ foldersync.HostOptions) (*foldersync.Result, error) {
					t.Fatal("host runner must not be called")
					return nil, nil
				},
				func(_ context.Context, _ foldersync.JoinOptions) (*foldersync.Result, error) {
					t.Fatal("join runner must not be called")
					return nil, nil
				},
			)

			t.Run("missing folder arg", func(t *testing.T) {
				err := newApp().Run([]string{"croc", "sync"})
				assertExitCode(t, err, 1)
				require.Contains(t, err.Error(), "croc sync <folder>")
			})

			t.Run("extra folder args", func(t *testing.T) {
				err := newApp().Run([]string{"croc", "sync", dir, dir})
				assertExitCode(t, err, 1)
				require.Contains(t, err.Error(), "expected exactly one folder")
			})

			t.Run("non-existent folder is rejected and not created", func(t *testing.T) {
				err := newApp().Run([]string{"croc", "sync", missingDir})
				assertExitCode(t, err, 1)
				require.Contains(t, err.Error(), "does not exist")
				_, statErr := os.Stat(missingDir)
				require.True(t, os.IsNotExist(statErr), "sync must not create missing folder")
			})

			t.Run("file instead of directory", func(t *testing.T) {
				err := newApp().Run([]string{"croc", "sync", filePath})
				assertExitCode(t, err, 1)
				require.Contains(t, err.Error(), "not a directory")
			})
		})
	}
}

func TestSyncExitCodes(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CROC_SECRET", "acid-acorn-acre")

	t.Run("success returns nil", func(t *testing.T) {
		stubSyncRunners(t, nil, func(_ context.Context, _ foldersync.JoinOptions) (*foldersync.Result, error) {
			return &foldersync.Result{Added: 2, Unchanged: 1}, nil
		})
		err := newApp().Run([]string{"croc", "--relay", "127.0.0.1:9009", "sync", dir})
		require.NoError(t, err)
	})

	t.Run("partial failures return exit code 2", func(t *testing.T) {
		stubSyncRunners(t, nil, func(_ context.Context, _ foldersync.JoinOptions) (*foldersync.Result, error) {
			return &foldersync.Result{
				Added:    1,
				Failures: []string{"a.txt: permission denied", "b.txt: locked"},
			}, nil
		})
		err := newApp().Run([]string{"croc", "--relay", "127.0.0.1:9009", "sync", dir})
		assertExitCode(t, err, 2)
		require.Contains(t, err.Error(), "2 item(s) failed")
		require.Contains(t, err.Error(), "a.txt: permission denied")
	})

	t.Run("deletes held return exit code 2", func(t *testing.T) {
		stubSyncRunners(t, nil, func(_ context.Context, _ foldersync.JoinOptions) (*foldersync.Result, error) {
			return &foldersync.Result{
				DeletesHeld:   3,
				DeleteBlocked: "deletes exceed 50% threshold",
			}, nil
		})
		err := newApp().Run([]string{"croc", "--relay", "127.0.0.1:9009", "sync", dir})
		assertExitCode(t, err, 2)
		require.Contains(t, err.Error(), "3 deletion(s) held")
	})

	t.Run("error returns exit code 1", func(t *testing.T) {
		boom := errors.New("relay unreachable")
		stubSyncRunners(t, nil, func(_ context.Context, _ foldersync.JoinOptions) (*foldersync.Result, error) {
			return nil, boom
		})
		err := newApp().Run([]string{"croc", "--relay", "127.0.0.1:9009", "sync", dir})
		assertExitCode(t, err, 1)
		require.ErrorIs(t, err, boom)
	})

	t.Run("cancellation returns exit code 1", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stubSyncRunners(t, nil, func(runCtx context.Context, _ foldersync.JoinOptions) (*foldersync.Result, error) {
			return nil, runCtx.Err()
		})
		err := newApp().RunContext(ctx, []string{"croc", "--relay", "127.0.0.1:9009", "sync", dir})
		assertExitCode(t, err, 1)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("RunContext preserves ExitError on canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stubSyncRunners(t, nil, func(runCtx context.Context, _ foldersync.JoinOptions) (*foldersync.Result, error) {
			return nil, runCtx.Err()
		})
		origArgs := os.Args
		os.Args = []string{"croc", "--relay", "127.0.0.1:9009", "sync", dir}
		t.Cleanup(func() { os.Args = origArgs })

		err := RunContext(ctx)
		assertExitCode(t, err, 1)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestFormatSyncInstructions(t *testing.T) {
	plain := formatSyncInstructions("alpha-beta-gamma", "", false)
	require.Contains(t, plain, "On the other computer, run:")
	require.Contains(t, plain, "CROC_SECRET='alpha-beta-gamma' croc sync <folder>")
	require.Contains(t, plain, "$env:CROC_SECRET='alpha-beta-gamma'; croc sync <folder>")
	require.False(t, strings.Contains(plain, "\x1b"))

	colored := formatSyncInstructions("alpha-beta-gamma", "", true)
	require.Equal(t, plain, termui.Plain(colored))

	withRelay := formatSyncInstructions("alpha-beta-gamma", "--relay '127.0.0.1:9009' --pass 'pw' ", false)
	require.Contains(t, withRelay, "CROC_SECRET='alpha-beta-gamma' croc --relay '127.0.0.1:9009' --pass 'pw' sync <folder>")
	require.Contains(t, withRelay, "$env:CROC_SECRET='alpha-beta-gamma'; croc --relay '127.0.0.1:9009' --pass 'pw' sync <folder>")

	escaped := formatSyncInstructions("don't-panic", "", false)
	require.Contains(t, escaped, `CROC_SECRET='don'\''t-panic'`)
	require.Contains(t, escaped, `$env:CROC_SECRET='don''t-panic'`)
}

func TestSyncReporterWarnings(t *testing.T) {
	var buf bytes.Buffer
	rep := newSyncReporter(&buf, false)
	rep.Report(foldersync.Event{Kind: foldersync.EventWarning, Message: "skipped symlink foo"})
	rep.Report(foldersync.Event{Kind: foldersync.EventScanned})
	require.Equal(t, "warning: skipped symlink foo\n", buf.String())

	buf.Reset()
	quietRep := newSyncReporter(&buf, true)
	quietRep.Report(foldersync.Event{Kind: foldersync.EventWarning, Message: "skipped symlink foo"})
	require.Empty(t, buf.String())
}
