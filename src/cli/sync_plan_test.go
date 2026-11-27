package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/schollz/croc/v11/src/foldersync"
	"github.com/schollz/croc/v11/src/receivefs"
	"github.com/schollz/croc/v11/src/termui"
	"github.com/stretchr/testify/require"
)

func fileEntry(p string, size int64) *foldersync.Entry {
	return &foldersync.Entry{Path: p, Size: size, Kind: receivefs.KindFile}
}

func dirEntry(p string) *foldersync.Entry {
	return &foldersync.Entry{Path: p, Kind: receivefs.KindDirectory}
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) {
	return 0, errors.New("unexpected read from stdin")
}

func TestDecideSync(t *testing.T) {
	simpleAddPlan := func() *foldersync.Plan {
		return &foldersync.Plan{
			Add:  []foldersync.Change{{Path: "a.txt", Src: fileEntry("a.txt", 10)}},
			Skip: []foldersync.Change{{Path: "same.txt", Src: fileEntry("same.txt", 5), Dst: fileEntry("same.txt", 5)}},
		}
	}
	deletePlan := func() *foldersync.Plan {
		return &foldersync.Plan{
			Add:    []foldersync.Change{{Path: "a.txt", Src: fileEntry("a.txt", 10)}},
			Delete: []foldersync.Change{{Path: "old1.txt", Dst: fileEntry("old1.txt", 1)}, {Path: "old2.txt", Dst: fileEntry("old2.txt", 2)}},
		}
	}
	confirmDeletePlan := func() *foldersync.Plan {
		return &foldersync.Plan{
			Add: []foldersync.Change{{Path: "a.txt", Src: fileEntry("a.txt", 10)}},
			Delete: []foldersync.Change{
				{Path: "dir/old1.txt", Dst: fileEntry("dir/old1.txt", 1)},
				{Path: "dir/old2.txt", Dst: fileEntry("dir/old2.txt", 2)},
				{Path: "dir", Dst: dirEntry("dir")},
			},
			RequireDeleteConfirm: true,
		}
	}

	tests := []struct {
		name           string
		plan           *foldersync.Plan
		opts           syncDecideOptions
		input          string
		forbidRead     bool
		wantDecision   foldersync.Decision
		wantErrSub     string
		wantPrefix     string
		wantEmpty      bool
		wantOutSubs    []string
		wantNotOutSubs []string
	}{
		{
			name:         "dry-run prints table and dry-run line and returns Proceed false",
			plan:         simpleAddPlan(),
			opts:         syncDecideOptions{DryRun: true},
			forbidRead:   true,
			wantDecision: foldersync.Decision{Proceed: false},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs: []string{
				"Plan: 1 to add, 0 to update, 0 to touch, 0 to delete, 0 conflicts, 1 unchanged",
				"  + a.txt",
				"Dry run: nothing was changed.",
			},
		},
		{
			name:         "dry-run prints table even when quiet",
			plan:         simpleAddPlan(),
			opts:         syncDecideOptions{DryRun: true, Quiet: true},
			forbidRead:   true,
			wantDecision: foldersync.Decision{Proceed: false},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs: []string{
				"Plan: 1 to add, 0 to update, 0 to touch, 0 to delete, 0 conflicts, 1 unchanged",
				"  + a.txt",
				"Dry run: nothing was changed.",
			},
		},
		{
			name: "already in sync prints message and returns Proceed true",
			plan: &foldersync.Plan{
				Skip: []foldersync.Change{{Path: "keep.txt", Src: fileEntry("keep.txt", 1), Dst: fileEntry("keep.txt", 1)}},
			},
			opts:         syncDecideOptions{},
			forbidRead:   true,
			wantDecision: foldersync.Decision{Proceed: true},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs: []string{
				"Plan: 0 to add, 0 to update, 0 to touch, 0 to delete, 0 conflicts, 1 unchanged",
				"Already in sync.",
			},
		},
		{
			name:           "already in sync is silent when quiet",
			plan:           &foldersync.Plan{},
			opts:           syncDecideOptions{Quiet: true},
			forbidRead:     true,
			wantDecision:   foldersync.Decision{Proceed: true},
			wantEmpty:      true,
			wantNotOutSubs: []string{"Plan:", "Already in sync."},
		},
		{
			name:         "no deletes proceeds without prompting",
			plan:         simpleAddPlan(),
			opts:         syncDecideOptions{IsTerminal: true},
			forbidRead:   true,
			wantDecision: foldersync.Decision{Proceed: true},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs:  []string{"Plan: 1 to add", "  + a.txt"},
		},
		{
			name:         "deletes with --yes proceeds without prompting",
			plan:         deletePlan(),
			opts:         syncDecideOptions{Yes: true, IsTerminal: false},
			forbidRead:   true,
			wantDecision: foldersync.Decision{Proceed: true},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs:  []string{"  - old1.txt", "  - old2.txt"},
		},
		{
			name:         "deletes on terminal answered y proceeds",
			plan:         deletePlan(),
			opts:         syncDecideOptions{IsTerminal: true},
			input:        "y\n",
			wantDecision: foldersync.Decision{Proceed: true},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs:  []string{"Apply these changes, including 2 deletion(s)? [y/N] "},
		},
		{
			name:         "deletes on terminal answered yes proceeds",
			plan:         deletePlan(),
			opts:         syncDecideOptions{IsTerminal: true},
			input:        "YES\n",
			wantDecision: foldersync.Decision{Proceed: true},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs:  []string{"Apply these changes, including 2 deletion(s)? [y/N] "},
		},
		{
			name:         "deletes on terminal answered n cancels",
			plan:         deletePlan(),
			opts:         syncDecideOptions{IsTerminal: true},
			input:        "n\n",
			wantDecision: foldersync.Decision{Proceed: false},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs: []string{
				"Apply these changes, including 2 deletion(s)? [y/N] ",
				"Nothing was changed.",
			},
		},
		{
			name:         "deletes on terminal with empty answer cancels",
			plan:         deletePlan(),
			opts:         syncDecideOptions{IsTerminal: true},
			input:        "\n",
			wantDecision: foldersync.Decision{Proceed: false},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs:  []string{"Nothing was changed."},
		},
		{
			name:       "deletes without terminal and without --yes returns error",
			plan:       deletePlan(),
			opts:       syncDecideOptions{IsTerminal: false, Yes: false},
			forbidRead: true,
			wantErrSub: "refusing to delete 2 item(s) without --yes: stdin is not a terminal",
			wantPrefix: "\r\nPlan: ",
		},
		{
			name:         "watch mode deletes without terminal and without --yes holds deletes",
			plan:         deletePlan(),
			opts:         syncDecideOptions{Watch: true, IsTerminal: false, Yes: false},
			forbidRead:   true,
			wantDecision: foldersync.Decision{Proceed: true, ConfirmDeletes: false},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs:  []string{"Deletions held: rerun with --yes to allow deletes in watch mode."},
		},
		{
			name:         "watch mode deletes without terminal and without --yes is silent when quiet",
			plan:         deletePlan(),
			opts:         syncDecideOptions{Watch: true, Quiet: true, IsTerminal: false, Yes: false},
			forbidRead:   true,
			wantDecision: foldersync.Decision{Proceed: true, ConfirmDeletes: false},
			wantEmpty:    true,
		},
		{
			name:         "over 50% deletes with --yes and terminal typing yes confirms deletes",
			plan:         confirmDeletePlan(),
			opts:         syncDecideOptions{Yes: true, IsTerminal: true},
			input:        "yes\n",
			wantDecision: foldersync.Decision{Proceed: true, ConfirmDeletes: true},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs: []string{
				"This deletes 2 of the destination files (more than half). Type yes to delete them: ",
			},
			wantNotOutSubs: []string{"Deletions held"},
		},
		{
			name:         "over 50% deletes with --yes and terminal typing y holds deletes",
			plan:         confirmDeletePlan(),
			opts:         syncDecideOptions{Yes: true, IsTerminal: true},
			input:        "y\n",
			wantDecision: foldersync.Decision{Proceed: true, ConfirmDeletes: false},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs: []string{
				"This deletes 2 of the destination files (more than half). Type yes to delete them: ",
				"Deletions held; other changes will be applied.",
			},
		},
		{
			name:         "over 50% deletes without terminal holds deletes even with --yes",
			plan:         confirmDeletePlan(),
			opts:         syncDecideOptions{Yes: true, IsTerminal: false},
			forbidRead:   true,
			wantDecision: foldersync.Decision{Proceed: true, ConfirmDeletes: false},
			wantPrefix:   "\r\nPlan: ",
			wantOutSubs:  []string{"Deletions held; other changes will be applied."},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			opts := tt.opts
			opts.Out = &out
			if tt.forbidRead {
				opts.In = failReader{}
			} else {
				opts.In = strings.NewReader(tt.input)
			}

			got, err := decideSync(tt.plan, opts)
			if tt.wantErrSub != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErrSub)
				if tt.wantPrefix != "" {
					require.True(t, strings.HasPrefix(out.String(), tt.wantPrefix), "output %q does not start with %q", out.String(), tt.wantPrefix)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantDecision, got)
			if tt.wantEmpty {
				require.Empty(t, out.String())
			}
			if tt.wantPrefix != "" {
				require.True(t, strings.HasPrefix(out.String(), tt.wantPrefix), "output %q does not start with %q", out.String(), tt.wantPrefix)
			}
			for _, sub := range tt.wantOutSubs {
				require.Contains(t, out.String(), sub)
			}
			for _, sub := range tt.wantNotOutSubs {
				require.NotContains(t, out.String(), sub)
			}
		})
	}
}

func TestFormatSyncPlanFormattingAndTruncation(t *testing.T) {
	t.Run("all groups, markers, directory slashes, type changes, conflicts, delete blocked", func(t *testing.T) {
		plan := &foldersync.Plan{
			Add: []foldersync.Change{
				{Path: "new.txt", Src: fileEntry("new.txt", 10)},
				{Path: "newdir", Src: dirEntry("newdir")},
			},
			Update: []foldersync.Change{
				{Path: "mod.txt", Src: fileEntry("mod.txt", 20), Dst: fileEntry("mod.txt", 10)},
				{Path: "wasfile", Src: dirEntry("wasfile"), Dst: fileEntry("wasfile", 5), TypeChanged: true},
				{Path: "wasdir", Src: fileEntry("wasdir", 5), Dst: dirEntry("wasdir"), TypeChanged: true},
			},
			Touch: []foldersync.Change{
				{Path: "meta.txt", Src: fileEntry("meta.txt", 1), Dst: fileEntry("meta.txt", 1)},
				{Path: "metadir", Src: dirEntry("metadir"), Dst: dirEntry("metadir")},
			},
			Delete: []foldersync.Change{
				{Path: "gone.txt", Dst: fileEntry("gone.txt", 1)},
				{Path: "gonedir", Dst: dirEntry("gonedir")},
			},
			Conflicts: []foldersync.Change{
				{
					Path:        "busy",
					Src:         fileEntry("busy", 1),
					Dst:         dirEntry("busy"),
					TypeChanged: true,
					Reason:      "conflict: destination directory is not empty (needs --delete)",
				},
			},
			Skip:                make([]foldersync.Change, 40),
			DeleteBlockedReason: "source is empty; refusing to delete 2 destination item(s) without --allow-empty-source",
		}

		plain := formatSyncPlan(plan, false)
		want := strings.Join([]string{
			"Plan: 2 to add, 3 to update, 2 to touch, 2 to delete, 1 conflicts, 40 unchanged",
			"  + new.txt",
			"  + newdir/",
			"  ~ mod.txt",
			"  ~ wasfile (file -> folder)",
			"  ~ wasdir (folder -> file)",
			"  * meta.txt",
			"  * metadir/",
			"  - gone.txt",
			"  - gonedir/",
			"  ! busy: conflict: destination directory is not empty (needs --delete)",
			"  deletions skipped: source is empty; refusing to delete 2 destination item(s) without --allow-empty-source",
			"",
		}, "\n")
		require.Equal(t, want, plain)
		require.NotContains(t, plain, "\x1b")

		colored := formatSyncPlan(plan, true)
		require.Contains(t, colored, "\x1b")
		require.Equal(t, plain, termui.Plain(colored))
	})

	t.Run("group truncates at 20 lines with overflow note", func(t *testing.T) {
		adds := make([]foldersync.Change, 25)
		for i := range adds {
			p := fmt.Sprintf("f%02d.txt", i)
			adds[i] = foldersync.Change{Path: p, Src: fileEntry(p, 1)}
		}
		exact20 := make([]foldersync.Change, 20)
		for i := range exact20 {
			p := fmt.Sprintf("u%02d.txt", i)
			exact20[i] = foldersync.Change{Path: p, Src: fileEntry(p, 1), Dst: fileEntry(p, 2)}
		}
		plan := &foldersync.Plan{Add: adds, Update: exact20}
		out := formatSyncPlan(plan, false)
		require.Contains(t, out, "  + f19.txt\n  ... and 5 more\n")
		require.NotContains(t, out, "f20.txt")
		require.Contains(t, out, "  ~ u19.txt\n")
		require.Equal(t, 1, strings.Count(out, "... and "))
	})
}

func TestDecideSyncWatchMarksDeleteConfirm(t *testing.T) {
	var out bytes.Buffer
	plan := &foldersync.Plan{
		Add:    []foldersync.Change{{Path: "a.txt", Src: fileEntry("a.txt", 10)}},
		Delete: []foldersync.Change{{Path: "old.txt", Dst: fileEntry("old.txt", 1)}},
	}

	got, err := decideSync(plan, syncDecideOptions{
		Watch:      true,
		IsTerminal: false,
		Yes:        false,
		In:         failReader{},
		Out:        &out,
	})
	require.NoError(t, err)
	require.Equal(t, foldersync.Decision{Proceed: true, ConfirmDeletes: false}, got)
	require.True(t, plan.RequireDeleteConfirm)
	require.Contains(t, out.String(), "Deletions held: rerun with --yes to allow deletes in watch mode.")
}

func TestDecideSyncCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()

	var out bytes.Buffer
	plan := &foldersync.Plan{
		Delete: []foldersync.Change{{Path: "old.txt", Dst: fileEntry("old.txt", 1)}},
	}
	_, err := decideSync(plan, syncDecideOptions{
		Ctx:        ctx,
		IsTerminal: true,
		In:         pr,
		Out:        &out,
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestFormatSyncSummary(t *testing.T) {
	t.Run("normal sync", func(t *testing.T) {
		res := &foldersync.Result{
			Added:   3,
			Updated: 1,
			Touched: 2,
			Deleted: 4,
			Bytes:   1536,
		}
		plain := formatSyncSummary(res, false)
		require.Equal(t, "Synced: 3 added, 1 updated, 2 touched, 4 deleted, 1.5 kB transferred\n", plain)
		require.Equal(t, plain, termui.Plain(formatSyncSummary(res, true)))
	})

	t.Run("dry run", func(t *testing.T) {
		res := &foldersync.Result{DryRun: true, Unchanged: 5}
		plain := formatSyncSummary(res, false)
		require.Equal(t, "Dry run complete.\n", plain)
		require.Equal(t, plain, termui.Plain(formatSyncSummary(res, true)))
	})

	t.Run("failures truncated at 10 and held deletions", func(t *testing.T) {
		failures := make([]string, 13)
		for i := range failures {
			failures[i] = fmt.Sprintf("file%02d.txt: permission denied", i)
		}
		res := &foldersync.Result{
			Added:         1,
			Bytes:         512,
			Failures:      failures,
			DeletesHeld:   3,
			DeleteBlocked: "deletes exceed 50% threshold",
		}
		plain := formatSyncSummary(res, false)
		require.Contains(t, plain, "Synced: 1 added, 0 updated, 0 touched, 0 deleted, 512 B transferred\n")
		require.Contains(t, plain, "  failed: file00.txt: permission denied\n")
		require.Contains(t, plain, "  failed: file09.txt: permission denied\n")
		require.NotContains(t, plain, "file10.txt")
		require.Contains(t, plain, "  ... and 3 more\n")
		require.Contains(t, plain, "  3 deletion(s) held (deletes exceed 50% threshold)\n")
		require.Equal(t, plain, termui.Plain(formatSyncSummary(res, true)))
	})

	t.Run("writeSyncSummary respects quiet", func(t *testing.T) {
		var buf bytes.Buffer
		writeSyncSummary(&buf, &foldersync.Result{Added: 1}, true, false)
		require.Empty(t, buf.String())
	})
}

func TestMapSyncError(t *testing.T) {
	require.NoError(t, mapSyncError(nil))

	t.Run("context.Canceled maps to friendly ExitError 1", func(t *testing.T) {
		err := mapSyncError(fmt.Errorf("transfer aborted: %w", context.Canceled))
		assertExitCode(t, err, 1)
		require.EqualError(t, err, "sync interrupted; files that were not committed are unchanged")
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("existing ExitError code is preserved", func(t *testing.T) {
		partial := &ExitError{Code: 2, Err: errors.New("partial")}
		err := mapSyncError(partial)
		assertExitCode(t, err, 2)
		require.EqualError(t, err, "partial")
	})

	t.Run("other errors map to ExitError 1", func(t *testing.T) {
		boom := errors.New("boom")
		err := mapSyncError(boom)
		assertExitCode(t, err, 1)
		require.ErrorIs(t, err, boom)
	})
}
