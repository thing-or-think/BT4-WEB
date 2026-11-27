package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/schollz/croc/v11/src/foldersync"
	"github.com/schollz/croc/v11/src/termui"
	"github.com/schollz/croc/v11/src/utils"
	"golang.org/x/term"
)

const (
	maxPlanGroupLines  = 20
	maxSummaryFailures = 10
)

var syncStdinIsTerminal = func() bool {
	return os.Stdin != nil && term.IsTerminal(int(os.Stdin.Fd()))
}

type syncDecideOptions struct {
	Ctx        context.Context
	DryRun     bool
	Yes        bool
	Quiet      bool
	Watch      bool
	IsTerminal bool
	In         io.Reader
	Out        io.Writer
	Color      bool
}

func decideSync(plan *foldersync.Plan, o syncDecideOptions) (foldersync.Decision, error) {
	if plan == nil {
		plan = &foldersync.Plan{}
	}
	out := o.Out
	if out == nil {
		out = io.Discard
	}

	if !o.Quiet || o.DryRun {
		_, _ = io.WriteString(out, "\r\n"+formatSyncPlan(plan, o.Color))
	}

	if o.DryRun {
		fmt.Fprintln(out, "Dry run: nothing was changed.")
		return foldersync.Decision{}, nil
	}

	if !planHasAction(plan) {
		if !o.Quiet {
			fmt.Fprintln(out, "Already in sync.")
		}
		return foldersync.Decision{Proceed: true}, nil
	}

	if len(plan.Delete) == 0 {
		return foldersync.Decision{Proceed: true}, nil
	}

	if plan.RequireDeleteConfirm {
		if o.IsTerminal {
			n := countDeleteFiles(plan.Delete)
			fmt.Fprintf(out, "This deletes %d of the destination files (more than half). Type yes to delete them: ", n)
			answer, err := readSyncLine(o.Ctx, o.In)
			if err != nil {
				return foldersync.Decision{}, err
			}
			if strings.EqualFold(answer, "yes") {
				return foldersync.Decision{Proceed: true, ConfirmDeletes: true}, nil
			}
			fmt.Fprintln(out, "Deletions held; other changes will be applied.")
			return foldersync.Decision{Proceed: true, ConfirmDeletes: false}, nil
		}
		if !o.Quiet {
			fmt.Fprintln(out, "Deletions held; other changes will be applied.")
		}
		return foldersync.Decision{Proceed: true, ConfirmDeletes: false}, nil
	}

	if o.Yes {
		return foldersync.Decision{Proceed: true}, nil
	}

	if o.IsTerminal {
		fmt.Fprintf(out, "Apply these changes, including %d deletion(s)? [y/N] ", len(plan.Delete))
		answer, err := readSyncLine(o.Ctx, o.In)
		if err != nil {
			return foldersync.Decision{}, err
		}
		lower := strings.ToLower(answer)
		if lower == "y" || lower == "yes" {
			return foldersync.Decision{Proceed: true}, nil
		}
		fmt.Fprintln(out, "Nothing was changed.")
		return foldersync.Decision{}, nil
	}

	if o.Watch {
		// Mark confirmation required so Commit holds deletions while applying other changes.
		plan.RequireDeleteConfirm = true
		if !o.Quiet {
			fmt.Fprintln(out, "Deletions held: rerun with --yes to allow deletes in watch mode.")
		}
		return foldersync.Decision{Proceed: true, ConfirmDeletes: false}, nil
	}

	return foldersync.Decision{}, fmt.Errorf("refusing to delete %d item(s) without --yes: stdin is not a terminal", len(plan.Delete))
}

func planHasAction(plan *foldersync.Plan) bool {
	return plan.HasChanges() || len(plan.Conflicts) > 0
}

func formatSyncPlan(plan *foldersync.Plan, color bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %d to add, %d to update, %d to touch, %d to delete, %d conflicts, %d unchanged\n",
		termui.Emphasis("Plan:", color),
		len(plan.Add),
		len(plan.Update),
		len(plan.Touch),
		len(plan.Delete),
		len(plan.Conflicts),
		len(plan.Skip),
	)

	writePlanGroup(&b, plan.Add, func(c foldersync.Change) string {
		return termui.Success("+ "+formatChangePath(c), color)
	})
	writePlanGroup(&b, plan.Update, func(c foldersync.Change) string {
		return termui.Warning("~ "+formatChangePath(c)+typeChangeSuffix(c), color)
	})
	writePlanGroup(&b, plan.Touch, func(c foldersync.Change) string {
		return termui.Color("* "+formatChangePath(c), termui.Cyan, color)
	})
	writePlanGroup(&b, plan.Delete, func(c foldersync.Change) string {
		return termui.Error("- "+formatChangePath(c), color)
	})
	writePlanGroup(&b, plan.Conflicts, func(c foldersync.Change) string {
		line := "! " + formatChangePath(c)
		if c.Reason != "" {
			line += ": " + c.Reason
		}
		return termui.Error(line, color)
	})

	if plan.DeleteBlockedReason != "" {
		fmt.Fprintf(&b, "  %s\n", termui.Warning("deletions skipped: "+plan.DeleteBlockedReason, color))
	}
	return b.String()
}

func writePlanGroup(b *strings.Builder, changes []foldersync.Change, format func(foldersync.Change) string) {
	limit := min(len(changes), maxPlanGroupLines)
	for _, c := range changes[:limit] {
		fmt.Fprintf(b, "  %s\n", format(c))
	}
	if len(changes) > maxPlanGroupLines {
		fmt.Fprintf(b, "  ... and %d more\n", len(changes)-maxPlanGroupLines)
	}
}

func formatChangePath(c foldersync.Change) string {
	p := c.Path
	if !c.TypeChanged && changeIsDir(c) && !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return p
}

func changeIsDir(c foldersync.Change) bool {
	if c.Src != nil {
		return c.Src.IsDir()
	}
	if c.Dst != nil {
		return c.Dst.IsDir()
	}
	return false
}

func typeChangeSuffix(c foldersync.Change) string {
	if !c.TypeChanged {
		return ""
	}
	if c.Src != nil {
		if c.Src.IsDir() {
			return " (file -> folder)"
		}
		if c.Src.IsFile() {
			return " (folder -> file)"
		}
	}
	if c.Dst != nil {
		if c.Dst.IsFile() {
			return " (file -> folder)"
		}
		if c.Dst.IsDir() {
			return " (folder -> file)"
		}
	}
	return ""
}

func countDeleteFiles(deletes []foldersync.Change) int {
	n := 0
	for _, c := range deletes {
		if c.Dst != nil && c.Dst.IsDir() {
			continue
		}
		n++
	}
	if n == 0 {
		return len(deletes)
	}
	return n
}

func readSyncLine(ctx context.Context, in io.Reader) (string, error) {
	if in == nil {
		return "", nil
	}
	read := func() (string, error) {
		text, err := bufio.NewReader(in).ReadString('\n')
		text = strings.TrimSpace(text)
		if errors.Is(err, io.EOF) {
			return text, nil
		}
		return text, err
	}
	if ctx == nil || ctx.Done() == nil {
		return read()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	type inputResult struct {
		text string
		err  error
	}
	result := make(chan inputResult, 1)
	go func() {
		text, err := read()
		result <- inputResult{text: text, err: err}
	}()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-result:
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return res.text, res.err
	}
}

func formatSyncSummary(res *foldersync.Result, color bool) string {
	if res == nil {
		return ""
	}
	var b strings.Builder
	if res.DryRun {
		fmt.Fprintln(&b, termui.Emphasis("Dry run complete.", color))
	} else {
		summary := fmt.Sprintf("Synced: %d added, %d updated, %d touched, %d deleted, %s transferred",
			res.Added, res.Updated, res.Touched, res.Deleted, utils.ByteCountDecimal(res.Bytes))
		if len(res.Failures) == 0 && res.DeletesHeld <= 0 {
			fmt.Fprintln(&b, termui.Success(summary, color))
		} else {
			fmt.Fprintln(&b, termui.Warning(summary, color))
		}
	}

	limit := min(len(res.Failures), maxSummaryFailures)
	for _, item := range res.Failures[:limit] {
		fmt.Fprintf(&b, "  %s\n", termui.Error("failed: "+item, color))
	}
	if len(res.Failures) > maxSummaryFailures {
		fmt.Fprintf(&b, "  ... and %d more\n", len(res.Failures)-maxSummaryFailures)
	}

	if res.DeletesHeld > 0 {
		line := fmt.Sprintf("%d deletion(s) held", res.DeletesHeld)
		if res.DeleteBlocked != "" {
			line += " (" + res.DeleteBlocked + ")"
		}
		fmt.Fprintf(&b, "  %s\n", termui.Warning(line, color))
	} else if !res.DryRun && res.DeleteBlocked != "" {
		fmt.Fprintf(&b, "  %s\n", termui.Warning("deletions skipped: "+res.DeleteBlocked, color))
	}

	return b.String()
}

func writeSyncSummary(w io.Writer, res *foldersync.Result, quiet, color bool) {
	if quiet || res == nil || w == nil {
		return
	}
	_, _ = io.WriteString(w, formatSyncSummary(res, color))
}

type syncInterruptedError struct {
	cause error
}

func (syncInterruptedError) Error() string {
	return "sync interrupted; files that were not committed are unchanged"
}

func (e syncInterruptedError) Unwrap() error {
	return e.cause
}

func mapSyncError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return &ExitError{
			Code: 1,
			Err:  syncInterruptedError{cause: err},
		}
	}
	var coded interface{ ExitCode() int }
	if errors.As(err, &coded) {
		return err
	}
	return &ExitError{Code: 1, Err: err}
}
