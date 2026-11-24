package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/schollz/croc/v11/internal/cli"
	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/croc"
	"github.com/schollz/croc/v11/src/foldersync"
	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/termui"
	"github.com/schollz/croc/v11/src/utils"
	"github.com/schollz/progressbar/v3"
)

// Engine entry points and terminal hooks. They are variables so tests and
// demos can run the CLI against a fake engine and capture its output.
var (
	runSyncHost = foldersync.RunSyncHost
	runSyncJoin = foldersync.RunSyncJoin

	syncTerminal   = func() (io.Writer, bool) { return termui.Output(os.Stderr) }
	syncReadInput  = utils.GetInputContext
	syncShowQRCode = croc.ShowReceiveCommandQrCode
)

const (
	// Plan lists longer than this are truncated outside of --dry-run.
	syncPlanListLimit = 20
	// Deletions are listed more generously because the user confirms them.
	syncDeleteListLimit = 50
	// Longest path shown in a plan row before it is shortened.
	syncPlanPathWidth   = 60
	syncProgressRefresh = 100 * time.Millisecond
)

const syncDescription = `One-way folder sync: only new or changed files are sent from the
   source computer to the target computer.

   Source (host):   croc sync [options] <folder>
   Target (join):   CROC_SECRET=<code> croc sync <folder>
   PowerShell:      $env:CROC_SECRET='<code>'; croc sync <folder>

   Preview changes without touching any file:
      croc sync --dry-run ./photos

   Mirror the source and delete extra files in the target:
      croc sync --delete ./photos

   Keep syncing as files change (Ctrl+C to stop):
      croc sync --watch ./photos

   Options that shape the plan (--delete, --dry-run, --checksum, --exclude,
   --exclude-file, --git) are chosen on the source computer.`

func syncCommand() *cli.Command {
	return &cli.Command{
		Name:        "sync",
		Usage:       "sync a folder to another computer, sending only new or changed files",
		Description: syncDescription,
		ArgsUsage:   "<folder>",
		Flags:       syncFlags(),
		HelpName:    "croc sync",
		Action:      syncAction,
	}
}

func syncFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "code", Aliases: []string{"c"}, Usage: "code phrase for the source computer (generated when empty); the target reads it from CROC_SECRET"},
		&cli.BoolFlag{Name: "delete", Usage: "delete files in the target folder that do not exist in the source"},
		&cli.BoolFlag{Name: "dry-run", Usage: "show what would be added, updated or deleted without changing any file"},
		&cli.BoolFlag{Name: "watch", Usage: "keep running and sync changes as they happen (Ctrl+C to stop)"},
		&cli.BoolFlag{Name: "checksum", Usage: "compare file contents by hash instead of size and modification time"},
		&cli.StringFlag{Name: "exclude", Usage: "skip paths containing any of the comma separated strings"},
		&cli.StringFlag{Name: "exclude-file", Usage: "skip paths matching any of the comma separated relative paths exactly"},
		&cli.BoolFlag{Name: "git", Usage: "respect .gitignore / do not sync ignored files"},
		&cli.StringFlag{Name: "transport", Value: string(croc.TransportAuto), Usage: "file data transport (auto, derp, relay)"},
		&cli.StringFlag{Name: "relay", Usage: "address of the relay (default: croc's public relay)"},
		&cli.BoolFlag{Name: "qrcode", Aliases: []string{"qr"}, Usage: "show the code phrase as a QR code"},
		&cli.BoolFlag{Name: "yes", Usage: "delete files without asking for confirmation"},
		&cli.BoolFlag{Name: "quiet", Usage: "only print the code, prompts, errors and the final summary"},
	}
}

// syncInvocation is the parsed command line. Config goes to the engine; the
// remaining fields are presentation choices that stay in the CLI.
type syncInvocation struct {
	Config foldersync.SyncConfig
	Join   bool
	Yes    bool
	Quiet  bool
	QRCode bool
}

func syncAction(c *cli.Context) error {
	setDebugLevel(c)
	comm.Socks5Proxy = c.String("socks5")
	comm.HttpProxy = c.String("connect")

	classic := utils.Exists(getClassicConfigFile(true))
	inv, warnings, err := parseSyncInvocation(c, os.Getenv, runtime.GOOS, classic)
	if err != nil {
		return err
	}
	if inv.Quiet {
		log.SetLevel("error")
	}

	output, colorEnabled := syncTerminal()
	for _, warning := range warnings {
		fmt.Fprintln(output, termui.Warning("Warning:", colorEnabled)+" "+warning)
	}

	reporter := newSyncReporter(c.Context, output, colorEnabled, inv)
	run := runSyncHost
	if inv.Join {
		run = runSyncJoin
	}
	reporter.OnStatus(syncStartMessage(inv))
	err = run(c.Context, inv.Config, reporter)
	reporter.close(err)
	return syncResultError(err, inv, reporter.failedCount())
}

func syncStartMessage(inv syncInvocation) string {
	if inv.Join {
		return fmt.Sprintf("Joining sync as the target: %s (code from CROC_SECRET)", inv.Config.Dir)
	}
	return fmt.Sprintf("Hosting sync from source: %s", inv.Config.Dir)
}

// parseSyncInvocation validates the command line without touching the disk
// beyond a stat of the folder, so --dry-run stays side-effect free.
func parseSyncInvocation(c *cli.Context, getenv func(string) string, goos string, classic bool) (syncInvocation, []string, error) {
	var inv syncInvocation
	var warnings []string

	switch c.Args().Len() {
	case 0:
		return inv, nil, errors.New("must specify a folder: croc sync [options] <folder>")
	case 1:
	default:
		return inv, nil, fmt.Errorf("croc sync takes exactly one folder, got %d arguments (quote paths that contain spaces)", c.Args().Len())
	}

	secret := strings.TrimSpace(getenv("CROC_SECRET"))
	inv.Join = secret != ""
	codeSet := c.IsSet("code")
	if inv.Join && codeSet {
		return inv, nil, errors.New("set either CROC_SECRET (to join as the target) or --code (to host as the source), not both")
	}

	dir, err := resolveSyncDir(c.Args().First(), inv.Join)
	if err != nil {
		return inv, nil, err
	}
	transport, err := croc.ParseTransportMode(c.String("transport"))
	if err != nil {
		return inv, nil, err
	}
	relay, err := syncRelay(c)
	if err != nil {
		return inv, nil, err
	}

	dryRun := c.Bool("dry-run")
	watch := c.Bool("watch")
	if dryRun && watch {
		return inv, nil, errors.New("--dry-run cannot be combined with --watch")
	}

	code := secret
	if !inv.Join {
		code = strings.TrimSpace(c.String("code"))
	}

	inv.Config = foldersync.SyncConfig{
		Dir:          dir,
		Code:         code,
		Relay:        relay,
		Transport:    string(transport),
		Delete:       c.Bool("delete"),
		DryRun:       dryRun,
		Watch:        watch,
		Checksum:     c.Bool("checksum"),
		Exclude:      splitSyncList(c.String("exclude")),
		ExcludeFiles: splitSyncExcludeFiles(c.String("exclude-file")),
		UseGitignore: c.Bool("git"),
	}
	inv.Yes = anyContextBool(c, "yes")
	inv.Quiet = anyContextBool(c, "quiet")
	inv.QRCode = c.Bool("qrcode")

	if !inv.Join && codeSet && goos != "windows" && !classic {
		warnings = append(warnings, "--code is visible to other users in the process list; omit it to get a generated code")
	}
	if c.IsSet("pass") {
		warnings = append(warnings, "--pass is not supported by croc sync yet; the default relay password is used")
	}
	return inv, warnings, nil
}

// resolveSyncDir returns the absolute folder path. The source must be an
// existing folder. The target may be missing (the engine creates it), but it
// must never be a filesystem root or the user's home folder, because a plan
// from the source can delete files there.
func resolveSyncDir(folder string, join bool) (string, error) {
	if strings.TrimSpace(folder) == "" {
		return "", errors.New("folder path is empty")
	}
	abs, err := filepath.Abs(folder)
	if err != nil {
		return "", fmt.Errorf("resolve folder %q: %w", folder, err)
	}
	info, err := os.Stat(abs)
	switch {
	case err == nil:
		if !info.IsDir() {
			return "", fmt.Errorf("%s is a file, not a folder", folder)
		}
	case errors.Is(err, os.ErrNotExist):
		if !join {
			return "", fmt.Errorf("source folder %s does not exist", folder)
		}
	default:
		return "", fmt.Errorf("check folder %s: %w", folder, err)
	}
	if join {
		if err := rejectDangerousSyncTarget(abs); err != nil {
			return "", err
		}
	}
	return abs, nil
}

func rejectDangerousSyncTarget(abs string) error {
	candidates := []string{filepath.Clean(abs)}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		candidates = append(candidates, filepath.Clean(resolved))
	}
	home, homeErr := os.UserHomeDir()
	for _, candidate := range candidates {
		if filepath.Dir(candidate) == candidate {
			return fmt.Errorf("refusing to sync into the filesystem root %s; choose a sub-folder", candidate)
		}
		if homeErr == nil && home != "" && sameSyncPath(candidate, filepath.Clean(home)) {
			return fmt.Errorf("refusing to sync into your home folder %s; choose a sub-folder", candidate)
		}
	}
	return nil
}

func sameSyncPath(a, b string) bool {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

// syncRelay returns the explicitly chosen relay, or "" for croc's default.
// It accepts --relay both before and after the sync command.
func syncRelay(c *cli.Context) (string, error) {
	relay, relaySet := lineageString(c, "relay")
	relay6, relay6Set := lineageString(c, "relay6")
	if relaySet && relay6Set {
		return "", errors.New("croc sync accepts only one explicit relay (--relay or --relay6)")
	}
	if relaySet {
		return strings.TrimSpace(relay), nil
	}
	if relay6Set {
		return strings.TrimSpace(relay6), nil
	}
	return "", nil
}

// lineageString returns the value from the innermost context where the flag
// was explicitly set (on the command line or through its environment variable).
func lineageString(c *cli.Context, name string) (string, bool) {
	for _, ctx := range c.Lineage() {
		if ctx.IsSet(name) {
			return ctx.String(name), true
		}
	}
	return "", false
}

// anyContextBool lets both `croc --yes sync` and `croc sync --yes` work.
func anyContextBool(c *cli.Context, name string) bool {
	for _, ctx := range c.Lineage() {
		if ctx.Bool(name) {
			return true
		}
	}
	return false
}

func splitSyncList(value string) []string {
	var items []string
	for item := range strings.SplitSeq(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

func splitSyncExcludeFiles(value string) []string {
	var items []string
	for _, item := range splitSyncList(value) {
		item = utils.NormalizeRelativePath(item)
		if item != "" && item != "." {
			items = append(items, item)
		}
	}
	return items
}

// syncUserError carries a friendly message while keeping the engine error
// reachable through errors.Is.
type syncUserError struct {
	msg string
	err error
}

func (e *syncUserError) Error() string { return e.msg }
func (e *syncUserError) Unwrap() error { return e.err }

func syncResultError(err error, inv syncInvocation, failed int) error {
	switch {
	case err == nil:
		if failed > 0 && !inv.Config.Watch {
			return fmt.Errorf("croc sync finished, but %s could not be synced", pluralize(failed, "item", "items"))
		}
		return nil
	case errors.Is(err, context.Canceled):
		return err
	case errors.Is(err, foldersync.ErrAborted):
		return &syncUserError{msg: "sync aborted: no files were transferred or deleted", err: err}
	case errors.Is(err, foldersync.ErrPeerUnsupported):
		return &syncUserError{msg: "the other computer's croc does not support croc sync (folder-sync-v1); update croc on both computers and try again", err: err}
	case errors.Is(err, foldersync.ErrNotImplemented):
		return &syncUserError{msg: "croc sync is not available in this build yet: the sync engine is still under development", err: err}
	default:
		return fmt.Errorf("croc sync: %w", err)
	}
}

// ---------------------------------------------------------------------
// Terminal reporter
// ---------------------------------------------------------------------

// syncReporter renders engine events on the terminal. Every method takes the
// mutex because the engine may report from several transfer goroutines.
type syncReporter struct {
	ctx       context.Context
	out       io.Writer
	color     bool
	quiet     bool
	yes       bool
	join      bool
	qr        bool
	dryRun    bool
	watch     bool
	relay     string
	readInput func(ctx context.Context, prompt string) (string, error)
	showQR    func(string)
	now       func() time.Time

	mu           sync.Mutex
	rounds       int
	alwaysDelete bool
	watching     bool
	bar          *progressbar.ProgressBar
	barTotal     int64
	barPath      string
	roundErrors  int
	lastSummary  *foldersync.Summary
}

var _ foldersync.ProgressReporter = (*syncReporter)(nil)

func newSyncReporter(ctx context.Context, out io.Writer, colorEnabled bool, inv syncInvocation) *syncReporter {
	if ctx == nil {
		ctx = context.Background()
	}
	return &syncReporter{
		ctx:       ctx,
		out:       out,
		color:     colorEnabled,
		quiet:     inv.Quiet,
		yes:       inv.Yes,
		join:      inv.Join,
		qr:        inv.QRCode,
		dryRun:    inv.Config.DryRun,
		watch:     inv.Config.Watch,
		relay:     inv.Config.Relay,
		readInput: syncReadInput,
		showQR:    syncShowQRCode,
		now:       time.Now,
	}
}

func (r *syncReporter) OnCode(code string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.breakBarLocked()
	fmt.Fprint(r.out, formatSyncJoinInstructions(code, r.relay, r.color))
	if r.qr {
		r.showQR(code)
	}
}

func (r *syncReporter) OnStatus(msg string) {
	if r.quiet || strings.TrimSpace(msg) == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.breakBarLocked()
	fmt.Fprintln(r.out, msg)
}

func (r *syncReporter) OnPlan(plan foldersync.PlanView) bool {
	r.mu.Lock()
	r.breakBarLocked()
	round := r.rounds
	r.rounds++
	r.roundErrors = 0

	needConfirm := !r.join && !plan.DryRun && len(plan.Delete) > 0 && !r.yes && !r.alwaysDelete
	listedDeletes := false
	if plan.DryRun || !r.quiet {
		if plan.DryRun || !r.watch || round == 0 {
			listLimit, deleteLimit := syncPlanListLimit, syncDeleteListLimit
			if plan.DryRun {
				listLimit, deleteLimit = 0, 0
			}
			fmt.Fprint(r.out, formatSyncPlan(plan, r.join, listLimit, deleteLimit, r.color))
			listedDeletes = true
		} else {
			fmt.Fprintln(r.out, formatSyncPlanCompact(plan, r.now(), r.color))
		}
	}
	if needConfirm && !listedDeletes {
		fmt.Fprint(r.out, formatSyncPlanItems(plan.Delete, syncKindDelete, syncDeleteListLimit, r.color))
	}
	watch := r.watch
	r.mu.Unlock()

	if !needConfirm {
		return true
	}

	// Ask without holding the lock so other goroutines are not stuck behind
	// the prompt. The prompt goes to the reporter's own writer: croc's quiet
	// mode swaps the global os.Stderr, which would hide a prompt printed there.
	prompt := fmt.Sprintf("Delete %s from the target folder? (y/N) ", pluralize(len(plan.Delete), "item", "items"))
	if watch {
		prompt = fmt.Sprintf("Delete %s from the target folder? (y/N, a = yes for the rest of this session) ", pluralize(len(plan.Delete), "item", "items"))
	}
	fmt.Fprint(r.out, termui.PromptChoices(prompt, r.color))
	answer, err := r.readInput(r.ctx, "")

	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		if r.ctx.Err() == nil {
			fmt.Fprintln(r.out, "Could not read the answer; run again with --yes to delete without asking.")
		}
		return false
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true
	case "a", "always":
		if watch {
			r.alwaysDelete = true
			return true
		}
	}
	fmt.Fprintln(r.out, "Deletion not confirmed.")
	return false
}

func (r *syncReporter) OnFileStart(path string, size int64) {
	if r.quiet {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.barPath = path
	if r.bar != nil {
		r.bar.Describe(r.progressDescription(path))
	}
}

func (r *syncReporter) OnProgress(p foldersync.Progress) {
	if r.quiet {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	total := p.OverallTotal
	if total <= 0 {
		total = p.FileTotal
	}
	if total <= 0 {
		return
	}
	if p.Path != "" {
		r.barPath = p.Path
	}
	if r.bar == nil || r.barTotal != total {
		r.breakBarLocked()
		r.bar = termui.NewProgress(termui.ProgressConfig{
			Max:          total,
			Description:  r.progressDescription(r.barPath),
			Writer:       r.out,
			ColorEnabled: r.color,
			Throttle:     syncProgressRefresh,
		})
		r.barTotal = total
	} else if p.Path != "" {
		r.bar.Describe(r.progressDescription(p.Path))
	}
	done := p.OverallDone
	if p.OverallTotal <= 0 {
		done = p.FileDone
	}
	_ = r.bar.Set64(min(max(done, 0), total))
}

func (r *syncReporter) progressDescription(path string) string {
	action := "Sending "
	if r.join {
		action = "Receiving "
	}
	return termui.ProgressDescription(action, path, r.color)
}

func (r *syncReporter) OnFileDone(path string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.roundErrors++
		r.breakBarLocked()
		fmt.Fprintf(r.out, "%s %s: %v\n", termui.Error("error:", r.color), path, err)
		return
	}
	if r.watch && !r.quiet {
		r.breakBarLocked()
		fmt.Fprintf(r.out, "  %s %s\n", termui.Success("synced", r.color), path)
	}
}

func (r *syncReporter) OnWatchEvent(ev foldersync.WatchEvent) {
	if r.quiet {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.breakBarLocked()
	when := ev.Time
	if when.IsZero() {
		when = r.now()
	}
	fmt.Fprintln(r.out, formatSyncWatchEvent(ev, when, r.color))
}

func (r *syncReporter) OnRoundDone(s foldersync.Summary) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishBarLocked()
	summary := s
	r.lastSummary = &summary

	switch {
	case r.dryRun:
		fmt.Fprintln(r.out, "Dry run complete: no files were transferred, written or deleted.")
	case r.watch:
		fmt.Fprintln(r.out, formatSyncWatchSummary(s, r.now(), r.color))
	default:
		fmt.Fprintln(r.out, formatSyncSummary(s, r.color))
	}

	if r.watch && !r.watching && !r.quiet {
		r.watching = true
		if r.join {
			fmt.Fprintln(r.out, "Waiting for changes from the source (Ctrl+C to stop)…")
		} else {
			fmt.Fprintln(r.out, "Watching for changes (Ctrl+C to stop)…")
		}
	}
}

// close finishes any progress bar after the engine returns.
func (r *syncReporter) close(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishBarLocked()
	if errors.Is(err, context.Canceled) && !r.quiet {
		if r.watch {
			fmt.Fprintln(r.out, "\nStopped watching.")
		} else {
			fmt.Fprintln(r.out, "\nSync stopped.")
		}
	}
}

// failedCount is the number of items that failed in the latest round.
func (r *syncReporter) failedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastSummary != nil && len(r.lastSummary.Errors) > r.roundErrors {
		return len(r.lastSummary.Errors)
	}
	return r.roundErrors
}

// breakBarLocked moves an active progress bar out of the way before a log
// line: an unfinished bar is erased (it redraws on the next update), a
// finished one is kept and ended with a newline.
func (r *syncReporter) breakBarLocked() {
	if r.bar == nil {
		return
	}
	if r.bar.IsFinished() {
		fmt.Fprintln(r.out)
		r.bar = nil
		r.barTotal = 0
		return
	}
	_ = r.bar.Clear()
}

func (r *syncReporter) finishBarLocked() {
	if r.bar == nil {
		return
	}
	_ = r.bar.Exit()
	fmt.Fprintln(r.out)
	r.bar = nil
	r.barTotal = 0
	r.barPath = ""
}

// ---------------------------------------------------------------------
// Formatting (pure functions, unit tested)
// ---------------------------------------------------------------------

type syncKind int

const (
	syncKindAdd syncKind = iota
	syncKindUpdate
	syncKindDelete
	syncKindSkip
)

// Each kind has a symbol and a word so it stays readable without color.
func (k syncKind) label(colorEnabled bool) string {
	switch k {
	case syncKindAdd:
		return termui.Color("+ add   ", termui.Green, colorEnabled)
	case syncKindUpdate:
		return termui.Color("~ update", termui.Cyan, colorEnabled)
	case syncKindDelete:
		return termui.Color("- delete", termui.Red, colorEnabled)
	default:
		return "= skip  "
	}
}

func (k syncKind) noun() string {
	switch k {
	case syncKindAdd:
		return "to add"
	case syncKindUpdate:
		return "to update"
	case syncKindDelete:
		return "to delete"
	default:
		return "unchanged"
	}
}

func formatSyncJoinInstructions(code, relay string, colorEnabled bool) string {
	flags := ""
	if relay != "" {
		flags = " --relay " + posixQuote(relay)
	}
	psFlags := ""
	if relay != "" {
		psFlags = " --relay " + powershellQuote(relay)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Code is: %s\n", termui.Secret(code, colorEnabled))
	b.WriteString("On the target computer, run:\n")
	fmt.Fprintf(&b, "  %s%s\n",
		formatShellAssignment("CROC_SECRET", code, colorEnabled),
		termui.Color(" croc sync"+flags+" <target-folder>", termui.Cyan, colorEnabled))
	fmt.Fprintf(&b, "  PowerShell: %s%s%s\n",
		termui.Color("$env:CROC_SECRET='", termui.Cyan, colorEnabled),
		termui.Secret(strings.ReplaceAll(code, "'", "''"), colorEnabled),
		termui.Color("'; croc sync"+psFlags+" <target-folder>", termui.Cyan, colorEnabled))
	return b.String()
}

func posixQuote(value string) string {
	if value == "" {
		return "''"
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("@%+=:,./_-[]", r)) {
			return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
		}
	}
	return value
}

func powershellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// formatSyncPlan renders the full plan table. A limit of 0 lists every item.
func formatSyncPlan(plan foldersync.PlanView, join bool, listLimit, deleteLimit int, colorEnabled bool) string {
	var b strings.Builder
	switch {
	case plan.DryRun:
		b.WriteString(termui.Emphasis("Sync plan (dry run: nothing will be changed)", colorEnabled) + "\n")
	case join:
		b.WriteString(termui.Emphasis("Incoming changes from the source", colorEnabled) + "\n")
	default:
		b.WriteString(termui.Emphasis("Sync plan", colorEnabled) + "\n")
	}

	if len(plan.Add)+len(plan.Update)+len(plan.Delete) == 0 {
		fmt.Fprintf(&b, "  Already in sync: %s unchanged.\n", pluralize(len(plan.Skip), "item", "items"))
		return b.String()
	}

	// One size column for the whole table.
	width := max(
		syncPathColumnWidth(visibleSyncItems(plan.Add, listLimit)),
		syncPathColumnWidth(visibleSyncItems(plan.Update, listLimit)),
		syncPathColumnWidth(visibleSyncItems(plan.Delete, deleteLimit)),
	)
	b.WriteString(formatSyncPlanItemsWidth(plan.Add, syncKindAdd, listLimit, width, colorEnabled))
	b.WriteString(formatSyncPlanItemsWidth(plan.Update, syncKindUpdate, listLimit, width, colorEnabled))
	b.WriteString(formatSyncPlanItemsWidth(plan.Delete, syncKindDelete, deleteLimit, width, colorEnabled))
	if len(plan.Skip) > 0 {
		fmt.Fprintf(&b, "  %s %s\n", syncKindSkip.label(colorEnabled), pluralize(len(plan.Skip), "unchanged item", "unchanged items"))
	}
	b.WriteString("  " + formatSyncPlanTotals(plan) + "\n")
	return b.String()
}

func formatSyncPlanItems(items []foldersync.PlanItem, kind syncKind, limit int, colorEnabled bool) string {
	return formatSyncPlanItemsWidth(items, kind, limit, syncPathColumnWidth(visibleSyncItems(items, limit)), colorEnabled)
}

// visibleSyncItems returns the items shown under a list limit (0 = all).
func visibleSyncItems(items []foldersync.PlanItem, limit int) []foldersync.PlanItem {
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

// syncPathColumnWidth is the width of the path column for rows that show a
// size. Folders have no size column, so they do not widen it.
func syncPathColumnWidth(items []foldersync.PlanItem) int {
	width := 0
	for _, item := range items {
		if !item.IsDir {
			width = max(width, len([]rune(syncDisplayPath(item))))
		}
	}
	return min(width, syncPlanPathWidth)
}

func formatSyncPlanItemsWidth(items []foldersync.PlanItem, kind syncKind, limit, width int, colorEnabled bool) string {
	if len(items) == 0 {
		return ""
	}
	shown := visibleSyncItems(items, limit)

	var b strings.Builder
	for _, item := range shown {
		name := shortenSyncPath(syncDisplayPath(item), syncPlanPathWidth)
		if item.IsDir {
			fmt.Fprintf(&b, "  %s %s\n", kind.label(colorEnabled), name)
			continue
		}
		padding := strings.Repeat(" ", max(width-len([]rune(name)), 0))
		fmt.Fprintf(&b, "  %s %s%s  %s\n", kind.label(colorEnabled), termui.Filename(name, colorEnabled), padding, utils.ByteCountDecimal(item.Size))
	}
	if hidden := len(items) - len(shown); hidden > 0 {
		fmt.Fprintf(&b, "  … and %d more %s (use --dry-run to list everything)\n", hidden, kind.noun())
	}
	return b.String()
}

func formatSyncPlanTotals(plan foldersync.PlanView) string {
	parts := []string{
		fmt.Sprintf("%d %s", len(plan.Add), syncKindAdd.noun()),
		fmt.Sprintf("%d %s", len(plan.Update), syncKindUpdate.noun()),
	}
	if len(plan.Delete) > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", len(plan.Delete), syncKindDelete.noun()))
	}
	parts = append(parts, fmt.Sprintf("%d %s", len(plan.Skip), syncKindSkip.noun()))
	totals := "Total: " + strings.Join(parts, ", ")
	totals += " · " + utils.ByteCountDecimal(plan.TransferBytes) + " to send"
	if plan.SavedBytes > 0 {
		totals += " · " + utils.ByteCountDecimal(plan.SavedBytes) + " not re-sent"
	}
	return totals
}

func formatSyncPlanCompact(plan foldersync.PlanView, now time.Time, colorEnabled bool) string {
	return fmt.Sprintf("[%s] %s: %d %s, %d %s, %d %s · %s",
		now.Format("15:04:05"),
		termui.Emphasis("changes", colorEnabled),
		len(plan.Add), syncKindAdd.noun(),
		len(plan.Update), syncKindUpdate.noun(),
		len(plan.Delete), syncKindDelete.noun(),
		utils.ByteCountDecimal(plan.TransferBytes))
}

func formatSyncWatchEvent(ev foldersync.WatchEvent, when time.Time, colorEnabled bool) string {
	path := ev.Path
	if ev.Kind == foldersync.WatchRename && ev.OldPath != "" {
		path = ev.OldPath + " → " + ev.Path
	}
	kind := string(ev.Kind)
	if kind == "" {
		kind = "change"
	}
	return fmt.Sprintf("[%s] %-6s %s", when.Format("15:04:05"), kind, termui.Filename(path, colorEnabled))
}

func formatSyncSummary(s foldersync.Summary, colorEnabled bool) string {
	head := termui.Success("Sync complete", colorEnabled)
	if len(s.Errors) > 0 {
		head = termui.Warning("Sync finished with errors", colorEnabled)
	}
	line := fmt.Sprintf("%s in %s: %s · %s sent", head, formatSyncDuration(s.Duration), formatSyncCounts(s), utils.ByteCountDecimal(s.BytesSent))
	if len(s.Errors) > 0 {
		line += " · " + termui.Error(pluralize(len(s.Errors), "failed item", "failed items"), colorEnabled)
	}
	return line
}

func formatSyncWatchSummary(s foldersync.Summary, now time.Time, colorEnabled bool) string {
	line := fmt.Sprintf("[%s] %s: %s · %s in %s", now.Format("15:04:05"), termui.Success("synced", colorEnabled), formatSyncCounts(s), utils.ByteCountDecimal(s.BytesSent), formatSyncDuration(s.Duration))
	if len(s.Errors) > 0 {
		line += " · " + termui.Error(pluralize(len(s.Errors), "failed item", "failed items"), colorEnabled)
	}
	return line
}

func formatSyncCounts(s foldersync.Summary) string {
	return fmt.Sprintf("%d added, %d updated, %d deleted, %d unchanged", s.Added, s.Updated, s.Deleted, s.Skipped)
}

func formatSyncDuration(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(100 * time.Millisecond).String()
}

func syncDisplayPath(item foldersync.PlanItem) string {
	if item.IsDir && !strings.HasSuffix(item.Path, "/") {
		return item.Path + "/"
	}
	return item.Path
}

// shortenSyncPath keeps the end of a long path, which is usually the most
// recognizable part.
func shortenSyncPath(path string, width int) string {
	runes := []rune(path)
	if width <= 1 || len(runes) <= width {
		return path
	}
	return "…" + string(runes[len(runes)-(width-1):])
}

func pluralize(n int, singular, plural string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, singular)
	}
	return fmt.Sprintf("%d %s", n, plural)
}
