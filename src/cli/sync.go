package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/schollz/croc/v11/internal/cli"
	"github.com/schollz/croc/v11/src/codephrase"
	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/croc"
	"github.com/schollz/croc/v11/src/foldersync"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/publicrelay"
	"github.com/schollz/croc/v11/src/tcp"
	"github.com/schollz/croc/v11/src/termui"
	"github.com/schollz/croc/v11/src/utils"
)

// ExitError carries a process exit code.
type ExitError struct {
	Code int
	Err  error
}

func (e *ExitError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *ExitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *ExitError) ExitCode() int {
	if e == nil {
		return 0
	}
	return e.Code
}

var (
	runSyncHost = foldersync.RunSyncHost
	runSyncJoin = foldersync.RunSyncJoin
)

var syncSourceOnlyFlags = []string{
	"code",
	"transport",
	"exclude",
	"exclude-file",
	"git",
	"qrcode",
	"watch",
	"no-local",
}

var syncDestinationOnlyFlags = []string{
	"delete",
	"dry-run",
	"checksum",
	"allow-empty-source",
}

func syncCommand(c *cli.Context) error {
	return mapSyncError(runSync(c))
}

func runSync(c *cli.Context) error {
	envSecret := strings.TrimSpace(os.Getenv("CROC_SECRET"))
	isDestination := envSecret != ""

	if err := validateSyncFlags(c, isDestination); err != nil {
		return err
	}
	dir, err := validateSyncDir(c)
	if err != nil {
		return err
	}

	setDebugLevel(c)
	comm.Socks5Proxy = c.String("socks5")
	comm.HttpProxy = c.String("connect")
	out, colorEnabled := termui.Output(os.Stderr)
	reporter := newSyncReporter(out, dir, c.Bool("quiet"))
	reporter.color = colorEnabled

	if isDestination {
		return syncJoin(c, dir, envSecret, reporter, out, colorEnabled)
	}
	return syncHost(c, dir, reporter, out, colorEnabled)
}

func validateSyncFlags(c *cli.Context, isDestination bool) error {
	if isDestination {
		if c.IsSet("code") {
			return errors.New("--code cannot be used with CROC_SECRET; --code only applies on the source (the computer that runs croc sync without CROC_SECRET)")
		}
		for _, name := range syncSourceOnlyFlags {
			if name == "code" {
				continue
			}
			if c.IsSet(name) {
				return fmt.Errorf("--%s only applies on the source (the computer that runs croc sync without CROC_SECRET)", name)
			}
		}
		return nil
	}

	for _, name := range syncDestinationOnlyFlags {
		if c.IsSet(name) {
			return fmt.Errorf("--%s only applies on the destination (the computer that runs croc sync with CROC_SECRET set)", name)
		}
	}
	return nil
}

func validateSyncDir(c *cli.Context) (string, error) {
	switch c.Args().Len() {
	case 0:
		return "", errors.New("must specify a folder: croc sync <folder>")
	case 1:
	default:
		return "", fmt.Errorf("expected exactly one folder, got %d: croc sync <folder>", c.Args().Len())
	}

	dir := c.Args().First()
	info, err := os.Stat(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("folder does not exist: %s", dir)
		}
		return "", fmt.Errorf("cannot access folder %s: %w", dir, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("not a directory: %s", dir)
	}
	return dir, nil
}

func syncHost(c *cli.Context, dir string, reporter *syncReporter, out io.Writer, colorEnabled bool) error {
	transport, err := croc.ParseTransportMode(c.String("transport"))
	if err != nil {
		return err
	}
	showQrCode := c.Bool("qrcode")
	if showQrCode && transport == croc.TransportDERP {
		return errors.New("web sync does not support --transport derp; use auto or relay")
	}

	classicInsecureMode := utils.Exists(getClassicConfigFile(true))
	if shouldExitForUnixSendCode(runtime.GOOS, c.IsSet("code"), classicInsecureMode, "") {
		fmt.Printf(`On UNIX systems, passing --code on the command line exposes the code phrase
in the host's process list.

Have the code phrase automatically generated:

  croc sync <folder>

Or enable classic mode if you accept that risk:

  croc --classic

`)
		return nil
	}

	var excludeStrings []string
	for v := range strings.SplitSeq(c.String("exclude"), ",") {
		v = strings.ToLower(strings.TrimSpace(v))
		if v != "" {
			excludeStrings = append(excludeStrings, v)
		}
	}
	var excludeFiles []string
	for v := range strings.SplitSeq(c.String("exclude-file"), ",") {
		v = utils.NormalizeRelativePath(strings.TrimSpace(v))
		if v != "" && v != "." {
			excludeFiles = append(excludeFiles, v)
		}
	}

	const (
		defaultSyncPort      = 9009
		defaultSyncTransfers = 4
	)
	ports := make([]string, defaultSyncTransfers+1)
	for i := 0; i <= defaultSyncTransfers; i++ {
		ports[i] = strconv.Itoa(defaultSyncPort + i)
	}

	watch := c.Bool("watch")
	quiet := c.Bool("quiet")
	webOrigin := c.String("web-url")

	crocOptions := croc.Options{
		SharedSecret:      c.String("code"),
		IsSender:          true,
		Debug:             c.Bool("debug"),
		NoPrompt:          c.Bool("yes"),
		RelayAddress:      c.String("relay"),
		RelayAddress6:     c.String("relay6"),
		DisableLocal:      c.Bool("no-local"),
		OnlyLocal:         c.Bool("local"),
		RelayPorts:        ports,
		RelayPassword:     determinePass(c),
		WebOrigin:         webOrigin,
		NoCompress:        c.Bool("no-compress"),
		Curve:             c.String("curve"),
		ThrottleUpload:    c.String("throttleUpload"),
		MulticastAddress:  c.String("multicast"),
		ShowQrCode:        showQrCode,
		Quiet:             quiet,
		DisableClipboard:  c.Bool("disable-clipboard"),
		ExtendedClipboard: c.Bool("extended-clipboard"),
		NoUpdateCheck:     c.Bool("no-update-check"),
		Transport:         transport,
		SendInstructions: func(secret, relayFlags string) string {
			_, colorEnabled := termui.Output(os.Stderr)
			webURL := ""
			if transport != croc.TransportDERP {
				webURL = croc.WebSyncURL(webOrigin, secret)
			}
			text := formatSyncInstructions(secret, relayFlags, webURL, colorEnabled)
			if watch && !quiet {
				text += fmt.Sprintf("Watching %s for changes (Ctrl+C to stop).\n", dir)
			}
			return text
		},
	}
	if crocOptions.RelayAddress != models.DEFAULT_RELAY {
		crocOptions.RelayAddress6 = ""
	} else if crocOptions.RelayAddress6 != models.DEFAULT_RELAY6 {
		crocOptions.RelayAddress = ""
	}

	downgradedTransport, err := resolveSendTransport(&crocOptions)
	if err != nil {
		return err
	}
	if crocOptions.OnlyLocal && crocOptions.Transport != croc.TransportAuto {
		return errors.New("--transport must be auto for local-only transfers")
	}
	writeTailcatRelayFallbackWarning(os.Stderr, crocOptions.Quiet, downgradedTransport)

	publicRelayMode := usesPublicRelay(c, crocOptions)
	generatedPublicCode := len(crocOptions.SharedSecret) == 0 && publicRelayMode
	if len(crocOptions.SharedSecret) == 0 {
		if publicRelayMode {
			var relayIndex int
			relayIndex, err = selectPublicRelay(c.Context, tcp.MeasureServerLatencyContext)
			if err != nil {
				if watch && errors.Is(err, context.Canceled) {
					if !quiet {
						fmt.Fprintln(out, "Stopped watching.")
					}
					return nil
				}
				return err
			}
			crocOptions.SharedSecret, err = codephrase.GenerateForRelay(relayIndex, len(publicrelay.Relays()))
			if err == nil {
				err = assignPublicRelay(&crocOptions, relayIndex)
			}
		} else {
			crocOptions.SharedSecret, err = codephrase.Generate()
		}
		if err != nil {
			return fmt.Errorf("could not generate code phrase: %w", err)
		}
	} else if publicRelayMode {
		if err = assignPublicRelayForCode(&crocOptions); err != nil {
			return fmt.Errorf("could not select public relay: %w", err)
		}
	}

	res, err := runSyncHost(c.Context, foldersync.HostOptions{
		Dir: dir,
		Filter: foldersync.FilterSpec{
			Exclude:      excludeStrings,
			ExcludeExact: excludeFiles,
			Git:          c.Bool("git"),
		},
		Croc:     crocOptions,
		Reporter: reporter,
		Watch:    watch,
	})
	clearBestPublicRelayOnSendError(generatedPublicCode, err)
	if watch {
		if err == nil || errors.Is(err, context.Canceled) {
			if !quiet {
				fmt.Fprintln(out, "Stopped watching.")
			}
			return nil
		}
		return err
	}
	if err != nil {
		return err
	}
	writeSyncSummary(out, res, quiet, colorEnabled)
	return syncResultError(res)
}

func syncJoin(c *cli.Context, dir, secret string, reporter *syncReporter, out io.Writer, colorEnabled bool) error {
	crocOptions := croc.Options{
		SharedSecret:     secret,
		IsSender:         false,
		Debug:            c.Bool("debug"),
		NoPrompt:         true,
		RelayAddress:     c.String("relay"),
		RelayAddress6:    c.String("relay6"),
		RelayPassword:    determinePass(c),
		OnlyLocal:        c.Bool("local"),
		IP:               c.String("ip"),
		Overwrite:        true,
		Curve:            c.String("curve"),
		TestFlag:         c.Bool("testing"),
		MulticastAddress: c.String("multicast"),
		Quiet:            c.Bool("quiet"),
		DisableClipboard: c.Bool("disable-clipboard"),
		NoUpdateCheck:    c.Bool("no-update-check"),
		Transport:        croc.TransportAuto,
	}
	if crocOptions.RelayAddress != models.DEFAULT_RELAY {
		crocOptions.RelayAddress6 = ""
	} else if crocOptions.RelayAddress6 != models.DEFAULT_RELAY6 {
		crocOptions.RelayAddress = ""
	}
	if usesPublicRelay(c, crocOptions) {
		if err := assignPublicRelayForCode(&crocOptions); err != nil {
			return fmt.Errorf("could not select public relay: %w", err)
		}
	}

	dryRun := c.Bool("dry-run")
	yes := c.Bool("yes")
	quiet := c.Bool("quiet")
	res, err := runSyncJoin(c.Context, foldersync.JoinOptions{
		Dir: dir,
		Diff: foldersync.DiffOptions{
			Delete:           c.Bool("delete"),
			Checksum:         c.Bool("checksum"),
			AllowEmptySource: c.Bool("allow-empty-source"),
		},
		Croc: crocOptions,
		Decide: func(plan *foldersync.Plan) (foldersync.Decision, error) {
			return decideSync(plan, syncDecideOptions{
				Ctx:        c.Context,
				DryRun:     dryRun,
				Yes:        yes,
				Quiet:      quiet,
				Watch:      reporter != nil && reporter.isWatch(),
				IsTerminal: syncStdinIsTerminal(),
				In:         os.Stdin,
				Out:        out,
				Color:      colorEnabled,
			})
		},
		Reporter: reporter,
	})
	if reporter != nil && reporter.isWatch() {
		if err == nil || errors.Is(err, context.Canceled) {
			if !quiet {
				fmt.Fprintln(out, "Stopped watching.")
			}
			return nil
		}
		return err
	}
	if err != nil {
		return err
	}
	writeSyncSummary(out, res, quiet, colorEnabled)
	return syncResultError(res)
}

type syncReporter struct {
	w      io.Writer
	folder string
	quiet  bool
	color  bool

	mu    sync.Mutex
	watch bool
	round int
}

func newSyncReporter(w io.Writer, folder string, quiet bool) *syncReporter {
	return &syncReporter{
		w:      w,
		folder: folder,
		quiet:  quiet,
	}
}

func (r *syncReporter) isWatch() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.watch
}

func (r *syncReporter) Report(e foldersync.Event) {
	if e.Kind == foldersync.EventRoundStart {
		r.mu.Lock()
		r.watch = true
		r.round = parseSyncRound(e.Message, r.round)
		r.mu.Unlock()
		return
	}
	if r.quiet || r.w == nil {
		return
	}
	switch e.Kind {
	case foldersync.EventScanned:
		if e.Manifest == nil {
			return
		}
		var files, folders int
		var totalBytes int64
		for _, entry := range e.Manifest.Entries {
			if entry.IsDir() {
				folders++
			} else {
				files++
				totalBytes += entry.Size
			}
		}
		fmt.Fprintf(r.w, "Scanned %s: %d files, %d folders (%s)\n", r.folder, files, folders, utils.ByteCountDecimal(totalBytes))
	case foldersync.EventWarning:
		if e.Message != "" {
			fmt.Fprintf(r.w, "warning: %s\n", e.Message)
		}
	case foldersync.EventResult, foldersync.EventCommitted:
		r.mu.Lock()
		watch := r.watch
		round := r.round
		r.mu.Unlock()
		if !watch || e.Result == nil {
			return
		}
		summary := formatSyncSummary(e.Result, r.color)
		if summary != "" {
			fmt.Fprintf(r.w, "Round %d: %s", round, summary)
		}
	case foldersync.EventReconnecting:
		fmt.Fprintf(r.w, "%s\n", formatReconnectingEvent(e.Message))
	case foldersync.EventReconnected:
		fmt.Fprintln(r.w, "Reconnected.")
	}
}

func parseSyncRound(msg string, prev int) int {
	if rest, ok := strings.CutPrefix(strings.TrimSpace(msg), "round "); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(rest)); err == nil && n > 0 {
			return n
		}
	}
	return prev + 1
}

func formatReconnectingEvent(msg string) string {
	const prefix = "reconnecting in "
	const sep = " after error: "
	if rest, ok := strings.CutPrefix(msg, prefix); ok {
		if delay, errText, ok := strings.Cut(rest, sep); ok {
			return fmt.Sprintf("Connection lost (%s); reconnecting in %s...", errText, delay)
		}
	}
	if msg == "" {
		return "Connection lost; reconnecting..."
	}
	return fmt.Sprintf("Connection lost (%s); reconnecting...", msg)
}

func syncResultError(res *foldersync.Result) error {
	if res == nil || (!res.Partial() && res.DeletesHeld <= 0) {
		return nil
	}
	var parts []string
	if len(res.Failures) > 0 {
		detail := fmt.Sprintf("%d item(s) failed", len(res.Failures))
		if len(res.Failures) <= 3 {
			detail += ": " + strings.Join(res.Failures, "; ")
		} else {
			detail += fmt.Sprintf(": %s (and %d more)", strings.Join(res.Failures[:3], "; "), len(res.Failures)-3)
		}
		parts = append(parts, detail)
	}
	if res.DeletesHeld > 0 {
		detail := fmt.Sprintf("%d deletion(s) held", res.DeletesHeld)
		if res.DeleteBlocked != "" {
			detail += " (" + res.DeleteBlocked + ")"
		}
		parts = append(parts, detail)
	}
	return &ExitError{
		Code: 2,
		Err:  errors.New("sync completed partially: " + strings.Join(parts, "; ")),
	}
}

func formatPowerShellAssignment(name, value string, colorEnabled bool) string {
	psValue := strings.ReplaceAll(value, "'", "''")
	return termui.Color("$env:"+name+"='", termui.Cyan, colorEnabled) +
		termui.Secret(psValue, colorEnabled) + termui.Color("'", termui.Cyan, colorEnabled)
}

func formatSyncInstructions(secret, relayFlags, webURL string, colorEnabled bool) string {
	trimmedFlags := strings.TrimSpace(relayFlags)
	cmd := "croc "
	if trimmedFlags != "" {
		cmd += trimmedFlags + " "
	}
	cmd += "sync <folder>"
	unixCmd := formatShellAssignment("CROC_SECRET", secret, colorEnabled) +
		termui.Color(" "+cmd, termui.Cyan, colorEnabled)
	psCmd := formatPowerShellAssignment("CROC_SECRET", secret, colorEnabled) +
		termui.Color("; "+cmd, termui.Cyan, colorEnabled)
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n  %s      (Linux/macOS)\n  %s  (Windows PowerShell)\n",
		termui.Emphasis("On the other computer, run:", colorEnabled),
		unixCmd,
		psCmd,
	)
	if webURL != "" {
		fmt.Fprintf(&b, "Or open in Chrome/Edge: %s\n", termui.Color(webURL, termui.Cyan, colorEnabled))
	}
	return b.String()
}
