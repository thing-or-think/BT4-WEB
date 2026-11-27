// Command sync-bench benchmarks croc folder send and croc sync across
// deterministic datasets over a local relay fronted by a counting TCP proxy.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	datasetVersion = 1
	datasetSeed    = uint64(20261127)
	relayPortCount = 5
	tailLimitBytes = 64 * 1024
)

var (
	datasetBaseTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	modifyBaseTime  = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
)

type config struct {
	crocBin  string
	workDir  string
	runs     int
	datasets []string
	modes    []string
	scale    float64
	outPath  string
	timeout  time.Duration
}

type datasetMeta struct {
	Version    int     `json:"version"`
	Name       string  `json:"name"`
	Seed       uint64  `json:"seed"`
	Scale      float64 `json:"scale"`
	Files      int     `json:"files"`
	Folders    int     `json:"folders"`
	TotalBytes int64   `json:"total_bytes"`
}

type datasetSpec struct {
	Name       string
	Dir        string
	Files      int
	Folders    int
	TotalBytes int64
}

type datasetInfo struct {
	Name       string `json:"name"`
	Files      int    `json:"files"`
	Folders    int    `json:"folders"`
	TotalBytes int64  `json:"total_bytes"`
}

type metricStats struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
}

type runResult struct {
	Run            int     `json:"run"`
	Code           string  `json:"code"`
	Seconds        float64 `json:"seconds"`
	NetworkBytes   int64   `json:"network_bytes"`
	SourceRAMBytes uint64  `json:"source_ram_bytes"`
	DestRAMBytes   uint64  `json:"dest_ram_bytes"`
	VerifiedHash   bool    `json:"verified_hash"`
}

type caseResult struct {
	Dataset        string      `json:"dataset"`
	Mode           string      `json:"mode"`
	Runs           int         `json:"runs"`
	TimeSeconds    metricStats `json:"time_seconds"`
	NetworkBytes   metricStats `json:"network_bytes"`
	SourceRAMBytes metricStats `json:"source_ram_bytes"`
	DestRAMBytes   metricStats `json:"dest_ram_bytes"`
	Trials         []runResult `json:"trials"`
}

type benchmarkReport struct {
	Timestamp     string        `json:"timestamp"`
	GOOS          string        `json:"goos"`
	GOARCH        string        `json:"goarch"`
	CrocBinary    string        `json:"croc_binary"`
	Scale         float64       `json:"scale"`
	RunsPerCase   int           `json:"runs_per_case"`
	RelayHost     string        `json:"relay_host"`
	ProxyAllPorts bool          `json:"proxy_all_ports"`
	Datasets      []datasetInfo `json:"datasets"`
	Cases         []caseResult  `json:"cases"`
}

func main() {
	if err := runMain(); err != nil {
		fmt.Fprintf(os.Stderr, "sync-bench: %v\n", err)
		os.Exit(1)
	}
}

func runMain() error {
	cfg, err := parseFlags()
	if err != nil {
		return err
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	registry := newChildRegistry()
	defer registry.killAll()

	go func() {
		<-ctx.Done()
		registry.killAll()
	}()

	if err := os.MkdirAll(cfg.workDir, 0o755); err != nil {
		return fmt.Errorf("create work directory: %w", err)
	}

	datasets := make([]datasetSpec, 0, len(cfg.datasets))
	datasetInfos := make([]datasetInfo, 0, len(cfg.datasets))
	for _, name := range cfg.datasets {
		if err := ctx.Err(); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Preparing dataset %q (scale=%g)...\n", name, cfg.scale)
		ds, err := ensureDataset(ctx, cfg.workDir, name, cfg.scale)
		if err != nil {
			return fmt.Errorf("prepare dataset %q: %w", name, err)
		}
		fmt.Fprintf(os.Stderr, "Dataset %q ready: %d files, %d folders, %.2f MB\n",
			ds.Name, ds.Files, ds.Folders, float64(ds.TotalBytes)/1e6)
		datasets = append(datasets, ds)
		datasetInfos = append(datasetInfos, datasetInfo{
			Name:       ds.Name,
			Files:      ds.Files,
			Folders:    ds.Folders,
			TotalBytes: ds.TotalBytes,
		})
	}

	relay, err := startRelayWithProxy(ctx, cfg.crocBin, cfg.workDir, registry)
	if err != nil {
		return fmt.Errorf("start local relay and proxy: %w", err)
	}
	defer relay.close()

	fmt.Fprintf(os.Stderr, "Relay running on %s (proxied via %s, all_ports_proxied=%v)\n",
		relay.relayHost, relay.controlAddr(), relay.proxyAllPorts)

	var runSeq atomic.Int64
	var cases []caseResult

	for dsIdx, ds := range datasets {
		if err := ctx.Err(); err != nil {
			return err
		}
		dsCases, err := benchmarkDataset(ctx, cfg, relay, registry, &runSeq, dsIdx, ds)
		if err != nil {
			return err
		}
		cases = append(cases, dsCases...)
	}

	report := benchmarkReport{
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
		CrocBinary:    cfg.crocBin,
		Scale:         cfg.scale,
		RunsPerCase:   cfg.runs,
		RelayHost:     relay.relayHost,
		ProxyAllPorts: relay.proxyAllPorts,
		Datasets:      datasetInfos,
		Cases:         cases,
	}

	if err := writeReportJSON(cfg.outPath, report); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Wrote JSON results to %s\n", cfg.outPath)

	printMarkdownTable(os.Stdout, cases)
	return nil
}

func parseFlags() (config, error) {
	var (
		crocFlag     = flag.String("croc", "", "path to the croc binary")
		workFlag     = flag.String("work", "", "working directory for datasets and temporary run folders")
		runsFlag     = flag.Int("runs", 5, "number of measured runs per dataset and mode")
		datasetsFlag = flag.String("datasets", "small,large,mixed", "comma-separated datasets: small,large,mixed")
		modesFlag    = flag.String("modes", "send,sync-first,sync-unchanged,sync-5pct", "comma-separated modes: send,sync-first,sync-unchanged,sync-5pct")
		scaleFlag    = flag.Float64("scale", 1.0, "scale factor multiplying dataset sizes/counts for quick runs")
		outFlag      = flag.String("out", "", "path to output JSON file (default <work>/results.json)")
		timeoutFlag  = flag.Duration("timeout", 20*time.Minute, "per-run timeout")
	)
	flag.Parse()

	if strings.TrimSpace(*crocFlag) == "" {
		return config{}, errors.New("-croc flag is required")
	}
	crocAbs, err := filepath.Abs(*crocFlag)
	if err != nil {
		return config{}, fmt.Errorf("resolve -croc path: %w", err)
	}
	info, err := os.Stat(crocAbs)
	if err != nil {
		return config{}, fmt.Errorf("stat -croc binary %q: %w", crocAbs, err)
	}
	if info.IsDir() {
		return config{}, fmt.Errorf("-croc path %q is a directory", crocAbs)
	}

	work := strings.TrimSpace(*workFlag)
	if work == "" {
		work = filepath.Join(os.TempDir(), "croc-sync-bench")
	}
	workAbs, err := filepath.Abs(work)
	if err != nil {
		return config{}, fmt.Errorf("resolve -work path: %w", err)
	}

	if *runsFlag < 1 {
		return config{}, fmt.Errorf("-runs must be >= 1, got %d", *runsFlag)
	}
	if *scaleFlag <= 0 || math.IsNaN(*scaleFlag) || math.IsInf(*scaleFlag, 0) {
		return config{}, fmt.Errorf("-scale must be a finite positive number, got %v", *scaleFlag)
	}
	if *timeoutFlag <= 0 {
		return config{}, fmt.Errorf("-timeout must be positive, got %v", *timeoutFlag)
	}

	datasets, err := parseList(*datasetsFlag, []string{"small", "large", "mixed"})
	if err != nil {
		return config{}, fmt.Errorf("invalid -datasets: %w", err)
	}
	modes, err := parseList(*modesFlag, []string{"send", "sync-first", "sync-unchanged", "sync-5pct"})
	if err != nil {
		return config{}, fmt.Errorf("invalid -modes: %w", err)
	}

	outPath := strings.TrimSpace(*outFlag)
	if outPath == "" {
		outPath = filepath.Join(workAbs, "results.json")
	} else {
		outPath, err = filepath.Abs(outPath)
		if err != nil {
			return config{}, fmt.Errorf("resolve -out path: %w", err)
		}
	}

	return config{
		crocBin:  crocAbs,
		workDir:  workAbs,
		runs:     *runsFlag,
		datasets: datasets,
		modes:    modes,
		scale:    *scaleFlag,
		outPath:  outPath,
		timeout:  *timeoutFlag,
	}, nil
}

func parseList(raw string, allowed []string) ([]string, error) {
	allowedSet := make(map[string]bool, len(allowed))
	for _, a := range allowed {
		allowedSet[a] = true
	}
	seen := make(map[string]bool)
	var out []string
	for _, part := range strings.Split(raw, ",") {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		if !allowedSet[item] {
			return nil, fmt.Errorf("unknown value %q (allowed: %s)", item, strings.Join(allowed, ", "))
		}
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("list must not be empty")
	}
	return out, nil
}

// childRegistry tracks active child processes so every child is killed on exit
// or signal interruption.
type childRegistry struct {
	mu   sync.Mutex
	cmds map[*exec.Cmd]struct{}
}

func newChildRegistry() *childRegistry {
	return &childRegistry{cmds: make(map[*exec.Cmd]struct{})}
}

func (r *childRegistry) add(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	r.mu.Lock()
	r.cmds[cmd] = struct{}{}
	r.mu.Unlock()
}

func (r *childRegistry) remove(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	r.mu.Lock()
	delete(r.cmds, cmd)
	r.mu.Unlock()
}

func (r *childRegistry) killAll() {
	r.mu.Lock()
	cmds := make([]*exec.Cmd, 0, len(r.cmds))
	for cmd := range r.cmds {
		cmds = append(cmds, cmd)
	}
	r.mu.Unlock()
	for _, cmd := range cmds {
		killChildProcess(cmd)
	}
}

// --- Dataset generation ---

func ensureDataset(ctx context.Context, workDir, name string, scale float64) (datasetSpec, error) {
	datasetsRoot := filepath.Join(workDir, "datasets")
	if err := os.MkdirAll(datasetsRoot, 0o755); err != nil {
		return datasetSpec{}, err
	}
	dir := filepath.Join(datasetsRoot, name)
	metaPath := filepath.Join(datasetsRoot, name+".meta.json")

	if data, err := os.ReadFile(metaPath); err == nil {
		var meta datasetMeta
		if json.Unmarshal(data, &meta) == nil &&
			meta.Version == datasetVersion &&
			meta.Name == name &&
			meta.Seed == datasetSeed &&
			math.Abs(meta.Scale-scale) < 1e-9 {
			if st, statErr := os.Stat(dir); statErr == nil && st.IsDir() {
				return datasetSpec{
					Name:       name,
					Dir:        dir,
					Files:      meta.Files,
					Folders:    meta.Folders,
					TotalBytes: meta.TotalBytes,
				}, nil
			}
		}
	}

	_ = os.Remove(metaPath)
	if err := os.RemoveAll(dir); err != nil {
		return datasetSpec{}, fmt.Errorf("clean dataset directory: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return datasetSpec{}, err
	}

	var (
		files      int
		folders    int
		totalBytes int64
		err        error
	)
	switch name {
	case "small":
		files, folders, totalBytes, err = generateSmallDataset(ctx, dir, scale)
	case "large":
		files, folders, totalBytes, err = generateLargeDataset(ctx, dir, scale)
	case "mixed":
		files, folders, totalBytes, err = generateMixedDataset(ctx, dir, scale)
	default:
		return datasetSpec{}, fmt.Errorf("unsupported dataset %q", name)
	}
	if err != nil {
		_ = os.RemoveAll(dir)
		return datasetSpec{}, err
	}

	meta := datasetMeta{
		Version:    datasetVersion,
		Name:       name,
		Seed:       datasetSeed,
		Scale:      scale,
		Files:      files,
		Folders:    folders,
		TotalBytes: totalBytes,
	}
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return datasetSpec{}, err
	}
	metaBytes = append(metaBytes, '\n')
	if err := os.WriteFile(metaPath, metaBytes, 0o644); err != nil {
		return datasetSpec{}, err
	}

	return datasetSpec{
		Name:       name,
		Dir:        dir,
		Files:      files,
		Folders:    folders,
		TotalBytes: totalBytes,
	}, nil
}

func generateSmallDataset(ctx context.Context, dir string, scale float64) (int, int, int64, error) {
	rng := rand.New(rand.NewPCG(datasetSeed, 1))
	numFiles := max(1, int(math.Round(10000*scale)))
	numFolders := max(1, int(math.Round(100*scale)))
	if numFolders > numFiles {
		numFolders = numFiles
	}

	buf := make([]byte, 16*1024)
	createdDirs := make(map[string]bool, numFolders)
	var totalBytes int64

	for i := range numFiles {
		if err := ctx.Err(); err != nil {
			return 0, 0, 0, err
		}
		sub := fmt.Sprintf("dir_%03d", i%numFolders)
		subDir := filepath.Join(dir, sub)
		if !createdDirs[sub] {
			if err := os.MkdirAll(subDir, 0o755); err != nil {
				return 0, 0, 0, err
			}
			createdDirs[sub] = true
		}
		size := int64(1024 + rng.IntN(8192-1024+1))
		p := filepath.Join(subDir, fmt.Sprintf("file_%05d.bin", i))
		if err := writePseudoRandomFile(p, size, rng, buf, datasetBaseTime); err != nil {
			return 0, 0, 0, err
		}
		totalBytes += size
	}
	return numFiles, len(createdDirs), totalBytes, nil
}

func generateLargeDataset(ctx context.Context, dir string, scale float64) (int, int, int64, error) {
	rng := rand.New(rand.NewPCG(datasetSeed, 2))
	const numFiles = 20
	fileSize := max(int64(1024), int64(math.Round(float64(40<<20)*scale)))

	buf := make([]byte, 1<<20)
	var totalBytes int64
	for i := range numFiles {
		if err := ctx.Err(); err != nil {
			return 0, 0, 0, err
		}
		p := filepath.Join(dir, fmt.Sprintf("large_%02d.bin", i))
		if err := writePseudoRandomFile(p, fileSize, rng, buf, datasetBaseTime); err != nil {
			return 0, 0, 0, err
		}
		totalBytes += fileSize
	}
	return numFiles, 0, totalBytes, nil
}

func generateMixedDataset(ctx context.Context, dir string, scale float64) (int, int, int64, error) {
	rng := rand.New(rand.NewPCG(datasetSeed, 3))
	type tier struct {
		count   int
		minSize int64
		maxSize int64
	}
	// At scale=1.0: 2,005 files spanning 1 KiB to 64 MiB totaling ~1 GiB.
	tiers := []tier{
		{count: 1350, minSize: 1 << 10, maxSize: 16 << 10},
		{count: 450, minSize: 16 << 10, maxSize: 256 << 10},
		{count: 160, minSize: 256 << 10, maxSize: 2 << 20},
		{count: 34, minSize: 2 << 20, maxSize: 16 << 20},
		{count: 11, minSize: 16 << 20, maxSize: 64 << 20},
	}

	buf := make([]byte, 1<<20)
	createdDirs := make(map[string]bool)
	fileIndex := 0
	var totalBytes int64

	for tierIdx, t := range tiers {
		scaled := float64(t.count) * scale
		count := 1
		sizeScale := 1.0
		if scaled >= 1.0 {
			count = int(math.Round(scaled))
		} else {
			sizeScale = scaled
		}
		minSize := max(int64(1024), int64(math.Round(float64(t.minSize)*sizeScale)))
		maxSize := max(minSize, int64(math.Round(float64(t.maxSize)*sizeScale)))

		for i := range count {
			if err := ctx.Err(); err != nil {
				return 0, 0, 0, err
			}
			var size int64
			if tierIdx == len(tiers)-1 && i == count-1 {
				// Ensure the upper bound (64 MiB at scale=1) is represented.
				size = maxSize
			} else if maxSize == minSize {
				size = minSize
			} else {
				size = minSize + rng.Int64N(maxSize-minSize+1)
			}

			relDir := mixedNestedDir(fileIndex)
			fullDir := filepath.Join(dir, filepath.FromSlash(relDir))
			if !createdDirs[relDir] {
				if err := os.MkdirAll(fullDir, 0o755); err != nil {
					return 0, 0, 0, err
				}
				// Record ancestor folders for accurate folder counting.
				cur := relDir
				for cur != "." && cur != "" {
					createdDirs[cur] = true
					idx := strings.LastIndexByte(cur, '/')
					if idx < 0 {
						break
					}
					cur = cur[:idx]
				}
			}

			p := filepath.Join(fullDir, fmt.Sprintf("t%d_%05d.bin", tierIdx, fileIndex))
			if err := writePseudoRandomFile(p, size, rng, buf, datasetBaseTime); err != nil {
				return 0, 0, 0, err
			}
			fileIndex++
			totalBytes += size
		}
	}
	return fileIndex, len(createdDirs), totalBytes, nil
}

func mixedNestedDir(index int) string {
	top := index % 10
	mid := (index / 10) % 5
	leaf := (index / 50) % 4
	switch index % 3 {
	case 0:
		return fmt.Sprintf("top_%02d", top)
	case 1:
		return fmt.Sprintf("top_%02d/sub_%d", top, mid)
	default:
		return fmt.Sprintf("top_%02d/sub_%d/leaf_%d", top, mid, leaf)
	}
}

func fillPseudoRandom(rng *rand.Rand, dst []byte) {
	i := 0
	for ; i+8 <= len(dst); i += 8 {
		binary.LittleEndian.PutUint64(dst[i:i+8], rng.Uint64())
	}
	if i < len(dst) {
		v := rng.Uint64()
		for ; i < len(dst); i++ {
			dst[i] = byte(v)
			v >>= 8
		}
	}
}

func writePseudoRandomFile(path string, size int64, rng *rand.Rand, buf []byte, modTime time.Time) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	remaining := size
	for remaining > 0 {
		chunk := buf
		if int64(len(chunk)) > remaining {
			chunk = buf[:remaining]
		}
		fillPseudoRandom(rng, chunk)
		if _, err := f.Write(chunk); err != nil {
			_ = f.Close()
			return err
		}
		remaining -= int64(len(chunk))
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Chtimes(path, modTime, modTime)
}

// --- Relay + Counting TCP Proxy ---

type relayProxy struct {
	relayHost     string
	relayPorts    []int
	proxyPorts    []int
	proxyAllPorts bool

	relayCmd   *exec.Cmd
	relayOut   *tailBuffer
	relayDone  chan struct{}
	registry   *childRegistry
	listeners  []net.Listener
	totalBytes atomic.Int64
	active     atomic.Int64
	connsMu    sync.Mutex
	conns      map[net.Conn]struct{}
	wg         sync.WaitGroup
	closeOnce  sync.Once
}

func startRelayWithProxy(ctx context.Context, crocBin, workDir string, registry *childRegistry) (*relayProxy, error) {
	relayHost, relayNet, samePorts := detectRelayBindHost()

	listeners := make([]net.Listener, 0, relayPortCount)
	proxyPorts := make([]int, 0, relayPortCount)
	relayPorts := make([]int, 0, relayPortCount)
	cleanupListeners := func() {
		for _, ln := range listeners {
			_ = ln.Close()
		}
	}

	if samePorts {
		used := make(map[int]bool, relayPortCount)
		for attempts := 0; len(listeners) < relayPortCount && attempts < 200; attempts++ {
			lnProxy, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				cleanupListeners()
				return nil, fmt.Errorf("listen on 127.0.0.1:0: %w", err)
			}
			port := lnProxy.Addr().(*net.TCPAddr).Port
			if used[port] {
				_ = lnProxy.Close()
				continue
			}
			probe, err := net.Listen(relayNet, net.JoinHostPort(relayHost, strconv.Itoa(port)))
			if err != nil {
				_ = lnProxy.Close()
				continue
			}
			_ = probe.Close()
			used[port] = true
			listeners = append(listeners, lnProxy)
			proxyPorts = append(proxyPorts, port)
			relayPorts = append(relayPorts, port)
		}
		if len(listeners) < relayPortCount {
			cleanupListeners()
			return nil, fmt.Errorf("could not allocate %d shared ports on 127.0.0.1 and %s", relayPortCount, relayHost)
		}
	} else {
		// Fallback when neither 127.0.0.2 nor ::1 is bindable: use 127.0.0.1
		// with distinct ports for the proxy and relay.
		for range relayPortCount {
			lnProxy, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				cleanupListeners()
				return nil, fmt.Errorf("listen proxy on 127.0.0.1:0: %w", err)
			}
			listeners = append(listeners, lnProxy)
			proxyPorts = append(proxyPorts, lnProxy.Addr().(*net.TCPAddr).Port)
		}
		relayProbes := make([]net.Listener, 0, relayPortCount)
		for range relayPortCount {
			probe, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				for _, p := range relayProbes {
					_ = p.Close()
				}
				cleanupListeners()
				return nil, fmt.Errorf("allocate relay port on 127.0.0.1: %w", err)
			}
			relayProbes = append(relayProbes, probe)
			relayPorts = append(relayPorts, probe.Addr().(*net.TCPAddr).Port)
		}
		for _, p := range relayProbes {
			_ = p.Close()
		}
	}

	portStrings := make([]string, len(relayPorts))
	for i, p := range relayPorts {
		portStrings[i] = strconv.Itoa(p)
	}

	relayConfigDir := filepath.Join(workDir, "tmp", "config-relay")
	if err := os.MkdirAll(relayConfigDir, 0o755); err != nil {
		cleanupListeners()
		return nil, err
	}

	relayOut := newTailBuffer(tailLimitBytes)
	cmd := exec.Command(crocBin,
		"--no-update-check",
		"relay",
		"--host", relayHost,
		"--ports", strings.Join(portStrings, ","),
		"--source-join-limit", "10000",
		"--room-join-limit", "10000",
		"--max-pending-handshakes", "1000",
		"--max-pending-handshakes-per-source", "1000",
	)
	cmd.Env = cleanEnv(
		"CROC_CONFIG_DIR="+relayConfigDir,
		"XDG_CONFIG_HOME="+relayConfigDir,
		"NO_COLOR=1",
		"TERM=dumb",
	)
	cmd.Stdout = relayOut
	cmd.Stderr = relayOut
	configureChildCmd(cmd)

	if err := cmd.Start(); err != nil {
		cleanupListeners()
		return nil, fmt.Errorf("start croc relay: %w", err)
	}
	registry.add(cmd)

	relayDone := make(chan struct{})
	var relayExitErr error
	go func() {
		relayExitErr = cmd.Wait()
		registry.remove(cmd)
		close(relayDone)
	}()

	if err := waitForRelayPorts(ctx, relayHost, relayPorts, relayDone, &relayExitErr, relayOut); err != nil {
		killChildProcess(cmd)
		<-relayDone
		cleanupListeners()
		return nil, err
	}

	rp := &relayProxy{
		relayHost:     relayHost,
		relayPorts:    relayPorts,
		proxyPorts:    proxyPorts,
		proxyAllPorts: samePorts,
		relayCmd:      cmd,
		relayOut:      relayOut,
		relayDone:     relayDone,
		registry:      registry,
		listeners:     listeners,
		conns:         make(map[net.Conn]struct{}),
	}

	for i, ln := range listeners {
		target := net.JoinHostPort(relayHost, strconv.Itoa(relayPorts[i]))
		rp.wg.Add(1)
		go rp.serveListener(ln, target)
	}

	return rp, nil
}

func detectRelayBindHost() (host, network string, samePorts bool) {
	if ln, err := net.Listen("tcp4", "127.0.0.2:0"); err == nil {
		_ = ln.Close()
		return "127.0.0.2", "tcp4", true
	}
	if ln, err := net.Listen("tcp6", "[::1]:0"); err == nil {
		_ = ln.Close()
		return "::1", "tcp6", true
	}
	return "127.0.0.1", "tcp4", false
}

func waitForRelayPorts(ctx context.Context, host string, ports []int, relayDone <-chan struct{}, relayExitErr *error, relayOut *tailBuffer) error {
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		allReady := true
		for _, port := range ports {
			conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 100*time.Millisecond)
			if err != nil {
				allReady = false
				break
			}
			_ = conn.Close()
		}
		if allReady {
			return nil
		}
		select {
		case <-relayDone:
			return fmt.Errorf("croc relay exited early (%v):\n%s", *relayExitErr, relayOut.String())
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for croc relay on %s ports %v:\n%s", host, ports, relayOut.String())
		case <-ticker.C:
		}
	}
}

func (rp *relayProxy) controlAddr() string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(rp.proxyPorts[0]))
}

func (rp *relayProxy) bytes() int64 {
	return rp.totalBytes.Load()
}

func (rp *relayProxy) waitIdle(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for rp.active.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}

func (rp *relayProxy) trackConn(c net.Conn) {
	rp.connsMu.Lock()
	rp.conns[c] = struct{}{}
	rp.connsMu.Unlock()
}

func (rp *relayProxy) untrackConn(c net.Conn) {
	rp.connsMu.Lock()
	delete(rp.conns, c)
	rp.connsMu.Unlock()
}

func (rp *relayProxy) serveListener(ln net.Listener, target string) {
	defer rp.wg.Done()
	for {
		clientConn, err := ln.Accept()
		if err != nil {
			return
		}
		rp.active.Add(1)
		rp.wg.Add(1)
		go func(c net.Conn) {
			defer rp.wg.Done()
			defer rp.active.Add(-1)
			rp.handleConn(c, target)
		}(clientConn)
	}
}

func (rp *relayProxy) handleConn(clientConn net.Conn, target string) {
	rp.trackConn(clientConn)
	defer func() {
		rp.untrackConn(clientConn)
		_ = clientConn.Close()
	}()

	backendConn, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		return
	}
	rp.trackConn(backendConn)
	defer func() {
		rp.untrackConn(backendConn)
		_ = backendConn.Close()
	}()

	var copyWG sync.WaitGroup
	copyWG.Add(2)
	pipe := func(dst, src net.Conn) {
		defer copyWG.Done()
		buf := make([]byte, 64*1024)
		_, _ = io.CopyBuffer(&countingWriter{w: dst, counter: &rp.totalBytes}, src, buf)
		if tc, ok := dst.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}
	go pipe(backendConn, clientConn)
	go pipe(clientConn, backendConn)
	copyWG.Wait()
}

func (rp *relayProxy) close() {
	rp.closeOnce.Do(func() {
		for _, ln := range rp.listeners {
			_ = ln.Close()
		}
		rp.connsMu.Lock()
		for c := range rp.conns {
			_ = c.Close()
		}
		rp.connsMu.Unlock()

		killChildProcess(rp.relayCmd)
		<-rp.relayDone
		rp.wg.Wait()
	})
}

type countingWriter struct {
	w       io.Writer
	counter *atomic.Int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	if n > 0 {
		cw.counter.Add(int64(n))
	}
	return n, err
}

// --- Benchmark orchestration ---

func benchmarkDataset(
	ctx context.Context,
	cfg config,
	relay *relayProxy,
	registry *childRegistry,
	runSeq *atomic.Int64,
	dsIdx int,
	ds datasetSpec,
) ([]caseResult, error) {
	dsTmpRoot := filepath.Join(cfg.workDir, "tmp", ds.Name)
	if err := os.MkdirAll(dsTmpRoot, 0o755); err != nil {
		return nil, err
	}

	var (
		syncedDest          string
		fullHashDoneForSync bool
		results             []caseResult
	)
	defer func() {
		if syncedDest != "" {
			_ = os.RemoveAll(syncedDest)
		}
	}()

	for _, mode := range cfg.modes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var trials []runResult

		switch mode {
		case "send":
			for r := 1; r <= cfg.runs; r++ {
				dstDir := filepath.Join(dsTmpRoot, fmt.Sprintf("send-run-%d", r))
				if err := resetEmptyDir(dstDir); err != nil {
					return nil, err
				}
				res, err := runTransfer(ctx, cfg, relay, registry, runSeq, transferParams{
					mode:   "send",
					srcDir: ds.Dir,
					dstDir: dstDir,
					runNum: r,
				})
				if err != nil {
					_ = os.RemoveAll(dstDir)
					return nil, fmt.Errorf("%s/%s run %d: %w", ds.Name, mode, r, err)
				}
				receivedRoot := filepath.Join(dstDir, filepath.Base(ds.Dir))
				if err := verifyTrees(ds.Dir, receivedRoot, false, nil, false); err != nil {
					return nil, fmt.Errorf("%s/%s run %d verification failed: %w", ds.Name, mode, r, err)
				}
				_ = os.RemoveAll(dstDir)
				trials = append(trials, res)
				logTrial(ds.Name, mode, r, cfg.runs, res)
			}

		case "sync-first":
			for r := 1; r <= cfg.runs; r++ {
				dstDir := filepath.Join(dsTmpRoot, fmt.Sprintf("sync-first-run-%d", r))
				if err := resetEmptyDir(dstDir); err != nil {
					return nil, err
				}
				res, err := runTransfer(ctx, cfg, relay, registry, runSeq, transferParams{
					mode:   "sync",
					srcDir: ds.Dir,
					dstDir: dstDir,
					runNum: r,
				})
				if err != nil {
					_ = os.RemoveAll(dstDir)
					return nil, fmt.Errorf("%s/%s run %d: %w", ds.Name, mode, r, err)
				}
				doFullHash := !fullHashDoneForSync
				if err := verifyTrees(ds.Dir, dstDir, doFullHash, nil, true); err != nil {
					return nil, fmt.Errorf("%s/%s run %d verification failed: %w", ds.Name, mode, r, err)
				}
				if doFullHash {
					fullHashDoneForSync = true
					res.VerifiedHash = true
				}
				if r < cfg.runs {
					_ = os.RemoveAll(dstDir)
				} else {
					if syncedDest != "" && syncedDest != dstDir {
						_ = os.RemoveAll(syncedDest)
					}
					syncedDest = dstDir
				}
				trials = append(trials, res)
				logTrial(ds.Name, mode, r, cfg.runs, res)
			}

		case "sync-unchanged":
			if syncedDest == "" {
				warmDst := filepath.Join(dsTmpRoot, "sync-warm-dst")
				if err := resetEmptyDir(warmDst); err != nil {
					return nil, err
				}
				fmt.Fprintf(os.Stderr, "[%s/%s] warming destination via croc sync...\n", ds.Name, mode)
				if _, err := runTransfer(ctx, cfg, relay, registry, runSeq, transferParams{
					mode:   "sync",
					srcDir: ds.Dir,
					dstDir: warmDst,
					runNum: 0,
				}); err != nil {
					_ = os.RemoveAll(warmDst)
					return nil, fmt.Errorf("%s/%s warm sync: %w", ds.Name, mode, err)
				}
				if err := verifyTrees(ds.Dir, warmDst, false, nil, true); err != nil {
					return nil, fmt.Errorf("%s/%s warm verification failed: %w", ds.Name, mode, err)
				}
				syncedDest = warmDst
			}
			for r := 1; r <= cfg.runs; r++ {
				res, err := runTransfer(ctx, cfg, relay, registry, runSeq, transferParams{
					mode:   "sync",
					srcDir: ds.Dir,
					dstDir: syncedDest,
					runNum: r,
				})
				if err != nil {
					return nil, fmt.Errorf("%s/%s run %d: %w", ds.Name, mode, r, err)
				}
				doFullHash := !fullHashDoneForSync
				if err := verifyTrees(ds.Dir, syncedDest, doFullHash, nil, true); err != nil {
					return nil, fmt.Errorf("%s/%s run %d verification failed: %w", ds.Name, mode, r, err)
				}
				if doFullHash {
					fullHashDoneForSync = true
					res.VerifiedHash = true
				}
				trials = append(trials, res)
				logTrial(ds.Name, mode, r, cfg.runs, res)
			}

		case "sync-5pct":
			srcCopy := filepath.Join(dsTmpRoot, "sync-5pct-src")
			dst5 := filepath.Join(dsTmpRoot, "sync-5pct-dst")
			if err := copyTreePreserveMeta(ctx, ds.Dir, srcCopy); err != nil {
				return nil, fmt.Errorf("%s/%s prepare source copy: %w", ds.Name, mode, err)
			}
			_ = os.RemoveAll(dst5)
			if syncedDest != "" {
				if err := os.Rename(syncedDest, dst5); err != nil {
					_ = os.RemoveAll(srcCopy)
					return nil, fmt.Errorf("%s/%s move synced destination: %w", ds.Name, mode, err)
				}
				syncedDest = ""
			} else {
				if err := resetEmptyDir(dst5); err != nil {
					_ = os.RemoveAll(srcCopy)
					return nil, err
				}
				fmt.Fprintf(os.Stderr, "[%s/%s] warming destination via croc sync...\n", ds.Name, mode)
				if _, err := runTransfer(ctx, cfg, relay, registry, runSeq, transferParams{
					mode:   "sync",
					srcDir: srcCopy,
					dstDir: dst5,
					runNum: 0,
				}); err != nil {
					_ = os.RemoveAll(srcCopy)
					_ = os.RemoveAll(dst5)
					return nil, fmt.Errorf("%s/%s warm sync: %w", ds.Name, mode, err)
				}
				if err := verifyTrees(srcCopy, dst5, false, nil, true); err != nil {
					return nil, fmt.Errorf("%s/%s warm verification failed: %w", ds.Name, mode, err)
				}
			}

			relFiles, err := collectRelativeFiles(srcCopy)
			if err != nil {
				_ = os.RemoveAll(srcCopy)
				_ = os.RemoveAll(dst5)
				return nil, fmt.Errorf("%s/%s scan source copy: %w", ds.Name, mode, err)
			}

			for r := 1; r <= cfg.runs; r++ {
				modified, err := mutateFivePercent(ctx, srcCopy, relFiles, dsIdx, r)
				if err != nil {
					_ = os.RemoveAll(srcCopy)
					_ = os.RemoveAll(dst5)
					return nil, fmt.Errorf("%s/%s run %d mutate 5%%: %w", ds.Name, mode, r, err)
				}
				res, err := runTransfer(ctx, cfg, relay, registry, runSeq, transferParams{
					mode:   "sync",
					srcDir: srcCopy,
					dstDir: dst5,
					runNum: r,
				})
				if err != nil {
					_ = os.RemoveAll(srcCopy)
					_ = os.RemoveAll(dst5)
					return nil, fmt.Errorf("%s/%s run %d: %w", ds.Name, mode, r, err)
				}
				doFullHash := !fullHashDoneForSync
				if err := verifyTrees(srcCopy, dst5, doFullHash, modified, true); err != nil {
					return nil, fmt.Errorf("%s/%s run %d verification failed: %w", ds.Name, mode, r, err)
				}
				if doFullHash {
					fullHashDoneForSync = true
				}
				res.VerifiedHash = true
				trials = append(trials, res)
				logTrial(ds.Name, mode, r, cfg.runs, res)
			}
			_ = os.RemoveAll(srcCopy)
			_ = os.RemoveAll(dst5)
		}

		cr := summarizeCase(ds.Name, mode, trials)
		fmt.Fprintf(os.Stderr,
			"[%s/%s] summary: median=%.3fs (min=%.3fs max=%.3fs) net=%.2f MB (%.2f..%.2f) srcRAM=%.2f MB (%.2f..%.2f) dstRAM=%.2f MB (%.2f..%.2f)\n",
			ds.Name, mode,
			cr.TimeSeconds.Median, cr.TimeSeconds.Min, cr.TimeSeconds.Max,
			cr.NetworkBytes.Median/1e6, cr.NetworkBytes.Min/1e6, cr.NetworkBytes.Max/1e6,
			cr.SourceRAMBytes.Median/1e6, cr.SourceRAMBytes.Min/1e6, cr.SourceRAMBytes.Max/1e6,
			cr.DestRAMBytes.Median/1e6, cr.DestRAMBytes.Min/1e6, cr.DestRAMBytes.Max/1e6,
		)
		results = append(results, cr)
	}

	return results, nil
}

func logTrial(dataset, mode string, run, total int, r runResult) {
	fmt.Fprintf(os.Stderr, "[%s/%s] run %d/%d: %.3fs, net=%.2f MB, srcRAM=%.2f MB, dstRAM=%.2f MB\n",
		dataset, mode, run, total, r.Seconds,
		float64(r.NetworkBytes)/1e6,
		float64(r.SourceRAMBytes)/1e6,
		float64(r.DestRAMBytes)/1e6,
	)
}

type transferParams struct {
	mode   string // "send" or "sync"
	srcDir string
	dstDir string
	runNum int
}

type procOutcome struct {
	peakRAM uint64
	err     error
}

func runTransfer(
	ctx context.Context,
	cfg config,
	relay *relayProxy,
	registry *childRegistry,
	runSeq *atomic.Int64,
	params transferParams,
) (runResult, error) {
	seq := runSeq.Add(1)
	presetCode := fmt.Sprintf("%04d-sync-bench-%d", 1000+int((seq-1)%9000), seq)
	useCodeFlag := runtime.GOOS == "windows"

	runCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()

	srcCfgDir := filepath.Join(cfg.workDir, "tmp", "config-src")
	dstCfgDir := filepath.Join(cfg.workDir, "tmp", "config-dst")
	if err := os.MkdirAll(srcCfgDir, 0o755); err != nil {
		return runResult{}, err
	}
	if err := os.MkdirAll(dstCfgDir, 0o755); err != nil {
		return runResult{}, err
	}

	globalFlags := []string{
		"--relay", relay.controlAddr(),
		"--no-update-check",
		"--disable-clipboard",
	}

	var srcArgs []string
	srcArgs = append(srcArgs, globalFlags...)
	if params.mode == "send" {
		srcArgs = append(srcArgs, "--ignore-stdin", "send")
	} else {
		srcArgs = append(srcArgs, "sync")
	}
	if useCodeFlag {
		srcArgs = append(srcArgs, "--code", presetCode)
	}
	srcArgs = append(srcArgs, "--no-local", "--transport", "relay", params.srcDir)

	sniffer := newCodeSniffer(tailLimitBytes)
	srcCmd := exec.Command(cfg.crocBin, srcArgs...)
	srcCmd.Env = cleanEnv(
		"CROC_CONFIG_DIR="+srcCfgDir,
		"XDG_CONFIG_HOME="+srcCfgDir,
		"NO_COLOR=1",
		"TERM=dumb",
	)
	srcCmd.Stdout = sniffer
	srcCmd.Stderr = sniffer
	configureChildCmd(srcCmd)

	relay.waitIdle(2 * time.Second)
	netBefore := relay.bytes()

	if err := srcCmd.Start(); err != nil {
		return runResult{}, fmt.Errorf("start source croc: %w", err)
	}
	registry.add(srcCmd)
	srcTracker := startProcessTracker(srcCmd)

	srcDone := make(chan procOutcome, 1)
	go func() {
		peak, err := srcTracker.waitAndPeakRAM(srcCmd)
		registry.remove(srcCmd)
		srcDone <- procOutcome{peakRAM: peak, err: err}
	}()

	var (
		dstMu  sync.Mutex
		dstCmd *exec.Cmd
	)
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-runCtx.Done():
			killChildProcess(srcCmd)
			dstMu.Lock()
			d := dstCmd
			dstMu.Unlock()
			killChildProcess(d)
		case <-stopWatch:
		}
	}()

	var activeCode string
	select {
	case <-sniffer.ready:
		if useCodeFlag {
			activeCode = presetCode
		} else {
			activeCode = sniffer.code()
		}
	case out := <-srcDone:
		return runResult{}, fmt.Errorf("source exited before printing code (%v):\n%s", out.err, sniffer.tail())
	case <-runCtx.Done():
		killChildProcess(srcCmd)
		<-srcDone
		return runResult{}, fmt.Errorf("timed out waiting for source code (%w):\n%s", runCtx.Err(), sniffer.tail())
	}

	if activeCode == "" {
		killChildProcess(srcCmd)
		<-srcDone
		return runResult{}, fmt.Errorf("failed to extract code from source output:\n%s", sniffer.tail())
	}

	var dstArgs []string
	dstArgs = append(dstArgs, globalFlags...)
	if params.mode == "send" {
		dstArgs = append(dstArgs, "--yes", "--overwrite")
	} else {
		dstArgs = append(dstArgs, "sync", params.dstDir)
	}

	dstOut := newTailBuffer(tailLimitBytes)
	cmdDst := exec.Command(cfg.crocBin, dstArgs...)
	if params.mode == "send" {
		cmdDst.Dir = params.dstDir
	}
	cmdDst.Env = cleanEnv(
		"CROC_SECRET="+activeCode,
		"CROC_CONFIG_DIR="+dstCfgDir,
		"XDG_CONFIG_HOME="+dstCfgDir,
		"NO_COLOR=1",
		"TERM=dumb",
	)
	cmdDst.Stdout = dstOut
	cmdDst.Stderr = dstOut
	configureChildCmd(cmdDst)

	startTime := time.Now()
	if err := cmdDst.Start(); err != nil {
		killChildProcess(srcCmd)
		<-srcDone
		return runResult{}, fmt.Errorf("start destination croc: %w", err)
	}
	dstMu.Lock()
	dstCmd = cmdDst
	dstMu.Unlock()
	registry.add(cmdDst)
	dstTracker := startProcessTracker(cmdDst)

	dstDone := make(chan procOutcome, 1)
	go func() {
		peak, err := dstTracker.waitAndPeakRAM(cmdDst)
		registry.remove(cmdDst)
		dstDone <- procOutcome{peakRAM: peak, err: err}
	}()

	srcRes, dstRes := waitBothProcesses(srcCmd, cmdDst, srcDone, dstDone)
	elapsed := time.Since(startTime)

	relay.waitIdle(2 * time.Second)
	netBytes := relay.bytes() - netBefore

	if runCtx.Err() != nil {
		return runResult{}, fmt.Errorf("run aborted (%w); srcErr=%v dstErr=%v\n--- source ---\n%s\n--- destination ---\n%s",
			runCtx.Err(), srcRes.err, dstRes.err, sniffer.tail(), dstOut.String())
	}
	if srcRes.err != nil || dstRes.err != nil {
		return runResult{}, fmt.Errorf("process failed: srcErr=%v dstErr=%v\n--- source ---\n%s\n--- destination ---\n%s",
			srcRes.err, dstRes.err, sniffer.tail(), dstOut.String())
	}

	return runResult{
		Run:            params.runNum,
		Code:           activeCode,
		Seconds:        elapsed.Seconds(),
		NetworkBytes:   netBytes,
		SourceRAMBytes: srcRes.peakRAM,
		DestRAMBytes:   dstRes.peakRAM,
	}, nil
}

func waitBothProcesses(
	srcCmd, dstCmd *exec.Cmd,
	srcDone, dstDone <-chan procOutcome,
) (procOutcome, procOutcome) {
	var srcRes, dstRes procOutcome
	select {
	case srcRes = <-srcDone:
		dstRes = waitPeer(dstCmd, dstDone, srcRes.err != nil)
	case dstRes = <-dstDone:
		srcRes = waitPeer(srcCmd, srcDone, dstRes.err != nil)
	}
	return srcRes, dstRes
}

func waitPeer(cmd *exec.Cmd, done <-chan procOutcome, peerFailed bool) procOutcome {
	if !peerFailed {
		return <-done
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case res := <-done:
		return res
	case <-timer.C:
		killChildProcess(cmd)
		return <-done
	}
}

func cleanEnv(extra ...string) []string {
	drop := []string{
		"CROC_SECRET",
		"CROC_RELAY",
		"CROC_RELAY6",
		"CROC_PASS",
		"CROC_PORTS",
		"CROC_PORT",
		"CROC_CONFIG_DIR",
		"XDG_CONFIG_HOME",
		"NO_COLOR",
		"TERM",
		"SOCKS5_PROXY",
		"HTTP_PROXY",
	}
	isDropped := func(key string) bool {
		for _, d := range drop {
			if strings.EqualFold(key, d) {
				return true
			}
		}
		return false
	}

	env := make([]string, 0, len(os.Environ())+len(extra))
	for _, kv := range os.Environ() {
		key, _, _ := strings.Cut(kv, "=")
		if isDropped(key) {
			continue
		}
		env = append(env, kv)
	}
	return append(env, extra...)
}

// --- Output buffering & code extraction ---

type tailBuffer struct {
	mu    sync.Mutex
	limit int
	buf   []byte
}

func newTailBuffer(limit int) *tailBuffer {
	return &tailBuffer{limit: limit}
}

func (tb *tailBuffer) Write(p []byte) (int, error) {
	tb.mu.Lock()
	tb.buf = append(tb.buf, p...)
	if len(tb.buf) > tb.limit {
		tb.buf = append(tb.buf[:0], tb.buf[len(tb.buf)-tb.limit:]...)
	}
	tb.mu.Unlock()
	return len(p), nil
}

func (tb *tailBuffer) String() string {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return string(tb.buf)
}

type codeSniffer struct {
	mu        sync.Mutex
	tailBuf   *tailBuffer
	lineBuf   bytes.Buffer
	foundCode string
	ready     chan struct{}
	once      sync.Once
}

func newCodeSniffer(tailLimit int) *codeSniffer {
	return &codeSniffer{
		tailBuf: newTailBuffer(tailLimit),
		ready:   make(chan struct{}),
	}
}

func (cs *codeSniffer) Write(p []byte) (int, error) {
	_, _ = cs.tailBuf.Write(p)
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.foundCode != "" {
		return len(p), nil
	}
	for _, b := range p {
		if b == '\n' {
			cs.inspectLine(cs.lineBuf.String())
			cs.lineBuf.Reset()
			if cs.foundCode != "" {
				return len(p), nil
			}
			continue
		}
		if cs.lineBuf.Len() < 4096 {
			cs.lineBuf.WriteByte(b)
		}
	}
	return len(p), nil
}

func (cs *codeSniffer) inspectLine(raw string) {
	line := strings.TrimSpace(stripANSI(raw))
	if line == "" {
		return
	}
	if code := extractCodeFromLine(line); code != "" {
		cs.foundCode = code
		cs.once.Do(func() { close(cs.ready) })
	}
}

func (cs *codeSniffer) code() string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.foundCode
}

func (cs *codeSniffer) tail() string {
	return cs.tailBuf.String()
}

func stripANSI(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			i += 2
			for i < len(s) && !((s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= 'a' && s[i] <= 'z')) {
				i++
			}
			continue
		}
		if s[i] != '\r' {
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func extractCodeFromLine(line string) string {
	// 1. CROC_SECRET='...' (sync instructions and Unix send instructions).
	if idx := strings.Index(line, "CROC_SECRET='"); idx >= 0 {
		rest := line[idx+len("CROC_SECRET='"):]
		var out strings.Builder
		for i := 0; i < len(rest); i++ {
			if rest[i] == '\'' {
				if strings.HasPrefix(rest[i:], `'\''`) {
					out.WriteByte('\'')
					i += 3
					continue
				}
				if s := strings.TrimSpace(out.String()); len(s) >= 6 {
					return s
				}
				break
			}
			out.WriteByte(rest[i])
		}
	}

	// 2. Unquoted CROC_SECRET=<value>
	if idx := strings.Index(line, "CROC_SECRET="); idx >= 0 {
		rest := line[idx+len("CROC_SECRET="):]
		if !strings.HasPrefix(rest, "'") {
			tok := rest
			if sp := strings.IndexAny(tok, " \t;"); sp >= 0 {
				tok = tok[:sp]
			}
			tok = strings.Trim(tok, `"'`)
			if len(tok) >= 6 && tok != "****" && tok != "***" {
				return tok
			}
		}
	}

	// 3. "Code is: <code>"
	if rest, ok := strings.CutPrefix(line, "Code is:"); ok {
		tok := strings.TrimSpace(rest)
		if sp := strings.IndexAny(tok, " \t"); sp >= 0 {
			tok = tok[:sp]
		}
		if len(tok) >= 6 {
			return tok
		}
	}

	// 4. "croc [flags...] <code>" (send instructions).
	if strings.HasPrefix(line, "croc ") {
		tokens := splitShellTokens(line)
		if len(tokens) >= 2 && tokens[0] == "croc" {
			var pos []string
			for i := 1; i < len(tokens); i++ {
				tok := tokens[i]
				if strings.HasPrefix(tok, "(") {
					break
				}
				switch tok {
				case "--relay", "--relay6", "--pass", "--out", "--ip", "--curve", "--multicast", "--web-url", "--socks5", "--connect", "--throttleUpload":
					i++
					continue
				}
				if strings.HasPrefix(tok, "-") {
					continue
				}
				pos = append(pos, tok)
			}
			if len(pos) == 1 && pos[0] != "sync" && pos[0] != "send" && !strings.HasPrefix(pos[0], "<") && len(pos[0]) >= 6 {
				return pos[0]
			}
		}
	}

	// 5. Web URL fallback (?code=... or ?sync=...).
	for _, marker := range []string{"?code=", "?sync="} {
		if idx := strings.Index(line, marker); idx >= 0 {
			rest := line[idx+len(marker):]
			if end := strings.IndexAny(rest, "&# \t"); end >= 0 {
				rest = rest[:end]
			}
			if decoded, err := url.QueryUnescape(rest); err == nil && len(decoded) >= 6 {
				return decoded
			}
		}
	}

	return ""
}

func splitShellTokens(s string) []string {
	var tokens []string
	var cur strings.Builder
	inSingle := false
	inDouble := false
	hasToken := false

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inSingle:
			if c == '\'' {
				inSingle = false
			} else {
				cur.WriteByte(c)
			}
		case inDouble:
			if c == '"' {
				inDouble = false
			} else {
				cur.WriteByte(c)
			}
		case c == '\'':
			inSingle = true
			hasToken = true
		case c == '"':
			inDouble = true
			hasToken = true
		case c == ' ' || c == '\t':
			if hasToken {
				tokens = append(tokens, cur.String())
				cur.Reset()
				hasToken = false
			}
		default:
			cur.WriteByte(c)
			hasToken = true
		}
	}
	if hasToken {
		tokens = append(tokens, cur.String())
	}
	return tokens
}

// --- Filesystem helpers & verification ---

func resetEmptyDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.MkdirAll(dir, 0o755)
}

func copyTreePreserveMeta(ctx context.Context, srcRoot, dstRoot string) error {
	if err := resetEmptyDir(dstRoot); err != nil {
		return err
	}
	buf := make([]byte, 1<<20)
	return filepath.WalkDir(srcRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(srcRoot, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dstRoot, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := copyFilePreserveMeta(p, target, info, buf); err != nil {
			return err
		}
		return nil
	})
}

func copyFilePreserveMeta(src, dst string, info fs.FileInfo, buf []byte) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.CopyBuffer(out, in, buf); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	modTime := info.ModTime()
	return os.Chtimes(dst, modTime, modTime)
}

func collectRelativeFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(files)
	return files, nil
}

func mutateFivePercent(ctx context.Context, root string, relFiles []string, dsIdx, runNum int) ([]string, error) {
	if len(relFiles) == 0 {
		return nil, errors.New("no files to mutate")
	}
	count := max(1, int(math.Round(float64(len(relFiles))*0.05)))
	if count > len(relFiles) {
		count = len(relFiles)
	}

	rng := rand.New(rand.NewPCG(datasetSeed^0x50505050, uint64(dsIdx+1)*10000+uint64(runNum)))
	indices := make([]int, len(relFiles))
	for i := range indices {
		indices[i] = i
	}
	// Deterministic partial Fisher-Yates shuffle to pick count unique files.
	for i := range count {
		j := i + rng.IntN(len(indices)-i)
		indices[i], indices[j] = indices[j], indices[i]
	}

	chosen := make([]string, count)
	for i := range count {
		chosen[i] = relFiles[indices[i]]
	}
	slices.Sort(chosen)

	buf := make([]byte, 1<<20)
	newMtime := modifyBaseTime.Add(time.Duration(runNum) * time.Minute)
	for _, rel := range chosen {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		full := filepath.Join(root, filepath.FromSlash(rel))
		info, err := os.Stat(full)
		if err != nil {
			return nil, err
		}
		if err := writePseudoRandomFile(full, info.Size(), rng, buf, newMtime); err != nil {
			return nil, err
		}
	}
	return chosen, nil
}

type treeEntry struct {
	isDir bool
	size  int64
}

const syncTempPrefix = ".croc-sync-tmp-"

func isSyncTempPath(slashRel string) bool {
	first, _, _ := strings.Cut(slashRel, "/")
	return strings.HasPrefix(first, syncTempPrefix)
}

func scanTree(root string, ignoreSyncTemp bool) (map[string]treeEntry, []string, error) {
	entries := make(map[string]treeEntry)
	var files []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		rel, relErr := filepath.Rel(root, p)
		if relErr == nil && rel != "." && ignoreSyncTemp && isSyncTempPath(filepath.ToSlash(rel)) {
			if err == nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if err != nil {
			return err
		}
		if relErr != nil {
			return relErr
		}
		if rel == "." {
			return nil
		}
		slash := filepath.ToSlash(rel)
		if d.IsDir() {
			entries[slash] = treeEntry{isDir: true}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		entries[slash] = treeEntry{isDir: false, size: info.Size()}
		files = append(files, slash)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	slices.Sort(files)
	return entries, files, nil
}

func verifyTrees(srcRoot, dstRoot string, fullHash bool, hashOnly []string, ignoreSyncTemp bool) error {
	srcEntries, srcFiles, err := scanTree(srcRoot, false)
	if err != nil {
		return fmt.Errorf("scan source %q: %w", srcRoot, err)
	}
	dstEntries, _, err := scanTree(dstRoot, ignoreSyncTemp)
	if err != nil {
		return fmt.Errorf("scan destination %q: %w", dstRoot, err)
	}

	for rel, se := range srcEntries {
		de, ok := dstEntries[rel]
		if !ok {
			return fmt.Errorf("missing path in destination: %s", rel)
		}
		if se.isDir != de.isDir {
			return fmt.Errorf("type mismatch at %s: src isDir=%v, dst isDir=%v", rel, se.isDir, de.isDir)
		}
		if !se.isDir && se.size != de.size {
			return fmt.Errorf("size mismatch at %s: src=%d, dst=%d", rel, se.size, de.size)
		}
	}
	for rel := range dstEntries {
		if _, ok := srcEntries[rel]; !ok {
			return fmt.Errorf("unexpected extra path in destination: %s", rel)
		}
	}

	var toHash []string
	if fullHash {
		toHash = srcFiles
	} else if len(hashOnly) > 0 {
		toHash = hashOnly
	}
	if len(toHash) == 0 {
		return nil
	}

	workers := min(max(runtime.NumCPU(), 1), 8)
	hashCtx, cancelHash := context.WithCancel(context.Background())
	defer cancelHash()
	jobs := make(chan string)
	errCh := make(chan error, 1)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
		cancelHash()
	}
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rel := range jobs {
				if hashCtx.Err() != nil {
					return
				}
				srcHash, err := sha256File(filepath.Join(srcRoot, filepath.FromSlash(rel)))
				if err != nil {
					reportErr(fmt.Errorf("hash source %s: %w", rel, err))
					return
				}
				dstHash, err := sha256File(filepath.Join(dstRoot, filepath.FromSlash(rel)))
				if err != nil {
					reportErr(fmt.Errorf("hash destination %s: %w", rel, err))
					return
				}
				if srcHash != dstHash {
					reportErr(fmt.Errorf("SHA-256 mismatch at %s", rel))
					return
				}
			}
		}()
	}

	for _, rel := range toHash {
		select {
		case <-hashCtx.Done():
			close(jobs)
			wg.Wait()
			return <-errCh
		case jobs <- rel:
		}
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func sha256File(path string) ([32]byte, error) {
	var zero [32]byte
	f, err := os.Open(path)
	if err != nil {
		return zero, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return zero, err
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

// --- Statistics & reporting ---

func summarizeCase(dataset, mode string, trials []runResult) caseResult {
	times := make([]float64, len(trials))
	nets := make([]float64, len(trials))
	srcRAM := make([]float64, len(trials))
	dstRAM := make([]float64, len(trials))
	for i, t := range trials {
		times[i] = t.Seconds
		nets[i] = float64(t.NetworkBytes)
		srcRAM[i] = float64(t.SourceRAMBytes)
		dstRAM[i] = float64(t.DestRAMBytes)
	}
	return caseResult{
		Dataset:        dataset,
		Mode:           mode,
		Runs:           len(trials),
		TimeSeconds:    computeStats(times),
		NetworkBytes:   computeStats(nets),
		SourceRAMBytes: computeStats(srcRAM),
		DestRAMBytes:   computeStats(dstRAM),
		Trials:         trials,
	}
}

func computeStats(vals []float64) metricStats {
	if len(vals) == 0 {
		return metricStats{}
	}
	sorted := slices.Clone(vals)
	slices.Sort(sorted)
	n := len(sorted)
	var median float64
	if n%2 == 1 {
		median = sorted[n/2]
	} else {
		median = (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return metricStats{
		Median: median,
		Min:    sorted[0],
		Max:    sorted[n-1],
	}
}

func writeReportJSON(path string, report benchmarkReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal JSON report: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write JSON report %q: %w", path, err)
	}
	return nil
}

func printMarkdownTable(w io.Writer, cases []caseResult) {
	fmt.Fprintln(w, "| Dataset | Mode | Median Time (s) | Network (MB) | Source RAM (MB) | Dest RAM (MB) | Runs |")
	fmt.Fprintln(w, "| --- | --- | ---: | ---: | ---: | ---: | ---: |")
	for _, c := range cases {
		fmt.Fprintf(w, "| %s | %s | %.3f | %.2f | %.2f | %.2f | %d |\n",
			c.Dataset,
			c.Mode,
			c.TimeSeconds.Median,
			c.NetworkBytes.Median/1e6,
			c.SourceRAMBytes.Median/1e6,
			c.DestRAMBytes.Median/1e6,
			c.Runs,
		)
	}
}
