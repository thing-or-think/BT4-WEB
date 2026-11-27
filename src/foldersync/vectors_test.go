package foldersync

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/receivefs"
)

type vectorFile struct {
	Version int          `json:"version"`
	Cases   []vectorCase `json:"cases"`
}

type vectorCase struct {
	Name string         `json:"name"`
	Src  vectorManifest `json:"src"`
	Dst  vectorManifest `json:"dst"`
	Opts vectorOptions  `json:"opts"`
	Want vectorWant     `json:"want"`
}

type vectorSkipped struct {
	Path     string     `json:"path,omitempty"`
	DiskPath string     `json:"disk_path,omitempty"`
	Reason   SkipReason `json:"reason"`
	Detail   string     `json:"detail,omitempty"`
}

type vectorManifest struct {
	Entries  []Entry         `json:"entries"`
	Skipped  []vectorSkipped `json:"skipped,omitempty"`
	Complete bool            `json:"complete"`
}

type vectorOptions struct {
	Checksum         bool `json:"checksum"`
	Delete           bool `json:"delete"`
	AllowEmptySource bool `json:"allow_empty_source"`
	IgnoreMetadata   bool `json:"ignore_metadata"`
}

type vectorWant struct {
	Error    bool        `json:"error"`
	NeedHash []string    `json:"need_hash,omitempty"`
	Plan     *vectorPlan `json:"plan,omitempty"`
}

type vectorPlan struct {
	Add                  []string          `json:"add"`
	Update               []string          `json:"update"`
	Touch                []string          `json:"touch"`
	Skip                 []string          `json:"skip"`
	Delete               []string          `json:"delete"`
	Conflicts            []string          `json:"conflicts"`
	TypeChanged          []string          `json:"type_changed"`
	Renames              map[string]string `json:"renames"`
	DeleteBlocked        bool              `json:"delete_blocked"`
	RequireDeleteConfirm bool              `json:"require_delete_confirm"`
}

var (
	vecBaseTime = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	vecHashA    = []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	vecHashB    = []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}
)

func vfe(p string, size int64, sec int, hash []byte) Entry {
	e := Entry{
		Path:    p,
		Size:    size,
		ModTime: vecBaseTime.Add(time.Duration(sec) * time.Second),
		Mode:    0o644,
		Kind:    receivefs.KindFile,
	}
	if len(hash) > 0 {
		e.Hash = slices.Clone(hash)
	}
	return e
}

func vde(p string) Entry {
	return Entry{
		Path:    p,
		ModTime: vecBaseTime,
		Mode:    0o755,
		Kind:    receivefs.KindDirectory,
	}
}

func vMode(e Entry, m os.FileMode) Entry {
	e.Mode = m
	return e
}

func vTime(e Entry, tm time.Time) Entry {
	e.ModTime = tm
	return e
}

func vman(entries ...Entry) *Manifest {
	m := &Manifest{Entries: entries, Complete: true}
	m.Sort()
	return m
}

func toVectorManifest(m *Manifest) vectorManifest {
	entries := m.Entries
	if entries == nil {
		entries = []Entry{}
	}
	var skipped []vectorSkipped
	for _, s := range m.Skipped {
		skipped = append(skipped, vectorSkipped{
			Path:     s.Path,
			DiskPath: filepath.ToSlash(s.DiskPath),
			Reason:   s.Reason,
			Detail:   s.Detail,
		})
	}
	return vectorManifest{
		Entries:  entries,
		Skipped:  skipped,
		Complete: m.Complete,
	}
}

func (vm vectorManifest) toManifest() *Manifest {
	entries := slices.Clone(vm.Entries)
	var skipped []SkippedPath
	for _, s := range vm.Skipped {
		skipped = append(skipped, SkippedPath{
			Path:     s.Path,
			DiskPath: filepath.FromSlash(s.DiskPath),
			Reason:   s.Reason,
			Detail:   s.Detail,
		})
	}
	return &Manifest{
		Entries:  entries,
		Skipped:  skipped,
		Complete: vm.Complete,
	}
}

func toVectorOptions(o DiffOptions) vectorOptions {
	return vectorOptions{
		Checksum:         o.Checksum,
		Delete:           o.Delete,
		AllowEmptySource: o.AllowEmptySource,
		IgnoreMetadata:   o.IgnoreMetadata,
	}
}

func (vo vectorOptions) toDiffOptions() DiffOptions {
	return DiffOptions{
		Checksum:         vo.Checksum,
		Delete:           vo.Delete,
		AllowEmptySource: vo.AllowEmptySource,
		IgnoreMetadata:   vo.IgnoreMetadata,
	}
}

func sortedChangePaths(cs []Change) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Path
	}
	slices.Sort(out)
	return out
}

func exactChangePaths(cs []Change) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Path
	}
	return out
}

// summarizeCompare rút gọn kết quả Compare về cấu trúc vectorWant để so sánh chéo Go/TS.
func summarizeCompare(plan *Plan, needHash []string, err error) vectorWant {
	if err != nil {
		return vectorWant{Error: true}
	}
	if len(needHash) > 0 {
		nh := slices.Clone(needHash)
		slices.Sort(nh)
		return vectorWant{NeedHash: nh}
	}
	typeChanged := []string{}
	renames := map[string]string{}
	for _, group := range [][]Change{plan.Add, plan.Update, plan.Touch, plan.Skip, plan.Delete, plan.Conflicts} {
		for _, c := range group {
			if c.TypeChanged {
				typeChanged = append(typeChanged, c.Path)
			}
			if c.Src != nil && c.Dst != nil && c.Dst.Path != c.Path {
				renames[c.Path] = c.Dst.Path
			}
		}
	}
	slices.Sort(typeChanged)
	return vectorWant{
		Plan: &vectorPlan{
			Add:                  sortedChangePaths(plan.Add),
			Update:               sortedChangePaths(plan.Update),
			Touch:                sortedChangePaths(plan.Touch),
			Skip:                 sortedChangePaths(plan.Skip),
			Delete:               exactChangePaths(plan.Delete),
			Conflicts:            sortedChangePaths(plan.Conflicts),
			TypeChanged:          typeChanged,
			Renames:              renames,
			DeleteBlocked:        plan.DeleteBlockedReason != "",
			RequireDeleteConfirm: plan.RequireDeleteConfirm,
		},
	}
}

type vectorCaseDef struct {
	name     string
	src, dst *Manifest
	opts     DiffOptions
}

// sharedVectorCaseDefs là bảng ca kiểm thử dùng chung cho Go và TypeScript.
func sharedVectorCaseDefs() []vectorCaseDef {
	incompleteSrc := vman(vfe("a", 1, 0, nil))
	incompleteSrc.Complete = false
	incompleteSrc.Skipped = []SkippedPath{{Path: "unreadable", Reason: SkipReadError}}

	protSrc := vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil), vfe("c", 1, 0, nil), vfe("d", 1, 0, nil))
	protSrc.Skipped = []SkippedPath{
		{Path: "build", Reason: SkipFiltered},
		{Path: "logs/app.log", Reason: SkipFiltered},
		{Path: "link", Reason: SkipSymlink},
		{Path: "unreadable", Reason: SkipSpecial},
	}
	protDst := vman(
		vfe("a", 1, 0, nil), vfe("b", 1, 0, nil), vfe("c", 1, 0, nil), vfe("d", 1, 0, nil),
		vde("build"), vde("build/out"), vfe("build/out/x", 1, 0, nil),
		vde("logs"), vfe("logs/app.log", 1, 0, nil), vfe("logs/old.txt", 1, 0, nil),
		vfe("link", 1, 0, nil),
		vfe("unreadable", 1, 0, nil),
		vde("withlink"), vfe("withlink/f", 1, 0, nil),
		vde("withdevice"), vfe("withdevice/g", 1, 0, nil),
		vde("gone"), vfe("gone/h", 1, 0, nil),
	)
	protDst.Skipped = []SkippedPath{
		{Path: "withlink/sym", Reason: SkipSymlink},
		{DiskPath: "withdevice/CON", Reason: SkipInvalidName},
		{DiskPath: "AUX", Reason: SkipInvalidName},
	}

	caseNormSrc := vman(vfe("keep1", 1, 0, nil), vfe("keep2", 1, 0, nil), vfe("keep3", 1, 0, nil))
	caseNormSrc.Skipped = []SkippedPath{
		{Path: "Data", Reason: SkipSymlink},
		{Path: "Caf\u00e9", Reason: SkipFiltered},
	}
	caseNormDst := vman(
		vfe("keep1", 1, 0, nil), vfe("keep2", 1, 0, nil), vfe("keep3", 1, 0, nil),
		vde("data"), vfe("data/important.db", 1, 0, nil),
		vde("cafe\u0301"), vfe("cafe\u0301/menu.txt", 1, 0, nil),
		vfe("extra.txt", 1, 0, nil),
	)

	directInvalidDst := vman(vde("x"), vfe("keep1", 1, 0, nil), vfe("keep2", 1, 0, nil))
	directInvalidDst.Skipped = []SkippedPath{{DiskPath: "x/CON", Reason: SkipInvalidName}}

	nestedInvalidDst := vman(vde("x"), vfe("keep1", 1, 0, nil), vfe("keep2", 1, 0, nil))
	nestedInvalidDst.Skipped = []SkippedPath{{DiskPath: "x/sub/CON", Reason: SkipInvalidName}}

	return []vectorCaseDef{
		{
			name: "identical",
			src:  vman(vfe("a", 3, 0, nil), vde("d"), vfe("d/b", 1, 0, nil)),
			dst:  vman(vfe("a", 3, 0, nil), vde("d"), vfe("d/b", 1, 0, nil)),
		},
		{
			name: "new file and directory",
			src:  vman(vfe("a", 3, 0, nil), vde("d"), vfe("d/b", 1, 0, nil)),
			dst:  vman(),
		},
		{
			name: "size differs",
			src:  vman(vfe("a", 4, 0, nil)),
			dst:  vman(vfe("a", 3, 0, nil)),
		},
		{
			name: "same size and mtime, different content: undetected without checksum",
			src:  vman(vfe("a", 3, 0, vecHashA)),
			dst:  vman(vfe("a", 3, 0, vecHashB)),
		},
		{
			name: "same size and mtime, different content: detected with checksum",
			src:  vman(vfe("a", 3, 0, vecHashA)),
			dst:  vman(vfe("a", 3, 0, vecHashB)),
			opts: DiffOptions{Checksum: true},
		},
		{
			name: "checksum with equal hashes",
			src:  vman(vfe("a", 3, 0, vecHashA)),
			dst:  vman(vfe("a", 3, 0, vecHashA)),
			opts: DiffOptions{Checksum: true},
		},
		{
			name: "checksum requests missing hashes",
			src:  vman(vfe("a", 3, 0, nil), vfe("b", 2, 0, nil)),
			dst:  vman(vfe("a", 3, 0, vecHashA), vfe("b", 5, 0, nil)),
			opts: DiffOptions{Checksum: true},
		},
		{
			name: "same size, mtime differs: needs hash",
			src:  vman(vfe("a", 3, 5, nil), vfe("b", 3, 0, nil)),
			dst:  vman(vfe("a", 3, 0, nil), vfe("b", 3, 0, nil)),
		},
		{
			name: "same size, mtime differs, same content: touch",
			src:  vman(vfe("a", 3, 5, vecHashA)),
			dst:  vman(vfe("a", 3, 0, vecHashA)),
		},
		{
			name: "same size, mtime differs, different content: update",
			src:  vman(vfe("a", 3, 5, vecHashA)),
			dst:  vman(vfe("a", 3, 0, vecHashB)),
		},
		{
			name: "mode differs only",
			src:  vman(vMode(vfe("a", 3, 0, nil), 0o755), vMode(vde("d"), 0o700)),
			dst:  vman(vfe("a", 3, 0, nil), vde("d")),
		},
		{
			name: "directory mtime is ignored",
			src:  vman(vTime(vde("d"), vecBaseTime.Add(time.Hour))),
			dst:  vman(vde("d")),
		},
		{
			name: "file becomes directory",
			src:  vman(vde("x"), vfe("x/a", 1, 0, nil)),
			dst:  vman(vfe("x", 1, 0, nil)),
		},
		{
			name: "empty directory becomes file",
			src:  vman(vfe("x", 1, 0, nil)),
			dst:  vman(vde("x")),
		},
		{
			name: "non-empty directory becomes file without delete: conflict",
			src:  vman(vfe("x", 1, 0, nil)),
			dst:  vman(vde("x"), vfe("x/a", 1, 0, nil)),
		},
		{
			name: "non-empty directory becomes file with delete",
			src:  vman(vfe("x", 1, 0, nil), vfe("keep", 1, 0, nil), vfe("keep2", 1, 0, nil)),
			dst:  vman(vde("x"), vfe("x/a", 1, 0, nil), vfe("keep", 1, 0, nil), vfe("keep2", 1, 0, nil)),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "case-only rename is not add+delete",
			src:  vman(vfe("Readme.md", 3, 0, nil), vde("Docs"), vfe("Docs/a", 1, 0, nil)),
			dst:  vman(vfe("README.md", 3, 0, nil), vde("docs"), vfe("docs/a", 1, 0, nil)),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "case-sensitive destination keeps exact match",
			src:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil)),
			dst:  vman(vfe("A", 1, 0, nil), vfe("a", 1, 0, nil), vfe("b", 1, 0, nil)),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "extra destination files kept without delete",
			src:  vman(vfe("a", 1, 0, nil)),
			dst:  vman(vfe("a", 1, 0, nil), vfe("extra", 1, 0, nil)),
		},
		{
			name: "extra destination tree deleted with delete",
			src:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil), vfe("c", 1, 0, nil)),
			dst:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil), vfe("c", 1, 0, nil), vde("old"), vfe("old/f", 1, 0, nil)),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "rename with mtime diff, type change, and mode diff",
			src:  vman(vfe("Readme.md", 3, 5, vecHashA), vde("x"), vfe("x/a", 1, 0, nil), vMode(vfe("m", 1, 0, nil), 0o600)),
			dst:  vman(vfe("README.md", 3, 0, vecHashA), vfe("x", 9, 0, nil), vfe("m", 1, 0, nil)),
		},
		{
			name: "delete protection for filtered, symlink, special, and invalid-name",
			src:  protSrc,
			dst:  protDst,
			opts: DiffOptions{Delete: true},
		},
		{
			name: "delete protection case-insensitive and NFC/NFD",
			src:  caseNormSrc,
			dst:  caseNormDst,
			opts: DiffOptions{Delete: true},
		},
		{
			name: "type change conflict with direct invalid-name child",
			src:  vman(vfe("x", 1, 0, nil), vfe("keep1", 1, 0, nil), vfe("keep2", 1, 0, nil)),
			dst:  directInvalidDst,
			opts: DiffOptions{Delete: true},
		},
		{
			name: "type change conflict with nested invalid-name child",
			src:  vman(vfe("x", 1, 0, nil), vfe("keep1", 1, 0, nil), vfe("keep2", 1, 0, nil)),
			dst:  nestedInvalidDst,
			opts: DiffOptions{Delete: true},
		},
		{
			name: "delete blocked by incomplete source",
			src:  incompleteSrc,
			dst:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil), vfe("c", 1, 0, nil)),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "delete blocked by empty source without allow",
			src:  vman(),
			dst:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil)),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "delete allowed by empty source with allow requires confirm",
			src:  vman(),
			dst:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil)),
			opts: DiffOptions{Delete: true, AllowEmptySource: true},
		},
		{
			name: "empty source without delete is not blocked",
			src:  vman(),
			dst:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil)),
		},
		{
			name: "delete 50 percent (1 of 2 files) does not require confirm",
			src:  vman(vfe("a", 1, 0, nil)),
			dst:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil)),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "delete over 50 percent (2 of 3 files) requires confirm",
			src:  vman(vfe("a", 1, 0, nil)),
			dst:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil), vfe("c", 1, 0, nil)),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "delete directories only does not require confirm",
			src:  vman(vfe("a", 1, 0, nil)),
			dst:  vman(vfe("a", 1, 0, nil), vde("d1"), vde("d2"), vde("d3")),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "delete nested tree over 50 percent requires confirm",
			src:  vman(vfe("a", 1, 0, nil)),
			dst:  vman(vde("x"), vfe("x/a", 1, 0, nil), vfe("x/b", 1, 0, nil), vfe("a", 1, 0, nil)),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "delete deep tree ordering deepest first",
			src:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil), vfe("c", 1, 0, nil), vfe("d", 1, 0, nil)),
			dst:  vman(vfe("a", 1, 0, nil), vfe("b", 1, 0, nil), vfe("c", 1, 0, nil), vfe("d", 1, 0, nil), vde("old"), vde("old/deep"), vfe("old/deep/x", 1, 0, nil), vfe("z", 1, 0, nil)),
			opts: DiffOptions{Delete: true},
		},
		{
			name: "invalid source: traversal path",
			src:  &Manifest{Entries: []Entry{vfe("../x", 1, 0, nil)}, Complete: true},
			dst:  vman(),
		},
		{
			name: "invalid source: non-canonical path",
			src:  &Manifest{Entries: []Entry{vfe("./a.txt", 1, 0, nil)}, Complete: true},
			dst:  vman(),
		},
		{
			name: "invalid source: non-canonical NFD path",
			src:  &Manifest{Entries: []Entry{vfe("cafe\u0301.txt", 1, 0, nil)}, Complete: true},
			dst:  vman(),
		},
		{
			name: "invalid source: case collision",
			src:  vman(vde("Docs"), vfe("Docs/ReadMe.md", 1, 0, nil), vfe("Docs/readme.md", 1, 0, nil)),
			dst:  vman(),
		},
		{
			name: "invalid source: missing parent directory",
			src:  vman(vfe("sub/a.txt", 1, 0, nil)),
			dst:  vman(),
		},
		{
			name: "invalid source: staging path",
			src:  vman(vde(".croc-sync-tmp-1"), vfe(".croc-sync-tmp-1/a.txt", 1, 0, nil)),
			dst:  vman(),
		},
		{
			name: "need hash with case-only rename",
			src:  vman(vfe("Readme.md", 3, 5, nil), vfe("a", 1, 5, nil), vfe("new", 1, 0, nil)),
			dst:  vman(vfe("README.md", 3, 0, nil), vfe("a", 1, 0, nil)),
		},
		{
			name: "nanosecond mtime difference needs hash",
			src:  vman(vTime(vfe("a", 3, 0, nil), vecBaseTime.Add(time.Nanosecond))),
			dst:  vman(vfe("a", 3, 0, nil)),
		},
		{
			name: "nanosecond mtime difference with equal hash touches",
			src:  vman(vTime(vfe("a", 3, 0, vecHashA), vecBaseTime.Add(time.Nanosecond))),
			dst:  vman(vfe("a", 3, 0, vecHashA)),
		},
		{
			name: "ignore_metadata: mtime differs with equal hash skips",
			src:  vman(vfe("a", 3, 5, vecHashA)),
			dst:  vman(vfe("a", 3, 0, vecHashA)),
			opts: DiffOptions{IgnoreMetadata: true},
		},
		{
			name: "ignore_metadata: mode differs only skips file and directory",
			src:  vman(vMode(vfe("a", 3, 0, nil), 0o755), vMode(vde("d"), 0o700)),
			dst:  vman(vfe("a", 3, 0, nil), vde("d")),
			opts: DiffOptions{IgnoreMetadata: true},
		},
		{
			name: "ignore_metadata: mtime differs with different hash still updates",
			src:  vman(vfe("a", 3, 5, vecHashA)),
			dst:  vman(vfe("a", 3, 0, vecHashB)),
			opts: DiffOptions{IgnoreMetadata: true},
		},
		{
			name: "ignore_metadata: mtime differs with missing hash still needs hash",
			src:  vman(vfe("a", 3, 5, nil)),
			dst:  vman(vfe("a", 3, 0, nil)),
			opts: DiffOptions{IgnoreMetadata: true},
		},
		{
			name: "ignore_metadata: case-only rename still touches",
			src:  vman(vMode(vfe("Readme.md", 3, 5, vecHashA), 0o755), vMode(vde("Docs"), 0o700), vfe("Docs/a", 1, 0, nil)),
			dst:  vman(vfe("README.md", 3, 0, vecHashA), vde("docs"), vfe("docs/a", 1, 0, nil)),
			opts: DiffOptions{Delete: true, IgnoreMetadata: true},
		},
	}
}

func buildVectorFile() vectorFile {
	defs := sharedVectorCaseDefs()
	cases := make([]vectorCase, 0, len(defs))
	for _, d := range defs {
		src := d.src
		dst := d.dst
		plan, need, err := Compare(src, dst, d.opts)
		cases = append(cases, vectorCase{
			Name: d.name,
			Src:  toVectorManifest(src),
			Dst:  toVectorManifest(dst),
			Opts: toVectorOptions(d.opts),
			Want: summarizeCompare(plan, need, err),
		})
	}
	return vectorFile{
		Version: 1,
		Cases:   cases,
	}
}

func assertVectorWantEqual(t *testing.T, got, want vectorWant) {
	t.Helper()
	if got.Error != want.Error {
		t.Fatalf("error = %v, want %v", got.Error, want.Error)
	}
	if !slices.Equal(got.NeedHash, want.NeedHash) {
		t.Fatalf("need_hash = %q, want %q", got.NeedHash, want.NeedHash)
	}
	if (got.Plan == nil) != (want.Plan == nil) {
		t.Fatalf("plan = %+v, want %+v", got.Plan, want.Plan)
	}
	if got.Plan == nil {
		return
	}
	gp, wp := got.Plan, want.Plan
	if !slices.Equal(gp.Add, wp.Add) {
		t.Errorf("add = %q, want %q", gp.Add, wp.Add)
	}
	if !slices.Equal(gp.Update, wp.Update) {
		t.Errorf("update = %q, want %q", gp.Update, wp.Update)
	}
	if !slices.Equal(gp.Touch, wp.Touch) {
		t.Errorf("touch = %q, want %q", gp.Touch, wp.Touch)
	}
	if !slices.Equal(gp.Skip, wp.Skip) {
		t.Errorf("skip = %q, want %q", gp.Skip, wp.Skip)
	}
	if !slices.Equal(gp.Delete, wp.Delete) {
		t.Errorf("delete = %q, want %q", gp.Delete, wp.Delete)
	}
	if !slices.Equal(gp.Conflicts, wp.Conflicts) {
		t.Errorf("conflicts = %q, want %q", gp.Conflicts, wp.Conflicts)
	}
	if !slices.Equal(gp.TypeChanged, wp.TypeChanged) {
		t.Errorf("type_changed = %q, want %q", gp.TypeChanged, wp.TypeChanged)
	}
	if !maps.Equal(gp.Renames, wp.Renames) {
		t.Errorf("renames = %v, want %v", gp.Renames, wp.Renames)
	}
	if gp.DeleteBlocked != wp.DeleteBlocked {
		t.Errorf("delete_blocked = %v, want %v", gp.DeleteBlocked, wp.DeleteBlocked)
	}
	if gp.RequireDeleteConfirm != wp.RequireDeleteConfirm {
		t.Errorf("require_delete_confirm = %v, want %v", gp.RequireDeleteConfirm, wp.RequireDeleteConfirm)
	}
}

// TestSharedDiffVectors kiểm chứng bộ vector diff dùng chung giữa Go và TypeScript.
// Đặt biến môi trường FOLDERSYNC_WRITE_VECTORS=1 để tái tạo lại tệp JSON từ bảng Go.
func TestSharedDiffVectors(t *testing.T) {
	vectorPath := filepath.Join("..", "..", "web", "testdata", "folder-sync-vectors.json")
	if os.Getenv("FOLDERSYNC_WRITE_VECTORS") == "1" {
		vf := buildVectorFile()
		data, err := json.MarshalIndent(vf, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, '\n')
		if err := os.MkdirAll(filepath.Dir(vectorPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(vectorPath, data, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}

	raw, err := os.ReadFile(vectorPath)
	if err != nil {
		t.Fatalf("read vectors %s: %v", vectorPath, err)
	}
	var vf vectorFile
	if err := json.Unmarshal(raw, &vf); err != nil {
		t.Fatalf("unmarshal vectors: %v", err)
	}
	if vf.Version != 1 {
		t.Fatalf("version = %d, want 1", vf.Version)
	}
	if len(vf.Cases) < 35 {
		t.Fatalf("cases = %d, want at least 35", len(vf.Cases))
	}
	for _, tc := range vf.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			src := tc.Src.toManifest()
			dst := tc.Dst.toManifest()
			plan, need, err := Compare(src, dst, tc.Opts.toDiffOptions())
			got := summarizeCompare(plan, need, err)
			assertVectorWantEqual(t, got, tc.Want)
		})
	}
}
