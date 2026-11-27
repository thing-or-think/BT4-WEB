package foldersync

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestDestLockIsExclusive(t *testing.T) {
	root := t.TempDir()
	first, err := AcquireLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireLock(root); !errors.Is(err, ErrDestinationLocked) {
		t.Fatalf("second lock: %v, want ErrDestinationLocked", err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal("second release must be a no-op")
	}
	again, err := AcquireLock(root)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	defer again.Release()

	// Tệp khóa không bao giờ lọt vào manifest.
	m := mustScan(t, root, ScanOptions{})
	if len(m.Entries) != 0 || len(m.Skipped) != 0 {
		t.Fatalf("lock file visible to the scanner: %+v", m)
	}
	if _, err := AcquireLock(filepath.Join(root, "missing")); err == nil || errors.Is(err, ErrDestinationLocked) {
		t.Fatalf("missing root: %v", err)
	}
	var nilLock *DestLock
	if err := nilLock.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestStagingLifecycleAndOrphanCleanup(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "user.txt", "keep")
	writeFile(t, root, ".croc-sync-tmp-notadir", "user file with the prefix")

	active, err := NewStaging(root)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(active.Name, StagingDirPrefix) || filepath.Dir(active.Dir()) != root {
		t.Fatalf("staging = %+v", active)
	}
	writeFile(t, active.Dir(), "a/b.txt", "partial")

	// Hai staging mồ côi: một có tệp khóa (không ai giữ), một không có.
	orphan, err := NewStaging(root)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, orphan.Dir(), "x", "x")
	if err := unlockFile(orphan.lock); err != nil {
		t.Fatal(err)
	}
	orphan.lock = nil
	bare := filepath.Join(root, StagingDirPrefix+"bare")
	writeFile(t, bare, "y", "y")

	removed, err := CleanOrphanStaging(root)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(removed)
	want := []string{StagingDirPrefix + "bare", orphan.Name}
	slices.Sort(want)
	if !slices.Equal(removed, want) {
		t.Fatalf("removed = %q, want %q", removed, want)
	}
	if !exists(active.Dir(), "a/b.txt") {
		t.Fatal("cleanup removed a staging directory that is still locked")
	}
	if !exists(root, "user.txt") || !exists(root, ".croc-sync-tmp-notadir") {
		t.Fatal("cleanup touched user files")
	}

	if err := active.Remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(active.Dir()); !os.IsNotExist(err) {
		t.Fatalf("staging still exists: %v", err)
	}
	if err := active.Remove(); err != nil {
		t.Fatal("second remove must be a no-op")
	}
	var nilStaging *Staging
	if err := nilStaging.Remove(); err != nil {
		t.Fatal(err)
	}
	if _, err := CleanOrphanStaging(filepath.Join(root, "missing")); err == nil {
		t.Fatal("missing root accepted")
	}
	if _, err := NewStaging(filepath.Join(root, "missing")); err == nil {
		t.Fatal("staging in a missing root accepted")
	}
}
