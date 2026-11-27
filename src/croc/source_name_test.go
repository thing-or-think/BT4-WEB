package croc

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceFilePathPrefersSourceName(t *testing.T) {
	dir := filepath.Join("tmp", "source")

	withDefault := FileInfo{
		Name:         "caf\u00e9.txt",
		FolderSource: dir,
	}
	if got, want := sourceFilePath(withDefault), filepath.Join(dir, "caf\u00e9.txt"); got != want {
		t.Fatalf("sourceFilePath(default) = %q; want %q", got, want)
	}

	withSourceName := FileInfo{
		Name:         "caf\u00e9.txt",
		SourceName:   "cafe\u0301.txt",
		FolderSource: dir,
	}
	if got, want := sourceFilePath(withSourceName), filepath.Join(dir, "cafe\u0301.txt"); got != want {
		t.Fatalf("sourceFilePath(SourceName) = %q; want %q", got, want)
	}
}

func TestSourceNameExcludedFromWireAndJSON(t *testing.T) {
	const localDiskName = "local-only-nfd-cafe\u0301.txt"
	info := FileInfo{
		Name:         "caf\u00e9.txt",
		FolderRemote: "./",
		FolderSource: "/private/source",
		SourceName:   localDiskName,
		IsIgnored:    true,
	}

	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("json.Marshal(FileInfo): %v", err)
	}
	if strings.Contains(string(raw), localDiskName) {
		t.Fatalf("JSON of FileInfo leaked SourceName: %s", raw)
	}

	wire := fileInfoForWire(info)
	if wire.SourceName != "" {
		t.Fatalf("fileInfoForWire retained SourceName = %q", wire.SourceName)
	}
	if wire.FolderSource != "" || wire.IsIgnored {
		t.Fatalf("fileInfoForWire retained sender-only metadata: %+v", wire)
	}
	if wire.Name != info.Name {
		t.Fatalf("fileInfoForWire altered Name = %q; want %q", wire.Name, info.Name)
	}

	wireJSON, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("json.Marshal(wire): %v", err)
	}
	if strings.Contains(string(wireJSON), localDiskName) {
		t.Fatalf("wire JSON leaked SourceName: %s", wireJSON)
	}
}
