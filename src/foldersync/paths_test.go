package foldersync

import (
	"errors"
	"testing"

	"github.com/schollz/croc/v11/src/receivefs"
)

func TestNormalizeWirePath_Valid(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "simple file",
			raw:  "notes.txt",
			want: "notes.txt",
		},
		{
			name: "nested posix path",
			raw:  "docs/2026/plan.md",
			want: "docs/2026/plan.md",
		},
		{
			name: "windows backslashes converted to slash",
			raw:  `docs\sub\file.txt`,
			want: "docs/sub/file.txt",
		},
		{
			name: "leading dot-slash cleaned",
			raw:  "./a/./b.txt",
			want: "a/b.txt",
		},
		{
			name: "NFD normalized to NFC (cafe)",
			raw:  "cafe\u0301.txt", // 'e' + dấu sắc tổ hợp (NFD, hay gặp trên macOS)
			want: "caf\u00e9.txt",  // 'é' dựng sẵn (NFC)
		},
		{
			name: "NFD normalized to NFC (Vietnamese)",
			raw:  "ta\u0300i_lie\u0323\u0302u.txt",
			want: "t\u00e0i_li\u1ec7u.txt",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeWirePath(tc.raw)
			if err != nil {
				t.Fatalf("NormalizeWirePath(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("NormalizeWirePath(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestNormalizeWirePath_TraversalAndAbsolute(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "empty path", raw: ""},
		{name: "dot root disallowed", raw: "."},
		{name: "parent traversal", raw: ".."},
		{name: "leading traversal", raw: "../secret.txt"},
		{name: "nested traversal", raw: "a/b/../../c.txt"},
		{name: "windows traversal", raw: `a\..\..\secret.txt`},
		{name: "unix absolute path", raw: "/etc/passwd"},
		{name: "windows drive path", raw: `C:\Windows\win.ini`},
		{name: "windows drive forward slash", raw: "D:/data/a.txt"},
		{name: "windows UNC path", raw: `\\server\share\file.txt`},
		{name: "invalid UTF-8 bytes", raw: "bad_\xff_name.txt"},
		{name: "control character NUL", raw: "a\x00b.txt"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeWirePath(tc.raw)
			if !errors.Is(err, receivefs.ErrUnsafePath) {
				t.Fatalf("NormalizeWirePath(%q) = (%q, %v), want ErrUnsafePath", tc.raw, got, err)
			}
		})
	}
}

func TestNormalizeWirePath_WindowsDeviceAndSensitive(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr error
	}{
		// Tên thiết bị đặt trước của Windows
		{name: "CON", raw: "CON", wantErr: receivefs.ErrUnsafePath},
		{name: "con with extension", raw: "con.txt", wantErr: receivefs.ErrUnsafePath},
		{name: "PRN", raw: "dir/PRN", wantErr: receivefs.ErrUnsafePath},
		{name: "AUX", raw: "AUX.log", wantErr: receivefs.ErrUnsafePath},
		{name: "NUL", raw: "sub/nul.dat", wantErr: receivefs.ErrUnsafePath},
		{name: "COM1", raw: "COM1", wantErr: receivefs.ErrUnsafePath},
		{name: "LPT9", raw: "LPT9.txt", wantErr: receivefs.ErrUnsafePath},

		// Ký tự lỗi trên Windows
		{name: "alternate data stream colon", raw: "file.txt:hidden", wantErr: receivefs.ErrUnsafePath},
		{name: "trailing dot", raw: "file.", wantErr: receivefs.ErrUnsafePath},
		{name: "trailing space", raw: "file ", wantErr: receivefs.ErrUnsafePath},

		// Thư mục nhạy cảm bị chặn bởi receivefs.ForbiddenComponent
		{name: "ssh directory", raw: ".ssh/authorized_keys", wantErr: receivefs.ErrSensitivePath},
		{name: "gnupg directory", raw: ".gnupg/secring.gpg", wantErr: receivefs.ErrSensitivePath},
		{name: "ntfs 8.3 alias for ssh", raw: "SSH~1/id_rsa", wantErr: receivefs.ErrSensitivePath},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeWirePath(tc.raw)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("NormalizeWirePath(%q) = (%q, %v), want %v", tc.raw, got, err, tc.wantErr)
			}
		})
	}
}

func TestPathKey_CaseAndUnicode(t *testing.T) {
	// Hoa/thường phải sinh cùng một khóa
	if PathKey("Docs/ReadMe.md") != PathKey("docs/readme.md") {
		t.Fatalf("expected case-folded keys to match: %q vs %q",
			PathKey("Docs/ReadMe.md"), PathKey("docs/readme.md"))
	}

	// NFC và NFD (kể cả khác hoa/thường) phải sinh cùng một khóa
	nfc := "caf\u00e9.txt"
	nfd := "CAFE\u0301.TXT"
	if PathKey(nfc) != PathKey(nfd) {
		t.Fatalf("expected NFC/NFD keys to match: %q vs %q", PathKey(nfc), PathKey(nfd))
	}
}

func TestIsStagingPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: ".croc-sync-tmp-abc", want: true},
		{path: ".croc-sync-tmp-123/file.txt", want: true},
		{path: "nested/.croc-sync-tmp-xyz/a.bin", want: true},
		{path: `.CROC-SYNC-TMP-99\a.bin`, want: true},
		{path: "normal.txt", want: false},
		{path: "my.croc-sync-tmp-note.txt", want: false},
		{path: "", want: false},
	}

	for _, tc := range tests {
		if got := IsStagingPath(tc.path); got != tc.want {
			t.Errorf("IsStagingPath(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestValidateManifest(t *testing.T) {
	t.Run("valid manifest", func(t *testing.T) {
		m := &Manifest{
			Complete: true,
			Entries: []Entry{
				{Path: "dir", Kind: receivefs.KindDirectory},
				{Path: "dir/file.txt", Kind: receivefs.KindFile, Size: 12},
				{Path: "caf\u00e9.txt", Kind: receivefs.KindFile, Size: 5},
			},
		}
		if err := ValidateManifest(m); err != nil {
			t.Fatalf("ValidateManifest() unexpected error: %v", err)
		}
	})

	t.Run("nil manifest rejected", func(t *testing.T) {
		if err := ValidateManifest(nil); err == nil {
			t.Fatal("expected error for nil manifest")
		}
	})

	t.Run("symlink entry rejected", func(t *testing.T) {
		m := &Manifest{
			Entries: []Entry{
				{Path: "link", Kind: receivefs.KindSymlink},
			},
		}
		if err := ValidateManifest(m); !errors.Is(err, ErrUnsupportedKind) {
			t.Fatalf("got %v, want ErrUnsupportedKind", err)
		}
	})

	t.Run("staging path rejected", func(t *testing.T) {
		m := &Manifest{
			Entries: []Entry{
				{Path: ".croc-sync-tmp-1/a.txt", Kind: receivefs.KindFile},
			},
		}
		if err := ValidateManifest(m); !errors.Is(err, ErrStagingPath) {
			t.Fatalf("got %v, want ErrStagingPath", err)
		}
	})

	t.Run("non-canonical NFD path rejected", func(t *testing.T) {
		m := &Manifest{
			Entries: []Entry{
				{Path: "cafe\u0301.txt", Kind: receivefs.KindFile},
			},
		}
		if err := ValidateManifest(m); !errors.Is(err, receivefs.ErrUnsafePath) {
			t.Fatalf("got %v, want ErrUnsafePath for non-canonical NFD path", err)
		}
	})

	t.Run("case collision rejected", func(t *testing.T) {
		m := &Manifest{
			Entries: []Entry{
				{Path: "Docs/ReadMe.md", Kind: receivefs.KindFile},
				{Path: "docs/readme.md", Kind: receivefs.KindFile},
			},
		}
		if err := ValidateManifest(m); !errors.Is(err, receivefs.ErrPathCollision) {
			t.Fatalf("got %v, want ErrPathCollision", err)
		}
	})

	t.Run("file beneath non-directory rejected", func(t *testing.T) {
		m := &Manifest{
			Entries: []Entry{
				{Path: "foo", Kind: receivefs.KindFile},
				{Path: "foo/bar.txt", Kind: receivefs.KindFile},
			},
		}
		if err := ValidateManifest(m); !errors.Is(err, receivefs.ErrPathCollision) {
			t.Fatalf("got %v, want ErrPathCollision", err)
		}
	})
}

func TestManifestSort_Stable(t *testing.T) {
	m := &Manifest{
		Entries: []Entry{
			{Path: "z.txt", Kind: receivefs.KindFile},
			{Path: "a/b.txt", Kind: receivefs.KindFile},
			{Path: "a", Kind: receivefs.KindDirectory},
		},
	}
	m.Sort()

	want := []string{"a", "a/b.txt", "z.txt"}
	for i, w := range want {
		if m.Entries[i].Path != w {
			t.Fatalf("Entries[%d].Path = %q, want %q", i, m.Entries[i].Path, w)
		}
	}
}
