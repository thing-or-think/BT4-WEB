package foldersync

import (
	"cmp"
	"io/fs"
	"slices"
	"time"

	"github.com/schollz/croc/v11/src/receivefs"
)

const (
	// Giới hạn bảo vệ bộ nhớ theo thiết kế mục 5.
	MaxManifestEntries   = 1_000_000
	MaxManifestJSONBytes = 512 << 20 // 512 MiB

	// Tiền tố thư mục staging tạm; bộ lọc luôn phải loại bỏ tiền tố này.
	StagingDirPrefix = ".croc-sync-tmp-"

	// HashSize là độ dài xxhash64 toàn file (byte).
	HashSize = 8
)

// SkipReason cho biết lý do một đường dẫn bị bỏ qua khi quét.
type SkipReason string

const (
	SkipFiltered    SkipReason = "filtered"     // Bị loại bởi --exclude / --exclude-file / .gitignore
	SkipSymlink     SkipReason = "symlink"      // Symlink (bản đầu không đi theo symlink)
	SkipInvalidName SkipReason = "invalid_name" // Tên không hợp lệ đa nền tảng (VD: CON, chứa ':', kết thúc bằng '.')
	SkipReadError   SkipReason = "read_error"   // Lỗi đọc quyền / file thay đổi lúc đang quét
)

// SkippedPath lưu một đường dẫn bị bỏ qua để báo cáo và bảo vệ khỏi --delete ở máy đích.
//
// Path là đường dẫn chuẩn trên dây. Máy đích không xóa Path và mọi thứ nằm dưới nó.
// Riêng SkipInvalidName có Path rỗng vì tên không chuẩn hóa được; Detail chứa tên
// gốc đã quote để hiển thị. Không cần bảo vệ theo đường dẫn trong trường hợp này:
// máy đích cũng không chuẩn hóa được tên đó, nên nó không bao giờ nằm trong
// Entries của đích và không thể bị đưa vào danh sách xóa.
type SkippedPath struct {
	Path     string     `json:"path,omitempty"`   // Đường dẫn chuẩn trên dây (rỗng chỉ với SkipInvalidName)
	DiskPath string     `json:"-"`                // Đường dẫn gốc trên đĩa cục bộ (không gửi qua mạng)
	Reason   SkipReason `json:"reason"`           // Lý do bỏ qua
	Detail   string     `json:"detail,omitempty"` // Thông tin lỗi/cảnh báo bổ sung
}

// FilterSpec là bộ lọc máy nguồn đã dùng. Máy nguồn gửi kèm manifest để máy đích
// áp lại lên cây của chính nó: mục ở đích khớp bộ lọc được bảo vệ khỏi --delete,
// kể cả khi máy nguồn không có mục cùng tên (luật 2.4.2).
type FilterSpec struct {
	Exclude      []string `json:"exclude,omitempty"`       // --exclude: khớp chuỗi con
	ExcludeExact []string `json:"exclude_exact,omitempty"` // --exclude-file: khớp chính xác đường dẫn tương đối
	Git          bool     `json:"git,omitempty"`           // --git: tôn trọng .gitignore
}

// Entry đại diện cho một tệp hoặc thư mục trong Manifest.
type Entry struct {
	Path     string         `json:"path"`              // Tên trên dây: chuẩn NFC, dấu '/'
	DiskPath string         `json:"-"`                 // Tên thật trên đĩa cục bộ (giữ riêng để mở file trên máy hiện tại)
	Size     int64          `json:"size,omitempty"`    // Kích thước byte (0 với thư mục)
	ModTime  time.Time      `json:"mod_time,omitzero"` // Thời điểm sửa gần nhất (UTC)
	Mode     fs.FileMode    `json:"mode,omitempty"`    // Chỉ lưu bit quyền (Mode.Perm())
	Kind     receivefs.Kind `json:"kind"`              // KindFile hoặc KindDirectory
	Hash     []byte         `json:"hash,omitempty"`    // xxhash64 toàn file (HashSize byte); rỗng cho đến khi tầng 2 yêu cầu
}

// IsDir trả về true nếu entry là thư mục.
func (e Entry) IsDir() bool {
	return e.Kind == receivefs.KindDirectory
}

// IsFile trả về true nếu entry là tệp thường.
func (e Entry) IsFile() bool {
	return e.Kind == receivefs.KindFile
}

// Manifest là ảnh chụp (snapshot) cấu trúc cây thư mục tại một thời điểm.
type Manifest struct {
	Entries  []Entry       `json:"entries"`
	Skipped  []SkippedPath `json:"skipped,omitempty"`
	Filter   FilterSpec    `json:"filter,omitzero"`
	Complete bool          `json:"complete"` // false => có lỗi đọc khi quét, máy đích bắt buộc cấm --delete
}

// Sort sắp xếp ổn định Entries và Skipped theo thứ tự từ điển của Path
// để kết quả quét và diff luôn nhất quán giữa Windows, Linux và macOS.
func (m *Manifest) Sort() {
	slices.SortStableFunc(m.Entries, func(a, b Entry) int {
		return cmp.Compare(a.Path, b.Path)
	})
	slices.SortStableFunc(m.Skipped, func(a, b SkippedPath) int {
		if c := cmp.Compare(a.Path, b.Path); c != 0 {
			return c
		}
		if c := cmp.Compare(a.DiskPath, b.DiskPath); c != 0 {
			return c
		}
		return cmp.Compare(a.Reason, b.Reason)
	})
}

// Change mô tả một thao tác cụ thể trong kế hoạch đồng bộ.
type Change struct {
	Path        string `json:"path"`
	Src         *Entry `json:"src,omitempty"`          // Nil nếu là Delete
	Dst         *Entry `json:"dst,omitempty"`          // Nil nếu là Add; với Delete lưu snapshot để hậu kiểm trước khi xóa
	TypeChanged bool   `json:"type_changed,omitempty"` // True khi đổi loại File <-> Directory
	Reason      string `json:"reason,omitempty"`       // Ghi chú ngắn phục vụ --dry-run / log
}

// Plan là kết quả của thuật toán Compare (diff.go).
type Plan struct {
	Add    []Change `json:"add,omitempty"`
	Update []Change `json:"update,omitempty"`
	Touch  []Change `json:"touch,omitempty"` // Chỉ cập nhật ModTime/Mode, không truyền lại dữ liệu
	Skip   []Change `json:"skip,omitempty"`
	Delete []Change `json:"delete,omitempty"`

	// Thông tin an toàn của luật xóa (mục 2.4).
	DeleteBlockedReason  string `json:"delete_blocked_reason,omitempty"`  // Khác rỗng nếu xóa bị chặn toàn lượt
	RequireDeleteConfirm bool   `json:"require_delete_confirm,omitempty"` // True khi xóa >50% số file đích (--yes không bỏ qua)
}

// HasChanges trả về true nếu có ít nhất một thao tác làm thay đổi đĩa đích.
func (p *Plan) HasChanges() bool {
	return len(p.Add) > 0 || len(p.Update) > 0 || len(p.Touch) > 0 || len(p.Delete) > 0
}
