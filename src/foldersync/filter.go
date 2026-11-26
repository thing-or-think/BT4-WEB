package foldersync

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	ignore "github.com/sabhiram/go-gitignore"
)

// FilterReason cho biết quy tắc nào đã loại bỏ một đường dẫn.
type FilterReason string

const (
	FilterNone        FilterReason = ""
	FilterStaging     FilterReason = "staging"      // Thư mục tạm nội bộ .croc-sync-tmp-*
	FilterExclude     FilterReason = "exclude"      // --exclude (khớp chuỗi con, không phân biệt hoa/thường)
	FilterExcludeFile FilterReason = "exclude-file" // --exclude-file (khớp chính xác đường dẫn tương đối)
	FilterGitIgnore   FilterReason = "gitignore"    // --git (.gitignore gốc và lồng nhau)
)

// Filter áp dụng các quy tắc lọc đường dẫn cho cả máy nguồn (khi quét)
// và máy đích (khi bảo vệ tệp bị lọc khỏi --delete theo luật 2.4.2).
//
// Thứ tự ưu tiên kiểm tra:
//  1. Thư mục staging nội bộ (.croc-sync-tmp-*) — luôn bị loại, không tắt được.
//  2. --exclude-file (khớp chính xác đường dẫn hoặc thư mục cha).
//  3. --exclude (khớp chuỗi con, không phân biệt hoa/thường, giống croc send).
//  4. --git (.gitignore lồng nhau; quy tắc ở thư mục sâu hơn ghi đè thư mục cha,
//     nhưng nếu thư mục cha đã bị ignore thì toàn bộ cây con cũng bị ignore).
type Filter struct {
	spec         FilterSpec
	excludes     []string
	excludeExact map[string]struct{}
	gitLayers    map[string]gitLayer // key: "" (root) hoặc đường dẫn thư mục chuẩn dùng '/'
}

// gitRule là một dòng .gitignore đã biên dịch ở dạng khẳng định; negate ghi nhớ tiền tố '!'.
type gitRule struct {
	match  *ignore.GitIgnore
	negate bool
}

// gitLayer là các quy tắc của một tệp .gitignore theo đúng thứ tự dòng.
// go-gitignore không báo được dòng phủ định nào đã quyết định kết quả, nên mỗi dòng
// được biên dịch riêng để áp luật "dòng khớp cuối cùng thắng" của git.
type gitLayer []gitRule

func compileGitLayer(lines []string) gitLayer {
	var layer gitLayer
	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		trimmed := strings.TrimLeft(line, " ")
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		negate := strings.HasPrefix(trimmed, "!")
		if negate {
			trimmed = trimmed[1:]
		}
		if strings.TrimSpace(trimmed) == "" {
			continue
		}
		layer = append(layer, gitRule{match: ignore.CompileIgnoreLines(trimmed), negate: negate})
	}
	return layer
}

// decide trả về (bị ignore, có dòng nào khớp).
func (l gitLayer) decide(p string) (bool, bool) {
	for i := len(l) - 1; i >= 0; i-- {
		if l[i].match.MatchesPath(p) {
			return !l[i].negate, true
		}
	}
	return false, false
}

// NewFilter tạo bộ lọc từ FilterSpec và chuẩn hóa sẵn các mẫu lọc.
func NewFilter(spec FilterSpec) *Filter {
	f := &Filter{
		excludeExact: make(map[string]struct{}),
		gitLayers:    make(map[string]gitLayer),
	}

	seenExclude := make(map[string]struct{}, len(spec.Exclude))
	for _, raw := range spec.Exclude {
		v := strings.ToLower(strings.TrimSpace(raw))
		if v == "" {
			continue
		}
		if _, dup := seenExclude[v]; dup {
			continue
		}
		seenExclude[v] = struct{}{}
		f.excludes = append(f.excludes, v)
	}

	var exactList []string
	for _, raw := range spec.ExcludeExact {
		v := normalizeFilterPath(raw)
		if v == "" {
			continue
		}
		if _, dup := f.excludeExact[v]; dup {
			continue
		}
		f.excludeExact[v] = struct{}{}
		exactList = append(exactList, v)
	}

	f.spec = FilterSpec{
		Exclude:      f.excludes,
		ExcludeExact: exactList,
		Git:          spec.Git,
	}
	return f
}

// Spec trả về FilterSpec đã chuẩn hóa để đính kèm vào Manifest.Filter.
func (f *Filter) Spec() FilterSpec {
	if f == nil {
		return FilterSpec{}
	}
	out := FilterSpec{Git: f.spec.Git}
	if len(f.spec.Exclude) > 0 {
		out.Exclude = append([]string(nil), f.spec.Exclude...)
	}
	if len(f.spec.ExcludeExact) > 0 {
		out.ExcludeExact = append([]string(nil), f.spec.ExcludeExact...)
	}
	return out
}

// GitEnabled cho biết bộ lọc có đang bật chế độ tôn trọng .gitignore hay không.
func (f *Filter) GitEnabled() bool {
	return f != nil && f.spec.Git
}

// LoadGitIgnoreDir nạp tệp .gitignore (nếu tồn tại) tại thư mục con relDir bên trong rootDir.
// relDir là "" hoặc "." đối với thư mục gốc, hoặc đường dẫn tương đối trên đĩa dạng "a/b".
// Lớp quy tắc được lưu theo đường dẫn chuẩn trên dây (NFC) để khớp với Match.
func (f *Filter) LoadGitIgnoreDir(rootDir, relDir string) error {
	if !f.GitEnabled() {
		return nil
	}
	diskDir := normalizeFilterPath(relDir)
	return f.LoadGitIgnoreFile(layerKey(diskDir), filepath.Join(rootDir, filepath.FromSlash(diskDir), ".gitignore"))
}

// LoadGitIgnoreFile nạp tệp .gitignore ở đường dẫn tuyệt đối ignorePath làm lớp quy tắc
// cho thư mục wireDir ("" là thư mục gốc). Tệp không tồn tại hoặc là thư mục thì bỏ qua.
// Scanner gọi hàm này mỗi khi bước vào một thư mục mới.
func (f *Filter) LoadGitIgnoreFile(wireDir, ignorePath string) error {
	if !f.GitEnabled() {
		return nil
	}
	info, err := os.Lstat(ignorePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !info.Mode().IsRegular() {
		return nil
	}
	data, err := os.ReadFile(ignorePath)
	if err != nil {
		return err
	}
	f.gitLayers[normalizeFilterPath(wireDir)] = compileGitLayer(strings.Split(string(data), "\n"))
	return nil
}

// layerKey đổi đường dẫn thư mục trên đĩa sang dạng dây (NFC) nếu được;
// tên không chuẩn hóa được giữ nguyên vì không bao giờ được Match với dạng dây.
func layerKey(diskDir string) string {
	if diskDir == "" {
		return ""
	}
	if wire, err := NormalizeWirePath(diskDir); err == nil {
		return wire
	}
	return diskDir
}

// AddGitIgnoreLines nạp trực tiếp các dòng quy tắc .gitignore cho một thư mục tương đối
// ("" là thư mục gốc). Hữu ích cho unit test hoặc trường hợp nạp từ bộ nhớ.
func (f *Filter) AddGitIgnoreLines(relDir string, lines ...string) {
	if !f.GitEnabled() {
		return
	}
	normDir := normalizeFilterPath(relDir)
	f.gitLayers[normDir] = compileGitLayer(lines)
}

// LoadGitIgnoreTree duyệt nhanh cây thư mục rootDir để nạp toàn bộ các tệp .gitignore lồng nhau.
// Bỏ qua các thư mục đã bị lọc (staging, exclude, exclude-file hoặc .gitignore cấp trên).
// Hữu ích ở máy đích khi cần áp lại FilterSpec từ nguồn để bảo vệ tệp khỏi --delete.
func (f *Filter) LoadGitIgnoreTree(rootDir string) error {
	if !f.GitEnabled() {
		return nil
	}
	return filepath.WalkDir(rootDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(rootDir, p)
		if relErr != nil {
			return relErr
		}
		rel = normalizeFilterPath(rel)
		if rel != "" && f.Match(layerKey(rel), true) {
			return fs.SkipDir
		}
		return f.LoadGitIgnoreDir(rootDir, rel)
	})
}

// Match trả về true nếu đường dẫn tương đối relPath bị bộ lọc loại bỏ.
func (f *Filter) Match(relPath string, isDir bool) bool {
	matched, _ := f.MatchReason(relPath, isDir)
	return matched
}

// MatchReason kiểm tra relPath có bị loại hay không và trả về lý do tương ứng.
func (f *Filter) MatchReason(relPath string, isDir bool) (bool, FilterReason) {
	norm := normalizeFilterPath(relPath)
	if norm == "" {
		return false, FilterNone
	}

	// 1. Luôn chặn thư mục staging nội bộ .croc-sync-tmp-*.
	if IsStagingPath(norm) {
		return true, FilterStaging
	}
	if f == nil {
		return false, FilterNone
	}

	// 2. --exclude-file: khớp chính xác đường dẫn hoặc bất kỳ thư mục cha nào của nó.
	if len(f.excludeExact) > 0 && f.matchExact(norm) {
		return true, FilterExcludeFile
	}

	// 3. --exclude: khớp chuỗi con, không phân biệt hoa/thường (khớp hành vi croc send).
	if len(f.excludes) > 0 {
		lower := strings.ToLower(norm)
		for _, sub := range f.excludes {
			if strings.Contains(lower, sub) {
				return true, FilterExclude
			}
		}
	}

	// 4. --git: kiểm tra .gitignore lồng nhau.
	if f.spec.Git && len(f.gitLayers) > 0 && f.matchGitIgnore(norm, isDir) {
		return true, FilterGitIgnore
	}

	return false, FilterNone
}

// matchExact kiểm tra norm và mọi tiền tố thư mục cha của nó trong excludeExact.
// Nếu thư mục "build" bị exclude-file thì "build/app.exe" cũng bị loại.
func (f *Filter) matchExact(norm string) bool {
	cur := norm
	for {
		if _, ok := f.excludeExact[cur]; ok {
			return true
		}
		idx := strings.LastIndex(cur, "/")
		if idx < 0 {
			return false
		}
		cur = cur[:idx]
	}
}

// matchGitIgnore kiểm tra quy tắc .gitignore theo đúng ngữ nghĩa của git:
//   - Kiểm tra từng thư mục cha từ nông đến sâu trước: nếu thư mục cha đã bị ignore,
//     toàn bộ tệp bên trong bị ignore (không thể phủ định lại ở cấp sâu hơn).
//   - Sau đó mới kiểm tra chính norm.
func (f *Filter) matchGitIgnore(norm string, isDir bool) bool {
	parts := strings.Split(norm, "/")
	// Kiểm tra các thư mục cha trước (a, a/b, ...).
	for i := 1; i < len(parts); i++ {
		ancestor := strings.Join(parts[:i], "/")
		if f.matchGitIgnoreSingle(ancestor, true) {
			return true
		}
	}
	// Kiểm tra chính đối tượng hiện tại.
	return f.matchGitIgnoreSingle(norm, isDir)
}

// matchGitIgnoreSingle kiểm tra một đường dẫn đơn lẻ bằng cách duyệt các lớp .gitignore
// từ thư mục cha gần nhất ngược lên thư mục gốc (""). Lớp gần nhất có quy tắc khớp
// (kể cả quy tắc phủ định '!') sẽ quyết định kết quả cuối cùng.
func (f *Filter) matchGitIgnoreSingle(cand string, isDir bool) bool {
	dir := parentDir(cand)
	for {
		if layer, ok := f.gitLayers[dir]; ok {
			relToLayer := cand
			if dir != "" {
				relToLayer = strings.TrimPrefix(cand, dir+"/")
			}
			if ignored, decided := layer.decide(gitIgnorePath(relToLayer, isDir)); decided {
				return ignored
			}
		}
		if dir == "" {
			return false
		}
		dir = parentDir(dir)
	}
}

// gitIgnorePath thêm dấu '/' ở cuối nếu là thư mục để các mẫu như "node_modules/"
// hoặc "build/" khớp đúng (giống helper trong src/croc/croc.go).
func gitIgnorePath(rel string, isDir bool) string {
	if isDir && !strings.HasSuffix(rel, "/") {
		return rel + "/"
	}
	return rel
}

// parentDir trả về thư mục cha của một đường dẫn chuẩn dùng '/', hoặc "" nếu ở cấp gốc.
func parentDir(p string) string {
	d := path.Dir(p)
	if d == "." || d == "/" {
		return ""
	}
	return d
}

// normalizeFilterPath chuẩn hóa đường dẫn tương đối cho bộ lọc:
// đổi '\' sang '/', gộp dấu '/', bỏ "./" và dấu '/' thừa ở hai đầu.
func normalizeFilterPath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	p = strings.ReplaceAll(p, `\`, "/")
	p = path.Clean(p)
	p = strings.TrimPrefix(p, "./")
	p = strings.Trim(p, "/")
	if p == "." {
		return ""
	}
	return p
}
