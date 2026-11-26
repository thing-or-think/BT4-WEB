package foldersync

import (
	"bytes"
	"cmp"
	"errors"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

// DiffOptions là các lựa chọn của máy đích khi tính kế hoạch.
type DiffOptions struct {
	// Checksum (--checksum) buộc so hash mọi cặp tệp cùng size, kể cả khi ModTime bằng nhau.
	// Không bật thì hai tệp cùng size và cùng ModTime được coi là giống nhau mà không đọc
	// nội dung: một sửa đổi giữ nguyên size và ModTime chỉ được phát hiện với --checksum.
	Checksum bool

	// Delete (--delete) cho phép xóa mục chỉ có ở đích. Luật an toàn ở delete.go.
	Delete bool

	// AllowEmptySource (--allow-empty-source) cho phép xóa khi nguồn rỗng.
	AllowEmptySource bool
}

// pair là một mục nguồn và mục đích tương ứng (nil nếu đích chưa có).
type pair struct {
	src *Entry
	dst *Entry
}

// matchEntries ghép mục nguồn với mục đích: trước hết theo đúng đường dẫn, sau đó theo
// PathKey cho các mục đích chưa được ghép. Ghép theo PathKey xử lý đổi tên chỉ khác
// hoa/thường hoặc NFC/NFD: trên Windows/macOS ghi "Readme.md" chính là ghi đè "README.md",
// nên nếu coi đây là Add + Delete thì bước xóa sẽ xóa luôn tệp vừa ghi.
// Trả về các cặp theo thứ tự của src và tập đường dẫn đích đã được ghép.
func matchEntries(src, dst *Manifest) ([]pair, map[string]bool) {
	byPath := make(map[string]*Entry, len(dst.Entries))
	byKey := make(map[string][]*Entry, len(dst.Entries))
	for i := range dst.Entries {
		d := &dst.Entries[i]
		byPath[d.Path] = d
		k := PathKey(d.Path)
		byKey[k] = append(byKey[k], d)
	}
	matched := make(map[string]bool, len(dst.Entries))
	pairs := make([]pair, len(src.Entries))
	var pending []int
	for i := range src.Entries {
		s := &src.Entries[i]
		pairs[i].src = s
		if d, ok := byPath[s.Path]; ok {
			pairs[i].dst = d
			matched[d.Path] = true
		} else {
			pending = append(pending, i)
		}
	}
	for _, i := range pending {
		for _, d := range byKey[PathKey(pairs[i].src.Path)] {
			if !matched[d.Path] {
				pairs[i].dst = d
				matched[d.Path] = true
				break
			}
		}
	}
	return pairs, matched
}

// DstHashTargets trả về đường dẫn đích tương ứng với các đường dẫn nguồn trong needHash
// (bằng nhau, hoặc chỉ khác hoa/thường khi đổi tên), để máy đích hash tệp của chính nó.
func DstHashTargets(src, dst *Manifest, needHash []string) []string {
	want := make(map[string]bool, len(needHash))
	for _, p := range needHash {
		want[p] = true
	}
	pairs, _ := matchEntries(src, dst)
	var out []string
	for _, pr := range pairs {
		if pr.dst != nil && want[pr.src.Path] {
			out = append(out, pr.dst.Path)
		}
	}
	return out
}

// Compare tính kế hoạch đồng bộ từ manifest nguồn src (nhận qua mạng) sang manifest
// đích dst (quét cục bộ bằng NewFilter(src.Filter)).
//
// Diff hai tầng:
//   - Tầng 1 (metadata): khác loại -> Update (TypeChanged); khác size -> Update;
//     cùng size và cùng ModTime -> Skip (hoặc Touch nếu chỉ khác Mode/hoa-thường).
//   - Tầng 2 (hash): cùng size nhưng khác ModTime, hoặc bật Checksum -> cần hash.
//     Nếu thiếu hash ở bất kỳ bên nào, Compare trả (nil, needHash, nil); bên gọi điền
//     Hash cho src (yêu cầu nguồn) và dst (DstHashTargets), rồi gọi lại Compare.
//     Hash bằng nhau -> Touch (chỉ sửa ModTime/Mode); khác nhau -> Update.
//
// Thư mục không so ModTime (đổi mỗi khi thêm/xóa con), chỉ so Mode.
// Change.Dst.Path khác Change.Path nghĩa là phải đổi tên mục đích (chỉ khác hoa/thường).
// src phải hợp lệ theo ValidateManifest; lỗi được trả về trước khi tính bất cứ gì.
func Compare(src, dst *Manifest, o DiffOptions) (*Plan, []string, error) {
	if err := ValidateManifest(src); err != nil {
		return nil, nil, err
	}
	if dst == nil {
		return nil, nil, errors.New("foldersync: nil destination manifest")
	}
	pairs, matched := matchEntries(src, dst)

	p := &Plan{}
	var needHash []string
	var typeChanges []int // chỉ số trong p.Update cần kiểm tra cây con đích
	for _, pr := range pairs {
		s, d := pr.src, pr.dst
		if d == nil {
			p.Add = append(p.Add, Change{Path: s.Path, Src: s, Reason: "new"})
			continue
		}
		c := Change{Path: s.Path, Src: s, Dst: d}
		renamed := d.Path != s.Path
		modeDiffers := s.Mode != d.Mode

		switch {
		case s.Kind != d.Kind:
			c.TypeChanged = true
			c.Reason = "type changed"
			if d.IsDir() {
				typeChanges = append(typeChanges, len(p.Update))
			}
			p.Update = append(p.Update, c)

		case s.IsDir():
			if renamed || modeDiffers {
				c.Reason = touchReason(renamed, modeDiffers, false)
				p.Touch = append(p.Touch, c)
			} else {
				p.Skip = append(p.Skip, c)
			}

		case s.Size != d.Size:
			c.Reason = "size changed"
			p.Update = append(p.Update, c)

		case !o.Checksum && s.ModTime.Equal(d.ModTime):
			if renamed || modeDiffers {
				c.Reason = touchReason(renamed, modeDiffers, false)
				p.Touch = append(p.Touch, c)
			} else {
				p.Skip = append(p.Skip, c)
			}

		case len(s.Hash) == 0 || len(d.Hash) == 0:
			needHash = append(needHash, s.Path)

		case !bytes.Equal(s.Hash, d.Hash):
			c.Reason = "content changed"
			p.Update = append(p.Update, c)

		default:
			mtimeDiffers := !s.ModTime.Equal(d.ModTime)
			if renamed || modeDiffers || mtimeDiffers {
				c.Reason = touchReason(renamed, modeDiffers, mtimeDiffers)
				p.Touch = append(p.Touch, c)
			} else {
				p.Skip = append(p.Skip, c)
			}
		}
	}
	if len(needHash) > 0 {
		return nil, needHash, nil
	}

	if o.Delete {
		p.Delete = deleteCandidates(src, dst, matched)
	}
	enforceDeleteRules(p, src, dst, o)
	resolveTypeChanges(p, dst, typeChanges)
	return p, nil, nil
}

func touchReason(renamed, mode, mtime bool) string {
	var parts []string
	if renamed {
		parts = append(parts, "rename case")
	}
	if mtime {
		parts = append(parts, "same content, mtime differs")
	}
	if mode {
		parts = append(parts, "mode differs")
	}
	return strings.Join(parts, ", ")
}

// deleteCandidates liệt kê các mục chỉ có ở đích và không được bảo vệ, xếp sâu trước nông.
//
// Mục được bảo vệ khi (luật 2.4.2):
//   - Trùng hoặc nằm dưới một đường dẫn nguồn đã bỏ qua (bị lọc, symlink, tên trùng,
//     lỗi đọc...): nguồn không nói gì về nó nên đích không được suy ra là đã bị xóa.
//   - Là thư mục tổ tiên của một mục được bảo vệ, hoặc của một mục đích bị bỏ qua khi
//     quét (symlink, tên không hợp lệ...): xóa thư mục đó cần RemoveAll, vốn bị cấm.
func deleteCandidates(src, dst *Manifest, matched map[string]bool) []Change {
	protectedRoots := make(map[string]bool)
	for _, s := range src.Skipped {
		if s.Path != "" {
			protectedRoots[s.Path] = true
		}
	}
	keepAncestors := make(map[string]bool)
	markAncestors := func(p string) {
		for dir := path.Dir(p); dir != "." && dir != "/" && !keepAncestors[dir]; dir = path.Dir(dir) {
			keepAncestors[dir] = true
		}
	}
	for _, s := range dst.Skipped {
		switch {
		case s.Path != "":
			markAncestors(s.Path)
		case s.DiskPath != "":
			if parent := filepath.Dir(s.DiskPath); parent != "." {
				if wire, err := NormalizeWirePathFromOS(parent); err == nil {
					keepAncestors[wire] = true
					markAncestors(wire)
				}
			}
		}
	}

	isProtected := func(p string) bool {
		for cur := p; cur != "." && cur != "/"; cur = path.Dir(cur) {
			if protectedRoots[cur] {
				return true
			}
		}
		return false
	}

	var out []Change
	for i := range dst.Entries {
		d := &dst.Entries[i]
		if matched[d.Path] {
			continue
		}
		if isProtected(d.Path) {
			markAncestors(d.Path)
			continue
		}
		out = append(out, Change{Path: d.Path, Dst: d, Reason: "not in source"})
	}
	// Bỏ các thư mục phải giữ lại vì còn chứa mục được bảo vệ.
	out = slices.DeleteFunc(out, func(c Change) bool { return keepAncestors[c.Path] })
	sortDeletes(out)
	return out
}

// sortDeletes xếp sâu trước nông (con trước cha), cùng độ sâu thì theo đường dẫn.
func sortDeletes(changes []Change) {
	slices.SortStableFunc(changes, func(a, b Change) int {
		if c := cmp.Compare(PathDepth(b.Path), PathDepth(a.Path)); c != 0 {
			return c
		}
		return cmp.Compare(a.Path, b.Path)
	})
}

// resolveTypeChanges chuyển các thay đổi "thư mục đích -> tệp nguồn" thành xung đột khi
// thư mục đích còn mục con không nằm trong danh sách xóa (ví dụ --delete tắt, bị chặn,
// hoặc mục con được bảo vệ). Thay thư mục bằng tệp khi đó sẽ cần RemoveAll.
func resolveTypeChanges(p *Plan, dst *Manifest, idx []int) {
	if len(idx) == 0 {
		return
	}
	deleting := make(map[string]bool, len(p.Delete))
	for _, c := range p.Delete {
		deleting[c.Path] = true
	}
	conflict := make(map[int]bool)
	for _, i := range idx {
		dir := p.Update[i].Dst.Path + "/"
		for _, d := range dst.Entries {
			if strings.HasPrefix(d.Path, dir) && !deleting[d.Path] {
				conflict[i] = true
				break
			}
		}
		if !conflict[i] {
			// Cây con chỉ còn dựa vào Delete: không được có cảnh báo chặn xóa ẩn.
			for _, s := range dst.Skipped {
				if s.Path != "" && strings.HasPrefix(s.Path, dir) {
					conflict[i] = true
					break
				}
			}
		}
	}
	if len(conflict) == 0 {
		return
	}
	kept := p.Update[:0]
	for i, c := range p.Update {
		if conflict[i] {
			c.Reason = "conflict: destination directory is not empty (needs --delete)"
			p.Conflicts = append(p.Conflicts, c)
			continue
		}
		kept = append(kept, c)
	}
	p.Update = kept
}
