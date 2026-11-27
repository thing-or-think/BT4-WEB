package foldersync

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/schollz/croc/v11/src/receivefs"
)

var (
	// ErrDeleteBlocked báo kế hoạch có lý do chặn xóa toàn lượt (DeleteBlockedReason).
	ErrDeleteBlocked = errors.New("foldersync: deletion blocked")

	// ErrDeleteNeedsConfirm báo kế hoạch xóa quá 50% số tệp mà người dùng chưa xác nhận.
	ErrDeleteNeedsConfirm = errors.New("foldersync: deletion needs explicit confirmation")

	// ErrDeleteBeforeCommit báo gọi xóa trước khi mọi tệp của lượt đã được commit.
	ErrDeleteBeforeCommit = errors.New("foldersync: deletion attempted before all files were committed")
)

// enforceDeleteRules áp các luật chặn xóa toàn lượt (mục 2.4) lên p.Delete:
//
//  1. Manifest nguồn không đầy đủ (Complete=false): tắt xóa trong lượt này.
//  3. Nguồn rỗng mà đích còn mục cần xóa: dừng xóa, trừ khi AllowEmptySource.
//     Xóa quá 50% số tệp đích: RequireDeleteConfirm (--yes không bỏ qua được).
//
// Luật 2 (bảo vệ mục bị lọc/bỏ qua) nằm trong deleteCandidates; luật 4 và 5 nằm
// trong ApplyDeletes.
func enforceDeleteRules(p *Plan, src, dst *Manifest, o DiffOptions) {
	if !o.Delete || len(p.Delete) == 0 {
		return
	}
	n := len(p.Delete)
	switch {
	case !src.Complete:
		p.DeleteBlockedReason = fmt.Sprintf("source scan was incomplete; %d deletion(s) skipped this round", n)
	case len(src.Entries) == 0 && !o.AllowEmptySource:
		p.DeleteBlockedReason = fmt.Sprintf("source is empty; refusing to delete %d destination item(s) without --allow-empty-source", n)
	}
	if p.DeleteBlockedReason != "" {
		p.Delete = nil
		return
	}

	dstFiles, deleteFiles := 0, 0
	for _, e := range dst.Entries {
		if e.IsFile() {
			dstFiles++
		}
	}
	for _, c := range p.Delete {
		if c.Dst.IsFile() {
			deleteFiles++
		}
	}
	p.RequireDeleteConfirm = deleteFiles*2 > dstFiles
}

// DeleteOptions là điều kiện bên gọi phải xác nhận trước khi xóa.
type DeleteOptions struct {
	// Committed phải true: mọi tệp Add/Update của lượt đã được commit (luật 2.4.4).
	Committed bool
	// Confirmed là xác nhận riêng của người dùng cho kế hoạch RequireDeleteConfirm.
	// Không được suy ra từ --yes.
	Confirmed bool
}

// DeleteFailure mô tả một mục không xóa được; các mục khác vẫn được xử lý.
type DeleteFailure struct {
	Path string
	Err  error
}

// ApplyDeletes xóa các mục trong p.Delete dưới root theo thứ tự sâu trước nông.
//
// Luật an toàn (mục 2.4):
//   - Từ chối toàn bộ nếu kế hoạch bị chặn, cần xác nhận mà chưa có, hoặc chưa commit.
//   - Mỗi mục: RejectSymlinkPath (Root.Remove không tự kiểm), rồi Lstat và so lại loại,
//     size, ModTime với ảnh chụp lúc quét; khác thì giữ nguyên (đích đã đổi sau khi quét).
//   - Chỉ dùng Root.Remove: thư mục còn mục con (ví dụ vừa có tệp mới) sẽ không bị xóa.
//
// Trả về danh sách đã xóa và các mục thất bại; lỗi chỉ khác nil khi từ chối cả lượt
// hoặc ctx bị hủy.
func ApplyDeletes(ctx context.Context, root *receivefs.Root, p *Plan, o DeleteOptions) ([]string, []DeleteFailure, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	switch {
	case p == nil || len(p.Delete) == 0:
		return nil, nil, nil
	case p.DeleteBlockedReason != "":
		return nil, nil, fmt.Errorf("%w: %s", ErrDeleteBlocked, p.DeleteBlockedReason)
	case !o.Committed:
		return nil, nil, ErrDeleteBeforeCommit
	case p.RequireDeleteConfirm && !o.Confirmed:
		return nil, nil, ErrDeleteNeedsConfirm
	}

	changes := append([]Change(nil), p.Delete...)
	sortDeletes(changes)
	var deleted []string
	var failed []DeleteFailure
	for _, c := range changes {
		if err := ctx.Err(); err != nil {
			return deleted, failed, err
		}
		if err := deleteOne(root, c); err != nil {
			failed = append(failed, DeleteFailure{Path: c.Path, Err: err})
			continue
		}
		deleted = append(deleted, c.Path)
	}
	return deleted, failed, nil
}

func deleteOne(root *receivefs.Root, c Change) error {
	if c.Dst == nil {
		return fmt.Errorf("%w: delete %q has no destination snapshot", ErrInvalidEntry, c.Path)
	}
	if err := root.RejectSymlinkPath(c.Path); err != nil {
		return err
	}
	info, err := root.Lstat(c.Path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil // đã biến mất: mục tiêu đã đạt
		}
		return err
	}
	if err := unchangedSince(c.Dst, info); err != nil {
		return err
	}
	makeWritable(root, c.Path)
	return root.Remove(c.Path)
}

// unchangedSince kiểm mục đích vẫn giống ảnh chụp lúc quét (luật 2.4.4).
func unchangedSince(snap *Entry, info fs.FileInfo) error {
	switch {
	case snap.IsDir():
		if !info.IsDir() {
			return fmt.Errorf("%w: no longer a directory", ErrFileChanged)
		}
		return nil
	case snap.IsFile():
		return sameFileMeta(snap, info)
	default:
		return fmt.Errorf("%w: unsupported kind %d", ErrUnsupportedKind, snap.Kind)
	}
}
