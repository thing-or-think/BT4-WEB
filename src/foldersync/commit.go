package foldersync

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/schollz/croc/v11/src/receivefs"
)

// ErrStagedMismatch báo dữ liệu nhận được không khớp size/hash nguồn đã khai báo.
var ErrStagedMismatch = errors.New("foldersync: received data does not match the source")

// CommitOptions điều khiển Commit.
type CommitOptions struct {
	// Staging là tên thư mục staging (tương đối so với root) chứa tệp croc đã nhận,
	// theo đúng đường dẫn trên dây.
	Staging  string
	Decision Decision
}

// commitTestHook được gọi trước mỗi bước ghi; lỗi trả về được coi như lỗi của bước đó.
// Chỉ dùng trong test tiêm lỗi.
var commitTestHook func(stage, path string) error

// Thử lại ngắn cho lỗi tạm thời (Windows: tệp đang bị chương trình khác mở).
var (
	fsRetries    = 5
	fsRetryDelay = 100 * time.Millisecond
)

func hook(stage, p string) error {
	if commitTestHook != nil {
		return commitTestHook(stage, p)
	}
	return nil
}

// Commit đưa các tệp đã nhận từ staging vào thư mục đích theo kế hoạch.
//
// Thứ tự: đổi tên thư mục chỉ khác hoa/thường và tạo thư mục mới (nông trước) ->
// tệp thường (kiểm size+hash bản nhận, kiểm đích chưa đổi so với lúc quét, rename
// nguyên tử, chmod, mtime) -> Touch tệp -> đổi loại thư mục thành tệp (sau khi mọi
// tệp khác đã commit không lỗi và cây con khớp đúng danh sách xóa) -> Touch thư mục
// và quyền thư mục mới -> xóa (chỉ khi không có lỗi hay xung đột nào).
// Mỗi tệp đích luôn là bản cũ hoặc bản mới hoàn chỉnh: dữ liệu chỉ vào đích qua
// Rename/Link từ staging cùng hệ tệp. Lỗi ở một mục được ghi vào Result.Failures
// và các mục khác vẫn tiếp tục; chỉ ctx bị hủy mới dừng giữa chừng.
func Commit(ctx context.Context, root *receivefs.Root, plan *Plan, o CommitOptions) (*Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r := &Result{DryRun: !o.Decision.Proceed, Unchanged: len(plan.Skip), DeleteBlocked: plan.DeleteBlockedReason}
	if !o.Decision.Proceed {
		return r, nil
	}
	c := &committer{
		ctx:             ctx,
		root:            root,
		plan:            plan,
		o:               o,
		r:               r,
		done:            map[string]bool{},
		renamedDirs:     map[string]string{},
		dirRenamed:      map[string]bool{},
		dirRenameFailed: map[string]bool{},
	}
	for _, ch := range plan.Conflicts {
		c.failf(ch.Path, "%s", ch.Reason)
	}
	r.Conflicts = len(plan.Conflicts)

	changes := slices.Concat(plan.Add, plan.Update)
	slices.SortStableFunc(changes, func(a, b Change) int {
		return cmp.Compare(PathDepth(a.Path), PathDepth(b.Path))
	})
	// 1. Đổi tên thư mục chỉ khác hoa/thường (từ plan.Touch) và tạo thư mục mới, nông trước.
	var dirRenames []Change
	for _, ch := range plan.Touch {
		if isDirCaseRename(ch) {
			dirRenames = append(dirRenames, ch)
		}
	}
	slices.SortStableFunc(dirRenames, func(a, b Change) int {
		return cmp.Compare(PathDepth(a.Path), PathDepth(b.Path))
	})
	for _, ch := range dirRenames {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		c.renameDirCase(ch)
	}
	for _, ch := range changes {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if ch.Src.IsDir() {
			c.commitDir(ch)
		}
	}
	// 2. Tệp thường (trừ đổi loại thư mục -> tệp, sẽ xử lý sau khi mọi tệp khác đã xong).
	for _, ch := range changes {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if ch.Src.IsFile() && !isDirToFile(ch) {
			c.commitFile(ch)
		}
	}
	// 3. Touch cho tệp.
	for _, ch := range plan.Touch {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if ch.Src.IsFile() {
			c.touch(ch)
		}
	}
	// 4. Đổi loại thư mục -> tệp, chỉ sau khi mọi tệp khác đã commit và không có lỗi nào trước đó.
	for _, ch := range changes {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if isDirToFile(ch) {
			if c.hasFailures() {
				c.failf(ch.Path, "skipped replacing directory: earlier changes failed")
				continue
			}
			c.commitFile(ch)
		}
	}
	// 5. Touch thư mục (đổi tên hoa/thường đã làm ở bước 1 nên chỉ áp quyền) và quyền thư mục mới
	// (sau khi đã ghi xong con, để 0o555 không chặn ghi).
	for _, ch := range plan.Touch {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if ch.Src.IsDir() {
			c.touch(ch)
		}
	}
	for _, ch := range changes {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if ch.Src.IsDir() && c.done[ch.Path] {
			if err := c.step("chmod", ch.Path, func() error { return root.Chmod(ch.Path, ch.Src.Mode) }); err != nil {
				c.fail(ch.Path, err)
			}
		}
	}
	// 6. Xóa, chỉ sau khi mọi thay đổi Add/Update/Touch đã commit không lỗi và không xung đột (luật 2.4.4).
	c.applyDeletes()
	return r, ctx.Err()
}

type committer struct {
	ctx  context.Context
	root *receivefs.Root
	plan *Plan
	o    CommitOptions
	r    *Result
	done map[string]bool // đường dẫn đã commit thành công
	// đường dẫn đích gốc đã bị xóa sớm để thay thư mục bằng tệp
	preDeleted map[string]bool
	// ánh xạ tiền tố thư mục đích gốc -> tên mới sau bước đổi tên hoa/thường
	renamedDirs map[string]string
	// đường dẫn thư mục trong plan.Touch đã được đổi tên ở bước 1 (chỉ còn cần áp mode)
	dirRenamed map[string]bool
	// đường dẫn thư mục trong plan.Touch đổi tên thất bại ở bước 1
	dirRenameFailed map[string]bool
}

func isDirCaseRename(ch Change) bool {
	return ch.Src != nil && ch.Dst != nil && ch.Src.IsDir() && ch.Dst.IsDir() && ch.Dst.Path != ch.Path
}

func isDirToFile(ch Change) bool {
	return ch.TypeChanged && ch.Dst != nil && ch.Dst.IsDir() && ch.Src != nil && ch.Src.IsFile()
}

func (c *committer) hasFailures() bool {
	return len(c.r.Failures) > 0 || c.r.Conflicts > 0
}

func (c *committer) fail(p string, err error) {
	c.r.Failures = append(c.r.Failures, fmt.Sprintf("%s: %v", p, err))
}

func (c *committer) failf(p, format string, args ...any) {
	c.fail(p, fmt.Errorf(format, args...))
}

// translateDst dịch một đường dẫn đích gốc (lúc quét) qua các tiền tố thư mục
// đã đổi tên hoa/thường ở bước 1 để trỏ đúng vào tên hiện tại trên đĩa.
func (c *committer) translateDst(p string) string {
	if len(c.renamedDirs) == 0 || p == "" {
		return p
	}
	if renamed, ok := c.renamedDirs[p]; ok {
		return renamed
	}
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			if renamed, ok := c.renamedDirs[p[:i]]; ok {
				return renamed + p[i:]
			}
		}
	}
	return p
}

func (c *committer) hasFailedRenamedAncestor(p string) bool {
	for dir := path.Dir(p); dir != "." && dir != "/"; dir = path.Dir(dir) {
		if c.dirRenameFailed[dir] {
			return true
		}
	}
	return false
}

// renameDirCase áp dụng đổi tên thư mục chỉ khác hoa/thường ở bước 1 (trước khi ghi tệp).
func (c *committer) renameDirCase(ch Change) {
	if c.hasFailedRenamedAncestor(ch.Path) {
		c.dirRenameFailed[ch.Path] = true
		c.failf(ch.Path, "parent directory rename failed")
		return
	}
	if c.translateDst(ch.Dst.Path) == ch.Path {
		// Thư mục đã ở đúng đường dẫn mới nhờ đổi tên thư mục cha trước đó.
		return
	}
	if err := hook("verify-dest", ch.Path); err != nil {
		c.dirRenameFailed[ch.Path] = true
		c.fail(ch.Path, err)
		return
	}
	if err := c.verifyDest(ch); err != nil {
		c.dirRenameFailed[ch.Path] = true
		c.fail(ch.Path, err)
		return
	}
	if err := c.prepareRename(ch); err != nil {
		c.dirRenameFailed[ch.Path] = true
		c.fail(ch.Path, err)
		return
	}
	c.renamedDirs[ch.Dst.Path] = ch.Path
	c.dirRenamed[ch.Path] = true
}

// step chạy op sau hook kiểm thử, thử lại khi gặp lỗi tạm thời.
func (c *committer) step(stage, p string, op func() error) error {
	if err := hook(stage, p); err != nil {
		return err
	}
	var err error
	for attempt := 0; ; attempt++ {
		err = op()
		if err == nil || !isTransientFSError(err) || attempt >= fsRetries {
			return err
		}
		select {
		case <-time.After(fsRetryDelay):
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}
}

// verifyDest kiểm đích vẫn như lúc quét: không tồn tại với Add, khớp ảnh chụp với Update.
func (c *committer) verifyDest(ch Change) error {
	if ch.Dst == nil {
		_, err := c.root.Lstat(ch.Path)
		switch {
		case err == nil:
			return fmt.Errorf("%w: destination appeared after the scan", ErrFileChanged)
		case errors.Is(err, fs.ErrNotExist):
			return nil
		default:
			return err
		}
	}
	dstPath := c.translateDst(ch.Dst.Path)
	if err := c.root.RejectSymlinkPath(dstPath); err != nil {
		return err
	}
	info, err := c.root.Lstat(dstPath)
	if err != nil {
		return err
	}
	if err := unchangedSince(ch.Dst, info); err != nil {
		return err
	}
	if dstPath != ch.Path {
		// Trên hệ tệp phân biệt hoa/thường, kiểm tên đích mới không trỏ vào một tệp khác
		// để tránh bước đổi tên ghi đè nhầm.
		targetInfo, err := c.root.Lstat(ch.Path)
		switch {
		case err == nil:
			if !os.SameFile(info, targetInfo) {
				return fmt.Errorf("%w: %q already exists as a different file", ErrFileChanged, ch.Path)
			}
		case errors.Is(err, fs.ErrNotExist):
		default:
			return err
		}
	}
	return nil
}

// prepareRename đổi tên mục đích chỉ khác hoa/thường sang tên nguồn.
func (c *committer) prepareRename(ch Change) error {
	if ch.Dst == nil {
		return nil
	}
	dstPath := c.translateDst(ch.Dst.Path)
	if dstPath == ch.Path {
		return nil
	}
	return c.step("case-rename", ch.Path, func() error { return c.root.Rename(dstPath, ch.Path) })
}

func (c *committer) commitDir(ch Change) {
	if err := hook("verify-dest", ch.Path); err != nil {
		c.fail(ch.Path, err)
		return
	}
	if err := c.verifyDest(ch); err != nil {
		c.fail(ch.Path, err)
		return
	}
	if ch.TypeChanged {
		// Tệp đích -> thư mục nguồn: thay tệp cũ (đã kiểm chưa đổi) bằng thư mục.
		if err := c.removeDest(ch.Dst); err != nil {
			c.fail(ch.Path, err)
			return
		}
	} else if err := c.prepareRename(ch); err != nil {
		c.fail(ch.Path, err)
		return
	}
	if err := c.step("mkdir", ch.Path, func() error { return c.root.MkdirAll(ch.Path, 0o700) }); err != nil {
		c.fail(ch.Path, err)
		return
	}
	c.done[ch.Path] = true
	if ch.Dst == nil || ch.TypeChanged {
		c.r.Added++
	} else {
		c.r.Updated++
	}
}

// verifyDirDeleteTree đọc toàn bộ cây con của dir bằng root.ReadDir và xác nhận
// nó chứa đúng các mục trong expected (chưa có mục mới xuất hiện sau lúc quét và
// mọi mục còn khớp ảnh chụp), trước khi cho phép xóa bất kỳ mục nào dưới dir.
func (c *committer) verifyDirDeleteTree(dir string, expected map[string]Change) error {
	seen := make(map[string]bool, len(expected))
	var walk func(cur string) error
	walk = func(cur string) error {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		entries, err := c.root.ReadDir(cur)
		if err != nil {
			return err
		}
		for _, de := range entries {
			child := path.Join(cur, de.Name())
			ch, ok := expected[child]
			if !ok {
				return fmt.Errorf("%w: unexpected entry %q in directory %q", ErrFileChanged, child, dir)
			}
			if seen[child] {
				return fmt.Errorf("%w: duplicate entry %q in directory %q", ErrFileChanged, child, dir)
			}
			seen[child] = true
			if de.Type()&fs.ModeSymlink != 0 {
				return fmt.Errorf("%w: symlink %q appeared in directory %q", ErrFileChanged, child, dir)
			}
			info, err := c.root.Lstat(child)
			if err != nil {
				return err
			}
			if ch.Dst == nil {
				return fmt.Errorf("%w: delete %q has no destination snapshot", ErrInvalidEntry, child)
			}
			if err := unchangedSince(ch.Dst, info); err != nil {
				return fmt.Errorf("%s: %w", child, err)
			}
			if info.IsDir() {
				if err := walk(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(dir); err != nil {
		return err
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("%w: directory %q contents changed after the scan", ErrFileChanged, dir)
	}
	return nil
}

// removeDest xóa một mục đích (kèm cây con đã nằm trong kế hoạch xóa nếu là thư mục).
func (c *committer) removeDest(dst *Entry) error {
	dstPath := c.translateDst(dst.Path)
	if dst.IsDir() {
		if !c.o.Decision.ConfirmDeletes && c.plan.RequireDeleteConfirm {
			return fmt.Errorf("%w: replacing directory needs delete confirmation", ErrDeleteNeedsConfirm)
		}
		prefix := dst.Path + "/"
		var sub []Change
		var origPaths []string
		expected := make(map[string]Change)
		for _, d := range c.plan.Delete {
			if strings.HasPrefix(d.Path, prefix) {
				translated := d
				translated.Path = c.translateDst(d.Path)
				sub = append(sub, translated)
				origPaths = append(origPaths, d.Path)
				expected[translated.Path] = d
			}
		}
		if len(sub) > 0 && c.plan.DeleteBlockedReason != "" {
			return fmt.Errorf("%w: %s", ErrDeleteBlocked, c.plan.DeleteBlockedReason)
		}
		if err := c.verifyDirDeleteTree(dstPath, expected); err != nil {
			return err
		}
		if len(sub) > 0 {
			_, failed, err := ApplyDeletes(c.ctx, c.root, &Plan{Delete: sub}, DeleteOptions{Committed: true, Confirmed: true})
			if err != nil {
				return err
			}
			if len(failed) > 0 {
				return fmt.Errorf("clear directory: %s: %w", failed[0].Path, failed[0].Err)
			}
			if c.preDeleted == nil {
				c.preDeleted = map[string]bool{}
			}
			for _, p := range origPaths {
				c.preDeleted[p] = true
			}
		}
	}
	return c.step("remove", dstPath, func() error {
		makeWritable(c.root, dstPath)
		return c.root.Remove(dstPath)
	})
}

func (c *committer) commitFile(ch Change) {
	staged := path.Join(c.o.Staging, ch.Path)
	if err := c.step("verify-staged", ch.Path, func() error { return c.verifyStaged(staged, ch.Src) }); err != nil {
		c.fail(ch.Path, err)
		return
	}
	if err := hook("verify-dest", ch.Path); err != nil {
		c.fail(ch.Path, err)
		return
	}
	if err := c.verifyDest(ch); err != nil {
		c.fail(ch.Path, err)
		return
	}
	if ch.TypeChanged {
		// Thư mục đích -> tệp nguồn: cây con đã được kế hoạch cho phép xóa (resolveTypeChanges).
		if err := c.removeDest(ch.Dst); err != nil {
			c.fail(ch.Path, err)
			return
		}
	} else if err := c.prepareRename(ch); err != nil {
		c.fail(ch.Path, err)
		return
	}

	var err error
	if ch.Dst == nil || ch.TypeChanged {
		err = c.step("rename", ch.Path, func() error { return publishNew(c.root, staged, ch.Path) })
	} else {
		err = c.step("rename", ch.Path, func() error {
			makeWritable(c.root, ch.Path)
			return c.root.Rename(staged, ch.Path)
		})
	}
	if err != nil {
		c.fail(ch.Path, err)
		return
	}
	c.done[ch.Path] = true
	c.r.Bytes += ch.Src.Size
	if ch.Dst == nil || ch.TypeChanged {
		c.r.Added++
	} else {
		c.r.Updated++
	}
	// Dữ liệu đã vào đích; lỗi metadata chỉ được báo, không hoàn tác.
	c.applyMeta(ch.Path, ch.Src)
}

// publishNew đặt tệp mới mà không ghi đè một tệp xuất hiện sau khi kiểm (Link nguyên tử);
// hệ tệp không hỗ trợ hard link (FAT, exFAT...) thì dùng Rename sau khi kiểm lại.
func publishNew(root *receivefs.Root, staged, dest string) error {
	err := root.PublishNew(staged, dest)
	if err == nil || errors.Is(err, fs.ErrExist) {
		return err
	}
	if _, statErr := root.Lstat(staged); statErr != nil {
		return err // Link đã thành công nhưng xóa staging lỗi, hoặc lỗi khác
	}
	if _, statErr := root.Lstat(dest); statErr == nil {
		return fmt.Errorf("%w: destination appeared after the scan", ErrFileChanged)
	}
	return root.Rename(staged, dest)
}

func (c *committer) applyMeta(p string, src *Entry) {
	if err := c.step("chmod", p, func() error { return c.root.Chmod(p, src.Mode) }); err != nil {
		c.fail(p, fmt.Errorf("set mode: %w", err))
	}
	if src.IsFile() {
		if err := c.step("chtimes", p, func() error { return c.root.Chtimes(p, src.ModTime, src.ModTime) }); err != nil {
			c.fail(p, fmt.Errorf("set modification time: %w", err))
		}
	}
}

// verifyStaged kiểm tệp đã nhận đúng size và xxhash nguồn.
func (c *committer) verifyStaged(staged string, src *Entry) error {
	if len(src.Hash) != HashSize {
		return fmt.Errorf("%w: no source hash", ErrStagedMismatch)
	}
	info, err := c.root.Lstat(staged)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStagedMismatch, err)
	}
	if !info.Mode().IsRegular() || info.Size() != src.Size {
		return fmt.Errorf("%w: size %d, want %d", ErrStagedMismatch, info.Size(), src.Size)
	}
	f, err := c.root.Open(staged)
	if err != nil {
		return err
	}
	sum, err := hashReader(c.ctx, f)
	_ = f.Close()
	if err != nil {
		return err
	}
	if !bytes.Equal(sum, src.Hash) {
		return fmt.Errorf("%w: content hash differs", ErrStagedMismatch)
	}
	return nil
}

func (c *committer) touch(ch Change) {
	if c.dirRenameFailed[ch.Path] {
		return
	}
	if c.dirRenamed[ch.Path] {
		// Thư mục đã được đổi tên hoa/thường ở bước 1; chỉ còn cần áp quyền.
		before := len(c.r.Failures)
		c.applyMeta(ch.Path, ch.Src)
		if len(c.r.Failures) == before {
			c.r.Touched++
		}
		return
	}
	if err := hook("verify-dest", ch.Path); err != nil {
		c.fail(ch.Path, err)
		return
	}
	if err := c.verifyDest(ch); err != nil {
		c.fail(ch.Path, err)
		return
	}
	if err := c.prepareRename(ch); err != nil {
		c.fail(ch.Path, err)
		return
	}
	before := len(c.r.Failures)
	c.applyMeta(ch.Path, ch.Src)
	if len(c.r.Failures) == before {
		c.r.Touched++
	}
}

func (c *committer) applyDeletes() {
	p := c.plan
	if len(p.Delete) == 0 {
		return
	}
	var remaining []Change
	for _, d := range p.Delete {
		if !c.preDeleted[d.Path] {
			translated := d
			translated.Path = c.translateDst(d.Path)
			remaining = append(remaining, translated)
		}
	}
	c.r.Deleted = len(c.preDeleted)
	if len(remaining) == 0 {
		return
	}
	if c.hasFailures() {
		// Luật 2.4.4: chỉ xóa khi mọi thay đổi Add/Update/Touch đã commit không lỗi và không xung đột.
		c.r.DeletesHeld = len(remaining)
		if c.r.DeleteBlocked == "" {
			c.r.DeleteBlocked = "some changes failed to commit; deletions held until a clean round"
		}
		return
	}
	sub := *p
	sub.Delete = remaining
	deleted, failed, err := ApplyDeletes(c.ctx, c.root, &sub, DeleteOptions{Committed: true, Confirmed: c.o.Decision.ConfirmDeletes})
	switch {
	case errors.Is(err, ErrDeleteNeedsConfirm):
		c.r.DeletesHeld = len(remaining)
		return
	case err != nil:
		c.fail("(delete)", err)
		return
	}
	c.r.Deleted += len(deleted)
	for _, f := range failed {
		c.fail(f.Path, f.Err)
	}
}

// makeWritable bỏ thuộc tính chỉ-đọc trên Windows để Rename/Remove thay được tệp.
// Trên Unix quyền ghi thư mục cha mới quyết định, nên không cần làm gì.
func makeWritable(root *receivefs.Root, p string) {
	if runtime.GOOS != "windows" {
		return
	}
	info, err := root.Lstat(p)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o200 != 0 {
		return
	}
	_ = root.Chmod(p, info.Mode().Perm()|0o200)
}
