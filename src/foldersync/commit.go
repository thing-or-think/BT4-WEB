package foldersync

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"runtime"
	"slices"
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
// Thứ tự: thư mục (nông trước) -> tệp (kiểm size+hash bản nhận, kiểm đích chưa đổi
// so với lúc quét, rename nguyên tử, chmod, mtime) -> Touch -> quyền thư mục -> xóa.
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
	c := &committer{ctx: ctx, root: root, plan: plan, o: o, r: r, done: map[string]bool{}}
	for _, ch := range plan.Conflicts {
		c.failf(ch.Path, "%s", ch.Reason)
	}
	r.Conflicts = len(plan.Conflicts)

	changes := slices.Concat(plan.Add, plan.Update)
	slices.SortStableFunc(changes, func(a, b Change) int {
		return cmp.Compare(PathDepth(a.Path), PathDepth(b.Path))
	})
	// 1. Thư mục, nông trước.
	for _, ch := range changes {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if ch.Src.IsDir() {
			c.commitDir(ch)
		}
	}
	// 2. Tệp.
	for _, ch := range changes {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if ch.Src.IsFile() {
			c.commitFile(ch)
		}
	}
	// 3. Touch.
	for _, ch := range plan.Touch {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		c.touch(ch)
	}
	// 4. Quyền thư mục mới (sau khi đã ghi xong con, để 0o555 không chặn ghi).
	for _, ch := range changes {
		if ch.Src.IsDir() && c.done[ch.Path] {
			if err := c.step("chmod", ch.Path, func() error { return root.Chmod(ch.Path, ch.Src.Mode) }); err != nil {
				c.fail(ch.Path, err)
			}
		}
	}
	// 5. Xóa, chỉ sau khi mọi tệp đã commit (luật 2.4.4).
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
	// đường dẫn đích đã bị xóa sớm để thay thư mục bằng tệp
	preDeleted map[string]bool
}

func (c *committer) fail(p string, err error) {
	c.r.Failures = append(c.r.Failures, fmt.Sprintf("%s: %v", p, err))
}

func (c *committer) failf(p, format string, args ...any) {
	c.fail(p, fmt.Errorf(format, args...))
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
	if err := c.root.RejectSymlinkPath(ch.Dst.Path); err != nil {
		return err
	}
	info, err := c.root.Lstat(ch.Dst.Path)
	if err != nil {
		return err
	}
	return unchangedSince(ch.Dst, info)
}

// prepareRename đổi tên mục đích chỉ khác hoa/thường sang tên nguồn.
func (c *committer) prepareRename(ch Change) error {
	if ch.Dst == nil || ch.Dst.Path == ch.Path {
		return nil
	}
	return c.step("case-rename", ch.Path, func() error { return c.root.Rename(ch.Dst.Path, ch.Path) })
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

// removeDest xóa một mục đích (kèm cây con đã nằm trong kế hoạch xóa nếu là thư mục).
func (c *committer) removeDest(dst *Entry) error {
	if dst.IsDir() {
		if !c.o.Decision.ConfirmDeletes && c.plan.RequireDeleteConfirm {
			return fmt.Errorf("%w: replacing directory needs delete confirmation", ErrDeleteNeedsConfirm)
		}
		var sub []Change
		for _, d := range c.plan.Delete {
			if len(d.Path) > len(dst.Path) && d.Path[:len(dst.Path)+1] == dst.Path+"/" {
				sub = append(sub, d)
			}
		}
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
		for _, d := range sub {
			c.preDeleted[d.Path] = true
		}
	}
	return c.step("remove", dst.Path, func() error {
		makeWritable(c.root, dst.Path)
		return c.root.Remove(dst.Path)
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
			remaining = append(remaining, d)
		}
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
	c.r.Deleted = len(deleted) + len(c.preDeleted)
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
