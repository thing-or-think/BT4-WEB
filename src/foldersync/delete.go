package foldersync

import "fmt"

// enforceDeleteRules áp các luật chặn xóa toàn lượt (mục 2.4) lên p.Delete:
//
//  1. Manifest nguồn không đầy đủ (Complete=false): tắt xóa trong lượt này.
//  3. Nguồn rỗng mà đích còn mục cần xóa: dừng xóa, trừ khi AllowEmptySource.
//     Xóa quá 50% số tệp đích: RequireDeleteConfirm (--yes không bỏ qua được).
//
// Luật 2 (bảo vệ mục bị lọc/bỏ qua) nằm trong deleteCandidates.
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
