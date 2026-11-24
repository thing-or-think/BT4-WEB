# Đồng bộ thư mục với `croc sync`

`croc sync` đồng bộ **một chiều** một thư mục từ máy nguồn (Host) sang máy đích
(Join). Lần đầu, toàn bộ thư mục được gửi. Các lần sau chỉ gửi file **mới** hoặc
**đã thay đổi**; file không đổi được bỏ qua. Lệnh dùng lại toàn bộ lớp bảo mật
của croc (mã PAKE, mã hóa đầu-cuối), vận chuyển P2P/relay, nén và đa luồng.

> **Trạng thái:** phần CLI (lệnh, cờ, hiển thị, xác nhận, Ctrl+C) đã xong.
> Engine đồng bộ (`src/foldersync/`) vẫn đang được phát triển. Cho đến khi engine
> hoàn thành, lệnh sẽ báo:
> `croc sync is not available in this build yet: the sync engine is still under development`.
> Muốn xem giao diện trước, chạy bản demo với engine giả (xem mục
> [Demo không cần engine](#demo-không-cần-engine)).

## Cách dùng nhanh

Trên **máy nguồn** (thư mục cần gửi đi):

```bash
croc sync ./photos
```

croc in mã kết nối và lệnh cần chạy ở máy đích:

```text
Hosting sync from source: /home/an/photos
Code is: acid-acorn-acre
On the target computer, run:
  CROC_SECRET='acid-acorn-acre' croc sync <target-folder>
  PowerShell: $env:CROC_SECRET='acid-acorn-acre'; croc sync <target-folder>
```

Trên **máy đích**, đặt mã vào biến môi trường `CROC_SECRET` rồi chỉ định thư
mục nhận:

```bash
# Linux / macOS
CROC_SECRET='acid-acorn-acre' croc sync ./photos-copy
```

```powershell
# Windows PowerShell
$env:CROC_SECRET='acid-acorn-acre'; croc sync .\photos-copy
```

Thư mục đích chưa tồn tại cũng được. CLI không tự tạo thư mục; việc tạo do
engine thực hiện khi ghi file đầu tiên, nhờ vậy `--dry-run` không đụng tới đĩa.

### Host hay Join được xác định thế nào?

| Điều kiện | Vai trò |
|---|---|
| Biến `CROC_SECRET` **có** giá trị | **Join** (máy đích), mã lấy từ `CROC_SECRET` |
| Biến `CROC_SECRET` **trống** | **Host** (máy nguồn). Mã lấy từ `--code`, hoặc để engine tự sinh |

Đặt cả `CROC_SECRET` lẫn `--code` cùng lúc sẽ bị báo lỗi để tránh nhầm vai trò.

> ⚠️ Trong PowerShell, `$env:CROC_SECRET` vẫn còn trong cửa sổ đó sau khi lệnh
> kết thúc. Nếu sau đó bạn muốn làm **máy nguồn** trong cùng cửa sổ, hãy xóa
> biến trước: `Remove-Item Env:CROC_SECRET`. Dòng đầu tiên croc in ra
> (`Hosting sync…` / `Joining sync…`) cho biết lệnh đang chạy ở vai trò nào.

## Các cờ

| Cờ | Ý nghĩa |
|---|---|
| `--code`, `-c` | Mã kết nối cho máy nguồn. Bỏ trống thì engine tự sinh. |
| `--delete` | Xóa file ở đích không còn tồn tại ở nguồn. Luôn hỏi xác nhận, trừ khi có `--yes`. |
| `--dry-run` | Chỉ in kế hoạch (thêm/sửa/xóa/bỏ qua), **không** ghi, xóa hay đổi mtime file nào. |
| `--watch` | Tiếp tục chạy và đồng bộ thay đổi theo thời gian thực. Nhấn Ctrl+C để dừng. |
| `--checksum` | So sánh nội dung bằng hash thay vì chỉ dựa vào kích thước và thời gian sửa. |
| `--exclude a,b` | Bỏ qua đường dẫn **chứa** một trong các chuỗi (phân tách bằng dấu phẩy). |
| `--exclude-file x/y.txt` | Bỏ qua đường dẫn tương đối **khớp chính xác** (`./x/y.txt` cũng được chuẩn hóa). |
| `--git` | Tôn trọng `.gitignore`. |
| `--transport` | `auto` (mặc định), `derp` hoặc `relay`. |
| `--relay host:port` | Dùng relay riêng. Đặt trước hay sau `sync` đều được, hoặc dùng biến `CROC_RELAY`. |
| `--qrcode`, `--qr` | In mã kết nối dưới dạng mã QR. |
| `--yes` | Không hỏi xác nhận khi xóa (`croc --yes sync` hay `croc sync --yes` đều được). |
| `--quiet` | Chỉ in mã, câu hỏi xác nhận, lỗi và dòng tổng kết. |

Các cờ quyết định **kế hoạch** (`--delete`, `--dry-run`, `--checksum`,
`--exclude`, `--exclude-file`, `--git`) được chọn ở **máy nguồn**. Máy đích làm
theo kế hoạch mà máy nguồn gửi sang.

Không được dùng `--dry-run` cùng `--watch`.

## Đọc kế hoạch đồng bộ

Trước khi truyền file, máy nguồn in kế hoạch. Mỗi dòng có **ký hiệu và chữ**
nên vẫn đọc được khi terminal không có màu:

```text
Sync plan (dry run: nothing will be changed)
  + add    photos/2026/
  + add    photos/2026/beach.jpg  4.0 MB
  + add    notes/todo.md          2.0 kB
  ~ update notes/ideas.md         8.0 kB
  - delete tmp/old.log            1.0 kB
  = skip   120 unchanged items
  Total: 3 to add, 1 to update, 1 to delete, 120 unchanged · 4.0 MB to send · 300.0 MB not re-sent
Dry run complete: no files were transferred, written or deleted.
```

| Ký hiệu | Nghĩa | Màu (nếu có) |
|---|---|---|
| `+ add` | File/thư mục mới, sẽ được gửi | xanh lá |
| `~ update` | File đã đổi, sẽ được gửi lại | xanh lơ |
| `- delete` | File thừa ở đích, sẽ bị xóa (chỉ khi `--delete`) | đỏ |
| `= skip` | Không đổi, không gửi lại | không màu |

Thư mục có dấu `/` ở cuối và không ghi kích thước. Khi không chạy `--dry-run`,
mỗi nhóm chỉ hiện 20 dòng đầu (nhóm xóa hiện 50 dòng), phần còn lại gộp thành
`… and N more`. Chạy `--dry-run` để xem toàn bộ danh sách.

Nếu không có gì cần làm, croc in `Already in sync: N items unchanged.`

## Xác nhận khi xóa

Với `--delete`, nếu kế hoạch có file cần xóa, máy nguồn liệt kê các file đó và hỏi:

```text
Delete 3 items from the target folder? (y/N)
```

- `y` / `yes`: tiếp tục.
- Phím khác, Enter hoặc không đọc được câu trả lời (ví dụ chạy trong script):
  **hủy cả phiên**, không file nào được truyền hay xóa, lệnh thoát với mã 1.
  Muốn chạy không hỏi thì thêm `--yes`.
- Ở chế độ `--watch`, câu hỏi có thêm lựa chọn `a` (đồng ý cho đến hết phiên)
  để không phải xác nhận lại ở mỗi lần có thay đổi.

Máy đích không bao giờ hỏi. Kế hoạch hiện ở đó chỉ để thông báo
(`Incoming changes from the source`).

## Theo dõi thời gian thực (`--watch`)

```bash
croc sync --watch --delete --yes ./project
```

Sau lần đồng bộ đầu tiên, croc in `Watching for changes (Ctrl+C to stop)…`. Từ
đó, mỗi thay đổi được ghi một dòng:

```text
[15:04:05] modify notes/ideas.md
[15:04:05] rename a.txt → b.txt
[15:04:06] changes: 0 to add, 1 to update, 0 to delete · 8.0 kB
  synced notes/ideas.md
[15:04:06] synced: 0 added, 1 updated, 0 deleted, 0 unchanged · 8.0 kB in 85ms
```

## Dừng bằng Ctrl+C

Nhấn Ctrl+C một lần: croc hủy context, engine dừng truyền và xóa các file tạm
`.croc-sync-tmp-*`, rồi croc in `Sync stopped.` (hoặc `Stopped watching.` khi
đang `--watch`). croc chờ tối đa 5 giây để dọn dẹp. Nhấn Ctrl+C lần thứ hai sẽ
thoát ngay lập tức.

## Thông báo lỗi và mã thoát

| Tình huống | Thông báo | Mã thoát |
|---|---|---|
| Thành công, hoặc dry-run xong | `Sync complete in …` / `Dry run complete …` | 0 |
| Ctrl+C | `Sync stopped.` | 0 |
| Từ chối xóa | `sync aborted: no files were transferred or deleted` | 1 |
| Máy bên kia dùng croc cũ | `the other computer's croc does not support croc sync (folder-sync-v1); update croc on both computers and try again` | 1 |
| Một số file lỗi (ví dụ file đang bị mở trên Windows) | `error: <path>: <lỗi>` ngay khi lỗi xảy ra, cuối cùng là `croc sync finished, but N items could not be synced` | 1 |
| Lỗi nghiêm trọng (mạng, PAKE, thư mục gốc) | `croc sync: <lỗi>` | 1 |

Lỗi ở một file **không** dừng cả phiên: các file khác vẫn được đồng bộ.

## Các lớp bảo vệ ở phía CLI

- **Không đồng bộ vào thư mục gốc ổ đĩa hay thư mục home.** Máy đích từ chối
  `/`, `C:\` và thư mục home (kể cả khi trỏ tới qua symlink), vì `--delete` từ
  máy nguồn có thể xóa file ở đó. Hãy chọn một thư mục con.
- **Nguồn phải là thư mục có sẵn.** Đích có thể chưa tồn tại, nhưng nếu đã tồn
  tại thì phải là thư mục, không được là file.
- **`--code` trên Linux/macOS.** Mã truyền bằng tham số dòng lệnh sẽ hiện trong
  danh sách tiến trình (`ps`) mà người dùng khác trên máy có thể đọc. croc in
  cảnh báo; an toàn nhất là bỏ `--code` để croc tự sinh mã.
- `--dry-run` ở phía CLI chỉ đọc thông tin thư mục (stat), không tạo hay sửa gì.

Kiểm tra bảo mật đường dẫn từ mạng (`..`, đường dẫn tuyệt đối, symlink ra ngoài
root, thư mục nhạy cảm như `.ssh`) do engine thực hiện (`src/foldersync/security.go`).

## Chạy thử trên một máy (smoke test)

Mở 3 terminal:

```bash
croc relay                                                         # 1: relay cục bộ
croc sync --relay localhost:9009 --code test-sync-code ./A         # 2: máy nguồn
CROC_SECRET=test-sync-code croc sync --relay localhost:9009 ./B    # 3: máy đích
```

Trên Windows PowerShell, terminal 3 là:

```powershell
$env:CROC_SECRET='test-sync-code'; croc sync --relay localhost:9009 .\B
```

## Demo không cần engine

Các test của CLI có sẵn một engine giả, chạy một phiên mẫu gồm mã kết nối, kế
hoạch, câu hỏi xác nhận xóa, thanh tiến trình và dòng tổng kết. Nên build test
binary rồi chạy trực tiếp để giữ màu và thanh tiến trình:

```bash
go test -c -o croc-sync-demo ./src/cli
CROC_SYNC_DEMO=1 ./croc-sync-demo -test.run TestSyncDemo                           # có hỏi xác nhận xóa
CROC_SYNC_DEMO=1 CROC_SYNC_DEMO_DRY_RUN=1 ./croc-sync-demo -test.run TestSyncDemo  # chế độ dry-run
```

```powershell
go test -c -o croc-sync-demo.exe ./src/cli
$env:CROC_SYNC_DEMO='1'; .\croc-sync-demo.exe -test.run TestSyncDemo
```

## Kiến trúc: CLI ↔ engine

```text
src/cli/sync.go                         src/foldersync/ (engine)
───────────────                         ────────────────────────
parse flags → SyncConfig ─────────────► RunSyncHost / RunSyncJoin(ctx, cfg, reporter)
syncReporter (implements                    │
 ProgressReporter)       ◄───────────── OnCode, OnStatus, OnPlan, OnFileStart,
                                         OnProgress, OnFileDone, OnWatchEvent,
                                         OnRoundDone
Ctrl+C → ctx bị hủy ────────────────────► engine dừng, dọn file tạm, trả context.Canceled
```

- Hợp đồng đầy đủ nằm ở [`src/foldersync/contract.go`](../foldersync/contract.go)
  (Contract C).
- Engine **không bao giờ** in thẳng ra terminal; mọi output đi qua
  `ProgressReporter`. Reporter của CLI an toàn khi bị gọi từ nhiều goroutine
  (có test `-race`).
- Các cờ thuần hiển thị (`--quiet`, `--qr`, `--yes`) ở lại CLI, không đưa vào
  `SyncConfig`.
- CLI giữ hai biến `runSyncHost` / `runSyncJoin` trỏ vào engine, nên test và
  demo có thể thay bằng engine giả.

## Hạn chế hiện tại

- `--pass` (mật khẩu relay) chưa được hỗ trợ vì `SyncConfig` (Contract C v1)
  chưa có trường tương ứng. croc in cảnh báo và dùng mật khẩu relay mặc định.
- Mã QR (`--qr`) hiện chỉ chứa mã kết nối. Khi giao diện Web chốt đường dẫn tham
  gia đồng bộ, QR sẽ chuyển sang chứa URL đó.
