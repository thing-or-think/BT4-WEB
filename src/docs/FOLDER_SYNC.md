# Đồng bộ thư mục với `croc sync` (Bản nháp)

`croc sync` đồng bộ **một chiều** một thư mục từ **máy nguồn (source)** sang **máy đích (destination)** qua kênh mã hóa đầu-cuối (PAKE) của `croc`.

Vai trò của mỗi máy được xác định qua biến môi trường `CROC_SECRET`:

- **Máy nguồn**: chạy `croc sync <folder>` khi **không** đặt `CROC_SECRET`. Máy nguồn quét thư mục và gửi bản kê metadata (manifest) kèm bộ lọc đã dùng sang máy đích.
- **Máy đích**: chạy `croc sync <folder>` khi **có** đặt `CROC_SECRET`. **Máy đích là bên tính toán diff**, lập kế hoạch thay đổi (`Plan`), chỉ yêu cầu máy nguồn gửi những tệp mới hoặc đã thay đổi, rồi áp dụng kế hoạch lên thư mục đích.

Cả hai phía đều yêu cầu `<folder>` phải tồn tại sẵn và là một thư mục (`croc sync` không tự tạo thư mục gốc nếu chưa có).

---

## Bắt đầu nhanh

### 1. Trên máy nguồn

```bash
croc sync ./du-an
```

`croc` sẽ sinh mã phiên (code phrase) và in sẵn lệnh chạy trên máy đích cho cả Linux/macOS và Windows PowerShell.

### 2. Trên máy đích

**Linux / macOS:**

```bash
CROC_SECRET='ma-phien-o-day' croc sync ./du-an
```

**Windows PowerShell:**

```powershell
$env:CROC_SECRET='ma-phien-o-day'; croc sync .\du-an
```

Để xem trước toàn bộ kế hoạch thay đổi mà không chỉnh sửa gì trên đĩa đích (kể cả khi dự định dùng `--delete`):

```bash
CROC_SECRET='ma-phien-o-day' croc sync --dry-run --delete ./du-an
```

---

## Bảng cờ theo vai trò

`croc sync` kiểm tra chặt vai trò: dùng cờ của máy nguồn trên máy đích hoặc ngược lại sẽ báo lỗi ngay với mã thoát `1`.

### Cờ phía nguồn (không đặt `CROC_SECRET`)

| Cờ | Mô tả |
| --- | --- |
| `--code`, `-c <mã>` | Đặt mã phiên tùy chọn (tối thiểu 6 ký tự). Trên Unix cần bật `croc --classic` để tránh lộ mã trong danh sách tiến trình. |
| `--transport <chế-độ>` | Chọn phương thức truyền dữ liệu tệp: `auto` (mặc định), `derp`, hoặc `relay`. |
| `--exclude <chuỗi,...>` | Loại bỏ đường dẫn chứa bất kỳ chuỗi con nào (phân tách bằng dấu phẩy, không phân biệt hoa/thường). |
| `--exclude-file <đường-dẫn,...>` | Loại bỏ chính xác các đường dẫn tương đối đã liệt kê (phân tách bằng dấu phẩy). |
| `--git` | Tôn trọng `.gitignore`, không đồng bộ các mục bị Git bỏ qua. |
| `--watch` | Theo dõi liên tục thư mục nguồn và tự động đồng bộ các thay đổi sang máy đích trong cùng một phiên cho tới khi bấm `Ctrl-C`. |
| `--no-local` | Tắt relay cục bộ và khám phá mạng LAN. |

### Cờ phía đích (có đặt `CROC_SECRET`)

| Cờ | Mô tả |
| --- | --- |
| `--delete` | Xóa các tệp/thư mục chỉ tồn tại ở đích mà không có ở nguồn (áp dụng đầy đủ các lớp bảo vệ an toàn). |
| `--dry-run` | In bảng kế hoạch thay đổi rồi dừng lại; không tải tệp và không ghi/xóa trên đĩa đích (vẫn in bảng kể cả khi kèm `--quiet`). |
| `--checksum` | So sánh nội dung tệp bằng hash toàn tệp (`xxhash64`) ngay cả khi `size` và `mtime` trùng nhau. |
| `--allow-empty-source` | Cho phép `--delete` xóa các mục ở đích khi thư mục nguồn rỗng. |

### Cờ toàn cục (đặt trước `sync`, dùng cho cả hai phía)

| Cờ | Mô tả |
| --- | --- |
| `--yes` | Tự động đồng ý với xác nhận xóa thông thường (`[y/N]`). **Không** bỏ qua yêu cầu gõ `yes` khi xóa quá 50% số tệp đích. |
| `--quiet` | Ẩn thông báo tiến trình, bảng kế hoạch và dòng tổng kết (trừ `--dry-run` vẫn in bảng kế hoạch). |
| `--relay <địa-chỉ>` | Chỉ định máy chủ relay tự vận hành (ví dụ `127.0.0.1:9009`). |
| `--pass <mật-khẩu>` | Mật khẩu kết nối tới relay (hoặc đường dẫn tệp chứa mật khẩu). |

### Các cờ chưa hỗ trợ

| Cờ | Trạng thái |
| --- | --- |
| `--qrcode`, `--qr` | Chưa khả dụng vì đồng bộ qua web chưa được hỗ trợ (`--qrcode needs web sync, which is not available yet`). |

---

## Chế độ theo dõi (--watch)

Để đồng bộ liên tục mỗi khi thư mục nguồn thay đổi, bật `--watch` trên **máy nguồn**:

```bash
croc sync --watch ./du-an
```

**Máy đích không cần thêm cờ nào**: chỉ cần chạy lệnh `CROC_SECRET='...' croc sync ./du-an` như bình thường (có thể kết hợp thêm `--delete`, `--dry-run`, `--checksum`, `--yes`, `--quiet`) và máy đích sẽ tự đi theo chế độ watch của máy nguồn.

Cách hoạt động của `--watch`:

- **Một phiên kết nối dài**: hai máy giữ nguyên một phiên `croc` đã mã hóa PAKE xuyên suốt nhiều lượt đồng bộ liên tiếp, không cần bắt tay lại sau mỗi lần chỉnh sửa tệp.
- **Gom sự kiện (debounce 400 ms / tối đa 2 s)**: máy nguồn theo dõi thay đổi hệ tệp (`fsnotify`, tự chuyển sang quét định kỳ dự phòng mỗi 2 giây nếu chạm giới hạn watch của hệ điều hành), chờ khoảng yên tĩnh **400 ms** để gom các thay đổi liên tiếp thành một lượt, và phát lượt mới chậm nhất sau **2 giây** kể từ sự kiện đầu tiên khi có ghi liên tục.
- **Keepalive và tự nối lại bằng mã nối lại mới (`RejoinCode`)**: kênh gửi nhịp `sync-ping`/`sync-pong` định kỳ để giữ kết nối qua NAT/relay. Ở mỗi lượt, máy nguồn cấp một `RejoinCode` mới ngay trong kênh đã mã hóa; nếu mất kết nối, cả hai phía tự động thử kết nối lại theo backoff tăng dần bằng mã nối lại mới nhất mà không bao giờ dùng lại mã phiên ban đầu.
- **Mỗi lượt là một lần so sánh đầy đủ**: sự kiện hệ tệp chỉ là tín hiệu kích hoạt. Ở mỗi lượt, máy nguồn quét lại toàn bộ cây thư mục và máy đích tính diff hai tầng đầy đủ như một lượt độc lập, giúp mọi thay đổi (kể cả lưu nguyên tử kiểu trình soạn thảo, tạo hàng loạt tệp, hay sau khi nối lại mạng) luôn hội tụ chính xác.
- **Luật xóa áp dụng trên từng lượt**: mọi lớp bảo vệ của `--delete` (bảo vệ mục bị lọc/bỏ qua, nguồn rỗng, ngưỡng >50% buộc gõ `yes` kể cả khi có `--yes`) vẫn chạy ở từng lượt. Riêng khi ở chế độ watch có mục cần xóa nhưng `stdin` không phải terminal và thiếu `--yes`, máy đích **giữ lại các lệnh xóa của lượt đó** thay vì trả lỗi làm dừng watch (in thông báo gợi ý chạy lại với `--yes` để cho phép xóa trong chế độ watch) và vẫn áp dụng các thay đổi thêm/sửa khác. Nếu máy đích bật `--dry-run`, mỗi lượt sẽ in bảng kế hoạch mà không thay đổi gì trên đĩa.
- **Dừng bằng `Ctrl-C` thoát mã `0`**: nhấn `Ctrl-C` là cách dừng bình thường của chế độ watch — cả hai phía in `Stopped watching.` và kết thúc với mã thoát `0` (chỉ trả mã `1` nếu chế độ watch dừng do lỗi nghiêm trọng).
- **Độ trễ thực đo**: độ trễ đồng bộ cho một chỉnh sửa đơn lẻ đo được ở phân vị **p95 ≈ 0,5 s** trên relay cục bộ.

---

## Cách phát hiện thay đổi

Máy đích so sánh manifest nguồn với cây thư mục đích theo cơ chế **hai tầng**:

1. **Tầng 1 — Metadata (`size` + `mtime`)**:
   - **Khác loại** (tệp ↔ thư mục): đánh dấu cập nhật đổi loại (`Update`; nếu thư mục đích còn tệp con không được phép xóa thì đưa vào danh sách xung đột `Conflicts`).
   - **Thư mục**: không so `mtime` (vì `mtime` thư mục đổi mỗi khi thêm/xóa mục con), chỉ so quyền (`mode`) và khác biệt chữ hoa/thường tên -> `Touch` hoặc bỏ qua (`Skip`).
   - **Tệp khác `size`**: chắc chắn đã thay đổi -> `Update`.
   - **Tệp cùng `size` và cùng `mtime`** (mặc định, không bật `--checksum`): coi là không đổi (`Skip`, hoặc `Touch` nếu chỉ khác `mode` hay chữ hoa/thường tên tệp) mà không cần đọc nội dung.
2. **Tầng 2 — Hash (`xxhash64`, chỉ tính khi cần)**:
   - Chạy khi hai tệp có **cùng `size` nhưng khác `mtime`**, hoặc khi máy đích bật **`--checksum`**.
   - Máy đích băm cục bộ và chỉ yêu cầu máy nguồn băm đúng các đường dẫn cần kiểm tra.
   - Hash trùng nhau -> `Touch` (chỉ cập nhật `mtime`/quyền/tên hoa-thường, không truyền lại dữ liệu).
   - Hash khác nhau -> `Update` (tải lại nội dung tệp).

> **Lưu ý:** Một chỉnh sửa làm đổi nội dung tệp nhưng giữ nguyên đồng thời cả kích thước (`size`) và thời gian sửa (`mtime`) sẽ **không** được phát hiện ở chế độ mặc định. Hãy dùng `--checksum` trên máy đích khi cần kiểm tra tuyệt đối theo nội dung.

---

## Cơ chế an toàn của `--delete`

Để tránh mất dữ liệu ngoài ý muốn khi dùng `--delete`, máy đích áp dụng các lớp bảo vệ sau:

1. **Quét nguồn không đầy đủ**: nếu quá trình quét phía nguồn gặp lỗi đọc (`Complete = false`), toàn bộ thao tác xóa trong lượt đó bị hủy.
2. **Bảo vệ mục bị lọc và mục bị bỏ qua**: bộ lọc của máy nguồn (`--exclude`, `--exclude-file`, `--git`) được gửi kèm manifest và áp lại khi quét đích. Mọi đường dẫn khớp bộ lọc, hoặc bị nguồn bỏ qua (như symlink, lỗi đọc), cùng các thư mục tổ tiên của chúng đều được bảo vệ khỏi `--delete`.
3. **Nguồn rỗng**: nếu thư mục nguồn không có mục nào mà đích còn tệp/thư mục sẽ bị xóa, máy đích từ chối xóa trừ khi truyền rõ `--allow-empty-source`.
4. **Ngưỡng xóa quá 50% số tệp đích**: nếu số tệp dự định xóa lớn hơn một nửa tổng số tệp hiện có ở đích, **kể cả khi đã truyền `--yes`**, người dùng trên terminal vẫn phải gõ đầy đủ `yes` để xác nhận. Nếu không gõ `yes` hoặc `stdin` không phải terminal, các thay đổi thêm/sửa vẫn được áp dụng nhưng toàn bộ thao tác xóa bị **giữ lại** (trả mã thoát `2`).
5. **Không có terminal và không có `--yes`**: khi có mục cần xóa (dưới ngưỡng 50%) nhưng `stdin` không phải terminal (ví dụ chạy trong script/CI) và không truyền `--yes`, `croc sync` từ chối ngay với mã thoát `1` và không sửa đổi bất kỳ tệp nào.
6. **Chỉ xóa sau khi mọi tệp đã commit**: bước xóa luôn thực hiện cuối cùng, chỉ khi mọi thao tác `Add`/`Update`/`Touch` đều commit thành công, không có lỗi và không có xung đột. Trước khi xóa từng mục, máy đích kiểm tra lại trên đĩa (chặn symlink, đối chiếu metadata lúc quét) và chỉ xóa từng mục đơn lẻ (không bao giờ chạy xóa đệ quy lên thư mục người dùng).

---

## Ghi nguyên tử, staging `.croc-sync-tmp-*` và tệp khóa `.croc-sync-tmp-lock`

- **Tệp khóa `.croc-sync-tmp-lock`**: ngay khi bắt đầu, máy đích mở và giữ khóa độc quyền trên `.croc-sync-tmp-lock` ở gốc thư mục đích để ngăn hai tiến trình `croc sync` ghi đồng thời vào cùng một thư mục. Tệp khóa này được giữ lại sau khi kết thúc để tránh trường hợp hai tiến trình giữ khóa trên hai inode khác nhau; bộ quét và bộ lọc luôn bỏ qua mọi tên bắt đầu bằng `.croc-sync-tmp-`.
- **Thư mục staging `.croc-sync-tmp-<id>/`**: mọi tệp tải từ máy nguồn được nhận vào một thư mục tạm nằm ngay trong thư mục đích (cùng hệ tệp).
- **Commit nguyên tử**: sau khi nhận xong, máy đích kiểm tra lại kích thước và `xxhash64` của bản staged so với manifest nguồn, xác nhận tệp đích chưa bị sửa kể từ lúc quét, rồi đưa vào vị trí thật bằng thao tác nguyên tử (`Link` cho tệp mới hoặc `Rename` cho tệp cập nhật), cuối cùng mới áp quyền (`chmod`) và `mtime`. Mỗi tệp đích luôn ở trạng thái bản cũ hoàn chỉnh hoặc bản mới hoàn chỉnh, không bao giờ bị ghi dở dang.
- **Dọn staging mồ côi**: thư mục staging được xóa khi lượt kết thúc. Nếu lượt trước bị ngắt đột ngột, lần chạy sau sẽ tự động dọn các thư mục `.croc-sync-tmp-*` không còn tiến trình nào khóa.

---

## Mã thoát (Exit codes)

| Mã | Ý nghĩa |
| --- | --- |
| `0` | Thành công hoàn toàn (bao gồm trường hợp đã đồng bộ sẵn, `--dry-run` hoàn tất, hoặc dừng chế độ `--watch` bằng `Ctrl-C`). |
| `1` | Lỗi nghiêm trọng: cờ sai vai trò, thư mục không hợp lệ, lỗi mạng/PAKE/giao thức, từ chối xóa do thiếu `--yes` khi không có terminal ở chế độ một lượt, hoặc bị ngắt (`Ctrl-C`) ở chế độ một lượt. |
| `2` | Hoàn tất một phần (chế độ một lượt): có ít nhất một mục lỗi khi commit/xóa, có xung đột, hoặc các lệnh xóa bị giữ lại (chẳng hạn xóa >50% không được xác nhận hoặc bị giữ vì có lỗi commit trước đó). |

---

## Giới hạn hiện tại

- **Chưa có giao diện web (`croc-web` / `--qrcode`)**: hiện chỉ hoạt động giữa hai máy dùng CLI.
- **Bỏ qua symlink** (và các tệp đặc biệt như pipe, socket, thiết bị): symlink không được đồng bộ mà chỉ in cảnh báo; phía đích bảo vệ các đường dẫn liên quan khỏi `--delete`.
- **Không resume giữa chừng một lượt bị ngắt**: nếu mất kết nối hoặc bấm `Ctrl-C` giữa lượt, các tệp chưa commit vẫn giữ nguyên trạng thái cũ trên đĩa đích. Chỉ cần chạy lại `croc sync` — những tệp đã commit sẽ được bỏ qua và hai thư mục sẽ hội tụ về trạng thái giống nhau.
- **Relay có thể nhận biết phiên `croc sync`**: tên tính năng `folder-sync-v1` được gửi trong thông điệp PAKE trước khi kênh được mã hóa, vì vậy máy chủ relay có thể phân biệt phiên `croc sync` với phiên `croc send` thông thường. Relay **không** nhìn thấy tên tệp, cấu trúc thư mục, bộ lọc, hash hay nội dung dữ liệu vì toàn bộ thông điệp `sync-*` và dữ liệu tệp chỉ truyền sau khi kênh mã hóa đầu-cuối đã thiết lập.
