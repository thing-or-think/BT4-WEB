# Đồng bộ thư mục với `croc sync`

`croc sync` đồng bộ **một chiều** một thư mục từ **máy nguồn (source)** sang **máy đích (destination)** qua kênh mã hóa đầu-cuối (PAKE) của `croc`. Chỉ những tệp mới hoặc đã thay đổi mới được truyền qua mạng.

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

`croc` sẽ sinh mã phiên (code phrase) và in sẵn lệnh chạy trên máy đích cho cả Linux/macOS, Windows PowerShell, đồng thời in đường dẫn mở trực tiếp trên trình duyệt Chrome/Edge (`<web-url>/?sync=<mã>#sync`, trừ khi dùng `--transport derp`).

Để hiển thị thêm mã QR cho đường dẫn Web Sync:

```bash
croc sync --qrcode ./du-an
```

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
| `--qrcode`, `--qr` | Hiển thị mã QR cho đường dẫn Web Sync (`<web-url>/?sync=<mã>#sync`). Không dùng được với `--transport derp`. |
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
| `--relay <địa-chỉ>` | Chỉ định máy chủ relay tự vận hành (ví dụ `127.0.0.1:9009`; biến môi trường `CROC_RELAY`). |
| `--relay6 <địa-chỉ>` | Địa chỉ IPv6 của máy chủ relay (biến môi trường `CROC_RELAY6`). |
| `--pass <mật-khẩu>` | Mật khẩu kết nối tới relay, hoặc đường dẫn tệp chứa mật khẩu (biến môi trường `CROC_PASS`). |
| `--web-url <url>` | Origin của giao diện web cho đường dẫn và mã QR Web Sync (mặc định `https://getcroc.com`; biến môi trường `CROC_WEB_URL`). |
| `--local` | Ép chỉ dùng kết nối mạng cục bộ (LAN); yêu cầu `--transport auto` ở máy nguồn. |
| `--socks5 <địa-chỉ>` / `--connect <url>` | Kết nối relay qua proxy SOCKS5 (`SOCKS5_PROXY`) hoặc HTTP (`HTTP_PROXY`). |
| `--curve <đường-cong>` | Chọn đường cong mã hóa PAKE (mặc định `p256`). |
| `--no-compress` / `--throttleUpload <tốc-độ>` | Tắt nén dữ liệu hoặc giới hạn băng thông tải lên (chỉ áp dụng ở máy nguồn). |
| `--disable-clipboard` / `--extended-clipboard` | Tắt tự động sao chép mã vào clipboard, hoặc sao chép đầy đủ lệnh kèm `CROC_SECRET`. |
| `--debug` | Bật log gỡ lỗi chi tiết. |

---

## Cách phát hiện thay đổi

Máy đích so sánh manifest nguồn với cây thư mục đích theo cơ chế **hai tầng**:

1. **Tầng 1 — Metadata (`size` + `mtime`)**:
   - **Khác loại** (tệp ↔ thư mục): đánh dấu cập nhật đổi loại (`Update`; nếu thư mục đích còn tệp con không được phép xóa thì đưa vào danh sách xung đột `Conflicts`).
   - **Thư mục**: không so `mtime` (vì `mtime` thư mục đổi mỗi khi thêm/xóa mục con), chỉ so quyền (`mode`) và khác biệt chữ hoa/thường tên → `Touch` hoặc bỏ qua (`Skip`).
   - **Tệp khác `size`**: chắc chắn đã thay đổi → `Update`.
   - **Đổi hoa/thường tên tệp hoặc thư mục**: phát hiện đổi tên chỉ khác chữ hoa/thường (ví dụ `README.md` → `Readme.md`) để xử lý đúng trên hệ tệp không phân biệt hoa/thường thay vì xóa rồi tải lại.
   - **Tệp cùng `size` và cùng `mtime`** (mặc định, không bật `--checksum`): coi là không đổi (`Skip`, hoặc `Touch` nếu chỉ khác `mode` hay chữ hoa/thường tên tệp) mà không cần đọc nội dung.
2. **Tầng 2 — Hash (`xxhash64`, chỉ tính khi cần)**:
   - Chạy khi hai tệp có **cùng `size` nhưng khác `mtime`**, hoặc khi máy đích bật **`--checksum`**.
   - Máy đích băm cục bộ và chỉ yêu cầu máy nguồn băm đúng các đường dẫn cần kiểm tra.
   - Hash trùng nhau → `Touch` (chỉ cập nhật `mtime`/quyền/tên hoa-thường, không truyền lại dữ liệu).
   - Hash khác nhau → `Update` (tải lại nội dung tệp).

> **Lưu ý:** Một chỉnh sửa làm đổi nội dung tệp nhưng giữ nguyên đồng thời cả kích thước (`size`) và thời gian sửa (`mtime`) sẽ **không** được phát hiện ở chế độ mặc định. Hãy dùng `--checksum` trên máy đích khi cần kiểm tra tuyệt đối theo nội dung.

---

## Cơ chế an toàn của `--delete`

Để tránh mất dữ liệu ngoài ý muốn khi dùng `--delete`, máy đích áp dụng 6 lớp bảo vệ độc lập:

1. **Quét nguồn không đầy đủ**: nếu quá trình quét phía nguồn gặp lỗi đọc (`Complete = false`), toàn bộ thao tác xóa trong lượt đó bị hủy.
2. **Bảo vệ mục bị lọc và mục bị bỏ qua**: bộ lọc của máy nguồn (`--exclude`, `--exclude-file`, `--git`) được gửi kèm manifest và áp lại khi quét đích. Mọi đường dẫn khớp bộ lọc, hoặc bị nguồn/đích bỏ qua (như symlink, lỗi đọc, tên không hợp lệ), cùng các thư mục tổ tiên của chúng đều được bảo vệ khỏi `--delete`.
3. **Nguồn rỗng**: nếu thư mục nguồn không có mục nào mà đích còn tệp/thư mục sẽ bị xóa, máy đích từ chối xóa trừ khi truyền rõ `--allow-empty-source`.
4. **Ngưỡng xóa quá 50% số tệp đích**: nếu số tệp dự định xóa lớn hơn một nửa tổng số tệp hiện có ở đích, **kể cả khi đã truyền `--yes`**, người dùng trên terminal vẫn phải gõ đầy đủ `yes` để xác nhận. Nếu không gõ `yes` hoặc `stdin` không phải terminal, các thay đổi thêm/sửa vẫn được áp dụng nhưng toàn bộ thao tác xóa bị **giữ lại** (trả mã thoát `2` ở chế độ một lượt).
5. **Không có terminal và không có `--yes`**: khi có mục cần xóa (dưới ngưỡng 50%) nhưng `stdin` không phải terminal (ví dụ chạy trong script/CI) và không truyền `--yes`, ở chế độ một lượt `croc sync` từ chối ngay với mã thoát `1` và không sửa đổi bất kỳ tệp nào.
6. **Chỉ xóa sau khi mọi tệp đã commit**: bước xóa luôn thực hiện cuối cùng theo thứ tự sâu nhất trước (tệp trước, thư mục con trước thư mục cha), chỉ khi mọi thao tác `Add`/`Update`/`Touch` đều commit thành công, không có lỗi và không có xung đột. Trước khi xóa từng mục, máy đích kiểm tra lại trên đĩa (chặn symlink, đối chiếu metadata lúc quét) và chỉ xóa từng mục đơn lẻ (không bao giờ chạy xóa đệ quy lên thư mục người dùng).

---

## Chế độ theo dõi (`--watch`)

Để đồng bộ liên tục mỗi khi thư mục nguồn thay đổi, bật `--watch` trên **máy nguồn**:

```bash
croc sync --watch ./du-an
```

**Máy đích không cần thêm cờ nào**: chỉ cần chạy `CROC_SECRET='...' croc sync ./du-an` như bình thường (có thể kết hợp thêm `--delete`, `--dry-run`, `--checksum`, `--yes`, `--quiet`) và máy đích sẽ tự đi theo chế độ watch của máy nguồn.

Cách hoạt động của `--watch`:

- **Một phiên kết nối dài**: hai máy giữ nguyên một phiên `croc` đã mã hóa PAKE xuyên suốt nhiều lượt đồng bộ liên tiếp, không cần bắt tay lại sau mỗi lần chỉnh sửa tệp và không chạm giới hạn số phiên vào relay.
- **Gom sự kiện (debounce 400 ms / tối đa 2 s)**: máy nguồn theo dõi thay đổi hệ tệp bằng `fsnotify` (tự chuyển sang quét định kỳ dự phòng mỗi **2 giây** nếu chạm giới hạn watch của hệ điều hành như `ENOSPC` inotify), chờ khoảng yên tĩnh **400 ms** để gom các thay đổi liên tiếp thành một lượt, và phát lượt mới chậm nhất sau **2 giây** kể từ sự kiện đầu tiên khi có ghi liên tục.
- **Keepalive và tự nối lại bằng `RejoinCode`**: kênh gửi nhịp `sync-ping`/`sync-pong` mỗi **20 giây** để giữ kết nối qua NAT/relay. Ở mỗi lượt, máy nguồn sinh một `RejoinCode` ngẫu nhiên mới và gửi bên trong kênh đã mã hóa; nếu mất kết nối mạng, cả hai phía tự động thử nối lại theo backoff cấp số nhân (**1 s → 2 s → … tối đa 30 s**) bằng `RejoinCode` mới nhất mà không bao giờ dùng lại mã phiên ban đầu. Các lỗi không phải mạng (sai giao thức, đích bị khóa, đối phương từ chối) sẽ dừng watch ngay.
- **Mỗi lượt là một lần so sánh đầy đủ**: sự kiện hệ tệp chỉ là tín hiệu kích hoạt. Ở mỗi lượt, máy nguồn quét lại toàn bộ cây thư mục và máy đích tính diff hai tầng đầy đủ như một lượt độc lập, giúp mọi thay đổi (kể cả lưu nguyên tử kiểu trình soạn thảo, tạo hàng loạt tệp, hay sau khi nối lại mạng) luôn hội tụ chính xác.
- **Luật xóa áp dụng trên từng lượt**: mọi lớp bảo vệ của `--delete` (bảo vệ mục bị lọc/bỏ qua, nguồn rỗng, ngưỡng >50% buộc gõ `yes` kể cả khi có `--yes`) vẫn chạy ở từng lượt. Riêng khi ở chế độ watch có mục cần xóa nhưng `stdin` không phải terminal và thiếu `--yes`, máy đích **giữ lại các lệnh xóa của lượt đó** thay vì trả lỗi làm dừng watch (in thông báo gợi ý chạy lại với `--yes`) và vẫn áp dụng các thay đổi thêm/sửa khác. Nếu máy đích bật `--dry-run`, mỗi lượt chỉ in bảng kế hoạch mà không thay đổi gì trên đĩa.
- **Dừng bằng `Ctrl-C` thoát mã `0`**: nhấn `Ctrl-C` là cách dừng bình thường của chế độ watch — cả hai phía in `Stopped watching.` và kết thúc với mã thoát `0` (chỉ trả mã `1` nếu chế độ watch dừng do lỗi nghiêm trọng).

---

## Đồng bộ vào trình duyệt (Web Sync)

Ngoài máy đích CLI, bạn có thể đồng bộ một chiều từ máy nguồn `croc sync` trực tiếp vào một thư mục trên máy tính thông qua giao diện web (**Sync folder** / `#sync`):

1. **Trình duyệt hỗ trợ**: yêu cầu **Chrome hoặc Edge trên máy tính** chạy trong ngữ cảnh bảo mật (HTTPS hoặc `localhost`), vì Web Sync dùng File System Access API (`showDirectoryPicker`) để đọc/ghi trực tiếp vào thư mục cục bộ. Trên các trình duyệt không hỗ trợ (Firefox, Safari, trình duyệt di động), giao diện hiển thị sẵn lệnh CLI `CROC_SECRET='...' croc sync <folder>` thay thế.
2. **Mở phiên Web Sync**:
   - Máy nguồn (khi không dùng `--transport derp`) luôn in thêm dòng `Or open in Chrome/Edge: <web-url>/?sync=<mã>#sync`, và có thể kèm `--qrcode` để hiện mã QR.
   - Mở trực tiếp đường dẫn đó (hoặc vào thẻ **Sync folder** / `#sync` và nhập mã phiên). Giao diện điền sẵn mã nhưng **không tự kết nối** — bạn cần chọn thư mục đích trước rồi bấm **Start sync** (hoặc **Preview sync** khi bật dry run).
3. **Chọn thư mục & tùy chọn**:
   - Bấm **Choose folder** để cấp quyền đọc/ghi (`readwrite`) cho thư mục đích. Nếu trình duyệt thu hồi quyền giữa chừng, giao diện cung cấp nút **Grant access again**.
   - Các tùy chọn tương ứng phía đích: xóa tệp không có ở nguồn (`--delete`), cho phép nguồn rỗng xóa toàn bộ (`--allow-empty-source`, chỉ hiện khi bật xóa), so sánh nội dung (`--checksum`), và chỉ xem trước không ghi (`--dry-run`).
   - Sau khi nhận kế hoạch thay đổi, trình duyệt hiển thị bảng thay đổi (`+`, `~`, `*`, `-`, `!`) và yêu cầu xác nhận trước khi xóa (nếu xóa quá 50% số tệp đích, phải gõ `yes`, chọn **Apply without deleting**, hoặc **Cancel**).
   - *Tùy chọn kiểm thử OPFS*: tham số truy vấn `?opfs=1` hiển thị thêm nút **Use browser test storage (OPFS)** để ghi vào Origin Private File System của trình duyệt; tùy chọn này **chỉ dành cho kiểm thử tự động (Playwright E2E)**, không dùng cho đồng bộ thư mục thông thường.
4. **Cơ chế ghi an toàn và bộ nhớ đệm hash cục bộ**:
   - Mọi tệp nhận về được ghi qua `createWritable()` và chỉ được commit (`close()`) sau khi xác minh khớp `xxhash64` với manifest nguồn; nếu lệch hash hoặc tệp đích thay đổi giữa chừng, luồng ghi bị hủy (`abort()`).
   - Vì trình duyệt không thể giữ nguyên thời gian sửa đổi (`mtime`), Web Sync lưu **bộ nhớ đệm hash cục bộ trên IndexedDB** gắn với thư mục đã chọn để các lần đồng bộ tiếp theo so sánh nội dung nhanh mà không phải băm lại toàn bộ các tệp chưa đổi.
5. **Giới hạn của Web Sync**:
   - **Chưa hỗ trợ `--watch`**: nếu máy nguồn bật `--watch`, trình duyệt sẽ từ chối và hiển thị lệnh CLI thay thế.
   - **Không hỗ trợ `--transport derp`**: trình duyệt kết nối qua relay WebSocket, vì vậy hãy dùng `--transport auto` hoặc `relay`.
   - **Không giữ được `mtime` và quyền tệp (`permissions`)**: File System Access API không cho phép đặt `mtime` hay quyền Unix cho tệp.
   - **Nguồn lọc bằng `--git` sẽ tắt thao tác xóa trên trình duyệt**: trình duyệt không đánh giá tệp `.gitignore`, nên mọi lệnh xóa tự động bị vô hiệu hóa khi máy nguồn dùng `--git`.

---

## Ghi nguyên tử, staging `.croc-sync-tmp-*` và tệp khóa `.croc-sync-tmp-lock`

- **Tệp khóa `.croc-sync-tmp-lock`**: ngay khi bắt đầu, máy đích mở và giữ khóa độc quyền trên `.croc-sync-tmp-lock` ở gốc thư mục đích để ngăn hai tiến trình `croc sync` ghi đồng thời vào cùng một thư mục. Tệp khóa này được giữ lại sau khi kết thúc để tránh tình trạng hai tiến trình khóa trên hai inode khác nhau; bộ quét và bộ lọc luôn bỏ qua mọi tên bắt đầu bằng `.croc-sync-tmp-`.
- **Thư mục staging `.croc-sync-tmp-<id>/`**: mọi tệp tải từ máy nguồn được nhận vào một thư mục tạm nằm ngay trong thư mục đích (cùng hệ tệp để đảm bảo đổi tên/liên kết nguyên tử).
- **Commit nguyên tử**: sau khi nhận xong, máy đích kiểm tra lại kích thước và `xxhash64` của bản staged so với manifest nguồn, xác nhận tệp đích chưa bị sửa kể từ lúc quét, rồi đưa vào vị trí thật bằng thao tác nguyên tử (`Link` cho tệp mới hoặc `Rename` cho tệp cập nhật), cuối cùng mới áp quyền (`chmod`) và `mtime`. Mỗi tệp đích luôn ở trạng thái bản cũ hoàn chỉnh hoặc bản mới hoàn chỉnh, không bao giờ bị ghi dở dang.
- **Dọn staging mồ côi**: thư mục staging được xóa khi lượt kết thúc. Nếu lượt trước bị ngắt đột ngột, lần chạy sau sẽ tự động dọn các thư mục `.croc-sync-tmp-*` không còn tiến trình nào khóa.

---

## Mã thoát (Exit codes)

| Mã | Ý nghĩa |
| --- | --- |
| `0` | Thành công hoàn toàn (bao gồm trường hợp đã đồng bộ sẵn, `--dry-run` hoàn tất, hoặc dừng chế độ `--watch` bằng `Ctrl-C`). |
| `1` | Lỗi nghiêm trọng: cờ sai vai trò, thư mục không hợp lệ, lỗi mạng/PAKE/giao thức, đối phương không hỗ trợ `folder-sync-v1`, từ chối xóa do thiếu `--yes` khi không có terminal ở chế độ một lượt, hoặc bị ngắt (`Ctrl-C`) ở chế độ một lượt. |
| `2` | Hoàn tất một phần (chế độ một lượt): có ít nhất một mục lỗi khi commit/xóa, có xung đột, hoặc các lệnh xóa bị giữ lại (chẳng hạn xóa >50% không được xác nhận hoặc bị giữ vì có lỗi commit trước đó). |

---

## Bảo mật

- **Mã hóa đầu-cuối (PAKE)**: mã phiên (`CROC_SECRET`) được dùng để thực hiện thỏa thuận khóa xác thực bằng mật khẩu (PAKE). Mọi thông điệp `sync-*` (manifest, đường dẫn, bộ lọc, yêu cầu/kết quả hash, kế hoạch) và toàn bộ dữ liệu tệp chỉ được gửi sau khi kênh mã hóa đầu-cuối đã thiết lập.
- **Thông tin relay có thể thấy**: tên tính năng `folder-sync-v1` được đàm phán trong thông điệp PAKE trước khi kênh được mã hóa (tương tự `ssh-rendezvous-v1` của `croc ssh`). Vì vậy máy chủ relay có thể phân biệt một phiên `croc sync` với phiên `croc send` thông thường, nhưng **không** nhìn thấy tên tệp, cấu trúc thư mục, bộ lọc, hash hay nội dung dữ liệu.
- **Mã nối lại (`RejoinCode`) trong `--watch`**: mỗi lượt watch sinh một mã nối lại ngẫu nhiên mới và trao đổi hoàn toàn trong kênh đã mã hóa. Khi rớt mạng và kết nối lại, hai phía luôn dùng `RejoinCode` mới nhất và không bao giờ dùng lại mã phiên ban đầu.
- **Máy đích xác thực chặt mọi đường dẫn và tệp nhận**: mọi đường dẫn trong manifest nguồn phải ở dạng đường dẫn tương đối chuẩn (UTF-8 NFC, phân cách `/`, có đủ thư mục cha). Máy đích từ chối đường dẫn thoát thư mục (`..`), đường dẫn tuyệt đối, ký tự điều khiển, tên dành riêng của Windows (`CON`, `NUL`, …), các thành phần nhạy cảm (`.ssh`, `.gnupg`), tiền tố staging (`.croc-sync-tmp-*`), và xung đột phân biệt hoa/thường hoặc NFC/NFD. Mọi thao tác đọc/ghi/xóa đều bị giới hạn trong gốc thư mục đích (`receivefs.Root` / `os.OpenRoot`), không đi theo symlink, và máy đích từ chối mọi tệp không nằm trong danh sách đã yêu cầu (`sync-need`).

---

## Giới hạn hiện tại

- **Chỉ đồng bộ một chiều** (từ máy nguồn sang máy đích); không hỗ trợ đồng bộ hai chiều và gộp xung đột nội dung.
- **Bỏ qua symlink và tệp đặc biệt** (FIFO/pipe, socket, tệp thiết bị): các mục này không được đồng bộ mà chỉ in cảnh báo; phía đích bảo vệ các đường dẫn liên quan khỏi `--delete`.
- **Không resume giữa chừng một lượt bị ngắt**: nếu mất kết nối hoặc bấm `Ctrl-C` giữa lượt, các tệp chưa commit vẫn giữ nguyên trạng thái cũ trên đĩa đích. Chỉ cần chạy lại `croc sync` — những tệp đã commit sẽ được bỏ qua và hai thư mục sẽ hội tụ về trạng thái giống nhau.
- **Giới hạn riêng của Web Sync**: không hỗ trợ `--watch`, `--transport derp`, giữ `mtime`/quyền Unix, và tự động tắt thao tác xóa khi máy nguồn dùng `--git`.

---

## Hiệu năng

Đo trên loopback Windows 11 (Intel Core i7-1165G7, 5 lượt mỗi cấu hình, lấy trung vị; dữ liệu `small` 10,000 tệp / 45.7 MB, `large` 20 × 40 MiB / 838.9 MB, `mixed` 2,005 tệp / ≈1 GiB):

- **Đồng bộ lặp lại**: khi không đổi (`sync-unchanged`), mỗi lượt mất **0.13–0.29 s** và truyền **≤ 1.9 MB** dữ liệu điều khiển; khi 5% tệp đổi (`sync-5pct`), thời gian còn **0.29–12.49 s** và lưu lượng xấp xỉ phần dữ liệu thay đổi (ví dụ `large`: 84.02 MB ≈ một tệp 40 MiB qua relay hai chiều).
- **Lần đầu (`sync-first`)**: chậm hơn `croc send` trên nhiều tệp nhỏ (`small` +22%, `mixed` +61%) do băm `xxhash`, xác minh trước khi đổi tên nguyên tử từ staging và áp quyền/`mtime`; trên `large` trung vị là 2.5 s so với 7.9 s của `send` (`send` dao động 2.2–9.0 s).
- **Bộ nhớ & mục tiêu quét/diff**: RAM đỉnh giữ trong khoảng **24–67 MB** mỗi tiến trình ở mọi kịch bản; đo riêng ở giai đoạn 1 cho quét + diff 100,000 tệp đạt **3.8 s** và **85 MB** peak heap (mục tiêu `< 10 s`, `< 200 MB`).

Xem đầy đủ bảng số liệu, biên độ dao động, giới hạn loopback/Windows và lệnh tái lập tại [`src/docs/FOLDER_SYNC_BENCHMARK.md`](FOLDER_SYNC_BENCHMARK.md) (dữ liệu thô: [`FOLDER_SYNC_BENCHMARK.json`](FOLDER_SYNC_BENCHMARK.json); kiểm thử: [`FOLDER_SYNC_TESTING.md`](FOLDER_SYNC_TESTING.md)).
