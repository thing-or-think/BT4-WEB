# `croc sync`: kết quả Giai đoạn 0

Nhánh `feat/folder-sync`, ngày 26/11/2026. Máy: Windows 11, Go 1.27.4, gcc MSYS2 UCRT64 15.2.

## Tóm tắt

| Việc | Kết quả |
|---|---|
| 0.1 EOL | **Đạt.** Thêm `.gitattributes` (`* text=auto eol=lf`). `git ls-files --eol` không còn `w/crlf` |
| 0.2 Baseline | **Đạt một phần.** Có baseline Go và Vitest trên Windows. Chưa có baseline Linux và Playwright (xem bên dưới) |
| 0.3 Luồng message | **Đạt.** Mục "Luồng message" |
| 0.4 Spike A | **Đạt.** `Options.ReceiveRoot`; hai bên nhận chạy đồng thời trong một tiến trình, CWD không đổi |
| 0.5 Spike B | **Đạt.** `Options.SyncHook`; trao đổi manifest giữa PAKE và `SenderInfo`, chỉ gửi file được chọn |
| 0.6 Spike C | **Đạt, kết luận rõ.** Mở phiên mới cho mỗi lượt watch **không dùng được** với relay mặc định |

Tổng thay đổi lõi `src/croc`: khoảng 190 dòng (gồm `sync_hook.go`), dưới ngưỡng dừng 400 dòng. **Không cần** chuyển sang kênh riêng kiểu `sshshare`.

## 0.1. Xuống dòng

`core.autocrlf=true` (cấu hình hệ thống của Git for Windows) đã đổi 471 tệp text sang CRLF. `.gitattributes` buộc LF trên mọi máy, không phụ thuộc cấu hình người dùng. Nội dung các tệp trong index vốn đã là LF nên commit này không đổi tệp nào khác.

## 0.2. Baseline

Chạy trên một clone sạch của `HEAD` với `core.autocrlf=false`. Tiêu chí cho mọi giai đoạn sau: **không có lỗi mới so với bảng này**.

### Go (`go test ./src/... ./internal/... .`), Windows

| Gói | Test trượt | Nguyên nhân |
|---|---|---|
| `src/cli` | `TestBestPublicRelayCacheUsesAddressAndCurrentPoolOrder` | Windows không có quyền kiểu Unix (666 thay vì 600) |
| `src/cli` | `TestFormatUnixSSHCodeMessage` | Test chỉ đúng trên Unix |
| `src/croc` | `TestCrocSymlink`, `TestReceiveStdinNamedFileCollision/symlink/*` | Thiếu quyền tạo symlink (cần Developer Mode) |
| `src/install` | `TestInstallerInstallsVerifiedPair`, `TestInstallerDeterminesLatestVersion`, `TestInstallerLatestVersionFailures` | Không có `/bin/bash`; thiếu quyền symlink |
| `src/install` | `TestStampVersion` | Quyền kiểu Unix |
| `src/tcp` | `TestWrongPassword` | Windows trả RST (`wsarecv`) trước khi đọc được thông báo lỗi; có thể chập chờn |
| `src/utils` | `TestUnzipThroughOpenedRootResistsDestinationReplacement` | Windows không cho đổi tên thư mục đang mở |

`TestLinux32BitTargets…` từng trượt do CRLF, nay đã qua.

### Vitest (`npx vitest run`), Windows

53/326 trượt (trước khi sửa EOL: 56).

| Tệp | Trượt | Nguyên nhân |
|---|---|---|
| `src/protocol/native-interop.test.ts` | 49/52 | Oracle Go được build thành `oracle` không có đuôi `.exe`, nên trên Windows không chạy được; mỗi test hết thời gian 5 s |
| `src/TaskLauncher.test.tsx` | 3 | Không tìm thấy nút "Send live"; chưa rõ nguyên nhân |
| `src/SSHPanel.test.tsx` | 1 | jsdom: "navigation to another Document" |

### Race detector

Chạy được khi gcc của MSYS2 đứng đầu `PATH`:

```powershell
$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"; go test -race ./src/foldersync ./src/croc
```

Nếu không, `cgo` dùng một `gcc` khác và báo `exit status 2`.

### Chưa làm

- **Linux:** máy không có bản phân phối WSL (chỉ có `docker-desktop`), Docker đang tắt. Cần cài WSL Ubuntu hoặc dùng CI.
- **Playwright:** chưa cài trình duyệt (`%LOCALAPPDATA%\ms-playwright` không tồn tại). Cần `npx playwright install`, tức tải trình duyệt về.

## 0.3. Luồng message

Mọi message sau PAKE đều được mã hóa; `processMessage` từ chối message không mã hóa ngoài `pake` và `pake-confirm`.

```text
BÊN GỬI (IsSender)                                   BÊN NHẬN
Send(files): transferBytes = tổng size  ─┐
             hash/chuẩn bị file (goroutine)          Receive(): OpenRoot(ReceiveRoot)
                               ◄──────── pake {features}          (không mã hóa)
pake {features, salt} ─────────►
                               ◄──────── pake-confirm
pake-confirm ──────────────────►
activateSecureChannel: chọn transport (auto|derp|relay)  ← dùng transferBytes
[externalip hoặc inline metadata]  ─────►  ChannelSecured = true ở cả hai bên
updateIfSenderChannelSecured:
  chờ file chuẩn bị xong
  fileinfo  hoặc  fileinfo-start/batch/end ──►  processSenderInfo
                                              validateReceiveMetadata, hỏi Accept
                                              tạo thư mục rỗng
                               ◄──────── bundle-request (file nhỏ)  hoặc
                               ◄──────── recipientready {chunk ranges}
dữ liệu (relay data / Tailcat) ─────────►    ghi vào file đích (O_RDWR + Truncate)
… lặp từng file …
                               ◄──────── finished
finished ──────────────────────►         transfer() kết thúc; phiên đóng
```

Hàm chính: `transfer` (vòng điều khiển), `processMessage` (bảng định tuyến), `processMessagePake`/`processMessagePakeConfirm`, `updateState` → `updateIfSenderChannelSecured` → `sendSenderInfo` (`file_manifest.go`), `processSenderInfo`, `updateIfRecipientHasFileInfo` → `preflightReceiveFile` (`receive_preflight.go`) → `recipientInitializeFile`.

Lưu ý: `processMessage` **bỏ qua im lặng** message có type lạ (không có nhánh `default`). Một client cũ nhận `sync-*` sẽ treo, không báo lỗi. Vì vậy bắt buộc phải đàm phán feature trước.

## 0.4. Spike A: `Options.ReceiveRoot`

**Thay đổi:** `src/croc/croc.go`, `src/croc/receive_preflight.go`.

- `receiveFilesystem` mở `ReceiveRoot` (rỗng thì vẫn là `"."`).
- Các chỗ bên nhận còn dùng đường dẫn theo CWD được đi qua `receiveDiskPath`: `isEmptyFolder`, hai lời gọi `MissingChunks`, hash file đích có sẵn và exact hash trong preflight.

**Test:** `TestReceiveRootIsolatesConcurrentReceivers` (`receive_root_test.go`). Hai cặp gửi/nhận chạy đồng thời, mỗi bên nhận một `ReceiveRoot`. Dữ liệu gồm một file 3 MiB (pipeline chunk) và 24 file nhỏ (pipeline bundle). Kiểm tra nội dung từng file và thư mục làm việc không đổi.

**Kiểm chứng ngược:** bỏ phần sửa thì test trượt (`bundle destination appeared after preflight`), nghĩa là hai bên nhận giẫm lên nhau trong CWD. Qua 10 lần chạy lặp và qua race detector.

**Còn lại cho 2B.1:** hash file đích vẫn mở theo đường dẫn chứ không qua `receivefs.Root` (như trước đây). Cần bổ sung `Root.Chmod` và đọc thư mục.

## 0.5. Spike B: `Options.SyncHook`

**Thay đổi:** `src/croc/sync_hook.go` (mới) và vài điểm nối trong `croc.go`, `tailcat_negotiation.go`.

```go
type SyncHook interface {
    Feature() string                                     // quảng bá khi PAKE
    Start(send func(message.Message) error) error        // khi kênh đã mã hóa
    Handle(m message.Message, send func(message.Message) error) error // type "sync-*"
    Ready() (ready bool, keep func(FileInfo) bool)       // bên gửi: chọn file
}
```

- Chỉ quảng bá feature khi có hook, nên `croc send` và nhận thường không đổi.
- `updateState` gọi `updateSyncHook` đầu tiên. Thiếu feature ở peer thì trả `ErrPeerLacksSync` ngay khi kênh bảo mật.
- Bên gửi có hook thì **hoãn** `sendCollectFiles` cho tới khi `Ready()`. Chỉ file được giữ mới bị hash và gửi.
- `transferBytes` vẫn đặt từ toàn bộ danh sách lúc gọi `Send`, nên transport auto chọn theo **cận trên**.
- `croc` không import `foldersync`; hướng phụ thuộc đúng như kế hoạch.

**Test** (`sync_hook_test.go`, dùng hook mẫu so theo tên):

| Test | Kiểm tra |
|---|---|
| `TestSyncHookSendsOnlyRequestedFiles` | Chỉ gửi file đích thiếu; file đã có không bị ghi đè; `transferBytes` = cận trên; thứ tự `sync-manifest` → `sync-need` |
| `TestSyncHookNothingToSend` | Đích đã đủ: phiên kết thúc thành công, 0 file được chuẩn bị |
| `TestSyncHookRequiresPeerFeature` | Bên gửi có hook / bên nhận không, và ngược lại: lỗi rõ ràng, không treo, không ghi file |

**Phát hiện cho Giai đoạn 2:**

1. Feature trong PAKE gửi **không mã hóa**, nên relay biết phiên là `croc sync` (giống `ssh-rendezvous-v1`). Ghi vào tài liệu bảo mật.
2. Khi chọn transport, chưa biết cần truyền bao nhiêu. Dùng cận trên có thể bật Tailcat cho một lượt chỉ đổi vài byte. Có thể chấp nhận cho MVP.
3. Hook chưa xử lý kết nối lại (`transferWithReconnect`): trạng thái `sync` không được đặt lại. Phải thiết kế ở 2A.4.
4. Thư mục rỗng cũng đi qua `keep`. Lời nhắc "already has some content" ở `processSenderInfo` vẫn hiện ra kể cả khi `NoPrompt`; sync phải lọc thư mục rỗng đã có.
5. `Send` vẫn in hướng dẫn `croc <mã>` và liên kết Web; sync phải thay (đã ghi ở kế hoạch).

## 0.6. Spike C: mở lại phiên hay giữ kênh

**Test:** `TestSpikeSessionReopenLatency` (`sync_spike_latency_test.go`), chỉ chạy khi đặt `CROC_SPIKE=1`. Relay cục bộ qua loopback, nên đây là **cận dưới**.

| Cách | p50 | p95 | Ghi chú |
|---|---|---|---|
| Phiên mới mỗi lượt (gửi 1 file nhỏ) | 75 ms | 90 ms | **Relay từ chối từ phiên thứ 16** (`relay admission rate limited`) |
| Một vòng `sync-ping`/`sync-pong` trên phiên đang mở | 0,6 ms | 1,2 ms | 20/20 |

Mỗi phiên tốn nhiều lượt vào relay (kết nối điều khiển và các kết nối dữ liệu của cả hai bên), trong khi relay giới hạn số lượt vào mỗi phút theo nguồn. Với relay công cộng, mỗi phiên còn thêm vài RTT mạng cho PAKE.

**Quyết định cho Giai đoạn 4:** `--watch` **giữ một phiên sống lâu** và gửi các lượt bằng message `sync-round` trên kênh điều khiển. Chỉ mở phiên mới khi kết nối lại, có backoff và dùng phòng ngẫu nhiên kiểu `NextReconnectRoom`. Mục tiêu p95 < 2 s trên LAN giữ nguyên. Việc này cần một vòng lặp sau `finished` thay vì kết thúc `transfer()`; ước lượng 4.3 có thể tăng.

## Tệp thay đổi

| Tệp | Nội dung |
|---|---|
| `.gitattributes` | Mới: LF cho mọi tệp text |
| `src/croc/croc.go` | `Options.ReceiveRoot`, `Options.SyncHook`, `receiveDiskPath`, định tuyến `sync-*`, hoãn chuẩn bị file |
| `src/croc/receive_preflight.go` | Hash và hỏi ghi đè theo `ReceiveRoot` |
| `src/croc/tailcat_negotiation.go` | Quảng bá feature của hook |
| `src/croc/sync_hook.go` | Mới: interface và trạng thái hook |
| `src/croc/receive_root_test.go`, `sync_hook_test.go`, `sync_spike_latency_test.go` | Mới: test cho Spike A, B, C |
