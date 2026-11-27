# Kiểm thử tính năng đồng bộ thư mục (`croc sync`)

Tài liệu này hướng dẫn cách chạy toàn bộ các tầng kiểm thử của `croc sync` (Go, race detector, fuzzing, vector dùng chung Go/TS, Vitest, Playwright E2E, kiểm thử ngân sách 100k tệp, độ trễ `--watch`, công cụ benchmark, chạy Linux qua Docker) cùng danh sách job CI, kết quả tương thích và ghi chú hồi quy.

---

## 1. Kiểm thử đơn vị và tích hợp Go

Chạy toàn bộ kiểm thử `foldersync` và các bài kiểm thử liên quan trong `croc` và `cli`:

```bash
go test ./src/foldersync
go test ./src/croc -run 'Sync|ReceiveRoot'
go test ./src/cli -run Sync
```

Trên CI, gói nền đường dẫn và khung bản tin cũng được chạy cùng:

```bash
go test ./src/foldersync ./src/message ./src/receivefs
```

Kiểm thử bản dựng không có Tailcat (relay-only):

```bash
CGO_ENABLED=0 go test -tags croc_no_tailcat ./src/foldersync
```

---

## 2. Race detector (`-race`)

**Linux / macOS:**

```bash
go test -race ./src/foldersync
go test -race ./src/croc -run 'Sync|ReceiveRoot'
```

**Windows (PowerShell, yêu cầu `gcc` của MSYS2 UCRT64 đứng đầu `PATH`):**

```powershell
$env:PATH = "C:\msys64\ucrt64\bin;$env:PATH"
go test -race ./src/foldersync
go test -race ./src/croc -run 'Sync|ReceiveRoot'
```

---

## 3. Fuzz smoke tests (5 fuzzer)

Gói `src/foldersync` có 5 fuzzer kiểm tra chuẩn hóa đường dẫn, bộ lọc, thuật toán diff, xác thực manifest và giải mã wire:

1. `FuzzNormalizeWirePathFromOS` (`src/foldersync/fuzz_test.go`)
2. `FuzzFilterMatch` (`src/foldersync/fuzz_test.go`)
3. `FuzzCompare` (`src/foldersync/fuzz_test.go`)
4. `FuzzValidateManifest` (`src/foldersync/manifest_test.go`)
5. `FuzzWireDecoders` (`src/foldersync/wire_test.go`)

Chạy smoke 10 giây cho từng fuzzer (giống job `security-fuzz-smoke` trên CI):

```bash
go test ./src/foldersync -run '^$' -fuzz '^FuzzNormalizeWirePathFromOS$' -fuzztime 10s
go test ./src/foldersync -run '^$' -fuzz '^FuzzFilterMatch$' -fuzztime 10s
go test ./src/foldersync -run '^$' -fuzz '^FuzzCompare$' -fuzztime 10s
go test ./src/foldersync -run '^$' -fuzz '^FuzzValidateManifest$' -fuzztime 10s
go test ./src/foldersync -run '^$' -fuzz '^FuzzWireDecoders$' -fuzztime 10s
```

*(Lưu ý: khi chạy `go test ./src/foldersync` thông thường, Go tự động chạy toàn bộ seed corpus của cả 5 fuzzer.)*

---

## 4. Bộ vector diff dùng chung Go / TypeScript

Bộ vector kiểm chứng chéo thuật toán diff giữa Go và TypeScript nằm tại `web/testdata/folder-sync-vectors.json` (được định nghĩa trong `src/foldersync/vectors_test.go` và kiểm tra ở phía TypeScript trong `web/src/protocol/sync-diff.test.ts`).

Chạy kiểm chứng cả hai phía:

```bash
go test ./src/foldersync -run '^TestSharedDiffVectors$'
npm --prefix web test -- --run src/protocol/sync-diff.test.ts
```

Khi cập nhật bảng ca kiểm thử trong `src/foldersync/vectors_test.go`, tái tạo tệp JSON bằng biến môi trường `FOLDERSYNC_WRITE_VECTORS=1`:

**Linux / macOS:**

```bash
FOLDERSYNC_WRITE_VECTORS=1 go test ./src/foldersync -run '^TestSharedDiffVectors$'
```

**Windows PowerShell:**

```powershell
$env:FOLDERSYNC_WRITE_VECTORS='1'; go test ./src/foldersync -run '^TestSharedDiffVectors$'
```

---

## 5. Vitest (Web Sync unit & UI tests)

Các tệp kiểm thử Vitest cho Web Sync:

- `web/src/protocol/sync-diff.test.ts` — vector dùng chung Go/TS và logic diff phía trình duyệt
- `web/src/protocol/sync-wire.test.ts` — mã hóa/giải mã thông điệp `sync-*` và giới hạn kích thước
- `web/src/protocol/sync-fs.test.ts` — quét đích, `SyncFileSink`, commit, xóa và chính sách `--git` trên web
- `web/src/protocol/sync-hash-cache.test.ts` — bộ nhớ đệm hash cục bộ (IndexedDB / memory store)
- `web/src/protocol/sync.test.ts` — state machine `SyncDestSession` và `syncReceive`
- `web/src/SyncPanel.test.tsx` — giao diện React **Sync folder**, luồng xác nhận xóa, thu hồi quyền, báo lỗi `--watch` và trình duyệt không hỗ trợ
- `web/src/protocol/native-interop.test.ts` — tương thích nguyên thủy mật mã/hash giữa Go và TypeScript

Chạy toàn bộ các tệp liên quan bằng một lệnh (khớp job `folder-sync` trên CI):

```bash
npm --prefix web test -- --run src/protocol/sync src/SyncPanel.test.tsx src/protocol/native-interop.test.ts
```

---

## 6. Playwright E2E (`web/e2e/sync.spec.ts`)

Bộ E2E kiểm thử đầy đủ CLI nguồn `croc sync` ↔ Web Sync (OPFS test storage và `showDirectoryPicker` giả lập, lượt thứ hai có thêm/sửa/xóa, `--dry-run`, lượt rỗng `0 B`, từ chối `--watch`, và thông báo trên Firefox/WebKit).

**Linux / macOS:**

```bash
# Cài trình duyệt lần đầu nếu chưa có
npx --prefix web playwright install --with-deps chromium firefox webkit

# Chạy toàn bộ kịch bản web sync
npm --prefix web run test:e2e -- --grep 'web sync'
```

**Windows (PowerShell, tận dụng Chrome đã cài sẵn trên máy):**

```powershell
$env:CROC_E2E_BINARY_NAME='croc-e2e.exe'
$env:CROC_E2E_CHANNEL='chrome'
npm --prefix web run test:e2e -- --project=chromium --grep 'web sync'
```

*(Đặt `CROC_E2E_BINARY_NAME=croc-e2e.exe` để binary tạm trên Windows có đuôi `.exe`, và đặt `CROC_E2E_CHANNEL=chrome` để Playwright chạy trực tiếp trình duyệt Chrome đã cài trên hệ thống.)*

---

## 7. Kiểm thử ngân sách 100.000 tệp (`FOLDERSYNC_BENCH_100K=1`)

`TestScanDiff100kBudget` (`src/foldersync/bench_test.go`) tạo cây 100.000 tệp cho cả nguồn và đích rồi kiểm tra mục tiêu quét + diff `< 10 s` và đỉnh heap `< 200 MiB`:

**Linux / macOS:**

```bash
FOLDERSYNC_BENCH_100K=1 go test -v ./src/foldersync -run '^TestScanDiff100kBudget$'
```

**Windows PowerShell:**

```powershell
$env:FOLDERSYNC_BENCH_100K='1'; go test -v ./src/foldersync -run '^TestScanDiff100kBudget$'
```

Chạy thêm micro-benchmark `BenchmarkCompare100k` và `BenchmarkScanDir10k`:

```bash
go test ./src/foldersync -run '^$' -bench 'Benchmark(Compare100k|ScanDir10k)' -benchmem
```

---

## 8. Kiểm thử độ trễ chế độ watch (`FOLDERSYNC_LATENCY=1`)

`TestWatchLatency` (`src/foldersync/watch_test.go`) thực hiện 20 lần chỉnh sửa cách nhau 1 giây với cấu hình debounce mặc định (`Quiet=400 ms`, `MaxWait=2 s`) trên relay cục bộ và kiểm tra phân vị `p95 < 2 s`:

**Linux / macOS:**

```bash
FOLDERSYNC_LATENCY=1 go test -v ./src/foldersync -run '^TestWatchLatency$'
```

**Windows PowerShell:**

```powershell
$env:FOLDERSYNC_LATENCY='1'; go test -v ./src/foldersync -run '^TestWatchLatency$'
```

---

## 9. Công cụ benchmark `scripts/sync-bench`

Công cụ `scripts/sync-bench` tạo dữ liệu xác định (`small`, `large`, `mixed`), chạy relay cục bộ đi qua TCP proxy đếm byte mạng, đo thời gian thực thi, tổng byte mạng và đỉnh RAM của nguồn/đích qua 4 chế độ (`send`, `sync-first`, `sync-unchanged`, `sync-5pct`), đồng thời xác minh hash cây đích.

```bash
# Biên dịch croc trước
go build -o ./bin/croc .

# Chạy đầy đủ (5 lượt mỗi kịch bản, scale=1.0)
go run ./scripts/sync-bench \
  -croc ./bin/croc \
  -work /tmp/croc-sync-bench \
  -runs 5 \
  -datasets small,large,mixed \
  -modes send,sync-first,sync-unchanged,sync-5pct \
  -scale 1.0 \
  -out /tmp/croc-sync-bench/results.json
```

Để chạy nhanh kiểm tra đường ống (smoke), hạ `-scale` và `-runs` (ví dụ `-scale 0.05 -runs 1`). Số liệu thực đo được ghi lại trong [`FOLDER_SYNC_BENCHMARK.md`](FOLDER_SYNC_BENCHMARK.md).

---

## 10. Chạy kiểm thử Linux qua Docker (từ máy Windows / không chạy Linux)

Để chạy kiểm thử trên Linux khi máy chủ là Windows, biên dịch chéo binary kiểm thử bằng `GOOS=linux` rồi chạy trong container `ubuntu:24.04` dưới **người dùng không phải root** (nhiều bài test về quyền Unix, `chmod` và thư mục không đọc được chỉ hoạt động đúng khi chạy non-root):

**Windows PowerShell:**

```powershell
mkdir -Force .\tmp-linux-tests
$env:CGO_ENABLED='0'; $env:GOOS='linux'; $env:GOARCH='amd64'
go test -c -o .\tmp-linux-tests\foldersync.test ./src/foldersync
go test -c -o .\tmp-linux-tests\croc.test ./src/croc
go test -c -o .\tmp-linux-tests\cli.test ./src/cli

docker run --rm --user 1000:1000 -v "${PWD}\tmp-linux-tests:/tests:ro" -e HOME=/tmp ubuntu:24.04 `
  /bin/sh -c '/tests/foldersync.test && /tests/croc.test -test.run "Sync|ReceiveRoot" && /tests/cli.test -test.run Sync'
```

Có thể biên dịch chéo tương tự cho các gói khác (`./src/sshshare`, `./src/store`, `./src/storeclient`, `./src/storecrypto`, `./src/webcli`, `./src/webrelay`, …) để chạy hồi quy đầy đủ trên Linux.

---

## 11. Các job CI (`.github/workflows/ci.yml`)

| Job | Môi trường | Phạm vi liên quan đến `croc sync` |
| --- | --- | --- |
| `folder-sync` | `ubuntu-24.04`, `windows-latest`, `macos-latest` | `go test ./src/foldersync ./src/message ./src/receivefs`, `go test ./src/croc -run 'Sync\|ReceiveRoot'`, `go test ./src/cli -run 'Sync'`; Vitest `src/protocol/sync src/SyncPanel.test.tsx src/protocol/native-interop.test.ts` |
| `folder-sync-race` | `ubuntu-24.04` | `go test -race ./src/foldersync` và `go test -race ./src/croc -run 'Sync\|ReceiveRoot'` |
| `folder-sync-e2e` | `ubuntu-24.04` | Playwright (`chromium`, `firefox`, `webkit`) chạy `npm --prefix web run test:e2e -- --grep 'web sync'`; tải lên artifact `folder-sync-e2e-report` khi trượt |
| `security-fuzz-smoke` | `ubuntu-24.04` (`workflow_dispatch`) | Chạy 10 giây cho cả 5 fuzzer của `src/foldersync` |
| `unit-tests` | `ubuntu-24.04` | Toàn bộ `go test -v ./...`, `CGO_ENABLED=0 go test -tags croc_no_tailcat ./src/foldersync`, và build/test `croc-web` |

---

## 12. Ma trận tương thích phiên bản cũ / mới

Đo ngày **2026-11-27** trên relay cục bộ (`cũ` = nhánh `main` trước khi thêm `croc sync`, `mới` = nhánh hiện tại):

| Kịch bản | Kết quả |
| --- | --- |
| croc mới send → croc cũ nhận | Đạt, tệp khớp |
| croc cũ send → croc mới nhận | Đạt, tệp khớp |
| croc mới send → croc mới nhận | Đạt, tệp khớp |
| nguồn `croc sync` mới → croc cũ chạy `croc sync` (bản cũ hiểu như một lần nhận thường) | Nguồn báo "peer does not support folder sync (folder-sync-v1)", cả hai thoát mã 1, không ghi tệp nào, không treo |
| croc cũ send → đích `croc sync` mới | Đích báo không hỗ trợ folder sync, thoát mã 1, không ghi tệp nào |

---

## 13. Ghi chú hồi quy (Linux & Windows)

- **Linux (`ubuntu:24.04` qua Docker)**: toàn bộ các gói `sshshare`, `store`, `storeclient`, `storecrypto`, `webcli`, `webrelay`, `croc`, `cli`, `foldersync` đều **đạt, không có lỗi nào**.
- **Windows**: không phát sinh lỗi mới; các lỗi duy nhất khi chạy toàn bộ suite là các lỗi môi trường có sẵn từ trước (baseline) đã liệt kê trong [`FOLDER_SYNC_PHASE0.md`](FOLDER_SYNC_PHASE0.md).
