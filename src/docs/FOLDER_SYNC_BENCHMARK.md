# Benchmark đồng bộ thư mục (`croc sync`) — 27/11/2026

Tài liệu này ghi nhận kết quả so sánh thực tế giữa `croc send` và `croc sync` (đồng bộ lần đầu, đồng bộ lặp lại không đổi và cập nhật 5% tệp) qua relay cục bộ. Dữ liệu thô theo từng lượt đo cùng giá trị trung vị, nhỏ nhất và lớn nhất được lưu trong [FOLDER_SYNC_BENCHMARK.json](FOLDER_SYNC_BENCHMARK.json).

## Môi trường và phương pháp đo

- **Thời điểm và công cụ đo**: đo ngày `2026-11-27` (`2026-11-27T07:31:19Z`) bằng [`scripts/sync-bench`](../../scripts/sync-bench/README.md).
- **Máy đo và bản build**: Windows 11 (`windows/amd64`), Intel Core i7-1165G7 (4 nhân / 8 luồng), Go `1.27.4`, `croc` biên dịch từ nhánh `feat/folder-sync`.
- **Cấu hình mạng**: `croc relay` chạy trên `127.0.0.2` đứng sau TCP proxy đếm byte trên `127.0.0.1` (proxy toàn bộ cổng điều khiển và dữ liệu). Đợt đo chỉ chạy trên loopback nên đo chi phí CPU, đĩa và giao thức, không đo độ trễ hay băng thông WAN.
- **Số lượt và chỉ số**: 5 lượt đo cho mỗi tổ hợp dữ liệu × chế độ (`scale = 1`); bảng dưới báo cáo **trung vị** (giá trị min/max và từng lượt nằm trong [FOLDER_SYNC_BENCHMARK.json](FOLDER_SYNC_BENCHMARK.json)).
- **Cách tính chỉ số**:
  - **Thời gian (s)**: thời gian đồng hồ thực từ lúc khởi chạy phía nhận cho đến khi cả hai tiến trình nguồn và đích kết thúc.
  - **Mạng (MB)**: tổng số byte đi qua relay theo **cả hai chiều** (nguồn → relay và relay → đích, xấp xỉ `2×` dung lượng payload), tính theo MB thập phân (`10^6` byte).
  - **RAM nguồn / RAM đích (MB)**: bộ nhớ làm việc đỉnh (`PeakWorkingSetSize` qua `GetProcessMemoryInfo` trên Windows) của từng tiến trình, tính theo MB thập phân (`10^6` byte).

## Tập dữ liệu và chế độ đo

Tập dữ liệu được sinh cố định theo hạt giống bởi `scripts/sync-bench`:

| Tập dữ liệu | Số tệp | Số thư mục | Đặc tả kích thước | Tổng dung lượng |
| --- | ---: | ---: | --- | ---: |
| `small` | 10,000 | 100 | 1–8 KiB mỗi tệp | `45,657,615` byte (45.7 MB) |
| `large` | 20 | 0 | 40 MiB mỗi tệp | `838,860,800` byte (838.9 MB) |
| `mixed` | 2,005 | 260 | 1 KiB–64 MiB trong các thư mục lồng nhau | `1,051,397,273` byte (≈ 1 GiB) |

Các chế độ đo:

- `send`: chạy `croc send` truyền toàn bộ thư mục vào một thư mục đích trống mới.
- `sync-first`: chạy `croc sync` lần đầu vào thư mục đích rỗng.
- `sync-unchanged`: chạy `croc sync` khi thư mục đích đã giống hệt nguồn.
- `sync-5pct`: chạy `croc sync` sau khi ghi lại 5% số tệp với cùng kích thước nhưng `mtime` mới hơn.

## Kết quả đo (trung vị 5 lượt)

| Tập dữ liệu | Chế độ | Thời gian (s) | Mạng (MB) | RAM nguồn (MB) | RAM đích (MB) |
| --- | --- | ---: | ---: | ---: | ---: |
| `small` | `send` | 101.946 | 93.97 | 57.91 | 52.76 |
| `small` | `sync-first` | 124.801 | 97.57 | 58.68 | 66.56 |
| `small` | `sync-unchanged` | 0.292 | 1.89 | 40.71 | 36.91 |
| `small` | `sync-5pct` | 12.493 | 6.70 | 46.54 | 44.28 |
| `large` | `send` | 7.895 | 1680.01 | 26.60 | 28.22 |
| `large` | `sync-first` | 2.477 | 1680.02 | 27.82 | 33.91 |
| `large` | `sync-unchanged` | 0.133 | 0.02 | 24.31 | 26.82 |
| `large` | `sync-5pct` | 0.293 | 84.02 | 26.60 | 28.47 |
| `mixed` | `send` | 23.952 | 2106.20 | 34.46 | 36.03 |
| `mixed` | `sync-first` | 38.675 | 2107.02 | 34.93 | 40.86 |
| `mixed` | `sync-unchanged` | 0.196 | 0.45 | 29.73 | 28.88 |
| `mixed` | `sync-5pct` | 2.266 | 61.61 | 32.85 | 34.97 |

Biên độ dao động (min–max) đáng chú ý trong dữ liệu từng lượt:

- `croc send` có độ biến thiên thời gian cao trên máy đo này: `small/send` dao động `77.1–198.8 s`, `large/send` dao động `2.17–8.96 s`, và `mixed/send` dao động `22.9–55.7 s` (trong khi `sync-first` lần lượt nằm trong `124.0–125.2 s`, `2.45–3.02 s` và `38.3–39.2 s`).
- `mixed/sync-5pct` có lưu lượng mạng dao động `19.6–136.9 MB` vì tập 5% tệp được ghi lại ở mỗi lượt gồm các tệp khác nhau với kích thước khác nhau.

## Nhận xét

- **Giá trị chính nằm ở các lượt đồng bộ lặp lại**: các lượt không có thay đổi (`sync-unchanged`) hoàn tất trong `0.13–0.29 s` và chỉ truyền `≤ 1.9 MB` lưu lượng điều khiển (`0.02 MB` trên `large`, `0.45 MB` trên `mixed`, `1.89 MB` trên `small`). Khi 5% tệp thay đổi (`sync-5pct`), lượng byte đi qua mạng xấp xỉ phần dữ liệu thay đổi (ví dụ trên `large`: `84.02 MB` xấp xỉ một tệp 40 MiB được đếm qua relay theo cả hai chiều, hoàn tất trong `0.293 s`).
- **Chi phí đồng bộ lần đầu (`sync-first`) so với `croc send`**: trên các tập nhiều tệp nhỏ, `sync-first` tốn thời gian hơn `croc send` (`small` +22%, `mixed` +61%) vì máy nguồn băm toàn bộ tệp truyền bằng `xxhash` để xác minh đầu-cuối, máy đích kiểm tra lại từng tệp trước khi đổi tên nguyên tử từ thư mục staging, đồng thời tạo thư mục và áp lại quyền/`mtime`. Trên tập ít tệp lớn (`large`), `sync-first` nhanh hơn `send` trong đợt đo này (`2.5 s` so với `7.9 s` trung vị), nhưng do `send` dao động mạnh trong khoảng `2.2–9.0 s`, không nên kết luận rằng `croc sync` nói chung nhanh hơn `croc send` ở lần đồng bộ đầu tiên.
- **Bộ nhớ được giữ trong giới hạn thấp**: RAM đỉnh (working set) của từng tiến trình nằm trong khoảng `24–67 MB` (trung vị) ở mọi tập dữ liệu và chế độ.
- **Mục tiêu quét và diff quy mô lớn**: mục tiêu kế hoạch cho 100,000 tệp (`< 10 s`, `< 200 MB`) đã được đo riêng ở giai đoạn 1 với kết quả `3.8 s` và `85 MB` peak heap.

## Giới hạn của đợt đo

- Chỉ đo qua loopback cục bộ (`127.0.0.1` / `127.0.0.2`), chưa đo hiệu năng qua mạng WAN.
- Chỉ thực hiện trên một máy chạy Windows 11 (`windows/amd64`); chưa có số liệu đo trên Linux hay hệ điều hành khác trong báo cáo này.

## Tái lập kết quả

Biên dịch `croc` từ nhánh hiện tại và chạy công cụ `scripts/sync-bench` (xem chi tiết các cờ trong [`scripts/sync-bench/README.md`](../../scripts/sync-bench/README.md)):

```sh
go build -o croc .
go run ./scripts/sync-bench \
  -croc ./croc \
  -work /tmp/sync-bench-work \
  -runs 5 \
  -datasets small,large,mixed \
  -out src/docs/FOLDER_SYNC_BENCHMARK.json
```

Trên Windows PowerShell, thay `./croc` bằng `.\croc.exe` và đường dẫn `-work` phù hợp (ví dụ `$env:TEMP\sync-bench-work`).
