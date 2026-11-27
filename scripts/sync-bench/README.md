# sync-bench

Local folder transfer and `croc sync` benchmark tool comparing `croc send` against `croc sync` (initial sync, unchanged re-sync, and 5% incremental update) over a local `croc relay` fronted by a byte-counting TCP proxy.

## Usage

```sh
go run ./scripts/sync-bench \
  -croc /path/to/croc \
  -work /tmp/sync-bench-work \
  -runs 5 \
  -datasets small,large,mixed \
  -out results.json
```

For a fast smoke run, use `-scale` (default `1.0`) to scale dataset counts and sizes:

```sh
go run ./scripts/sync-bench -croc ./croc -work ./bench-work -runs 2 -scale 0.01 -out smoke.json
```

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-croc` | *(required)* | Path to the `croc` executable to benchmark |
| `-work` | `<temp>/croc-sync-bench` | Directory for cached datasets and temporary run folders |
| `-runs` | `5` | Number of measured runs per dataset × mode |
| `-datasets` | `small,large,mixed` | Comma-separated datasets to run |
| `-modes` | `send,sync-first,sync-unchanged,sync-5pct` | Comma-separated modes to run |
| `-scale` | `1.0` | Multiplier for dataset counts/sizes |
| `-out` | `<work>/results.json` | Output JSON report path |
| `-timeout` | `20m` | Timeout per individual run |

## Datasets

Generated once deterministically under `<work>/datasets/` from a fixed seed and reused on subsequent runs with the same `-scale`:

- `small`: 10,000 files of 1–8 KiB across 100 folders (~45 MB).
- `large`: 20 files of 40 MiB (~839 MB).
- `mixed`: ~2,005 files from 1 KiB to 64 MiB in nested folders (~1 GiB).

## Measurements

- **Wall time**: Receiver launch until both source and destination processes exit.
- **Network bytes**: Total bytes forwarded in both directions across all proxied relay ports (control + 4 data ports). The relay binds `127.0.0.2` (falling back to `::1` or distinct ports on `127.0.0.1` if unavailable) with counting proxies on `127.0.0.1`.
- **Peak RAM**: `PeakWorkingSetSize` via `GetProcessMemoryInfo` on Windows; `Rusage.Maxrss` on Linux (KiB → bytes) and macOS (bytes).
- **Units**: JSON stores exact bytes and seconds (median, min, max, and per-trial values); the printed Markdown table reports decimal MB (`10^6` bytes).
