import { describe, expect, it } from "vitest";
import { bytesEqual, textDecoder, textEncoder } from "./bytes";
import { MAX_MANIFEST_ENTRIES, SyncKind, type SyncManifest } from "./sync-diff";
import {
  HashListAssembler,
  ListAssembler,
  MAX_SYNC_BATCH_BYTES,
  ManifestAssembler,
  PathListAssembler,
  SYNC_PROTOCOL_VERSION,
  SyncProtocolError,
  decodeHashList,
  decodeHello,
  decodeManifestBatch,
  decodeManifestEnd,
  decodeManifestStart,
  decodePathList,
  decodeResult,
  encodeHashList,
  encodeHashLists,
  encodeHello,
  encodeManifest,
  encodeManifestBatch,
  encodeManifestEnd,
  encodeManifestStart,
  encodePathList,
  encodePathLists,
  encodeResult,
  encodeSyncPayload,
  isPartialSyncResult,
  type SyncPathHash,
  type SyncWireMessage,
} from "./sync-wire";

function bigManifest(n: number): SyncManifest {
  const manifest: SyncManifest = {
    complete: true,
    filter: { exclude: [".log"], git: true },
    entries: [
      { path: "d", size: 0, modTime: "", mode: 0o755, kind: SyncKind.Directory },
    ],
    skipped: [
      { path: "d/link", reason: "symlink" },
      { path: "", diskPath: "secret/CON", reason: "invalid_name", detail: "CON" },
    ],
  };
  for (let i = 0; i < n; i += 1) {
    manifest.entries.push({
      path: `d/file-${String(i).padStart(5, "0")}.txt`,
      size: i,
      modTime: "2026-11-01T12:00:00Z",
      mode: 0o644,
      kind: SyncKind.File,
      hash: new Uint8Array([1, 2, 3, 4, 5, 6, 7, i & 0xff]),
    });
  }
  return manifest;
}

function assemble(
  msgs: SyncWireMessage[],
  maxJsonBytes?: number,
): SyncManifest {
  const asm = new ManifestAssembler(maxJsonBytes);
  let result: SyncManifest | undefined;
  for (const msg of msgs) {
    if (msg.t === "sync-manifest-start") {
      asm.start(msg);
    } else if (msg.t === "sync-manifest-batch") {
      asm.batch(msg);
    } else if (msg.t === "sync-manifest-end") {
      result = asm.end(msg);
    } else {
      throw new Error(`unexpected ${msg.t}`);
    }
  }
  if (!result) {
    throw new Error("no end");
  }
  return result;
}

describe("sync-wire round trips", () => {
  it("round-trips Hello with optional fields", () => {
    const basic = encodeHello({
      version: SYNC_PROTOCOL_VERSION,
      role: "destination",
    });
    expect(decodeHello(basic)).toEqual({
      version: 1,
      role: "destination",
    });

    const full = encodeHello({
      version: 1,
      role: "source",
      round: 3,
      watch: true,
      rejoinCode: "rejoin-123",
    });
    expect(decodeHello(full)).toEqual({
      version: 1,
      role: "source",
      round: 3,
      watch: true,
      rejoinCode: "rejoin-123",
    });
  });

  it("round-trips large, default, and empty manifests", () => {
    const src = bigManifest(500);
    const msgs = encodeManifest(src, 4096);
    expect(msgs.length).toBeGreaterThanOrEqual(10);
    for (const msg of msgs) {
      expect(msg.b!.byteLength).toBeLessThanOrEqual(4096);
    }
    const got = assemble(msgs);
    expect(got.entries.length).toBe(src.entries.length);
    expect(got.skipped.length).toBe(2);
    expect(got.complete).toBe(true);
    expect(got.filter?.git).toBe(true);
    expect(got.filter?.exclude).toEqual([".log"]);
    for (let i = 0; i < src.entries.length; i += 1) {
      expect(got.entries[i].path).toBe(src.entries[i].path);
      expect(got.entries[i].size).toBe(src.entries[i].size);
      expect(got.entries[i].modTime).toBe(src.entries[i].modTime);
      expect(got.entries[i].mode).toBe(src.entries[i].mode);
      expect(got.entries[i].kind).toBe(src.entries[i].kind);
      if (src.entries[i].hash) {
        expect(bytesEqual(got.entries[i].hash!, src.entries[i].hash!)).toBe(true);
      }
    }

    // Default 1 MiB limit: 500 entries fit in 1 batch -> start + 1 batch + end = 3.
    const def = encodeManifest(src);
    expect(def.length).toBe(3);

    // Empty manifest: start + end.
    const empty = encodeManifest({ entries: [], skipped: [], complete: true });
    expect(empty.length).toBe(2);
    const assembledEmpty = assemble(empty);
    expect(assembledEmpty.entries).toEqual([]);
    expect(assembledEmpty.complete).toBe(true);
  });

  it("never sends diskPath on the wire", () => {
    const manifest: SyncManifest = {
      entries: [
        { path: "a", size: 1, modTime: "", mode: 0o644, kind: SyncKind.File },
      ],
      skipped: [
        {
          path: "",
          diskPath: "secret\\local\\CON",
          reason: "invalid_name",
          detail: "CON",
        },
      ],
      complete: true,
    };
    const msgs = encodeManifest(manifest);
    for (const msg of msgs) {
      expect(textDecoder.decode(msg.b)).not.toContain("secret");
    }
  });

  it("round-trips PathList batches and empty PathList", () => {
    const paths: string[] = [];
    for (let i = 0; i < 300; i += 1) {
      paths.push(`dir/file-${String(i).padStart(4, "0")}`);
    }
    const msgs = encodePathLists("sync-need", paths, 1024);
    expect(msgs.length).toBeGreaterThanOrEqual(5);
    const asm = new PathListAssembler();
    for (let i = 0; i < msgs.length; i += 1) {
      const done = asm.push(msgs[i]);
      expect(done).toBe(i === msgs.length - 1);
    }
    expect(asm.items).toEqual(paths);

    // Empty path list still emits one final batch.
    const empty = encodePathLists("sync-need", []);
    expect(empty.length).toBe(1);
    const emptyAsm = new PathListAssembler();
    expect(emptyAsm.push(empty[0])).toBe(true);
    expect(emptyAsm.items).toEqual([]);
  });

  it("round-trips HashList batches including error entries", () => {
    const hashes: SyncPathHash[] = [];
    for (let i = 0; i < 200; i += 1) {
      hashes.push({
        path: `f${String(i).padStart(3, "0")}`,
        hash: new Uint8Array([1, 2, 3, 4, 5, 6, 7, i & 0xff]),
      });
    }
    hashes.push({ path: "changed", error: "file changed" });
    const msgs = encodeHashLists(hashes, 1024);
    expect(msgs.length).toBeGreaterThanOrEqual(3);
    const asm = new HashListAssembler();
    for (const msg of msgs) {
      asm.push(msg);
    }
    expect(asm.done).toBe(true);
    expect(asm.items.length).toBe(hashes.length);
    expect(asm.items[200].error).toBe("file changed");
    expect(bytesEqual(asm.items[10].hash!, hashes[10].hash!)).toBe(true);
  });

  it("round-trips Result and Partial helper", () => {
    const res = {
      dryRun: true,
      added: 2,
      updated: 1,
      touched: 0,
      deleted: 3,
      unchanged: 5,
      conflicts: 1,
      bytes: 4096,
      failures: ["a.txt: changed"],
      deleteBlocked: "blocked",
      deletesHeld: 4,
    };
    expect(decodeResult(encodeResult(res))).toEqual(res);
    expect(isPartialSyncResult(res)).toBe(true);
    expect(
      isPartialSyncResult({
        added: 0,
        updated: 0,
        touched: 0,
        deleted: 0,
        unchanged: 0,
        conflicts: 0,
        bytes: 0,
      }),
    ).toBe(false);
  });
});

describe("sync-wire rejections (mirroring wire_test.go)", () => {
  const msgs = encodeManifest(bigManifest(200), 2048);
  const start = msgs[0];
  const batches = msgs.slice(1, -1);
  const end = msgs[msgs.length - 1];

  const cases: Array<[string, SyncWireMessage[]]> = [
    ["batch out of order", [start, batches[1], batches[0]]],
    ["duplicate batch", [start, batches[0], batches[0]]],
    ["missing batch", [start, batches[0], ...batches.slice(2), end]],
    [
      "missing entries",
      [start, ...batches.slice(0, -1), encodeManifestEnd({ batches: batches.length - 1 })],
    ],
    [
      "end count mismatch",
      [start, ...batches, encodeManifestEnd({ batches: batches.length + 1 })],
    ],
    [
      "oversized batch",
      [
        start,
        { t: "sync-manifest-batch", b: new Uint8Array(MAX_SYNC_BATCH_BYTES + 1) },
      ],
    ],
    ["batch before start", [batches[0]]],
    ["duplicate start", [start, start]],
    [
      "declared too large",
      [
        encodeManifestStart({
          entries: MAX_MANIFEST_ENTRIES,
          skipped: 1,
          complete: true,
        }),
      ],
    ],
    [
      "negative count",
      [encodeManifestStart({ entries: -1, skipped: 0, complete: true })],
    ],
    ["empty batch", [start, encodeManifestBatch({ seq: 0 })]],
    [
      "unknown field on start",
      [
        {
          t: "sync-manifest-start",
          b: textEncoder.encode(
            '{"entries":0,"skipped":0,"complete":true,"evil":1}',
          ),
        },
      ],
    ],
    [
      "unknown field disk_path on skipped",
      [
        encodeManifestStart({ entries: 0, skipped: 1, complete: true }),
        {
          t: "sync-manifest-batch",
          b: textEncoder.encode(
            '{"seq":0,"skipped":[{"path":"a","disk_path":"a","reason":"filtered"}]}',
          ),
        },
        encodeManifestEnd({ batches: 1 }),
      ],
    ],
    [
      "trailing data",
      [
        {
          t: "sync-manifest-start",
          b: textEncoder.encode('{"entries":0,"skipped":0} {}'),
        },
      ],
    ],
    [
      "more than declared",
      [
        encodeManifestStart({ entries: 1, skipped: 0, complete: true }),
        batches[0],
      ],
    ],
    [
      "invalid manifest path traversal",
      encodeManifest({
        entries: [
          {
            path: "../escape",
            size: 1,
            modTime: "",
            mode: 0o644,
            kind: SyncKind.File,
          },
        ],
        skipped: [],
        complete: true,
      }),
    ],
    ["end before start", [end]],
    ["batch after end", [...msgs, batches[0]]],
    [
      "garbage json",
      [{ t: "sync-manifest-start", b: textEncoder.encode("{") }],
    ],
    [
      "non-object json",
      [{ t: "sync-manifest-start", b: textEncoder.encode("[]") }],
    ],
  ];

  for (const [name, stream] of cases) {
    it(`rejects ${name}`, () => {
      expect(() => assemble(stream)).toThrow();
    });
  }

  it("rejects manifest exceeding total JSON byte limit", () => {
    expect(() => assemble(msgs, 512)).toThrow(SyncProtocolError);
  });

  it("enforces unknown-field and type checks across all decoders", () => {
    expect(() =>
      decodeHello({
        t: "sync-hello",
        b: textEncoder.encode('{"version":1,"role":"source","extra":true}'),
      }),
    ).toThrow(SyncProtocolError);

    expect(() =>
      decodeManifestStart({
        t: "sync-manifest-start",
        b: textEncoder.encode(
          '{"entries":0,"skipped":0,"filter":{"unknown":1}}',
        ),
      }),
    ).toThrow(SyncProtocolError);

    expect(() =>
      decodeManifestBatch({
        t: "sync-manifest-batch",
        b: textEncoder.encode(
          '{"seq":0,"entries":[{"path":"a","kind":0,"bogus":1}]}',
        ),
      }),
    ).toThrow(SyncProtocolError);

    expect(() =>
      decodeManifestBatch({
        t: "sync-manifest-batch",
        b: textEncoder.encode(
          '{"seq":0,"entries":[{"path":"a","kind":0,"mod_time":"not-a-date"}]}',
        ),
      }),
    ).toThrow(SyncProtocolError);

    expect(() =>
      decodeManifestBatch({
        t: "sync-manifest-batch",
        b: textEncoder.encode(
          '{"seq":0,"entries":[{"path":"a","kind":0,"hash":"!!!notbase64"}]}',
        ),
      }),
    ).toThrow(SyncProtocolError);

    expect(() =>
      decodeManifestEnd({
        t: "sync-manifest-end",
        b: textEncoder.encode('{"batches":0,"extra":1}'),
      }),
    ).toThrow(SyncProtocolError);

    expect(() =>
      decodePathList({
        t: "sync-need",
        b: textEncoder.encode('{"seq":0,"paths":["a"],"last":true,"x":1}'),
      }),
    ).toThrow(SyncProtocolError);

    expect(() =>
      decodeHashList({
        t: "sync-hash-result",
        b: textEncoder.encode(
          '{"seq":0,"hashes":[{"path":"a","extra":1}],"last":true}',
        ),
      }),
    ).toThrow(SyncProtocolError);

    expect(() =>
      decodeResult({
        t: "sync-result",
        b: textEncoder.encode('{"added":0,"unknown":1}'),
      }),
    ).toThrow(SyncProtocolError);
  });

  it("enforces encode size and count limits", () => {
    expect(() =>
      encodeManifest(
        {
          entries: [
            {
              path: "a".repeat(5000),
              size: 0,
              modTime: "",
              mode: 0o644,
              kind: SyncKind.File,
            },
          ],
          skipped: [],
          complete: true,
        },
        1024,
      ),
    ).toThrow(SyncProtocolError);

    expect(() =>
      encodeManifest(
        {
          entries: [],
          skipped: [{ path: "a".repeat(5000), reason: "filtered" }],
          complete: true,
        },
        1024,
      ),
    ).toThrow(SyncProtocolError);

    expect(() =>
      encodePathLists("sync-need", ["a".repeat(2000)], 1024),
    ).toThrow(SyncProtocolError);

    expect(() =>
      encodeHashLists([{ path: "a".repeat(2000) }], 1024),
    ).toThrow(SyncProtocolError);

    expect(() =>
      encodePathList("sync-need", {
        seq: 0,
        paths: ["a".repeat(MAX_SYNC_BATCH_BYTES)],
        last: true,
      }),
    ).toThrow(SyncProtocolError);

    expect(() =>
      encodeHashList(
        { seq: 0, hashes: [{ path: "a".repeat(2000) }], last: true },
        1024,
      ),
    ).toThrow(SyncProtocolError);

    expect(() =>
      encodeSyncPayload("sync-need", undefined),
    ).toThrow(SyncProtocolError);
  });

  it("enforces ListAssembler ordering, non-empty, limit, and post-last rules", () => {
    const asm = new ListAssembler<string>();
    expect(() => asm.add("sync-need", 1, ["a"], false)).toThrow(
      SyncProtocolError,
    );
    expect(() => asm.add("sync-need", 0, [], false)).toThrow(
      SyncProtocolError,
    );
    asm.reset(2);
    expect(() => asm.add("sync-need", 0, ["a", "b", "c"], true)).toThrow(
      SyncProtocolError,
    );
    asm.reset(10);
    expect(asm.add("sync-need", 0, ["a"], true)).toBe(true);
    expect(() => asm.add("sync-need", 1, [], true)).toThrow(SyncProtocolError);
  });
});
