import { describe, expect, it } from "vitest";
import { textDecoder, textEncoder } from "./bytes";
import { compare, SyncKind, type SyncManifest } from "./sync-diff";
import {
  GIT_DELETE_BLOCKED_REASON,
  SyncFileSink,
  SyncFsCommitter,
  applyDeletes,
  applyWebDeletePolicy,
  ensureReadWritePermission,
  scanDestination,
} from "./sync-fs";
import { XXH64 } from "./xxh64";

interface FakeFileNode {
  kind: "file";
  content: Uint8Array<ArrayBuffer>;
  lastModified: number;
}

interface FakeDirNode {
  kind: "directory";
  entries: Map<string, FakeNode>;
}

type FakeNode = FakeFileNode | FakeDirNode;

interface RemoveCall {
  dir: string;
  name: string;
  options?: FileSystemRemoveOptions;
}

class FakeWritableStream {
  private buffer: Uint8Array<ArrayBuffer>;
  private closed = false;
  aborted = false;

  constructor(
    private readonly node: FakeFileNode,
    private readonly onClose: (bytes: Uint8Array<ArrayBuffer>) => void,
  ) {
    this.buffer = new Uint8Array(0);
  }

  async write(
    data:
      | Uint8Array
      | { type: "write"; position?: number; data: Uint8Array },
  ): Promise<void> {
    if (this.closed) throw new Error("Writable stream is closed");
    const position =
      data instanceof Uint8Array ? this.buffer.byteLength : (data.position ?? 0);
    const bytes = data instanceof Uint8Array ? data : data.data;
    const end = position + bytes.byteLength;
    if (end > this.buffer.byteLength) {
      const next = new Uint8Array(end);
      next.set(this.buffer, 0);
      this.buffer = next;
    }
    this.buffer.set(bytes, position);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.onClose(this.buffer.slice());
  }

  async abort(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.aborted = true;
  }

  getNode(): FakeFileNode {
    return this.node;
  }
}

class FakeFileHandle {
  readonly kind = "file" as const;

  constructor(
    readonly name: string,
    readonly node: FakeFileNode,
  ) {}

  async isSameEntry(other: unknown): Promise<boolean> {
    return other instanceof FakeFileHandle && other.node === this.node;
  }

  async getFile(): Promise<File> {
    const bytes = this.node.content.slice();
    const lastModified = this.node.lastModified;
    const file = new File([bytes], this.name, { lastModified });
    // Ensure arrayBuffer() and stream() work reliably across jsdom versions.
    Object.defineProperty(file, "arrayBuffer", {
      value: async () => bytes.buffer.slice(0),
    });
    Object.defineProperty(file, "stream", {
      value: () =>
        new ReadableStream<Uint8Array<ArrayBuffer>>({
          start(controller) {
            if (bytes.byteLength > 0) controller.enqueue(bytes.slice());
            controller.close();
          },
        }),
    });
    return file;
  }

  async createWritable(options?: {
    keepExistingData?: boolean;
  }): Promise<FileSystemWritableFileStream> {
    if (options?.keepExistingData) {
      throw new Error("Unexpected keepExistingData=true");
    }
    const stream = new FakeWritableStream(this.node, (bytes) => {
      this.node.content = bytes;
      this.node.lastModified = Date.now() + 1000;
    });
    return stream as unknown as FileSystemWritableFileStream;
  }
}

export class FakeDirectoryHandle {
  readonly kind = "directory" as const;
  readonly removeCalls: RemoveCall[];
  permissionState: "granted" | "prompt" | "denied";
  requestResult: "granted" | "denied";
  queryCalls = 0;
  requestCalls = 0;

  constructor(
    readonly name: string,
    readonly node: FakeDirNode = { kind: "directory", entries: new Map() },
    options?: {
      removeCalls?: RemoveCall[];
      permissionState?: "granted" | "prompt" | "denied";
      requestResult?: "granted" | "denied";
    },
  ) {
    this.removeCalls = options?.removeCalls ?? [];
    this.permissionState = options?.permissionState ?? "granted";
    this.requestResult = options?.requestResult ?? "granted";
  }

  async isSameEntry(other: unknown): Promise<boolean> {
    return other instanceof FakeDirectoryHandle && other.node === this.node;
  }

  async queryPermission(): Promise<"granted" | "prompt" | "denied"> {
    this.queryCalls += 1;
    return this.permissionState;
  }

  async requestPermission(): Promise<"granted" | "denied"> {
    this.requestCalls += 1;
    this.permissionState = this.requestResult;
    return this.requestResult;
  }

  async getDirectoryHandle(
    name: string,
    options?: { create?: boolean },
  ): Promise<FileSystemDirectoryHandle> {
    const existing = this.node.entries.get(name);
    if (existing) {
      if (existing.kind !== "directory") {
        throw new DOMException("Not a directory", "TypeMismatchError");
      }
      return new FakeDirectoryHandle(name, existing, {
        removeCalls: this.removeCalls,
        permissionState: this.permissionState,
        requestResult: this.requestResult,
      }) as unknown as FileSystemDirectoryHandle;
    }
    if (!options?.create) {
      throw new DOMException("Not found", "NotFoundError");
    }
    const created: FakeDirNode = { kind: "directory", entries: new Map() };
    this.node.entries.set(name, created);
    return new FakeDirectoryHandle(name, created, {
      removeCalls: this.removeCalls,
      permissionState: this.permissionState,
      requestResult: this.requestResult,
    }) as unknown as FileSystemDirectoryHandle;
  }

  async getFileHandle(
    name: string,
    options?: { create?: boolean },
  ): Promise<FileSystemFileHandle> {
    const existing = this.node.entries.get(name);
    if (existing) {
      if (existing.kind !== "file") {
        throw new DOMException("Not a file", "TypeMismatchError");
      }
      return new FakeFileHandle(
        name,
        existing,
      ) as unknown as FileSystemFileHandle;
    }
    if (!options?.create) {
      throw new DOMException("Not found", "NotFoundError");
    }
    const created: FakeFileNode = {
      kind: "file",
      content: new Uint8Array(0),
      lastModified: 1_700_000_000_000,
    };
    this.node.entries.set(name, created);
    return new FakeFileHandle(
      name,
      created,
    ) as unknown as FileSystemFileHandle;
  }

  async removeEntry(
    name: string,
    options?: FileSystemRemoveOptions,
  ): Promise<void> {
    this.removeCalls.push({ dir: this.name, name, options });
    const existing = this.node.entries.get(name);
    if (!existing) {
      throw new DOMException("Not found", "NotFoundError");
    }
    if (
      existing.kind === "directory" &&
      existing.entries.size > 0 &&
      !options?.recursive
    ) {
      throw new DOMException("Directory not empty", "InvalidModificationError");
    }
    this.node.entries.delete(name);
  }

  async *entries(): AsyncIterableIterator<[string, FileSystemHandle]> {
    for (const [name, child] of this.node.entries.entries()) {
      if (child.kind === "directory") {
        yield [
          name,
          new FakeDirectoryHandle(name, child, {
            removeCalls: this.removeCalls,
          }) as unknown as FileSystemHandle,
        ];
      } else {
        yield [name, new FakeFileHandle(name, child) as unknown as FileSystemHandle];
      }
    }
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<[string, FileSystemHandle]> {
    return this.entries();
  }

  // Test helpers to populate and inspect the in-memory tree directly.
  setFile(path: string, content: string | Uint8Array, lastModified = 1_700_000_000_000): void {
    const parts = path.split("/").filter(Boolean);
    const name = parts.pop()!;
    let cur = this.node;
    for (const seg of parts) {
      let next = cur.entries.get(seg);
      if (!next) {
        next = { kind: "directory", entries: new Map() };
        cur.entries.set(seg, next);
      }
      if (next.kind !== "directory") throw new Error(`${seg} is not a dir`);
      cur = next;
    }
    const bytes =
      typeof content === "string"
        ? textEncoder.encode(content).slice()
        : content.slice();
    const existing = cur.entries.get(name);
    if (existing && existing.kind === "file") {
      existing.content = bytes;
      existing.lastModified = lastModified;
    } else {
      cur.entries.set(name, { kind: "file", content: bytes, lastModified });
    }
  }

  setDir(path: string): void {
    const parts = path.split("/").filter(Boolean);
    let cur = this.node;
    for (const seg of parts) {
      let next = cur.entries.get(seg);
      if (!next) {
        next = { kind: "directory", entries: new Map() };
        cur.entries.set(seg, next);
      }
      if (next.kind !== "directory") throw new Error(`${seg} is not a dir`);
      cur = next;
    }
  }

  readText(path: string): string | undefined {
    const node = this.getNode(path);
    if (!node || node.kind !== "file") return undefined;
    return textDecoder.decode(node.content);
  }

  getNode(path: string): FakeNode | undefined {
    const parts = path.split("/").filter(Boolean);
    let cur: FakeNode = this.node;
    for (const seg of parts) {
      if (cur.kind !== "directory") return undefined;
      const next = cur.entries.get(seg);
      if (!next) return undefined;
      cur = next;
    }
    return cur;
  }

  asHandle(): FileSystemDirectoryHandle {
    return this as unknown as FileSystemDirectoryHandle;
  }
}

function xxhash(data: string | Uint8Array): Uint8Array {
  const hasher = new XXH64();
  hasher.update(typeof data === "string" ? textEncoder.encode(data) : data);
  return hasher.digest();
}

describe("sync-fs scanDestination", () => {
  it("handles invalid names, case/NFC collisions, staging dirs, and filters", async () => {
    const root = new FakeDirectoryHandle("root");
    root.setFile("ok.txt", "hello", 1_700_000_001_000);
    root.setFile("sub/nested.txt", "world", 1_700_000_002_000);
    // Staging names must be ignored completely.
    root.setFile(".croc-sync-tmp-123/temp.bin", "tmp");
    // Invalid names fail normalization -> skipped invalid_name with diskPath.
    root.setFile("CON", "reserved");
    root.setFile("sub/bad:name", "colon");
    // Case collision -> second skipped invalid_name with path.
    root.setFile("Readme.md", "upper");
    root.setFile("readme.md", "lower");
    // NFC vs NFD collision -> second skipped invalid_name with path.
    root.setFile("caf\u00e9.txt", "nfc");
    root.setFile("cafe\u0301.txt", "nfd");
    // Filtered entries.
    root.setFile("debug.LOG", "log");
    root.setFile("build/app.js", "js");

    const scan = await scanDestination(root.asHandle(), {
      exclude: [".log"],
      excludeExact: ["build"],
    });

    const entryPaths = scan.manifest.entries.map((e) => e.path);
    expect(entryPaths).toEqual([
      "Readme.md",
      "caf\u00e9.txt",
      "ok.txt",
      "sub",
      "sub/nested.txt",
    ]);

    // Staging never appears in entries or skipped.
    expect(
      scan.manifest.skipped.some((s) => (s.diskPath ?? "").includes(".croc-sync-tmp")),
    ).toBe(false);

    // Invalid names have empty wire path + diskPath.
    const invalidWithoutPath = scan.manifest.skipped.filter(
      (s) => s.reason === "invalid_name" && s.path === "",
    );
    expect(invalidWithoutPath.map((s) => s.diskPath).sort()).toEqual([
      "CON",
      "sub/bad:name",
    ]);

    // Case & NFC collisions have non-empty wire path.
    const collisions = scan.manifest.skipped.filter(
      (s) => s.reason === "invalid_name" && s.path !== "",
    );
    expect(collisions.map((s) => s.path).sort()).toEqual([
      "caf\u00e9.txt",
      "readme.md",
    ]);

    // Filtered items.
    const filtered = scan.manifest.skipped.filter((s) => s.reason === "filtered");
    expect(filtered.map((s) => s.path).sort()).toEqual(["build", "debug.LOG"]);
  });

  it("blocks deletions when source uses git filter", async () => {
    const root = new FakeDirectoryHandle("root");
    root.setFile("keep.txt", "a", 1_700_000_000_000);
    root.setFile("extra.txt", "b", 1_700_000_000_000);

    const src: SyncManifest = {
      entries: [
        {
          path: "keep.txt",
          size: 1,
          modTime: new Date(1_700_000_000_000).toISOString(),
          mode: 0o644,
          kind: SyncKind.File,
        },
      ],
      skipped: [],
      filter: { git: true },
      complete: true,
    };

    const scan = await scanDestination(root.asHandle(), src.filter);
    expect(scan.gitFilterUnsupported).toBe(true);

    const res = compare(src, scan.manifest, {
      delete: true,
      ignoreMetadata: true,
    });
    expect(res.plan).toBeDefined();
    expect(res.plan!.delete.map((d) => d.path)).toEqual(["extra.txt"]);

    applyWebDeletePolicy(res.plan!, scan, {
      delete: true,
      ignoreMetadata: true,
    });
    expect(res.plan!.delete).toEqual([]);
    expect(res.plan!.deleteBlockedReason).toBe(GIT_DELETE_BLOCKED_REASON);
  });
});

describe("sync-fs SyncFileSink write verification", () => {
  it("aborts on hash mismatch and preserves existing content", async () => {
    const root = new FakeDirectoryHandle("root");
    root.setFile("data.txt", "original-content", 1_700_000_000_000);
    const scan = await scanDestination(root.asHandle());
    const snap = scan.snapshots.get("data.txt");
    expect(snap?.kind).toBe("file");

    const sink = await SyncFileSink.open({
      root: root.asHandle(),
      wirePath: "data.txt",
      expectedSize: 11,
      expectedHash: xxhash("expected-ok"),
      snapshot: snap?.kind === "file" ? snap : undefined,
    });

    await sink.writeAt(0, textEncoder.encode("corrupted!!"));
    await sink.finalize();
    await expect(sink.commit()).rejects.toThrow(/hash/);
    // Old content stays untouched because writable was aborted, not closed.
    expect(root.readText("data.txt")).toBe("original-content");
  });

  it("hashes out-of-order chunks in file order and commits on match", async () => {
    const root = new FakeDirectoryHandle("root");
    const c0 = textEncoder.encode("AAAA-chunk0-");
    const c1 = textEncoder.encode("BBBB-chunk1-");
    const c2 = textEncoder.encode("CCCC-chunk2");
    const full = new Uint8Array(c0.byteLength + c1.byteLength + c2.byteLength);
    full.set(c0, 0);
    full.set(c1, c0.byteLength);
    full.set(c2, c0.byteLength + c1.byteLength);

    const sink = await SyncFileSink.open({
      root: root.asHandle(),
      wirePath: "sub/out.bin",
      expectedSize: full.byteLength,
      expectedHash: xxhash(full),
    });

    // Deliver out of order: c2, then c0, then c1.
    await sink.writeAt(c0.byteLength + c1.byteLength, c2);
    await sink.writeAt(0, c0);
    await sink.writeAt(c0.byteLength, c1);
    await sink.finalize();
    await sink.commit();

    expect(root.readText("sub/out.bin")).toBe(
      "AAAA-chunk0-BBBB-chunk1-CCCC-chunk2",
    );
  });

  it("aborts if destination file changed since the scan before close", async () => {
    const root = new FakeDirectoryHandle("root");
    root.setFile("note.txt", "v1", 1_700_000_000_000);
    const scan = await scanDestination(root.asHandle());
    const snap = scan.snapshots.get("note.txt");

    const sink = await SyncFileSink.open({
      root: root.asHandle(),
      wirePath: "note.txt",
      expectedSize: 2,
      expectedHash: xxhash("v2"),
      snapshot: snap?.kind === "file" ? snap : undefined,
    });
    await sink.writeAt(0, textEncoder.encode("v2"));
    await sink.finalize();

    // Another process edits note.txt before close().
    root.setFile("note.txt", "external-edit", 1_700_000_999_000);

    await expect(sink.commit()).rejects.toThrow(/changed since the scan/);
    expect(root.readText("note.txt")).toBe("external-edit");
  });

  it("rejects Add if destination file appeared after the scan", async () => {
    const root = new FakeDirectoryHandle("root");
    root.setFile("new.txt", "surprise", 1_700_000_000_000);

    await expect(
      SyncFileSink.open({
        root: root.asHandle(),
        wirePath: "new.txt",
        expectedSize: 3,
        expectedHash: xxhash("abc"),
      }),
    ).rejects.toThrow(/changed since the scan/);
    expect(root.readText("new.txt")).toBe("surprise");
  });
});

describe("sync-fs delete rules, type changes, and permissions", () => {
  it("holds deletes when any write failed", async () => {
    const root = new FakeDirectoryHandle("root");
    root.setFile("update.txt", "old", 1_700_000_000_000);
    root.setFile("stale.txt", "delete-me", 1_700_000_000_000);

    const scan = await scanDestination(root.asHandle());
    const src: SyncManifest = {
      entries: [
        {
          path: "update.txt",
          size: 4,
          modTime: "2026-11-01T12:00:00Z",
          mode: 0o644,
          kind: SyncKind.File,
          hash: xxhash("good"),
        },
      ],
      skipped: [],
      complete: true,
    };
    const { plan } = compare(src, scan.manifest, {
      delete: true,
      ignoreMetadata: true,
    });
    expect(plan!.delete.map((d) => d.path)).toEqual(["stale.txt"]);

    const committer = new SyncFsCommitter(root.asHandle(), scan, plan!);
    await committer.prepareDirectories();
    const sink = await committer.openSinkForChange(plan!.update[0], {
      swallowCommitError: true,
    });
    // Send wrong content so commit fails.
    await sink.writeAt(0, textEncoder.encode("bad!"));
    await sink.finalize();
    await sink.commit();

    const result = await committer.finish({
      proceed: true,
      confirmDeletes: true,
    });
    expect(result.failures?.length).toBe(1);
    expect(result.deleted).toBe(0);
    expect(result.deletesHeld).toBe(1);
    expect(root.readText("update.txt")).toBe("old");
    expect(root.readText("stale.txt")).toBe("delete-me");
  });

  it("deletes deepest-first non-recursively and re-checks before deleting", async () => {
    const root = new FakeDirectoryHandle("root");
    root.setFile("keep.txt", "k", 1_700_000_000_000);
    root.setFile("keep2.txt", "k2", 1_700_000_000_000);
    root.setFile("keep3.txt", "k3", 1_700_000_000_000);
    root.setFile("old/sub/a.txt", "a", 1_700_000_000_000);
    root.setFile("changed.txt", "before", 1_700_000_000_000);

    const scan = await scanDestination(root.asHandle());
    const src: SyncManifest = {
      entries: [
        {
          path: "keep.txt",
          size: 1,
          modTime: new Date(1_700_000_000_000).toISOString(),
          mode: 0o644,
          kind: SyncKind.File,
        },
        {
          path: "keep2.txt",
          size: 2,
          modTime: new Date(1_700_000_000_000).toISOString(),
          mode: 0o644,
          kind: SyncKind.File,
        },
        {
          path: "keep3.txt",
          size: 2,
          modTime: new Date(1_700_000_000_000).toISOString(),
          mode: 0o644,
          kind: SyncKind.File,
        },
      ],
      skipped: [],
      complete: true,
    };
    const { plan } = compare(src, scan.manifest, {
      delete: true,
      ignoreMetadata: true,
    });
    expect(plan!.requireDeleteConfirm).toBe(false);

    // Mutate changed.txt after the scan so re-check refuses to delete it.
    root.setFile("changed.txt", "after-scan", 1_700_000_999_000);

    const outcome = await applyDeletes(root.asHandle(), scan, plan!, {
      committed: true,
      confirmed: false,
    });
    // old/sub/a.txt, old/sub, old deleted; changed.txt skipped with failure.
    expect(outcome.deleted).toBe(3);
    expect(outcome.failures.length).toBe(1);
    expect(outcome.failures[0]).toContain("changed.txt");
    expect(root.getNode("old")).toBeUndefined();
    expect(root.readText("changed.txt")).toBe("after-scan");
    // Verify removeEntry was NEVER called with recursive: true.
    for (const call of root.removeCalls) {
      expect(call.options?.recursive).not.toBe(true);
    }
    expect(root.removeCalls.map((c) => c.name)).toEqual(["a.txt", "sub", "old"]);
  });

  it("handles dir->file replacement only when directory matches planned deletions", async () => {
    const root = new FakeDirectoryHandle("root");
    root.setFile("target/child.txt", "old", 1_700_000_000_000);
    const scan = await scanDestination(root.asHandle());

    const src: SyncManifest = {
      entries: [
        {
          path: "target",
          size: 4,
          modTime: "2026-11-01T12:00:00Z",
          mode: 0o644,
          kind: SyncKind.File,
          hash: xxhash("file"),
        },
      ],
      skipped: [],
      complete: true,
    };
    const { plan } = compare(src, scan.manifest, {
      delete: true,
      ignoreMetadata: true,
    });
    expect(plan!.update.length).toBe(1);
    expect(plan!.update[0].typeChanged).toBe(true);

    // Unexpected file appears inside target/ before commit -> must fail & keep dir.
    root.setFile("target/surprise.txt", "keep-me", 1_700_000_111_000);
    const committer = new SyncFsCommitter(root.asHandle(), scan, plan!);
    const sink = await committer.openSinkForChange(plan!.update[0], {
      swallowCommitError: true,
    });
    await sink.writeAt(0, textEncoder.encode("file"));
    await sink.finalize();
    await sink.commit();

    const result = await committer.finish({
      proceed: true,
      confirmDeletes: true,
    });
    expect(result.failures?.length).toBe(1);
    expect(root.readText("target/surprise.txt")).toBe("keep-me");

    // When directory contains only the planned deletions, dir->file succeeds.
    const cleanRoot = new FakeDirectoryHandle("clean-root");
    cleanRoot.setFile("target/child.txt", "old", 1_700_000_000_000);
    const cleanScan = await scanDestination(cleanRoot.asHandle());
    const cleanPlan = compare(src, cleanScan.manifest, {
      delete: true,
      ignoreMetadata: true,
    }).plan!;
    const cleanCommitter = new SyncFsCommitter(
      cleanRoot.asHandle(),
      cleanScan,
      cleanPlan,
    );
    const cleanSink = await cleanCommitter.openSinkForChange(
      cleanPlan.update[0],
      { swallowCommitError: true },
    );
    await cleanSink.writeAt(0, textEncoder.encode("file"));
    await cleanSink.finalize();
    await cleanSink.commit();
    const cleanResult = await cleanCommitter.finish({
      proceed: true,
      confirmDeletes: true,
    });
    expect(cleanResult.failures).toBeUndefined();
    expect(cleanResult.added).toBe(1);
    expect(cleanResult.deleted).toBe(1);
    expect(cleanRoot.readText("target")).toBe("file");
  });

  it("fails with a clear error before writing when write permission is denied", async () => {
    const root = new FakeDirectoryHandle("root", undefined, {
      permissionState: "prompt",
      requestResult: "denied",
    });
    const scan = await scanDestination(root.asHandle());
    const src: SyncManifest = {
      entries: [
        {
          path: "dir",
          size: 0,
          modTime: "",
          mode: 0o755,
          kind: SyncKind.Directory,
        },
        {
          path: "dir/file.txt",
          size: 2,
          modTime: "2026-11-01T12:00:00Z",
          mode: 0o644,
          kind: SyncKind.File,
          hash: xxhash("hi"),
        },
      ],
      skipped: [],
      complete: true,
    };
    const { plan } = compare(src, scan.manifest, { ignoreMetadata: true });
    const committer = new SyncFsCommitter(root.asHandle(), scan, plan!);

    await expect(ensureReadWritePermission(root.asHandle())).rejects.toThrow(
      /permission/i,
    );
    await expect(committer.prepareDirectories()).rejects.toThrow(/permission/i);
    expect(root.getNode("dir")).toBeUndefined();
    expect(root.queryCalls).toBeGreaterThanOrEqual(1);
    expect(root.requestCalls).toBeGreaterThanOrEqual(1);
  });
});
