import { beforeEach, describe, expect, it, vi } from "vitest";

const engineMocks = vi.hoisted(() => ({
  decrypt: vi.fn(async (bytes: Uint8Array) => bytes),
  decompress: vi.fn(async (bytes: Uint8Array) => bytes),
  compress: vi.fn(async (bytes: Uint8Array) => bytes),
}));

vi.mock("./compression", () => ({
  checkProtocolCapabilities: () => undefined,
}));

vi.mock("./engine", () => ({
  protocolEngine: () => engineMocks,
}));

import { bytesToBase64, textDecoder, textEncoder } from "./bytes";
import { SyncKind, type SyncManifest, type SyncPlan } from "./sync-diff";
import { hashFileXXH64 } from "./sync-fs";
import { MemorySyncHashCacheStore, SyncHashCache } from "./sync-hash-cache";
import {
  SENDER_NOT_SYNC_ERROR,
  SyncDestSession,
  SyncSourceChangedError,
  SyncWatchUnsupportedError,
  WATCH_UNSUPPORTED_ERROR,
  runSyncOverChannel,
  type SyncChannel,
  type SyncDestEnv,
} from "./sync";
import {
  SYNC_FEATURE,
  SYNC_PROTOCOL_VERSION,
  SyncProtocolError,
  decodeHello,
  decodePathList,
  decodeResult,
  encodeHashLists,
  encodeHello,
  encodeManifest,
  type SyncResult,
  type SyncRole,
  type SyncWireMessage,
} from "./sync-wire";
import type { CrocSocket } from "./transport";
import type {
  CrocMessage,
  RemoteFileRequestWire,
  SenderInfoWire,
} from "./types";
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

class FakeWritable {
  private buffer: Uint8Array<ArrayBuffer> = new Uint8Array(0);
  private closed = false;

  constructor(
    private readonly onClose: (bytes: Uint8Array<ArrayBuffer>) => void,
  ) {}

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
    this.closed = true;
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
    const file = new File([bytes], this.name, {
      lastModified: this.node.lastModified,
    });
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

  async createWritable(): Promise<FileSystemWritableFileStream> {
    const writable = new FakeWritable((bytes) => {
      this.node.content = bytes;
      this.node.lastModified = Date.now() + 1000;
    });
    return writable as unknown as FileSystemWritableFileStream;
  }
}

class FakeDirHandle {
  readonly kind = "directory" as const;
  permissionState: "granted" | "prompt" | "denied";
  requestResult: "granted" | "denied";

  constructor(
    readonly name: string,
    readonly node: FakeDirNode = { kind: "directory", entries: new Map() },
    options?: {
      permissionState?: "granted" | "prompt" | "denied";
      requestResult?: "granted" | "denied";
    },
  ) {
    this.permissionState = options?.permissionState ?? "granted";
    this.requestResult = options?.requestResult ?? "granted";
  }

  async isSameEntry(other: unknown): Promise<boolean> {
    return other instanceof FakeDirHandle && other.node === this.node;
  }

  async queryPermission(): Promise<"granted" | "prompt" | "denied"> {
    return this.permissionState;
  }

  async requestPermission(): Promise<"granted" | "denied"> {
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
      return new FakeDirHandle(name, existing, {
        permissionState: this.permissionState,
        requestResult: this.requestResult,
      }) as unknown as FileSystemDirectoryHandle;
    }
    if (!options?.create) {
      throw new DOMException("Not found", "NotFoundError");
    }
    const created: FakeDirNode = { kind: "directory", entries: new Map() };
    this.node.entries.set(name, created);
    return new FakeDirHandle(name, created, {
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
    return new FakeFileHandle(name, created) as unknown as FileSystemFileHandle;
  }

  async removeEntry(
    name: string,
    options?: FileSystemRemoveOptions,
  ): Promise<void> {
    const existing = this.node.entries.get(name);
    if (!existing) throw new DOMException("Not found", "NotFoundError");
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
        yield [name, new FakeDirHandle(name, child) as unknown as FileSystemHandle];
      } else {
        yield [name, new FakeFileHandle(name, child) as unknown as FileSystemHandle];
      }
    }
  }

  setFile(path: string, content: string, lastModified = 1_700_000_000_000): void {
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
    const bytes = textEncoder.encode(content).slice();
    const existing = cur.entries.get(name);
    if (existing && existing.kind === "file") {
      existing.content = bytes;
      existing.lastModified = lastModified;
    } else {
      cur.entries.set(name, { kind: "file", content: bytes, lastModified });
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

function makeChunkFrame(position: number, bytes: Uint8Array): Uint8Array {
  const frame = new Uint8Array(8 + bytes.byteLength);
  new DataView(frame.buffer).setBigUint64(0, BigInt(position), true);
  frame.set(bytes, 8);
  return frame;
}

class AsyncQueue<T> {
  private items: T[] = [];
  private waiters: Array<(item: T) => void> = [];

  push(item: T): void {
    const waiter = this.waiters.shift();
    if (waiter) waiter(item);
    else this.items.push(item);
  }

  shift(): Promise<T> {
    if (this.items.length > 0) {
      return Promise.resolve(this.items.shift()!);
    }
    return new Promise<T>((resolve) => {
      this.waiters.push(resolve);
    });
  }
}

interface ScriptedSourceOptions {
  manifest: SyncManifest;
  files: Record<string, string>;
  watch?: boolean;
  helloVersion?: number;
  helloRole?: SyncRole;
  peerFeatures?: string[];
  tamperOffer?: (offer: SenderInfoWire) => SenderInfoWire;
  changedHashPaths?: Set<string>;
}

function createScriptedChannel(opts: ScriptedSourceOptions) {
  const toDestControl = new AsyncQueue<CrocMessage>();
  const toDestData = new AsyncQueue<Uint8Array>();
  const sentByDest: Array<SyncWireMessage | CrocMessage> = [];
  let offeredFiles: Array<{ path: string; content: Uint8Array }> = [];
  let sourceResult: SyncResult | undefined;

  const initial: SyncWireMessage[] = [
    encodeHello({
      version: opts.helloVersion ?? SYNC_PROTOCOL_VERSION,
      role: opts.helloRole ?? "source",
      watch: opts.watch,
    }),
    ...encodeManifest(opts.manifest),
  ];
  for (const msg of initial) {
    toDestControl.push(msg as unknown as CrocMessage);
  }

  let pendingHashPaths: string[] = [];
  let pendingNeedPaths: string[] = [];

  const dataSocket = {
    receive: () => toDestData.shift(),
    send: async () => undefined,
    close: () => undefined,
  } as unknown as CrocSocket;

  const channel: SyncChannel = {
    async sendControl(msg) {
      sentByDest.push(msg);
      const t = msg.t as string;
      if (t === "sync-hello") {
        const h = decodeHello(msg);
        expect(h.version).toBe(SYNC_PROTOCOL_VERSION);
        expect(h.role).toBe("destination");
        return;
      }
      if (t === "sync-hash-request") {
        const list = decodePathList(msg);
        pendingHashPaths.push(...list.paths);
        if (list.last) {
          const paths = pendingHashPaths;
          pendingHashPaths = [];
          const hashes = paths.map((p) => {
            if (opts.changedHashPaths?.has(p)) {
              return { path: p, error: "file changed" };
            }
            return { path: p, hash: xxhash(opts.files[p] ?? "") };
          });
          for (const out of encodeHashLists(hashes)) {
            toDestControl.push(out as unknown as CrocMessage);
          }
        }
        return;
      }
      if (t === "sync-need") {
        const list = decodePathList(msg);
        pendingNeedPaths.push(...list.paths);
        if (list.last) {
          const need = pendingNeedPaths;
          pendingNeedPaths = [];
          offeredFiles = need.map((p) => ({
            path: p,
            content: textEncoder.encode(opts.files[p] ?? ""),
          }));
          let senderInfo: SenderInfoWire = {
            FilesToTransfer: offeredFiles.map(({ path, content }) => {
              const slash = path.lastIndexOf("/");
              const folder = slash < 0 ? "./" : `${path.slice(0, slash)}/`;
              const name = slash < 0 ? path : path.slice(slash + 1);
              return {
                n: name,
                fr: folder,
                h: bytesToBase64(xxhash(content)),
                s: content.byteLength,
                md: 0o644,
                c: false,
                p: true,
              };
            }),
            EmptyFoldersToTransfer: null,
            TotalNumberFolders: 0,
            MachineID: "scripted-source",
            Ask: false,
            SendingText: false,
            NoCompress: true,
            HashAlgorithm: "xxhash",
          };
          if (opts.tamperOffer) {
            senderInfo = opts.tamperOffer(senderInfo);
          }
          toDestControl.push({
            t: "fileinfo",
            b: textEncoder.encode(JSON.stringify(senderInfo)),
          });
        }
        return;
      }
      if (t === "recipientready") {
        const req = JSON.parse(
          textDecoder.decode(msg.b),
        ) as RemoteFileRequestWire;
        const item = offeredFiles[req.FilesToTransferCurrentNum];
        const chunkSize = 32 * 1024;
        const chunks: Array<[number, Uint8Array]> = [];
        for (let pos = 0; pos < item.content.byteLength; pos += chunkSize) {
          chunks.push([pos, item.content.subarray(pos, pos + chunkSize)]);
        }
        for (const [pos, bytes] of chunks.reverse()) {
          toDestData.push(makeChunkFrame(pos, bytes));
        }
        return;
      }
      if (t === "close-sender") {
        toDestControl.push({ t: "close-recipient" });
        return;
      }
      if (t === "sync-result") {
        sourceResult = decodeResult(msg);
        return;
      }
      if (t === "finished") {
        toDestControl.push({ t: "finished" });
        return;
      }
    },
    receiveControl() {
      return toDestControl.shift();
    },
    dataSockets: [dataSocket],
    key: new Uint8Array(32),
    peerFeatures: opts.peerFeatures ?? [SYNC_FEATURE],
  };

  return {
    channel,
    sentByDest,
    getSourceResult: () => sourceResult,
  };
}

beforeEach(() => {
  engineMocks.decrypt.mockImplementation(async (bytes: Uint8Array) => bytes);
  engineMocks.decompress.mockImplementation(async (bytes: Uint8Array) => bytes);
});

describe("sync destination protocol (scripted source, no network)", () => {
  it("runs a full round with tier-2 hashing, transfers, deletes, and sync-result", async () => {
    const srcFiles: Record<string, string> = {
      "same.txt": "same",
      "same-diff-mtime.txt": "identical-content",
      "edited.txt": "updated-content!",
      "sub/new.txt": "brand-new",
    };
    const srcManifest: SyncManifest = {
      entries: [
        {
          path: "edited.txt",
          size: srcFiles["edited.txt"].length,
          modTime: "2026-11-01T12:00:05Z",
          mode: 0o644,
          kind: SyncKind.File,
        },
        {
          path: "same-diff-mtime.txt",
          size: srcFiles["same-diff-mtime.txt"].length,
          modTime: "2026-11-01T12:00:05Z",
          mode: 0o644,
          kind: SyncKind.File,
        },
        {
          path: "same.txt",
          size: srcFiles["same.txt"].length,
          modTime: new Date(1_700_000_000_000).toISOString(),
          mode: 0o644,
          kind: SyncKind.File,
        },
        {
          path: "sub",
          size: 0,
          modTime: "",
          mode: 0o755,
          kind: SyncKind.Directory,
        },
        {
          path: "sub/new.txt",
          size: srcFiles["sub/new.txt"].length,
          modTime: "2026-11-01T12:00:00Z",
          mode: 0o644,
          kind: SyncKind.File,
        },
      ],
      skipped: [],
      complete: true,
    };

    const root = new FakeDirHandle("dest");
    root.setFile("same.txt", "same", 1_700_000_000_000);
    root.setFile("same-diff-mtime.txt", "identical-content", 1_700_000_000_000);
    root.setFile("edited.txt", "old-content-here", 1_700_000_000_000);
    root.setFile("obsolete.txt", "remove-me", 1_700_000_000_000);

    const cache = new SyncHashCache(new MemorySyncHashCacheStore());
    const computeSpy = vi.fn(hashFileXXH64);
    const seenPlans: SyncPlan[] = [];
    const seenResults: SyncResult[] = [];

    const scripted = createScriptedChannel({
      manifest: srcManifest,
      files: srcFiles,
    });

    const result = await runSyncOverChannel({
      channel: scripted.channel,
      root: root.asHandle(),
      diff: { delete: true },
      decide: async () => ({ proceed: true, confirmDeletes: true }),
      callbacks: {
        onPlan: (p) => seenPlans.push(p),
        onResult: (r) => seenResults.push(r),
      },
      hashCache: cache,
      computeHash: computeSpy,
    });

    expect(seenPlans.length).toBe(1);
    expect(seenResults).toEqual([result]);
    expect(scripted.getSourceResult()).toEqual(result);

    expect(result.dryRun).toBeUndefined();
    expect(result.added).toBe(2);
    expect(result.updated).toBe(1);
    expect(result.touched).toBe(0);
    expect(result.deleted).toBe(1);
    expect(result.unchanged).toBe(2);
    expect(result.bytes).toBe(
      srcFiles["edited.txt"].length + srcFiles["sub/new.txt"].length,
    );

    expect(root.readText("same.txt")).toBe("same");
    expect(root.readText("same-diff-mtime.txt")).toBe("identical-content");
    expect(root.readText("edited.txt")).toBe("updated-content!");
    expect(root.readText("sub/new.txt")).toBe("brand-new");
    expect(root.getNode("obsolete.txt")).toBeUndefined();

    // Second round over the now-synced unchanged folder hashes nothing locally.
    computeSpy.mockClear();
    const round2 = createScriptedChannel({
      manifest: srcManifest,
      files: srcFiles,
    });
    const result2 = await runSyncOverChannel({
      channel: round2.channel,
      root: root.asHandle(),
      diff: { delete: true },
      decide: async () => ({ proceed: true, confirmDeletes: true }),
      hashCache: cache,
      computeHash: computeSpy,
    });
    expect(computeSpy).toHaveBeenCalledTimes(0);
    expect(result2.added).toBe(0);
    expect(result2.updated).toBe(0);
    expect(result2.deleted).toBe(0);
    expect(result2.unchanged).toBe(5);
  });

  it("handles dry run with empty sync-need, no writes, and dry_run result", async () => {
    const srcFiles = { "new.txt": "hello" };
    const srcManifest: SyncManifest = {
      entries: [
        {
          path: "new.txt",
          size: 5,
          modTime: "2026-11-01T12:00:00Z",
          mode: 0o644,
          kind: SyncKind.File,
        },
      ],
      skipped: [],
      complete: true,
    };

    const root = new FakeDirHandle("dest", undefined, {
      permissionState: "denied",
      requestResult: "denied",
    });
    root.setFile("existing.txt", "keep", 1_700_000_000_000);

    const scripted = createScriptedChannel({
      manifest: srcManifest,
      files: srcFiles,
    });

    const result = await runSyncOverChannel({
      channel: scripted.channel,
      root: root.asHandle(),
      diff: { delete: true },
      decide: async () => ({ proceed: false, confirmDeletes: false }),
    });

    expect(result.dryRun).toBe(true);
    expect(result.added).toBe(0);
    expect(result.deleted).toBe(0);
    expect(scripted.getSourceResult()?.dryRun).toBe(true);
    const needMsgs = scripted.sentByDest.filter((m) => m.t === "sync-need");
    expect(needMsgs.length).toBe(1);
    expect(decodePathList(needMsgs[0]).paths).toEqual([]);
    expect(root.getNode("new.txt")).toBeUndefined();
    expect(root.readText("existing.txt")).toBe("keep");
  });

  it("refuses watch mode and sends a peer error before closing", async () => {
    const root = new FakeDirHandle("dest");
    const scripted = createScriptedChannel({
      manifest: { entries: [], skipped: [], complete: true },
      files: {},
      watch: true,
    });

    await expect(
      runSyncOverChannel({
        channel: scripted.channel,
        root: root.asHandle(),
        decide: async () => ({ proceed: true, confirmDeletes: false }),
      }),
    ).rejects.toThrow(WATCH_UNSUPPORTED_ERROR);

    expect(scripted.sentByDest.map((m) => m.t)).toEqual([
      "sync-hello",
      "error",
    ]);
    expect((scripted.sentByDest[1] as CrocMessage).m).toBe(
      WATCH_UNSUPPORTED_ERROR,
    );
  });

  it("sends a peer error when refusing unsupported protocol version or wrong role", async () => {
    const versionRoot = new FakeDirHandle("dest");
    const badVersion = createScriptedChannel({
      manifest: { entries: [], skipped: [], complete: true },
      files: {},
      helloVersion: SYNC_PROTOCOL_VERSION + 1,
    });

    await expect(
      runSyncOverChannel({
        channel: badVersion.channel,
        root: versionRoot.asHandle(),
        decide: async () => ({ proceed: true, confirmDeletes: false }),
      }),
    ).rejects.toThrow(SyncProtocolError);

    expect(badVersion.sentByDest.map((m) => m.t)).toEqual([
      "sync-hello",
      "error",
    ]);
    expect((badVersion.sentByDest[1] as CrocMessage).m).toContain(
      `peer speaks sync protocol ${SYNC_PROTOCOL_VERSION + 1}`,
    );

    const roleRoot = new FakeDirHandle("dest");
    const badRole = createScriptedChannel({
      manifest: { entries: [], skipped: [], complete: true },
      files: {},
      helloRole: "destination",
    });

    await expect(
      runSyncOverChannel({
        channel: badRole.channel,
        root: roleRoot.asHandle(),
        decide: async () => ({ proceed: true, confirmDeletes: false }),
      }),
    ).rejects.toThrow(SyncProtocolError);

    expect(badRole.sentByDest.map((m) => m.t)).toEqual(["sync-hello", "error"]);
    expect((badRole.sentByDest[1] as CrocMessage).m).toContain(
      'peer role "destination"',
    );
  });

  it("fails clearly when the sender did not advertise folder-sync-v1", async () => {
    const root = new FakeDirHandle("dest");
    const scripted = createScriptedChannel({
      manifest: { entries: [], skipped: [], complete: true },
      files: {},
      peerFeatures: ["small-file-bundles-v1"],
    });

    await expect(
      runSyncOverChannel({
        channel: scripted.channel,
        root: root.asHandle(),
        decide: async () => ({ proceed: true, confirmDeletes: false }),
      }),
    ).rejects.toThrow(SENDER_NOT_SYNC_ERROR);
    expect(scripted.sentByDest).toEqual([]);
  });

  it("rejects unrequested files, wrong sizes, directories, symlinks, and archives", async () => {
    const srcFiles = { "wanted.txt": "abcd" };
    const srcManifest: SyncManifest = {
      entries: [
        {
          path: "wanted.txt",
          size: 4,
          modTime: "2026-11-01T12:00:00Z",
          mode: 0o644,
          kind: SyncKind.File,
        },
      ],
      skipped: [],
      complete: true,
    };

    const tampers: Array<[string, (info: SenderInfoWire) => SenderInfoWire]> = [
      [
        "unrequested file",
        (info) => ({
          ...info,
          FilesToTransfer: [
            ...(info.FilesToTransfer ?? []),
            {
              n: "evil.txt",
              fr: "./",
              h: bytesToBase64(xxhash("x")),
              s: 1,
              md: 0o644,
            },
          ],
        }),
      ],
      [
        "wrong size",
        (info) => ({
          ...info,
          FilesToTransfer: [{ ...info.FilesToTransfer![0], s: 99 }],
        }),
      ],
      [
        "offered directory",
        (info) => ({
          ...info,
          EmptyFoldersToTransfer: [{ fr: "extra-dir/" }],
        }),
      ],
      [
        "offered symlink",
        (info) => ({
          ...info,
          FilesToTransfer: [{ ...info.FilesToTransfer![0], sy: "other" }],
        }),
      ],
      [
        "offered archive (TempFile)",
        (info) => ({
          ...info,
          FilesToTransfer: [{ ...info.FilesToTransfer![0], tf: true }],
        }),
      ],
      [
        "offered text",
        (info) => ({
          ...info,
          SendingText: true,
        }),
      ],
    ];

    for (const [_name, tamper] of tampers) {
      const root = new FakeDirHandle("dest");
      const scripted = createScriptedChannel({
        manifest: srcManifest,
        files: srcFiles,
        tamperOffer: tamper,
      });

      await expect(
        runSyncOverChannel({
          channel: scripted.channel,
          root: root.asHandle(),
          decide: async () => ({ proceed: true, confirmDeletes: false }),
        }),
      ).rejects.toThrow(SyncProtocolError);
      expect(root.getNode("wanted.txt")).toBeUndefined();
    }
  });

  it("rejects when source omits a requested file", async () => {
    const srcFiles = { "a.txt": "a", "b.txt": "b" };
    const srcManifest: SyncManifest = {
      entries: [
        {
          path: "a.txt",
          size: 1,
          modTime: "2026-11-01T12:00:00Z",
          mode: 0o644,
          kind: SyncKind.File,
        },
        {
          path: "b.txt",
          size: 1,
          modTime: "2026-11-01T12:00:00Z",
          mode: 0o644,
          kind: SyncKind.File,
        },
      ],
      skipped: [],
      complete: true,
    };

    const root = new FakeDirHandle("dest");
    const scripted = createScriptedChannel({
      manifest: srcManifest,
      files: srcFiles,
      tamperOffer: (info) => ({
        ...info,
        FilesToTransfer: info.FilesToTransfer!.slice(0, 1),
      }),
    });

    await expect(
      runSyncOverChannel({
        channel: scripted.channel,
        root: root.asHandle(),
        decide: async () => ({ proceed: true, confirmDeletes: false }),
      }),
    ).rejects.toThrow(/did not offer requested file/);
    expect(root.getNode("a.txt")).toBeUndefined();
  });

  it("reports SyncSourceChangedError when source reports a file changed during hashing", async () => {
    const srcFiles = { "a.txt": "a" };
    const srcManifest: SyncManifest = {
      entries: [
        {
          path: "a.txt",
          size: 1,
          modTime: "2026-11-01T12:00:00Z",
          mode: 0o644,
          kind: SyncKind.File,
        },
      ],
      skipped: [],
      complete: true,
    };
    const root = new FakeDirHandle("dest");
    const scripted = createScriptedChannel({
      manifest: srcManifest,
      files: srcFiles,
      changedHashPaths: new Set(["a.txt"]),
    });

    await expect(
      runSyncOverChannel({
        channel: scripted.channel,
        root: root.asHandle(),
        decide: async () => ({ proceed: true, confirmDeletes: false }),
      }),
    ).rejects.toThrow(SyncSourceChangedError);
  });

  it("enforces SyncDestSession state machine invariants directly", async () => {
    let committed = false;
    const env: SyncDestEnv = {
      scanDestination: async () => ({
        entries: [],
        skipped: [],
        complete: true,
      }),
      hashDestination: async () => undefined,
      decide: async () => ({ proceed: true, confirmDeletes: false }),
      commit: async () => {
        committed = true;
        return {
          added: 1,
          updated: 0,
          touched: 0,
          deleted: 0,
          unchanged: 0,
          conflicts: 0,
          bytes: 1,
        };
      },
    };

    const session = new SyncDestSession(env);
    expect(session.start().map((m) => m.t)).toEqual(["sync-hello"]);

    const watchSession = new SyncDestSession(env);
    watchSession.start();
    await expect(
      watchSession.handle(
        encodeHello({ version: 1, role: "source", watch: true }),
      ),
    ).rejects.toThrow(SyncWatchUnsupportedError);

    const src: SyncManifest = {
      entries: [
        {
          path: "one.txt",
          size: 1,
          modTime: "2026-11-01T12:00:00Z",
          mode: 0o644,
          kind: SyncKind.File,
          hash: xxhash("x"),
        },
      ],
      skipped: [],
      complete: true,
    };
    await session.handle(encodeHello({ version: 1, role: "source" }));
    for (const m of encodeManifest(src)) {
      await session.handle(m);
    }
    expect(session.need()).toEqual(["one.txt"]);
    await expect(session.finish()).rejects.toThrow(SyncProtocolError);
    expect(committed).toBe(false);
  });
});
