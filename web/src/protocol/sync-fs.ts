import { bytesEqual, errorMessage } from "./bytes";
import { normalizeFilePath } from "./metadata";
import {
  HASH_SIZE,
  SyncKind,
  isStagingPath,
  pathDepth,
  pathKey,
  resolveTypeChanges,
  sortDeletes,
  type SyncChange,
  type SyncDiffOptions,
  type SyncEntry,
  type SyncFilterSpec,
  type SyncManifest,
  type SyncPlan,
  type SyncSkipped,
} from "./sync-diff";
import type { SyncHashCache } from "./sync-hash-cache";
import type { SyncResult } from "./sync-wire";
import type { ReceiveSink } from "./types";
import { XXH64 } from "./xxh64";

export const GIT_DELETE_BLOCKED_REASON =
  "the source respects .gitignore, which web sync cannot evaluate; deletions are disabled";

export const DEFAULT_MAX_REORDER_BYTES = 16 << 20;

type PermissionStateLike = "granted" | "denied" | "prompt";

type DirectoryHandleWithExtras = FileSystemDirectoryHandle & {
  queryPermission?(descriptor?: {
    mode?: "read" | "readwrite";
  }): Promise<PermissionStateLike>;
  requestPermission?(descriptor?: {
    mode?: "read" | "readwrite";
  }): Promise<PermissionStateLike>;
  entries?(): AsyncIterableIterator<[string, FileSystemHandle]>;
  [Symbol.asyncIterator]?(): AsyncIterableIterator<[string, FileSystemHandle]>;
};

export interface DestinationFileSnapshot {
  kind: "file";
  wirePath: string;
  diskSegments: string[];
  size: number;
  lastModified: number;
}

export interface DestinationDirSnapshot {
  kind: "directory";
  wirePath: string;
  diskSegments: string[];
}

export type DestinationEntrySnapshot =
  | DestinationFileSnapshot
  | DestinationDirSnapshot;

export interface DestinationScan {
  manifest: SyncManifest;
  snapshots: Map<string, DestinationEntrySnapshot>;
  fileHandles: Map<string, FileSystemFileHandle>;
  gitFilterUnsupported: boolean;
}

export interface ApplyDeletesOptions {
  committed: boolean;
  confirmed: boolean;
  hasFailures?: boolean;
  preDeleted?: ReadonlySet<string>;
  signal?: AbortSignal;
}

export interface ApplyDeletesResult {
  deleted: number;
  deletesHeld: number;
  deleteBlocked: string;
  failures: string[];
}

function checkAbort(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new DOMException("Transfer cancelled", "AbortError");
  }
}

function isNotFoundError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "NotFoundError";
}

function isTypeMismatchError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "TypeMismatchError";
}

export async function ensureReadWritePermission(
  root: FileSystemDirectoryHandle,
): Promise<void> {
  const handle = root as DirectoryHandleWithExtras;
  if (typeof handle.queryPermission === "function") {
    const state = await handle.queryPermission({ mode: "readwrite" });
    if (state === "granted") return;
    if (typeof handle.requestPermission === "function") {
      const requested = await handle.requestPermission({ mode: "readwrite" });
      if (requested === "granted") return;
    }
    throw new Error("Write permission for the destination folder was denied");
  }
  if (typeof handle.requestPermission === "function") {
    const requested = await handle.requestPermission({ mode: "readwrite" });
    if (requested !== "granted") {
      throw new Error("Write permission for the destination folder was denied");
    }
  }
}

async function listDirectoryEntries(
  dir: FileSystemDirectoryHandle,
): Promise<Array<[string, FileSystemHandle]>> {
  const extra = dir as DirectoryHandleWithExtras;
  const iter =
    typeof extra.entries === "function"
      ? extra.entries()
      : typeof extra[Symbol.asyncIterator] === "function"
        ? extra[Symbol.asyncIterator]()
        : undefined;
  if (!iter) {
    throw new Error("Directory handle does not support entry iteration");
  }
  const out: Array<[string, FileSystemHandle]> = [];
  for await (const [name, handle] of iter) {
    out.push([name, handle]);
  }
  out.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  return out;
}

function normalizeFilterPath(raw: string): string {
  const trimmed = raw.trim().replaceAll("\\", "/");
  if (trimmed === "") return "";
  const parts: string[] = [];
  for (const part of trimmed.split("/")) {
    if (part === "" || part === ".") continue;
    if (part === "..") {
      parts.pop();
      continue;
    }
    parts.push(part);
  }
  return parts.join("/");
}

class SyncFilterMatcher {
  private readonly excludes: string[];
  private readonly excludeExact: Set<string>;

  constructor(spec?: SyncFilterSpec) {
    const seenExclude = new Set<string>();
    this.excludes = [];
    for (const raw of spec?.exclude ?? []) {
      const value = raw.trim().toLowerCase();
      if (value && !seenExclude.has(value)) {
        seenExclude.add(value);
        this.excludes.push(value);
      }
    }

    this.excludeExact = new Set<string>();
    for (const raw of spec?.excludeExact ?? []) {
      const value = normalizeFilterPath(raw);
      if (value) this.excludeExact.add(value);
    }
  }

  matches(wirePath: string): boolean {
    if (this.excludeExact.size > 0) {
      let cur = wirePath;
      for (;;) {
        if (this.excludeExact.has(cur)) return true;
        const idx = cur.lastIndexOf("/");
        if (idx < 0) break;
        cur = cur.slice(0, idx);
      }
    }
    if (this.excludes.length > 0) {
      const lower = wirePath.toLowerCase();
      for (const sub of this.excludes) {
        if (lower.includes(sub)) return true;
      }
    }
    return false;
  }
}

export function sortManifest(manifest: SyncManifest): void {
  manifest.entries.sort((a, b) =>
    a.path < b.path ? -1 : a.path > b.path ? 1 : 0,
  );
  manifest.skipped.sort((a, b) => {
    if (a.path !== b.path) return a.path < b.path ? -1 : 1;
    const ad = a.diskPath ?? "";
    const bd = b.diskPath ?? "";
    if (ad !== bd) return ad < bd ? -1 : 1;
    return a.reason < b.reason ? -1 : a.reason > b.reason ? 1 : 0;
  });
}

export async function scanDestination(
  root: FileSystemDirectoryHandle,
  filter?: SyncFilterSpec,
  signal?: AbortSignal,
): Promise<DestinationScan> {
  checkAbort(signal);
  const matcher = new SyncFilterMatcher(filter);
  const entries: SyncEntry[] = [];
  const skipped: SyncSkipped[] = [];
  const snapshots = new Map<string, DestinationEntrySnapshot>();
  const fileHandles = new Map<string, FileSystemFileHandle>();
  const seenByKey = new Map<string, string>();
  let complete = true;

  const walk = async (
    dir: FileSystemDirectoryHandle,
    parentWire: string,
    parentDisk: string,
    parentSegments: string[],
  ): Promise<void> => {
    checkAbort(signal);
    let children: Array<[string, FileSystemHandle]>;
    try {
      children = await listDirectoryEntries(dir);
    } catch (error) {
      if (parentWire === "") throw error;
      complete = false;
      skipped.push({
        path: parentWire,
        diskPath: parentDisk,
        reason: "read_error",
        detail: errorMessage(error),
      });
      return;
    }

    for (const [rawName, handle] of children) {
      checkAbort(signal);
      if (isStagingPath(rawName)) {
        continue;
      }
      const diskPath = parentDisk ? `${parentDisk}/${rawName}` : rawName;
      const diskSegments = [...parentSegments, rawName];

      let wirePath: string;
      try {
        wirePath = normalizeFilePath(parentWire || ".", rawName).path;
      } catch {
        skipped.push({
          path: "",
          diskPath,
          reason: "invalid_name",
          detail: rawName,
        });
        continue;
      }

      if (matcher.matches(wirePath)) {
        skipped.push({
          path: wirePath,
          diskPath,
          reason: "filtered",
        });
        continue;
      }

      const key = pathKey(wirePath);
      const existing = seenByKey.get(key);
      if (existing !== undefined) {
        skipped.push({
          path: wirePath,
          diskPath,
          reason: "invalid_name",
          detail: `collides with ${existing}`,
        });
        continue;
      }
      seenByKey.set(key, wirePath);

      if (handle.kind === "directory") {
        entries.push({
          path: wirePath,
          size: 0,
          modTime: "",
          mode: 0o755,
          kind: SyncKind.Directory,
        });
        snapshots.set(wirePath, {
          kind: "directory",
          wirePath,
          diskSegments,
        });
        await walk(
          handle as FileSystemDirectoryHandle,
          wirePath,
          diskPath,
          diskSegments,
        );
      } else if (handle.kind === "file") {
        const fileHandle = handle as FileSystemFileHandle;
        let file: File;
        try {
          file = await fileHandle.getFile();
        } catch (error) {
          complete = false;
          skipped.push({
            path: wirePath,
            diskPath,
            reason: "read_error",
            detail: errorMessage(error),
          });
          continue;
        }
        entries.push({
          path: wirePath,
          size: file.size,
          modTime: new Date(file.lastModified).toISOString(),
          mode: 0o644,
          kind: SyncKind.File,
        });
        snapshots.set(wirePath, {
          kind: "file",
          wirePath,
          diskSegments,
          size: file.size,
          lastModified: file.lastModified,
        });
        fileHandles.set(wirePath, fileHandle);
      } else {
        skipped.push({
          path: wirePath,
          diskPath,
          reason: "special",
        });
      }
    }
  };

  await walk(root, "", "", []);

  const manifest: SyncManifest = {
    entries,
    skipped,
    complete,
  };
  if (filter) manifest.filter = filter;
  sortManifest(manifest);

  return {
    manifest,
    snapshots,
    fileHandles,
    gitFilterUnsupported: Boolean(filter?.git),
  };
}

export function isDirToFile(change: SyncChange): boolean {
  return Boolean(
    change.typeChanged &&
      change.dst?.kind === SyncKind.Directory &&
      change.src?.kind === SyncKind.File,
  );
}

export function applyWebDeletePolicy(
  plan: SyncPlan,
  scan: DestinationScan,
  opts: SyncDiffOptions,
): void {
  if (!scan.gitFilterUnsupported || !opts.delete) return;
  if (!plan.deleteBlockedReason) {
    plan.deleteBlockedReason = GIT_DELETE_BLOCKED_REASON;
  }
  plan.delete = [];
  plan.requireDeleteConfirm = false;

  const dirToFileIdx: number[] = [];
  for (let i = 0; i < plan.update.length; i += 1) {
    if (isDirToFile(plan.update[i])) {
      dirToFileIdx.push(i);
    }
  }
  resolveTypeChanges(plan, scan.manifest, dirToFileIdx);
}

export async function hashFileXXH64(
  file: Blob,
  signal?: AbortSignal,
): Promise<Uint8Array> {
  checkAbort(signal);
  const hasher = new XXH64();
  if (typeof file.stream === "function") {
    const reader = file.stream().getReader();
    try {
      for (;;) {
        checkAbort(signal);
        const { done, value } = await reader.read();
        checkAbort(signal);
        if (done) break;
        if (value) hasher.update(value);
      }
      return hasher.digest();
    } finally {
      await reader.cancel().catch(() => undefined);
      reader.releaseLock();
    }
  }
  const buffer = new Uint8Array(await file.arrayBuffer());
  for (let offset = 0; offset < buffer.byteLength; offset += 32 << 10) {
    checkAbort(signal);
    hasher.update(
      buffer.subarray(offset, Math.min(buffer.byteLength, offset + (32 << 10))),
    );
  }
  return hasher.digest();
}

export async function hashDestinationEntries(
  root: FileSystemDirectoryHandle,
  scan: DestinationScan,
  paths: string[],
  options?: {
    hashCache?: SyncHashCache;
    computeHash?: (file: File, signal?: AbortSignal) => Promise<Uint8Array>;
    signal?: AbortSignal;
  },
): Promise<void> {
  const compute = options?.computeHash ?? hashFileXXH64;
  const cache = options?.hashCache;
  const signal = options?.signal;

  const byPath = new Map<string, SyncEntry>();
  for (const entry of scan.manifest.entries) {
    byPath.set(entry.path, entry);
  }

  let dirId: string | undefined;
  if (cache) {
    try {
      dirId = await cache.resolveDirId(root);
    } catch {
      dirId = undefined;
    }
  }

  for (const path of paths) {
    checkAbort(signal);
    const entry = byPath.get(path);
    const handle = scan.fileHandles.get(path);
    if (!entry || entry.kind !== SyncKind.File || !handle) {
      throw new Error(`Destination file not found during hashing: ${path}`);
    }
    const file = await handle.getFile();
    let hash: Uint8Array | undefined;
    if (cache && dirId) {
      try {
        hash = await cache.get(dirId, path, file.size, file.lastModified);
      } catch {
        hash = undefined;
      }
    }
    if (!hash) {
      hash = await compute(file, signal);
      if (cache && dirId) {
        try {
          await cache.put(dirId, path, file.size, file.lastModified, hash);
        } catch {
          // Cache write failures must not abort sync.
        }
      }
    }
    entry.hash = hash;
  }
}

export class ReorderXXH64 {
  private readonly hasher = new XXH64();
  private readonly pending = new Map<number, Uint8Array<ArrayBuffer>>();
  private nextOffset = 0;
  private pendingBytes = 0;
  private totalWritten = 0;
  private digestBytes?: Uint8Array;

  constructor(
    private readonly expectedSize: number,
    private readonly maxBufferBytes = DEFAULT_MAX_REORDER_BYTES,
  ) {}

  add(position: number, bytes: Uint8Array): void {
    if (this.digestBytes) {
      throw new Error("Hasher is already finalized");
    }
    if (
      position < 0 ||
      !Number.isSafeInteger(position) ||
      position + bytes.byteLength > this.expectedSize
    ) {
      throw new Error("Chunk is outside the expected file size");
    }
    if (bytes.byteLength === 0) return;
    if (position < this.nextOffset) {
      throw new Error(`Duplicate or overlapping chunk at offset ${position}`);
    }
    if (position === this.nextOffset) {
      this.hasher.update(bytes);
      this.nextOffset += bytes.byteLength;
      this.totalWritten += bytes.byteLength;
      this.drain();
      return;
    }
    if (this.pending.has(position)) {
      throw new Error(`Duplicate chunk at offset ${position}`);
    }
    if (this.pendingBytes + bytes.byteLength > this.maxBufferBytes) {
      throw new Error(
        `Out-of-order chunk buffer exceeded ${this.maxBufferBytes} bytes`,
      );
    }
    const copy = bytes.slice();
    this.pending.set(position, copy);
    this.pendingBytes += copy.byteLength;
    this.totalWritten += copy.byteLength;
  }

  private drain(): void {
    while (this.pending.has(this.nextOffset)) {
      const chunk = this.pending.get(this.nextOffset)!;
      this.pending.delete(this.nextOffset);
      this.pendingBytes -= chunk.byteLength;
      this.hasher.update(chunk);
      this.nextOffset += chunk.byteLength;
    }
    for (const key of this.pending.keys()) {
      if (key < this.nextOffset) {
        throw new Error(`Overlapping chunk at offset ${key}`);
      }
    }
  }

  finalize(): Uint8Array {
    if (this.digestBytes) return this.digestBytes;
    if (
      this.pending.size > 0 ||
      this.nextOffset !== this.expectedSize ||
      this.totalWritten !== this.expectedSize
    ) {
      throw new Error(
        `Incomplete file data: hashed ${this.nextOffset} of ${this.expectedSize} bytes`,
      );
    }
    this.digestBytes = this.hasher.digest();
    return this.digestBytes;
  }

  clear(): void {
    this.pending.clear();
    this.pendingBytes = 0;
  }
}

function splitWirePath(wirePath: string): { parent: string[]; name: string } {
  const parts = wirePath.split("/").filter(Boolean);
  const name = parts.pop();
  if (!name) throw new Error(`Invalid sync path: ${wirePath}`);
  return { parent: parts, name };
}

async function resolveDirectory(
  root: FileSystemDirectoryHandle,
  segments: string[],
  create: boolean,
): Promise<FileSystemDirectoryHandle> {
  let dir = root;
  for (const segment of segments) {
    dir = await dir.getDirectoryHandle(segment, { create });
  }
  return dir;
}

async function entryExists(
  dir: FileSystemDirectoryHandle,
  name: string,
): Promise<"file" | "directory" | "none"> {
  try {
    await dir.getFileHandle(name, { create: false });
    return "file";
  } catch (error) {
    if (isTypeMismatchError(error)) return "directory";
    if (!isNotFoundError(error)) throw error;
  }
  try {
    await dir.getDirectoryHandle(name, { create: false });
    return "directory";
  } catch (error) {
    if (isTypeMismatchError(error)) return "file";
    if (!isNotFoundError(error)) throw error;
  }
  return "none";
}

export interface SyncFileSinkOptions {
  root: FileSystemDirectoryHandle;
  wirePath: string;
  expectedSize: number;
  expectedHash: Uint8Array;
  snapshot?: DestinationFileSnapshot;
  maxReorderBytes?: number;
  hashCache?: SyncHashCache;
}

export class SyncFileSink implements ReceiveSink {
  private readonly reorder: ReorderXXH64;
  private digest?: Uint8Array;
  private closed = false;
  private committed = false;

  private constructor(
    private readonly root: FileSystemDirectoryHandle,
    private readonly parent: FileSystemDirectoryHandle,
    private readonly name: string,
    private readonly wirePath: string,
    private readonly handle: FileSystemFileHandle,
    private readonly writable: FileSystemWritableFileStream,
    private readonly expectedHash: Uint8Array,
    private readonly snapshot: DestinationFileSnapshot | undefined,
    private readonly createdNew: boolean,
    private readonly createdMeta:
      | { size: number; lastModified: number }
      | undefined,
    expectedSize: number,
    maxReorderBytes: number,
    private readonly hashCache?: SyncHashCache,
  ) {
    this.reorder = new ReorderXXH64(expectedSize, maxReorderBytes);
  }

  static async open(options: SyncFileSinkOptions): Promise<SyncFileSink> {
    const {
      root,
      wirePath,
      expectedSize,
      expectedHash,
      snapshot,
      maxReorderBytes = DEFAULT_MAX_REORDER_BYTES,
      hashCache,
    } = options;
    if (expectedHash.byteLength !== HASH_SIZE) {
      throw new Error(`Missing source hash for "${wirePath}"`);
    }
    const { parent: parentSegments, name } = splitWirePath(wirePath);
    const parent = await resolveDirectory(root, parentSegments, true);

    if (!snapshot) {
      const existing = await entryExists(parent, name);
      if (existing !== "none") {
        throw new Error(`"${wirePath}" changed since the scan`);
      }
    } else {
      let existingHandle: FileSystemFileHandle;
      try {
        existingHandle = await parent.getFileHandle(name, { create: false });
      } catch (error) {
        if (isNotFoundError(error) || isTypeMismatchError(error)) {
          throw new Error(`"${wirePath}" changed since the scan`);
        }
        throw error;
      }
      const current = await existingHandle.getFile();
      if (
        current.size !== snapshot.size ||
        current.lastModified !== snapshot.lastModified
      ) {
        throw new Error(`"${wirePath}" changed since the scan`);
      }
    }

    const handle = await parent.getFileHandle(name, { create: true });
    let createdMeta: { size: number; lastModified: number } | undefined;
    const createdNew = !snapshot;
    if (createdNew) {
      const initial = await handle.getFile();
      createdMeta = { size: initial.size, lastModified: initial.lastModified };
    }

    let writable: FileSystemWritableFileStream;
    try {
      writable = await handle.createWritable({ keepExistingData: false });
    } catch (error) {
      if (createdNew) {
        await parent.removeEntry(name).catch(() => undefined);
      }
      throw error;
    }

    return new SyncFileSink(
      root,
      parent,
      name,
      wirePath,
      handle,
      writable,
      expectedHash,
      snapshot,
      createdNew,
      createdMeta,
      expectedSize,
      maxReorderBytes,
      hashCache,
    );
  }

  async writeAt(position: number, bytes: Uint8Array): Promise<void> {
    if (this.closed) throw new Error("Destination file is already closed");
    try {
      this.reorder.add(position, bytes);
      await this.writable.write({
        type: "write",
        position,
        data: bytes.slice(),
      });
    } catch (error) {
      await this.abort();
      throw error;
    }
  }

  async finalize(): Promise<void> {
    if (this.closed) throw new Error("Destination file is already closed");
    if (this.digest) return;
    try {
      this.digest = this.reorder.finalize();
    } catch (error) {
      await this.abort();
      throw error;
    }
  }

  async hash(
    algorithm: "xxhash" | "sha256" = "xxhash",
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    checkAbort(signal);
    if (algorithm !== "xxhash") {
      throw new Error("Sync destination supports xxhash verification only");
    }
    if (!this.digest) {
      throw new Error("Destination must be finalized before hashing");
    }
    return this.digest;
  }

  async commit(): Promise<void> {
    if (this.committed) return;
    if (this.closed) throw new Error("Destination file is already closed");
    if (!this.digest) {
      await this.abort();
      throw new Error("Destination must be finalized before commit");
    }
    if (!bytesEqual(this.digest, this.expectedHash)) {
      await this.abort();
      throw new Error(
        `Received data does not match the source hash for "${this.wirePath}"`,
      );
    }

    try {
      let latestHandle: FileSystemFileHandle;
      try {
        latestHandle = await this.parent.getFileHandle(this.name, {
          create: false,
        });
      } catch (error) {
        if (isNotFoundError(error) || isTypeMismatchError(error)) {
          throw new Error(`"${this.wirePath}" changed since the scan`);
        }
        throw error;
      }
      const current = await latestHandle.getFile();
      if (this.snapshot) {
        if (
          current.size !== this.snapshot.size ||
          current.lastModified !== this.snapshot.lastModified
        ) {
          throw new Error(`"${this.wirePath}" changed since the scan`);
        }
      } else if (this.createdMeta) {
        if (
          current.size !== this.createdMeta.size ||
          current.lastModified !== this.createdMeta.lastModified
        ) {
          throw new Error(`"${this.wirePath}" changed since the scan`);
        }
      }
    } catch (error) {
      await this.abort();
      throw error;
    }

    this.closed = true;
    try {
      await this.writable.close();
      this.committed = true;
    } catch (error) {
      await this.writable.abort().catch(() => undefined);
      throw error;
    }

    if (this.hashCache) {
      try {
        const committedFile = await this.handle.getFile();
        await this.hashCache.putForRoot(
          this.root,
          this.wirePath,
          committedFile.size,
          committedFile.lastModified,
          this.digest,
        );
      } catch {
        // Cache updates are best-effort.
      }
    }
  }

  async abort(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.reorder.clear();
    await this.writable.abort().catch(() => undefined);
    if (this.createdNew && !this.committed) {
      try {
        const current = await this.handle.getFile();
        if (
          !this.createdMeta ||
          (current.size === this.createdMeta.size &&
            current.lastModified === this.createdMeta.lastModified)
        ) {
          await this.parent.removeEntry(this.name);
        }
      } catch {
        // Ignore cleanup errors on abort.
      }
    }
  }
}

class DeferredDirToFileSink implements ReceiveSink {
  private readonly reorder: ReorderXXH64;
  private buffer?: Uint8Array<ArrayBuffer>;
  private digest?: Uint8Array;
  private aborted = false;

  constructor(
    private readonly wirePath: string,
    private readonly expectedSize: number,
    private readonly expectedHash: Uint8Array,
    maxReorderBytes: number,
    private readonly onVerified: (
      bytes: Uint8Array<ArrayBuffer>,
      digest: Uint8Array,
    ) => void,
    private readonly onFailure: (error: unknown) => void,
    private readonly swallowCommitError: boolean,
  ) {
    this.reorder = new ReorderXXH64(expectedSize, maxReorderBytes);
    this.buffer = new Uint8Array(expectedSize);
  }

  async writeAt(position: number, bytes: Uint8Array): Promise<void> {
    if (this.aborted || !this.buffer) {
      throw new Error("Deferred sink is already closed");
    }
    this.reorder.add(position, bytes);
    this.buffer.set(bytes, position);
  }

  async finalize(): Promise<void> {
    if (this.aborted) throw new Error("Deferred sink is already closed");
    this.digest = this.reorder.finalize();
  }

  async hash(
    algorithm: "xxhash" | "sha256" = "xxhash",
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    checkAbort(signal);
    if (algorithm !== "xxhash" || !this.digest) {
      throw new Error("Deferred sink is not ready for hashing");
    }
    return this.digest;
  }

  async commit(): Promise<void> {
    if (this.aborted || !this.buffer || !this.digest) {
      const err = new Error(`Deferred file "${this.wirePath}" not finalized`);
      this.onFailure(err);
      if (!this.swallowCommitError) throw err;
      return;
    }
    if (
      this.buffer.byteLength !== this.expectedSize ||
      !bytesEqual(this.digest, this.expectedHash)
    ) {
      const err = new Error(
        `Received data does not match the source hash for "${this.wirePath}"`,
      );
      await this.abort();
      this.onFailure(err);
      if (!this.swallowCommitError) throw err;
      return;
    }
    this.onVerified(this.buffer, this.digest);
    this.buffer = undefined;
  }

  async abort(): Promise<void> {
    this.aborted = true;
    this.reorder.clear();
    this.buffer = undefined;
  }
}

class FailedDiscardingSink implements ReceiveSink {
  private readonly reorder: ReorderXXH64;
  private digest?: Uint8Array;

  constructor(expectedSize: number, maxReorderBytes: number) {
    this.reorder = new ReorderXXH64(expectedSize, maxReorderBytes);
  }

  async writeAt(position: number, bytes: Uint8Array): Promise<void> {
    this.reorder.add(position, bytes);
  }

  async finalize(): Promise<void> {
    this.digest = this.reorder.finalize();
  }

  async hash(algorithm: "xxhash" | "sha256" = "xxhash"): Promise<Uint8Array> {
    if (algorithm !== "xxhash" || !this.digest) {
      throw new Error("Sink is not finalized");
    }
    return this.digest;
  }

  async commit(): Promise<void> {
    // Already recorded as failed; keep destination untouched.
  }

  async abort(): Promise<void> {
    this.reorder.clear();
  }
}

async function deleteOneEntry(
  root: FileSystemDirectoryHandle,
  change: SyncChange,
  scan: DestinationScan,
): Promise<void> {
  if (!change.dst) {
    throw new Error(`delete "${change.path}" has no destination snapshot`);
  }
  const { parent: parentSegments, name } = splitWirePath(change.path);
  let parent: FileSystemDirectoryHandle;
  try {
    parent = await resolveDirectory(root, parentSegments, false);
  } catch (error) {
    if (isNotFoundError(error)) return;
    throw error;
  }

  const snap = scan.snapshots.get(change.path);

  if (change.dst.kind === SyncKind.Directory) {
    let dirHandle: FileSystemDirectoryHandle;
    try {
      dirHandle = await parent.getDirectoryHandle(name, { create: false });
    } catch (error) {
      if (isNotFoundError(error)) return;
      if (isTypeMismatchError(error)) {
        throw new Error("changed since the scan: no longer a directory");
      }
      throw error;
    }
    const children = await listDirectoryEntries(dirHandle);
    if (children.length > 0) {
      throw new Error("directory is not empty");
    }
    // Never pass { recursive: true }.
    await parent.removeEntry(name);
    return;
  }

  if (change.dst.kind === SyncKind.File) {
    let fileHandle: FileSystemFileHandle;
    try {
      fileHandle = await parent.getFileHandle(name, { create: false });
    } catch (error) {
      if (isNotFoundError(error)) return;
      if (isTypeMismatchError(error)) {
        throw new Error("changed since the scan: no longer a file");
      }
      throw error;
    }
    const file = await fileHandle.getFile();
    const expectedSize =
      snap?.kind === "file" ? snap.size : change.dst.size;
    const expectedModMs =
      snap?.kind === "file"
        ? snap.lastModified
        : Date.parse(change.dst.modTime);
    if (file.size !== expectedSize || file.lastModified !== expectedModMs) {
      throw new Error("changed since the scan");
    }
    // Never pass { recursive: true }.
    await parent.removeEntry(name);
    return;
  }

  throw new Error(`unsupported entry kind ${change.dst.kind}`);
}

export async function applyDeletes(
  root: FileSystemDirectoryHandle,
  scan: DestinationScan,
  plan: SyncPlan,
  options: ApplyDeletesOptions,
): Promise<ApplyDeletesResult> {
  const preDeletedCount = options.preDeleted?.size ?? 0;
  const remaining = plan.delete.filter(
    (d) => !options.preDeleted?.has(d.path),
  );
  if (remaining.length === 0) {
    return {
      deleted: preDeletedCount,
      deletesHeld: 0,
      deleteBlocked: plan.deleteBlockedReason,
      failures: [],
    };
  }
  if (!options.committed) {
    throw new Error(
      "foldersync: deletion attempted before all files were committed",
    );
  }
  if (options.hasFailures || plan.conflicts.length > 0) {
    return {
      deleted: preDeletedCount,
      deletesHeld: remaining.length,
      deleteBlocked:
        plan.deleteBlockedReason ||
        "some changes failed to commit; deletions held until a clean round",
      failures: [],
    };
  }
  if (plan.deleteBlockedReason !== "") {
    return {
      deleted: preDeletedCount,
      deletesHeld: remaining.length,
      deleteBlocked: plan.deleteBlockedReason,
      failures: [],
    };
  }
  if (plan.requireDeleteConfirm && !options.confirmed) {
    return {
      deleted: preDeletedCount,
      deletesHeld: remaining.length,
      deleteBlocked: "",
      failures: [],
    };
  }

  const ordered = sortDeletes([...remaining]);
  let deleted = preDeletedCount;
  const failures: string[] = [];

  for (const change of ordered) {
    checkAbort(options.signal);
    try {
      await deleteOneEntry(root, change, scan);
      deleted += 1;
    } catch (error) {
      failures.push(`${change.path}: ${errorMessage(error)}`);
    }
  }

  return {
    deleted,
    deletesHeld: 0,
    deleteBlocked: "",
    failures,
  };
}

async function verifyDirDeleteTree(
  root: FileSystemDirectoryHandle,
  dirPath: string,
  expected: Map<string, SyncChange>,
  scan: DestinationScan,
  signal?: AbortSignal,
): Promise<void> {
  const { parent: parentSegments, name } = splitWirePath(dirPath);
  const parent = await resolveDirectory(root, parentSegments, false);
  const dirHandle = await parent.getDirectoryHandle(name, { create: false });
  const seen = new Set<string>();

  const walk = async (
    handle: FileSystemDirectoryHandle,
    curPath: string,
  ): Promise<void> => {
    checkAbort(signal);
    const children = await listDirectoryEntries(handle);
    for (const [childName, childHandle] of children) {
      checkAbort(signal);
      const childPath = `${curPath}/${childName}`;
      const planned = expected.get(childPath);
      if (!planned || !planned.dst) {
        throw new Error(
          `unexpected entry "${childPath}" in directory "${dirPath}"`,
        );
      }
      if (seen.has(childPath)) {
        throw new Error(
          `duplicate entry "${childPath}" in directory "${dirPath}"`,
        );
      }
      seen.add(childPath);

      const snap = scan.snapshots.get(childPath);
      if (planned.dst.kind === SyncKind.Directory) {
        if (childHandle.kind !== "directory") {
          throw new Error(`"${childPath}" changed since the scan`);
        }
        await walk(childHandle as FileSystemDirectoryHandle, childPath);
      } else if (planned.dst.kind === SyncKind.File) {
        if (childHandle.kind !== "file") {
          throw new Error(`"${childPath}" changed since the scan`);
        }
        const file = await (childHandle as FileSystemFileHandle).getFile();
        const expectedSize =
          snap?.kind === "file" ? snap.size : planned.dst.size;
        const expectedModMs =
          snap?.kind === "file"
            ? snap.lastModified
            : Date.parse(planned.dst.modTime);
        if (file.size !== expectedSize || file.lastModified !== expectedModMs) {
          throw new Error(`"${childPath}" changed since the scan`);
        }
      } else {
        throw new Error(`unsupported entry kind in "${dirPath}"`);
      }
    }
  };

  await walk(dirHandle, dirPath);
  if (seen.size !== expected.size) {
    throw new Error(`directory "${dirPath}" contents changed since the scan`);
  }
}

export class SyncFsCommitter {
  private added = 0;
  private updated = 0;
  private bytes = 0;
  private readonly failures: string[] = [];
  private readonly preDeleted = new Set<string>();
  private readonly deferredDirToFile = new Map<
    string,
    { bytes: Uint8Array<ArrayBuffer>; digest: Uint8Array }
  >();
  private prepared = false;
  private permissionChecked = false;
  private readonly maxReorderBytes: number;
  private readonly hashCache?: SyncHashCache;
  private readonly signal?: AbortSignal;

  constructor(
    private readonly root: FileSystemDirectoryHandle,
    private readonly scan: DestinationScan,
    private readonly plan: SyncPlan,
    options?: {
      hashCache?: SyncHashCache;
      maxReorderBytes?: number;
      signal?: AbortSignal;
    },
  ) {
    this.hashCache = options?.hashCache;
    this.maxReorderBytes =
      options?.maxReorderBytes ?? DEFAULT_MAX_REORDER_BYTES;
    this.signal = options?.signal;
    for (const conflict of plan.conflicts) {
      this.failures.push(`${conflict.path}: ${conflict.reason ?? "conflict"}`);
    }
  }

  hasFailures(): boolean {
    return this.failures.length > 0 || this.plan.conflicts.length > 0;
  }

  recordFailure(path: string, error: unknown): void {
    this.failures.push(`${path}: ${errorMessage(error)}`);
  }

  async ensurePermission(): Promise<void> {
    if (this.permissionChecked) return;
    await ensureReadWritePermission(this.root);
    this.permissionChecked = true;
  }

  async prepareDirectories(): Promise<void> {
    if (this.prepared) return;
    await this.ensurePermission();
    this.prepared = true;

    const dirChanges = [...this.plan.add, ...this.plan.update]
      .filter((ch) => ch.src?.kind === SyncKind.Directory)
      .sort((a, b) => pathDepth(a.path) - pathDepth(b.path));

    for (const ch of dirChanges) {
      checkAbort(this.signal);
      try {
        const { parent: parentSegments, name } = splitWirePath(ch.path);
        const parent = await resolveDirectory(this.root, parentSegments, true);
        if (!ch.dst) {
          const existing = await entryExists(parent, name);
          if (existing !== "none") {
            throw new Error("changed since the scan");
          }
        } else if (ch.typeChanged && ch.dst.kind === SyncKind.File) {
          const snap = this.scan.snapshots.get(ch.dst.path);
          let fileHandle: FileSystemFileHandle;
          try {
            fileHandle = await parent.getFileHandle(name, { create: false });
          } catch (error) {
            if (isNotFoundError(error) || isTypeMismatchError(error)) {
              throw new Error("changed since the scan");
            }
            throw error;
          }
          const file = await fileHandle.getFile();
          const expectedSize =
            snap?.kind === "file" ? snap.size : ch.dst.size;
          const expectedModMs =
            snap?.kind === "file"
              ? snap.lastModified
              : Date.parse(ch.dst.modTime);
          if (
            file.size !== expectedSize ||
            file.lastModified !== expectedModMs
          ) {
            throw new Error("changed since the scan");
          }
          await parent.removeEntry(name);
        }
        await parent.getDirectoryHandle(name, { create: true });
        if (!ch.dst || ch.typeChanged) {
          this.added += 1;
        } else {
          this.updated += 1;
        }
      } catch (error) {
        this.recordFailure(ch.path, error);
      }
    }
  }

  async openSinkForChange(
    change: SyncChange,
    options?: { swallowCommitError?: boolean },
  ): Promise<ReceiveSink> {
    await this.ensurePermission();
    const swallowCommitError = Boolean(options?.swallowCommitError);
    if (!change.src || change.src.kind !== SyncKind.File) {
      throw new Error(`Cannot open file sink for non-file "${change.path}"`);
    }
    const src = change.src;
    const expectedHash = src.hash ?? new Uint8Array();

    if (isDirToFile(change)) {
      return new DeferredDirToFileSink(
        change.path,
        src.size,
        expectedHash,
        this.maxReorderBytes,
        (bytes, digest) => {
          this.deferredDirToFile.set(change.path, { bytes, digest });
        },
        (err) => {
          this.recordFailure(change.path, err);
        },
        swallowCommitError,
      );
    }

    const snap =
      change.dst && change.dst.kind === SyncKind.File
        ? this.scan.snapshots.get(change.dst.path)
        : undefined;
    const fileSnap = snap?.kind === "file" ? snap : undefined;

    let inner: SyncFileSink;
    try {
      inner = await SyncFileSink.open({
        root: this.root,
        wirePath: change.path,
        expectedSize: src.size,
        expectedHash,
        snapshot: fileSnap,
        maxReorderBytes: this.maxReorderBytes,
        hashCache: this.hashCache,
      });
    } catch (error) {
      if (swallowCommitError) {
        this.recordFailure(change.path, error);
        return new FailedDiscardingSink(src.size, this.maxReorderBytes);
      }
      throw error;
    }

    const committer = this;
    return {
      writeAt(position, bytes) {
        return inner.writeAt(position, bytes);
      },
      finalize() {
        return inner.finalize();
      },
      hash(algorithm, signal) {
        return inner.hash(algorithm, signal);
      },
      async commit() {
        try {
          await inner.commit();
          committer.bytes += src.size;
          if (!change.dst || change.typeChanged) {
            committer.added += 1;
          } else {
            committer.updated += 1;
          }
        } catch (error) {
          committer.recordFailure(change.path, error);
          if (!swallowCommitError) throw error;
        }
      },
      abort() {
        return inner.abort();
      },
    };
  }

  private async commitDirToFile(
    change: SyncChange,
    confirmDeletes: boolean,
  ): Promise<void> {
    if (this.hasFailures()) {
      this.recordFailure(
        change.path,
        "skipped replacing directory: earlier changes failed",
      );
      return;
    }
    const staged = this.deferredDirToFile.get(change.path);
    if (!staged || !change.src || !change.dst) {
      this.recordFailure(change.path, "staged file data is missing");
      return;
    }
    if (!confirmDeletes && this.plan.requireDeleteConfirm) {
      this.recordFailure(
        change.path,
        "replacing directory needs delete confirmation",
      );
      return;
    }

    const prefix = `${change.dst.path}/`;
    const sub: SyncChange[] = [];
    const expected = new Map<string, SyncChange>();
    for (const del of this.plan.delete) {
      if (del.path.startsWith(prefix)) {
        sub.push(del);
        expected.set(del.path, del);
      }
    }
    if (sub.length > 0 && this.plan.deleteBlockedReason !== "") {
      this.recordFailure(
        change.path,
        `deletion blocked: ${this.plan.deleteBlockedReason}`,
      );
      return;
    }

    try {
      await verifyDirDeleteTree(
        this.root,
        change.dst.path,
        expected,
        this.scan,
        this.signal,
      );
      if (sub.length > 0) {
        const subOutcome = await applyDeletes(
          this.root,
          this.scan,
          { ...this.plan, delete: sub, deleteBlockedReason: "", requireDeleteConfirm: false },
          {
            committed: true,
            confirmed: true,
            hasFailures: false,
            signal: this.signal,
          },
        );
        if (subOutcome.failures.length > 0) {
          throw new Error(`clear directory: ${subOutcome.failures[0]}`);
        }
        for (const d of sub) this.preDeleted.add(d.path);
      }
      const { parent: parentSegments, name } = splitWirePath(change.dst.path);
      const parent = await resolveDirectory(this.root, parentSegments, false);
      await parent.removeEntry(name);

      const target = splitWirePath(change.path);
      const targetParent = await resolveDirectory(
        this.root,
        target.parent,
        true,
      );
      const fileHandle = await targetParent.getFileHandle(target.name, {
        create: true,
      });
      const writable = await fileHandle.createWritable({
        keepExistingData: false,
      });
      try {
        if (staged.bytes.byteLength > 0) {
          await writable.write({
            type: "write",
            position: 0,
            data: staged.bytes,
          });
        }
        await writable.close();
      } catch (error) {
        await writable.abort().catch(() => undefined);
        throw error;
      }
      this.added += 1;
      this.bytes += change.src.size;
      if (this.hashCache) {
        try {
          const committedFile = await fileHandle.getFile();
          await this.hashCache.putForRoot(
            this.root,
            change.path,
            committedFile.size,
            committedFile.lastModified,
            staged.digest,
          );
        } catch {
          // Best-effort cache update.
        }
      }
    } catch (error) {
      this.recordFailure(change.path, error);
    } finally {
      this.deferredDirToFile.delete(change.path);
    }
  }

  async finish(decision: {
    proceed: boolean;
    confirmDeletes: boolean;
  }): Promise<SyncResult> {
    if (!decision.proceed) {
      const result: SyncResult = {
        dryRun: true,
        added: 0,
        updated: 0,
        touched: 0,
        deleted: 0,
        unchanged: this.plan.skip.length,
        conflicts: 0,
        bytes: 0,
      };
      if (this.plan.deleteBlockedReason) {
        result.deleteBlocked = this.plan.deleteBlockedReason;
      }
      return result;
    }

    await this.prepareDirectories();

    for (const ch of this.plan.update) {
      if (isDirToFile(ch)) {
        checkAbort(this.signal);
        await this.commitDirToFile(ch, decision.confirmDeletes);
      }
    }

    const deleteOutcome = await applyDeletes(this.root, this.scan, this.plan, {
      committed: true,
      confirmed: decision.confirmDeletes,
      hasFailures: this.hasFailures(),
      preDeleted: this.preDeleted,
      signal: this.signal,
    });
    for (const failure of deleteOutcome.failures) {
      this.failures.push(failure);
    }

    const result: SyncResult = {
      added: this.added,
      updated: this.updated,
      touched: 0,
      deleted: deleteOutcome.deleted,
      unchanged: this.plan.skip.length,
      conflicts: this.plan.conflicts.length,
      bytes: this.bytes,
    };
    if (this.failures.length > 0) result.failures = [...this.failures];
    if (deleteOutcome.deleteBlocked) {
      result.deleteBlocked = deleteOutcome.deleteBlocked;
    }
    if (deleteOutcome.deletesHeld > 0) {
      result.deletesHeld = deleteOutcome.deletesHeld;
    }
    return result;
  }
}
