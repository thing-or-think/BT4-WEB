// Browser client for `croc sync` (wire protocol folder-sync-v1).
//
// This module mirrors Contract A (manifest, diff) and Contract B (wire
// messages) from CLAUDE.md so the browser can act as either side of a
// one-way sync: Host (source) -> Join (target).
//
// Everything here is transport-agnostic: the state machines talk to a
// SyncConnection. createLoopbackConnection() wires two sessions together in
// the same page (local folder-to-folder sync, unit tests, E2E). The network
// connection over croc-web's /ws needs the Go message types from
// src/message (Member 2) and is not wired yet; see connectSyncPeer().
import { isLosslessNumber, parse, stringify } from "lossless-json";
import { errorMessage, formatBytes, hex, textDecoder, textEncoder } from "./bytes";
import { collisionKey, normalizeFolder } from "./metadata";
import type { TransferSettings } from "./types";
import { XXH64 } from "./xxh64";

// ---------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------

export const FEATURE_FOLDER_SYNC = "folder-sync-v1";
export const SYNC_TEMP_PREFIX = ".croc-sync-tmp-";

// Message type strings. PROVISIONAL until Member 2 adds the Go constants
// (TypeSyncManifestStart, ...) to src/message/message.go; keep both sides
// byte-for-byte identical.
export const SyncMessageTypes = {
  ManifestStart: "sync-manifest-start",
  ManifestBatch: "sync-manifest-batch",
  ManifestEnd: "sync-manifest-end",
  Plan: "sync-plan",
  Delete: "sync-delete",
  Event: "sync-event",
  Ack: "sync-ack",
} as const;
export type SyncMessageType =
  (typeof SyncMessageTypes)[keyof typeof SyncMessageTypes];

const syncMessageTypeSet = new Set<string>(Object.values(SyncMessageTypes));

export const MANIFEST_BATCH_ENTRIES = 1_000;
export const MAX_MANIFEST_BATCH_ENTRIES = 10_000;
// Mirrors maxChunkedManifestEntries in src/croc/file_manifest.go.
export const MAX_MANIFEST_ENTRIES = 1_000_000;
// Mirrors maxDecompressedMessageSize in src/message/message.go.
export const MAX_SYNC_PAYLOAD_BYTES = 64 * 1024 * 1024;

const HASH_CHUNK_BYTES = 4 * 1024 * 1024;
const MODE_DIR = 0x80000000; // fs.ModeDir
const DEFAULT_FILE_MODE = 0o644;
const DEFAULT_DIR_MODE = MODE_DIR + 0o755;
const NS_PER_MS = 1_000_000n;
const ABORT_REASON = "aborted by user";
export const SYNC_NETWORK_READY = false;

// ---------------------------------------------------------------------
// Contract A types (TypeScript twin of src/foldersync)
// ---------------------------------------------------------------------

export type SyncHashAlgo = "xxhash" | "imohash";

export interface SyncEntry {
  /** POSIX relative path, no leading '/', no '.', no '..'. */
  path: string;
  size: number;
  /** Unix nanoseconds (UTC). bigint because ns timestamps exceed 2^53. */
  mtime: bigint;
  mode: number;
  dir: boolean;
  /** Lowercase hex digest, absent when not computed. */
  hash?: string;
}

export interface SyncManifest {
  hashAlgo: SyncHashAlgo;
  entries: SyncEntry[];
}

export interface SyncPlan {
  add: SyncEntry[];
  update: SyncEntry[];
  skip: SyncEntry[];
  delete: SyncEntry[];
}

export interface SyncScanOptions {
  exclude?: string[];
  excludeFiles?: string[];
  computeHash?: boolean;
  signal?: AbortSignal;
  onProgress?(scanned: number): void;
}

export interface SyncDiffOptions {
  checksum?: boolean;
  delete?: boolean;
}

// Contract C PlanView twin, used by the UI.
export interface SyncPlanItem {
  path: string;
  size: number;
  dir: boolean;
}

export interface SyncPlanView {
  add: SyncPlanItem[];
  update: SyncPlanItem[];
  delete: SyncPlanItem[];
  skip: SyncPlanItem[];
  /** Only set when the view was built from a received plan (Join). */
  skipCount: number;
  dryRun: boolean;
  transferBytes: number;
  savedBytes: number;
}

export interface SyncProgress {
  path: string;
  fileDone: number;
  fileTotal: number;
  overallDone: number;
  overallTotal: number;
}

export type SyncWatchKind = "create" | "modify" | "delete" | "rename";

export interface SyncWatchEvent {
  kind: SyncWatchKind;
  path: string;
  oldPath?: string;
  /** Unix milliseconds. */
  time: number;
}

export interface SyncFileError {
  path: string;
  error: string;
}

export interface SyncSummary {
  added: number;
  updated: number;
  deleted: number;
  skipped: number;
  bytesSent: number;
  durationMs: number;
  errors: SyncFileError[];
}

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

export class SyncPathError extends Error {
  constructor(path: string, reason: string) {
    super(`Unsafe sync path ${JSON.stringify(path)}: ${reason}`);
    this.name = "SyncPathError";
  }
}

export class SyncProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SyncProtocolError";
  }
}

export class SyncPeerUnsupportedError extends Error {
  constructor() {
    super("The other side does not support croc sync (folder-sync-v1). Update croc on both computers.");
    this.name = "SyncPeerUnsupportedError";
  }
}

export class SyncAbortedError extends Error {
  constructor() {
    super("Sync was cancelled before any file changed.");
    this.name = "SyncAbortedError";
  }
}

export class SyncUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SyncUnavailableError";
  }
}

function abortError() {
  return new DOMException("Sync cancelled", "AbortError");
}

function checkAbort(signal?: AbortSignal) {
  if (signal?.aborted) throw abortError();
}

export function isAbortError(error: unknown) {
  return error instanceof DOMException && error.name === "AbortError";
}

// ---------------------------------------------------------------------
// Path rules (§5 of CLAUDE.md). Reuses the receivefs browser twin in
// metadata.ts so .ssh/.gnupg, device names, ADS syntax, etc. stay aligned.
// ---------------------------------------------------------------------

export function isSyncTempName(name: string) {
  return name.startsWith(SYNC_TEMP_PREFIX);
}

/**
 * Validates an untrusted relative path and returns its NFC form.
 * Rejects absolute paths, drive letters, backslashes, '.', '..', empty
 * segments, sensitive folders and sync temp files.
 */
export function validateSyncPath(path: string): string {
  if (typeof path !== "string" || path === "")
    throw new SyncPathError(String(path), "empty path");
  if (path.includes("\0")) throw new SyncPathError(path, "null byte");
  if (path.includes("\\")) throw new SyncPathError(path, "backslash");
  if (path.startsWith("/") || /^[a-zA-Z]:/.test(path))
    throw new SyncPathError(path, "absolute path");
  const segments = path.split("/");
  if (segments.includes("..")) throw new SyncPathError(path, "parent traversal");
  if (segments.some((segment) => segment === "" || segment === "."))
    throw new SyncPathError(path, "non-canonical path");
  if (segments.some(isSyncTempName))
    throw new SyncPathError(path, "sync temp file");
  let normalized: string;
  try {
    normalized = normalizeFolder(path);
  } catch (error) {
    throw new SyncPathError(path, errorMessage(error));
  }
  if (normalized === ".") throw new SyncPathError(path, "root path");
  return normalized;
}

function parentPath(path: string) {
  const at = path.lastIndexOf("/");
  return at < 0 ? "" : path.slice(0, at);
}

function baseName(path: string) {
  return path.slice(path.lastIndexOf("/") + 1);
}

function depth(path: string) {
  return path.split("/").length;
}

/** Orders paths by Unicode code point, which matches Go's byte-wise sort. */
export function comparePaths(left: string, right: string) {
  if (left === right) return 0;
  const a = [...left];
  const b = [...right];
  const length = Math.min(a.length, b.length);
  for (let index = 0; index < length; index += 1) {
    const difference = a[index].codePointAt(0)! - b[index].codePointAt(0)!;
    if (difference !== 0) return difference;
  }
  return a.length - b.length;
}

function sortEntries(entries: SyncEntry[]) {
  return entries.sort((left, right) => comparePaths(left.path, right.path));
}

// ---------------------------------------------------------------------
// Filters (--exclude substring, --exclude-file exact path)
// ---------------------------------------------------------------------

export function parseList(value: string) {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeExcludeFile(value: string) {
  return value.replaceAll("\\", "/").replace(/^(\.\/)+/, "").replace(/\/+$/, "");
}

export function isExcluded(
  path: string,
  options: Pick<SyncScanOptions, "exclude" | "excludeFiles">,
) {
  if (options.exclude?.some((pattern) => pattern && path.includes(pattern)))
    return true;
  return (
    options.excludeFiles?.some(
      (file) => file && normalizeExcludeFile(file) === path,
    ) ?? false
  );
}

/** Drops excluded entries so --delete never touches excluded target files. */
export function applyExclusions(
  manifest: SyncManifest,
  options: Pick<SyncScanOptions, "exclude" | "excludeFiles">,
): SyncManifest {
  if (!options.exclude?.length && !options.excludeFiles?.length) return manifest;
  const excludedDirs: string[] = [];
  const entries = manifest.entries.filter((entry) => {
    if (excludedDirs.some((dir) => entry.path.startsWith(`${dir}/`))) return false;
    if (!isExcluded(entry.path, options)) return true;
    if (entry.dir) excludedDirs.push(entry.path);
    return false;
  });
  return { ...manifest, entries };
}

// ---------------------------------------------------------------------
// File System Access API surface. Real FileSystemDirectoryHandle objects
// satisfy these shapes; tests use in-memory fakes.
// ---------------------------------------------------------------------

export interface SyncWritable {
  write(data: Uint8Array): Promise<void>;
  close(): Promise<void>;
  abort(reason?: unknown): Promise<void>;
}

export interface SyncFileHandle {
  readonly kind: "file";
  readonly name: string;
  getFile(): Promise<File>;
  createWritable(options?: { keepExistingData?: boolean }): Promise<SyncWritable>;
}

export interface SyncDirectoryHandle {
  readonly kind: "directory";
  readonly name: string;
  values(): AsyncIterable<SyncFileHandle | SyncDirectoryHandle>;
  getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<SyncDirectoryHandle>;
  getFileHandle(name: string, options?: { create?: boolean }): Promise<SyncFileHandle>;
  removeEntry(name: string, options?: { recursive?: boolean }): Promise<void>;
}

export function asSyncDirectory(handle: FileSystemDirectoryHandle) {
  return handle as unknown as SyncDirectoryHandle;
}

export function supportsSyncDirectories() {
  return typeof window !== "undefined" && typeof window.showDirectoryPicker === "function";
}

function isNotFound(error: unknown) {
  return error instanceof DOMException && error.name === "NotFoundError";
}

function isTypeMismatch(error: unknown) {
  return error instanceof DOMException && error.name === "TypeMismatchError";
}

// ---------------------------------------------------------------------
// Hashing (xxhash = XXH64 seed 0, same bytes as cespare/xxhash Sum(nil))
// ---------------------------------------------------------------------

export async function hashBlob(blob: Blob, signal?: AbortSignal) {
  const hasher = new XXH64();
  for (let offset = 0; offset < blob.size; offset += HASH_CHUNK_BYTES) {
    checkAbort(signal);
    const chunk = blob.slice(offset, offset + HASH_CHUNK_BYTES);
    hasher.update(new Uint8Array(await chunk.arrayBuffer()));
  }
  checkAbort(signal);
  return hex(hasher.digest());
}

// ---------------------------------------------------------------------
// Scanner
// ---------------------------------------------------------------------

export interface SyncScanResult {
  manifest: SyncManifest;
  /** Entries that were skipped because their name is unsafe to sync. */
  rejected: SyncFileError[];
}

export async function scanDirectory(
  root: SyncDirectoryHandle,
  options: SyncScanOptions = {},
): Promise<SyncScanResult> {
  const entries: SyncEntry[] = [];
  const rejected: SyncFileError[] = [];
  const pending: Array<{ handle: SyncDirectoryHandle; prefix: string }> = [
    { handle: root, prefix: "" },
  ];
  while (pending.length) {
    const { handle, prefix } = pending.pop()!;
    for await (const child of handle.values()) {
      checkAbort(options.signal);
      if (isSyncTempName(child.name)) continue;
      const raw = prefix ? `${prefix}/${child.name}` : child.name;
      let path: string;
      try {
        path = validateSyncPath(raw);
      } catch (error) {
        rejected.push({ path: raw, error: errorMessage(error) });
        continue;
      }
      if (isExcluded(path, options)) continue;
      if (entries.length >= MAX_MANIFEST_ENTRIES)
        throw new Error(`Folder has more than ${MAX_MANIFEST_ENTRIES} entries`);
      if (child.kind === "directory") {
        entries.push({ path, size: 0, mtime: 0n, mode: DEFAULT_DIR_MODE, dir: true });
        pending.push({ handle: child, prefix: path });
      } else {
        const file = await child.getFile();
        const entry: SyncEntry = {
          path,
          size: file.size,
          mtime: BigInt(Math.trunc(file.lastModified)) * NS_PER_MS,
          mode: DEFAULT_FILE_MODE,
          dir: false,
        };
        if (options.computeHash) entry.hash = await hashBlob(file, options.signal);
        entries.push(entry);
      }
      options.onProgress?.(entries.length);
    }
  }
  return { manifest: { hashAlgo: "xxhash", entries: sortEntries(entries) }, rejected };
}

async function resolveDirectory(
  root: SyncDirectoryHandle,
  path: string,
  create: boolean,
) {
  let directory = root;
  if (!path) return directory;
  for (const segment of path.split("/")) {
    directory = await directory.getDirectoryHandle(segment, { create });
  }
  return directory;
}

export async function openSyncFile(root: SyncDirectoryHandle, path: string) {
  const safe = validateSyncPath(path);
  const directory = await resolveDirectory(root, parentPath(safe), false);
  return (await directory.getFileHandle(baseName(safe))).getFile();
}

// ---------------------------------------------------------------------
// Diff (TypeScript twin of foldersync.ComputeDiff)
// ---------------------------------------------------------------------

/**
 * Browsers only expose File.lastModified in milliseconds, so ModTime is
 * compared at millisecond resolution whenever the web client is involved.
 */
export function sameModTime(left: bigint, right: bigint) {
  return left / NS_PER_MS === right / NS_PER_MS;
}

function isSuspect(source: SyncEntry, target: SyncEntry | undefined) {
  return (
    !!target &&
    !source.dir &&
    !target.dir &&
    source.size === target.size &&
    !sameModTime(source.mtime, target.mtime)
  );
}

export function computeDiff(
  source: SyncManifest,
  target: SyncManifest,
  options: SyncDiffOptions = {},
): SyncPlan {
  if (options.checksum && source.hashAlgo !== target.hashAlgo)
    throw new SyncProtocolError(
      `Hash algorithms differ: ${source.hashAlgo} vs ${target.hashAlgo}`,
    );
  const targetByPath = new Map(target.entries.map((entry) => [entry.path, entry]));
  const sourcePaths = new Set<string>();
  const plan: SyncPlan = { add: [], update: [], skip: [], delete: [] };
  for (const entry of source.entries) {
    sourcePaths.add(entry.path);
    const existing = targetByPath.get(entry.path);
    if (!existing) {
      plan.add.push(entry);
    } else if (entry.dir !== existing.dir) {
      plan.update.push(entry);
    } else if (entry.dir) {
      plan.skip.push(entry);
    } else if (entry.size !== existing.size) {
      plan.update.push(entry);
    } else if (options.checksum) {
      if (!entry.hash || !existing.hash)
        throw new SyncProtocolError(`Missing hash for ${entry.path} in checksum mode`);
      (entry.hash === existing.hash ? plan.skip : plan.update).push(entry);
    } else if (sameModTime(entry.mtime, existing.mtime)) {
      plan.skip.push(entry);
    } else if (entry.hash && existing.hash) {
      // Tier 2: same size, different mtime, both hashes known.
      (entry.hash === existing.hash ? plan.skip : plan.update).push(entry);
    } else {
      // Unknown content: transferring is always safe.
      plan.update.push(entry);
    }
  }
  if (options.delete) {
    for (const entry of target.entries) {
      if (!sourcePaths.has(entry.path)) plan.delete.push(entry);
    }
  }
  return plan;
}

/**
 * Fills source hashes only for "suspect" files (same size, different
 * mtime) when the target already reported a hash, so the diff can skip
 * files whose content did not change without hashing the whole tree.
 */
export async function hashSuspects(
  source: SyncManifest,
  target: SyncManifest,
  open: (path: string) => Promise<Blob>,
  signal?: AbortSignal,
) {
  const targetByPath = new Map(target.entries.map((entry) => [entry.path, entry]));
  for (const entry of source.entries) {
    const existing = targetByPath.get(entry.path);
    if (!entry.hash && existing?.hash && isSuspect(entry, existing)) {
      entry.hash = await hashBlob(await open(entry.path), signal);
    }
  }
}

function toItem(entry: SyncEntry): SyncPlanItem {
  return { path: entry.path, size: entry.size, dir: entry.dir };
}

function fileBytes(entries: SyncEntry[]) {
  return entries.reduce((total, entry) => total + (entry.dir ? 0 : entry.size), 0);
}

export function planView(plan: SyncPlan, dryRun: boolean): SyncPlanView {
  return {
    add: plan.add.map(toItem),
    update: plan.update.map(toItem),
    delete: plan.delete.map(toItem),
    skip: plan.skip.map(toItem),
    skipCount: plan.skip.length,
    dryRun,
    transferBytes: fileBytes(plan.add) + fileBytes(plan.update),
    savedBytes: fileBytes(plan.skip),
  };
}

export function describePlan(view: SyncPlanView) {
  return `${view.add.length} to add, ${view.update.length} to update, ${view.delete.length} to delete, ${view.skipCount} unchanged (${formatBytes(view.transferBytes)} to send)`;
}

/**
 * Returns plan paths that would silently overwrite or alias another entry on
 * a case-insensitive target (Windows/macOS), e.g. adding `a.txt` while
 * `A.txt` exists. Such paths, and deletes of their counterparts, are skipped.
 */
export function findCaseConflicts(
  incoming: SyncEntry[],
  existing: SyncEntry[],
  deletes: SyncEntry[] = [],
) {
  const conflicts = new Map<string, string>();
  const byKey = new Map<string, string>();
  const existingPaths = new Set<string>();
  for (const entry of existing) {
    byKey.set(collisionKey(entry.path), entry.path);
    existingPaths.add(entry.path);
  }
  const seen = new Map<string, string>();
  for (const entry of incoming) {
    const key = collisionKey(entry.path);
    const twin = seen.get(key);
    if (twin !== undefined && twin !== entry.path) {
      conflicts.set(entry.path, twin);
      conflicts.set(twin, entry.path);
    }
    seen.set(key, entry.path);
    const local = byKey.get(key);
    if (local !== undefined && local !== entry.path && !existingPaths.has(entry.path)) {
      conflicts.set(entry.path, local);
      if (deletes.some((item) => item.path === local)) conflicts.set(local, entry.path);
    }
  }
  return conflicts;
}

// ---------------------------------------------------------------------
// Wire messages (Contract B). Payload is JSON in the croc message `b`
// field, batch index in `n`, like fileinfo-batch in file_manifest.go.
// ---------------------------------------------------------------------

export type SyncMessage =
  | { type: "sync-manifest-start"; total: number; hashAlgo: SyncHashAlgo }
  | { type: "sync-manifest-batch"; index: number; entries: SyncEntry[] }
  | { type: "sync-manifest-end" }
  | {
      type: "sync-plan";
      dryRun: boolean;
      watch: boolean;
      add: SyncEntry[];
      update: SyncEntry[];
      delete: SyncEntry[];
      skipped: number;
      savedBytes: number;
    }
  | { type: "sync-delete"; paths: string[] }
  | { type: "sync-event"; events: SyncWatchEvent[] }
  | { type: "sync-ack"; ok: boolean; errors: SyncFileError[] };

export interface SyncFrame {
  t: SyncMessageType;
  b?: Uint8Array;
  n?: number;
}

function entryToWire(entry: SyncEntry) {
  const wire: Record<string, unknown> = {
    path: entry.path,
    size: entry.size,
    mtime: entry.mtime,
    mode: entry.mode,
    dir: entry.dir,
  };
  if (entry.hash) wire.hash = entry.hash;
  return wire;
}

export function encodeSyncMessage(message: SyncMessage): SyncFrame {
  const { type, ...rest } = message;
  let payload: Record<string, unknown> = rest;
  let index: number | undefined;
  switch (message.type) {
    case "sync-manifest-batch":
      index = message.index;
      payload = { entries: message.entries.map(entryToWire) };
      break;
    case "sync-plan":
      payload = {
        ...rest,
        add: message.add.map(entryToWire),
        update: message.update.map(entryToWire),
        delete: message.delete.map(entryToWire),
      };
      break;
  }
  const json = stringify(payload) ?? "{}";
  const bytes = textEncoder.encode(json);
  if (bytes.byteLength > MAX_SYNC_PAYLOAD_BYTES)
    throw new SyncProtocolError(`${type} payload is too large (${bytes.byteLength} bytes)`);
  return { t: type, b: bytes, ...(index ? { n: index } : {}) };
}

type JSONObject = Record<string, unknown>;

function readObject(value: unknown, field: string): JSONObject {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new SyncProtocolError(`${field} must be an object`);
  return value as JSONObject;
}

function readArray(value: unknown, field: string, max = MAX_MANIFEST_ENTRIES) {
  if (!Array.isArray(value)) throw new SyncProtocolError(`${field} must be an array`);
  if (value.length > max) throw new SyncProtocolError(`${field} has too many items`);
  return value as unknown[];
}

function readInteger(value: unknown, field: string): bigint {
  const text = isLosslessNumber(value)
    ? value.value
    : typeof value === "bigint" || typeof value === "number"
      ? String(value)
      : undefined;
  if (text === undefined || !/^-?(0|[1-9][0-9]*)$/.test(text))
    throw new SyncProtocolError(`${field} must be an integer`);
  return BigInt(text);
}

function readSafeInteger(value: unknown, field: string, min = 0, max = Number.MAX_SAFE_INTEGER) {
  const big = readInteger(value, field);
  if (big < BigInt(min) || big > BigInt(max))
    throw new SyncProtocolError(`${field} is out of range`);
  return Number(big);
}

function readString(value: unknown, field: string, max = 4096) {
  if (typeof value !== "string" || value.length > max)
    throw new SyncProtocolError(`${field} must be a string`);
  return value;
}

function readBoolean(value: unknown, field: string, fallback?: boolean) {
  if (value === undefined && fallback !== undefined) return fallback;
  if (typeof value !== "boolean") throw new SyncProtocolError(`${field} must be a boolean`);
  return value;
}

function readHashAlgo(value: unknown): SyncHashAlgo {
  if (value === "xxhash" || value === "imohash") return value;
  throw new SyncProtocolError(`Unsupported hash algorithm ${JSON.stringify(value)}`);
}

function readEntry(value: unknown): SyncEntry {
  const wire = readObject(value, "entry");
  const path = validateSyncPath(readString(wire.path, "entry.path"));
  const entry: SyncEntry = {
    path,
    size: readSafeInteger(wire.size, "entry.size"),
    mtime: readInteger(wire.mtime ?? 0, "entry.mtime"),
    mode: readSafeInteger(wire.mode ?? 0, "entry.mode", 0, 0xffffffff),
    dir: readBoolean(wire.dir, "entry.dir", false),
  };
  if (wire.hash !== undefined && wire.hash !== "") {
    const hash = readString(wire.hash, "entry.hash", 128);
    if (!/^[0-9a-f]+$/.test(hash)) throw new SyncProtocolError("entry.hash must be lowercase hex");
    entry.hash = hash;
  }
  if (entry.dir && entry.size !== 0) throw new SyncProtocolError("Directory entry has a size");
  return entry;
}

function readEntries(value: unknown, field: string, max = MAX_MANIFEST_ENTRIES) {
  return readArray(value, field, max).map(readEntry);
}

function readWatchEvent(value: unknown): SyncWatchEvent {
  const wire = readObject(value, "event");
  const kind = wire.kind;
  if (kind !== "create" && kind !== "modify" && kind !== "delete" && kind !== "rename")
    throw new SyncProtocolError("event.kind is invalid");
  const event: SyncWatchEvent = {
    kind,
    path: validateSyncPath(readString(wire.path, "event.path")),
    time: readSafeInteger(wire.time ?? 0, "event.time"),
  };
  if (wire.oldPath !== undefined && wire.oldPath !== "")
    event.oldPath = validateSyncPath(readString(wire.oldPath, "event.oldPath"));
  return event;
}

export function decodeSyncMessage(frame: SyncFrame): SyncMessage {
  if (!syncMessageTypeSet.has(frame.t))
    throw new SyncProtocolError(`Unexpected message type ${JSON.stringify(frame.t)}`);
  const bytes = frame.b ?? new Uint8Array();
  if (bytes.byteLength > MAX_SYNC_PAYLOAD_BYTES)
    throw new SyncProtocolError(`${frame.t} payload is too large`);
  let payload: JSONObject = {};
  if (bytes.byteLength) {
    try {
      payload = readObject(parse(textDecoder.decode(bytes)), frame.t);
    } catch (error) {
      if (error instanceof SyncProtocolError) throw error;
      throw new SyncProtocolError(`${frame.t} payload is not valid JSON`);
    }
  }
  switch (frame.t) {
    case "sync-manifest-start":
      return {
        type: frame.t,
        total: readSafeInteger(payload.total, "total", 0, MAX_MANIFEST_ENTRIES),
        hashAlgo: readHashAlgo(payload.hashAlgo),
      };
    case "sync-manifest-batch":
      return {
        type: frame.t,
        index: frame.n ?? 0,
        entries: readEntries(payload.entries, "entries", MAX_MANIFEST_BATCH_ENTRIES),
      };
    case "sync-manifest-end":
      return { type: frame.t };
    case "sync-plan":
      return {
        type: frame.t,
        dryRun: readBoolean(payload.dryRun, "dryRun", false),
        watch: readBoolean(payload.watch, "watch", false),
        add: readEntries(payload.add ?? [], "add"),
        update: readEntries(payload.update ?? [], "update"),
        delete: readEntries(payload.delete ?? [], "delete"),
        skipped: readSafeInteger(payload.skipped ?? 0, "skipped"),
        savedBytes: readSafeInteger(payload.savedBytes ?? 0, "savedBytes"),
      };
    case "sync-delete":
      return {
        type: frame.t,
        paths: readArray(payload.paths ?? [], "paths").map((path) =>
          validateSyncPath(readString(path, "paths[]")),
        ),
      };
    case "sync-event":
      return {
        type: frame.t,
        events: readArray(payload.events ?? [], "events", MAX_MANIFEST_BATCH_ENTRIES).map(readWatchEvent),
      };
    case "sync-ack":
      return {
        type: frame.t,
        ok: readBoolean(payload.ok, "ok"),
        errors: readArray(payload.errors ?? [], "errors").map((value) => {
          const item = readObject(value, "error");
          return {
            path: readString(item.path ?? "", "error.path"),
            error: readString(item.error ?? "", "error.error"),
          };
        }),
      };
  }
  throw new SyncProtocolError(`Unexpected message type ${JSON.stringify(frame.t)}`);
}

/** Splits a manifest into Start, Batch x N, End (never one giant JSON). */
export function manifestMessages(
  manifest: SyncManifest,
  batchSize = MANIFEST_BATCH_ENTRIES,
): SyncMessage[] {
  const size = Math.max(1, Math.min(batchSize, MAX_MANIFEST_BATCH_ENTRIES));
  const messages: SyncMessage[] = [
    { type: "sync-manifest-start", total: manifest.entries.length, hashAlgo: manifest.hashAlgo },
  ];
  for (let at = 0, index = 0; at < manifest.entries.length; at += size, index += 1) {
    messages.push({
      type: "sync-manifest-batch",
      index,
      entries: manifest.entries.slice(at, at + size),
    });
  }
  messages.push({ type: "sync-manifest-end" });
  return messages;
}

/** Rebuilds an untrusted manifest from Start/Batch/End messages. */
export class ManifestAssembler {
  private started = false;
  private total = 0;
  private hashAlgo: SyncHashAlgo = "xxhash";
  private nextIndex = 0;
  private entries: SyncEntry[] = [];
  private paths = new Set<string>();

  /** Returns the manifest after End, undefined while incomplete. */
  accept(message: SyncMessage): SyncManifest | undefined {
    switch (message.type) {
      case "sync-manifest-start":
        if (this.started) throw new SyncProtocolError("Duplicate manifest start");
        this.started = true;
        this.total = message.total;
        this.hashAlgo = message.hashAlgo;
        return undefined;
      case "sync-manifest-batch":
        if (!this.started) throw new SyncProtocolError("Manifest batch before start");
        if (message.index !== this.nextIndex)
          throw new SyncProtocolError(`Manifest batch ${message.index} is out of order`);
        this.nextIndex += 1;
        if (this.entries.length + message.entries.length > this.total)
          throw new SyncProtocolError("Manifest has more entries than announced");
        for (const entry of message.entries) {
          if (this.paths.has(entry.path))
            throw new SyncProtocolError(`Duplicate manifest path ${entry.path}`);
          this.paths.add(entry.path);
          this.entries.push(entry);
        }
        return undefined;
      case "sync-manifest-end":
        if (!this.started) throw new SyncProtocolError("Manifest end before start");
        if (this.entries.length !== this.total)
          throw new SyncProtocolError(
            `Manifest announced ${this.total} entries but sent ${this.entries.length}`,
          );
        return { hashAlgo: this.hashAlgo, entries: sortEntries(this.entries) };
      default:
        throw new SyncProtocolError(`Expected manifest message, got ${message.type}`);
    }
  }
}

// ---------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------

export interface SyncConnection {
  readonly peerFeatures: readonly string[];
  send(message: SyncMessage): Promise<void>;
  receive(signal?: AbortSignal): Promise<SyncMessage>;
  /** Host -> Join file data. The network version streams over croc's pipeline. */
  sendFile(path: string, data: Blob, signal?: AbortSignal): Promise<void>;
  receiveFile(signal?: AbortSignal): Promise<{ path: string; data: Blob }>;
  close(reason?: string): void;
}

export class SyncConnectionClosedError extends Error {
  constructor(readonly reason = "") {
    super(reason ? `Connection closed: ${reason}` : "Connection closed");
    this.name = "SyncConnectionClosedError";
  }
}

class AsyncQueue<T> {
  private items: T[] = [];
  private waiters: Array<{ resolve(value: T): void; reject(error: unknown): void }> = [];
  private closed?: SyncConnectionClosedError;

  push(item: T) {
    if (this.closed) throw this.closed;
    const waiter = this.waiters.shift();
    if (waiter) waiter.resolve(item);
    else this.items.push(item);
  }

  shift(signal?: AbortSignal) {
    checkAbort(signal);
    if (this.items.length) return Promise.resolve(this.items.shift()!);
    if (this.closed) return Promise.reject(this.closed);
    return new Promise<T>((resolve, reject) => {
      const waiter = {
        resolve: (value: T) => {
          signal?.removeEventListener("abort", onAbort);
          resolve(value);
        },
        reject: (error: unknown) => {
          signal?.removeEventListener("abort", onAbort);
          reject(error);
        },
      };
      const onAbort = () => {
        this.waiters = this.waiters.filter((item) => item !== waiter);
        reject(abortError());
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      this.waiters.push(waiter);
    });
  }

  close(reason?: string) {
    if (this.closed) return;
    this.closed = new SyncConnectionClosedError(reason);
    for (const waiter of this.waiters.splice(0)) waiter.reject(this.closed);
  }
}

/**
 * Two connected endpoints in the same page. Messages go through the real
 * encoder/decoder so wire validation is exercised exactly like on the network.
 */
export function createLoopbackConnection(
  features: { host?: string[]; join?: string[] } = {},
): [SyncConnection, SyncConnection] {
  const toHost = new AsyncQueue<SyncFrame>();
  const toJoin = new AsyncQueue<SyncFrame>();
  const files = new AsyncQueue<{ path: string; data: Blob }>();
  const closeAll = (reason?: string) => {
    toHost.close(reason);
    toJoin.close(reason);
    files.close(reason);
  };
  const endpoint = (
    peerFeatures: string[],
    outbox: AsyncQueue<SyncFrame>,
    inbox: AsyncQueue<SyncFrame>,
  ): SyncConnection => ({
    peerFeatures,
    async send(message) {
      outbox.push(encodeSyncMessage(message));
    },
    async receive(signal) {
      return decodeSyncMessage(await inbox.shift(signal));
    },
    async sendFile(path, data, signal) {
      checkAbort(signal);
      files.push({ path, data });
    },
    receiveFile(signal) {
      return files.shift(signal);
    },
    close: closeAll,
  });
  return [
    endpoint(features.join ?? [FEATURE_FOLDER_SYNC], toJoin, toHost),
    endpoint(features.host ?? [FEATURE_FOLDER_SYNC], toHost, toJoin),
  ];
}

/**
 * Opens a sync session with a croc CLI or another browser through croc-web.
 * Needs the folder-sync-v1 message types in src/message/message.go and the
 * Go engine (Member 2) before it can be implemented on top of client.ts.
 */
export async function connectSyncPeer(_options: {
  role: "host" | "join";
  /** Join: the code from the source. Host: empty to generate one. */
  code: string;
  settings: TransferSettings;
  signal?: AbortSignal;
  /** Host: called with the code phrase before waiting for the peer. */
  onCode?(code: string): void;
}): Promise<SyncConnection> {
  throw new SyncUnavailableError(
    "Browser ↔ CLI sync is not available yet: the folder-sync-v1 wire protocol has not landed in croc. Use “Two local folders” to try sync in this browser.",
  );
}

// ---------------------------------------------------------------------
// Target writes (§6): stream into a writable, verify, then commit.
// Chromium's createWritable() writes to a swap file and replaces the target
// only on close(); abort() discards it. That is the browser's equivalent of
// write-temp -> verify -> rename. Browsers cannot set mtime (no Chtimes).
// ---------------------------------------------------------------------

export interface SyncTarget {
  mkdir(path: string): Promise<void>;
  writeFile(
    entry: SyncEntry,
    data: Blob,
    onBytes: (written: number) => void,
    signal?: AbortSignal,
  ): Promise<void>;
  remove(path: string, dir: boolean): Promise<void>;
}

export class DirectorySyncTarget implements SyncTarget {
  constructor(private root: SyncDirectoryHandle) {}

  async mkdir(path: string) {
    await resolveDirectory(this.root, validateSyncPath(path), true);
  }

  async writeFile(
    entry: SyncEntry,
    data: Blob,
    onBytes: (written: number) => void,
    signal?: AbortSignal,
  ) {
    const path = validateSyncPath(entry.path);
    if (data.size !== entry.size)
      throw new Error(`size mismatch: expected ${entry.size} bytes, got ${data.size}`);
    const directory = await resolveDirectory(this.root, parentPath(path), true);
    const name = baseName(path);
    let existed = true;
    try {
      await directory.getFileHandle(name);
    } catch (error) {
      if (isTypeMismatch(error)) throw new Error("a folder with this name exists on the target");
      if (!isNotFound(error)) throw error;
      existed = false;
    }
    const handle = await directory.getFileHandle(name, { create: true });
    const writable = await handle.createWritable({ keepExistingData: false });
    const hasher = new XXH64();
    let written = 0;
    try {
      for (let offset = 0; offset < data.size; offset += HASH_CHUNK_BYTES) {
        checkAbort(signal);
        const chunk = new Uint8Array(
          await data.slice(offset, offset + HASH_CHUNK_BYTES).arrayBuffer(),
        );
        hasher.update(chunk);
        await writable.write(chunk);
        written += chunk.byteLength;
        onBytes(written);
      }
      checkAbort(signal);
      if (entry.hash) {
        const actual = hex(hasher.digest());
        if (actual !== entry.hash)
          throw new Error(`hash mismatch: expected ${entry.hash}, got ${actual}`);
      }
      await writable.close();
    } catch (error) {
      await writable.abort().catch(() => undefined);
      if (!existed) await directory.removeEntry(name).catch(() => undefined);
      throw error;
    }
  }

  async remove(path: string, dir: boolean) {
    const safe = validateSyncPath(path);
    const directory = await resolveDirectory(this.root, parentPath(safe), false);
    const name = baseName(safe);
    if (dir) {
      await directory.getDirectoryHandle(name);
    } else {
      await directory.getFileHandle(name);
    }
    // Never recursive: a folder that still holds excluded or unknown files
    // stays, and the error is reported for that path only.
    await directory.removeEntry(name, { recursive: false });
  }
}

// ---------------------------------------------------------------------
// Sessions (Contract B state machine)
// ---------------------------------------------------------------------

export interface SyncReporter {
  onStatus?(message: string): void;
  /** Host: return false to abort. Join/dry-run: return value ignored. */
  onPlan?(plan: SyncPlanView): boolean | Promise<boolean>;
  onFileStart?(path: string, size: number): void;
  onProgress?(progress: SyncProgress): void;
  onFileDone?(path: string, error?: string): void;
  onWatchEvent?(event: SyncWatchEvent): void;
  onRoundDone?(summary: SyncSummary): void;
}

export interface SyncWebConfig {
  delete?: boolean;
  dryRun?: boolean;
  watch?: boolean;
  checksum?: boolean;
  exclude?: string[];
  excludeFiles?: string[];
}

export interface SyncSessionOptions {
  connection: SyncConnection;
  directory: SyncDirectoryHandle;
  config?: SyncWebConfig;
  reporter?: SyncReporter;
  signal?: AbortSignal;
  /** Watch polling interval; File System Access has no portable change events. */
  watchIntervalMs?: number;
  /** Quiet period before a burst of changes is synced. */
  watchDebounceMs?: number;
  /** Target file writer, defaults to DirectorySyncTarget(directory). */
  target?: SyncTarget;
}

function requireFeature(connection: SyncConnection) {
  if (!connection.peerFeatures.includes(FEATURE_FOLDER_SYNC))
    throw new SyncPeerUnsupportedError();
}

async function expectMessage<T extends SyncMessage["type"]>(
  connection: SyncConnection,
  type: T,
  signal?: AbortSignal,
): Promise<Extract<SyncMessage, { type: T }>> {
  let message: SyncMessage;
  try {
    message = await connection.receive(signal);
  } catch (error) {
    if (error instanceof SyncConnectionClosedError && error.reason === ABORT_REASON)
      throw new SyncAbortedError();
    throw error;
  }
  if (message.type !== type)
    throw new SyncProtocolError(`Expected ${type}, got ${message.type}`);
  return message as Extract<SyncMessage, { type: T }>;
}

async function receiveManifest(connection: SyncConnection, signal?: AbortSignal) {
  const assembler = new ManifestAssembler();
  for (;;) {
    const manifest = assembler.accept(await connection.receive(signal));
    if (manifest) return manifest;
  }
}

async function sendManifest(connection: SyncConnection, manifest: SyncManifest) {
  for (const message of manifestMessages(manifest)) await connection.send(message);
}

function manifestKey(entry: SyncEntry) {
  return `${entry.dir ? "d" : "f"}:${entry.size}:${entry.mtime / NS_PER_MS}`;
}

/** Derives watch events from two scans of the same folder. */
export function manifestChanges(previous: SyncManifest, next: SyncManifest, now = Date.now()) {
  const before = new Map(previous.entries.map((entry) => [entry.path, entry]));
  const events: SyncWatchEvent[] = [];
  for (const entry of next.entries) {
    const old = before.get(entry.path);
    if (!old) events.push({ kind: "create", path: entry.path, time: now });
    else if (manifestKey(old) !== manifestKey(entry))
      events.push({ kind: "modify", path: entry.path, time: now });
    before.delete(entry.path);
  }
  for (const path of before.keys()) events.push({ kind: "delete", path, time: now });
  return events.sort((left, right) => comparePaths(left.path, right.path));
}

function sleep(ms: number, signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) return reject(abortError());
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(abortError());
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/** Applies a finished round to the Host's model of the target folder. */
function applyRound(
  target: SyncManifest,
  plan: SyncPlan,
  failed: Set<string>,
): SyncManifest {
  const byPath = new Map(target.entries.map((entry) => [entry.path, entry]));
  for (const entry of [...plan.add, ...plan.update]) {
    if (!failed.has(entry.path)) byPath.set(entry.path, entry);
  }
  for (const entry of plan.delete) {
    if (!failed.has(entry.path)) byPath.delete(entry.path);
  }
  return { ...target, entries: sortEntries([...byPath.values()]) };
}

function summaryFor(plan: SyncPlan, errors: SyncFileError[], bytesSent: number, startedAt: number): SyncSummary {
  const failed = new Set(errors.map((error) => error.path));
  const ok = (entries: SyncEntry[]) => entries.filter((entry) => !failed.has(entry.path)).length;
  return {
    added: ok(plan.add),
    updated: ok(plan.update),
    deleted: ok(plan.delete),
    skipped: plan.skip.length,
    bytesSent,
    durationMs: Date.now() - startedAt,
    errors,
  };
}

/** Source side. Resolves after one round, or when `signal` aborts in watch mode. */
export async function runSyncHost(options: SyncSessionOptions): Promise<void> {
  const { connection, directory, reporter = {}, signal } = options;
  const config = options.config ?? {};
  const filters = { exclude: config.exclude, excludeFiles: config.excludeFiles };
  const onAbort = () => connection.close("cancelled");
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    requireFeature(connection);
    reporter.onStatus?.("Waiting for the target folder listing…");
    let target = applyExclusions(await receiveManifest(connection, signal), filters);
    reporter.onStatus?.("Scanning source folder…");
    let source = (await scanDirectory(directory, { ...filters, computeHash: config.checksum, signal })).manifest;
    const open = (path: string) => openSyncFile(directory, path);
    let events: SyncWatchEvent[] | undefined;
    for (;;) {
      const startedAt = Date.now();
      await hashSuspects(source, target, open, signal);
      const plan = computeDiff(source, target, { checksum: config.checksum, delete: config.delete });
      const dryRun = !!config.dryRun;
      const watch = !!config.watch && !dryRun;
      const view = planView(plan, dryRun);
      const proceed = await reporter.onPlan?.(view);
      if (!dryRun && proceed === false) {
        connection.close(ABORT_REASON);
        throw new SyncAbortedError();
      }
      if (events) await connection.send({ type: "sync-event", events });
      await connection.send({
        type: "sync-plan",
        dryRun,
        watch,
        add: plan.add,
        update: plan.update,
        delete: plan.delete,
        skipped: view.skipCount,
        savedBytes: view.savedBytes,
      });
      if (dryRun) {
        reporter.onRoundDone?.(summaryFor({ ...plan, add: [], update: [], delete: [] }, [], 0, startedAt));
        reporter.onStatus?.("Dry run finished. No file was changed.");
        return;
      }
      const transfers = [...plan.add, ...plan.update].filter((entry) => !entry.dir);
      const overallTotal = fileBytes(transfers);
      let overallDone = 0;
      for (const entry of transfers) {
        reporter.onFileStart?.(entry.path, entry.size);
        try {
          await connection.sendFile(entry.path, await open(entry.path), signal);
          overallDone += entry.size;
          reporter.onProgress?.({
            path: entry.path,
            fileDone: entry.size,
            fileTotal: entry.size,
            overallDone,
            overallTotal,
          });
          reporter.onFileDone?.(entry.path);
        } catch (error) {
          if (isAbortError(error)) throw error;
          // The source file vanished or became unreadable: send an empty
          // marker so Join still advances, and let it report the mismatch.
          await connection.sendFile(entry.path, new Blob([]), signal);
          reporter.onFileDone?.(entry.path, errorMessage(error));
        }
      }
      if (plan.delete.length)
        await connection.send({ type: "sync-delete", paths: plan.delete.map((entry) => entry.path) });
      const ack = await expectMessage(connection, "sync-ack", signal);
      reporter.onRoundDone?.(summaryFor(plan, ack.errors, overallDone, startedAt));
      if (!watch) {
        reporter.onStatus?.(ack.ok ? "Sync complete." : "Sync finished with errors.");
        return;
      }
      target = applyRound(target, plan, new Set(ack.errors.map((error) => error.path)));
      reporter.onStatus?.("Watching for changes…");
      ({ manifest: source, events } = await waitForChanges(directory, source, options, filters));
      for (const event of events) reporter.onWatchEvent?.(event);
    }
  } catch (error) {
    if (signal?.aborted) throw abortError();
    throw error;
  } finally {
    signal?.removeEventListener("abort", onAbort);
    connection.close();
  }
}

async function waitForChanges(
  directory: SyncDirectoryHandle,
  previous: SyncManifest,
  options: SyncSessionOptions,
  filters: Pick<SyncScanOptions, "exclude" | "excludeFiles">,
) {
  const interval = options.watchIntervalMs ?? 500;
  const debounce = options.watchDebounceMs ?? 400;
  const scan = async () =>
    (await scanDirectory(directory, { ...filters, computeHash: options.config?.checksum, signal: options.signal })).manifest;
  for (;;) {
    await sleep(interval, options.signal);
    let next = await scan();
    if (!manifestChanges(previous, next).length) continue;
    // Debounce: wait until the folder is quiet before syncing the burst.
    for (;;) {
      await sleep(debounce, options.signal);
      const settled = await scan();
      if (!manifestChanges(next, settled).length) break;
      next = settled;
    }
    return { manifest: next, events: manifestChanges(previous, next) };
  }
}

/** Target side. Resolves after the Host finishes (or the signal aborts). */
export async function runSyncJoin(options: SyncSessionOptions): Promise<void> {
  const { connection, directory, reporter = {}, signal } = options;
  const target = options.target ?? new DirectorySyncTarget(directory);
  const onAbort = () => connection.close("cancelled");
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    requireFeature(connection);
    reporter.onStatus?.("Scanning target folder…");
    // Browsers cannot restore mtimes, so the target always reports hashes:
    // this lets the Host skip unchanged files on the next sync.
    let own = (await scanDirectory(directory, { computeHash: true, signal })).manifest;
    reporter.onStatus?.("Sending folder listing…");
    await sendManifest(connection, own);
    let first = true;
    for (;;) {
      if (!first) {
        let message: SyncMessage;
        try {
          message = await connection.receive(signal);
        } catch (error) {
          checkAbort(signal);
          if (error instanceof SyncConnectionClosedError) {
            reporter.onStatus?.("The source stopped watching.");
            return;
          }
          throw error;
        }
        if (message.type !== "sync-event")
          throw new SyncProtocolError(`Expected sync-event, got ${message.type}`);
        for (const event of message.events) reporter.onWatchEvent?.(event);
      }
      first = false;
      reporter.onStatus?.("Waiting for the sync plan…");
      const planMessage = await expectMessage(connection, "sync-plan", signal);
      const startedAt = Date.now();
      const plan: SyncPlan = {
        add: planMessage.add,
        update: planMessage.update,
        delete: planMessage.delete,
        skip: [],
      };
      const view = { ...planView(plan, planMessage.dryRun), skipCount: planMessage.skipped, savedBytes: planMessage.savedBytes };
      await reporter.onPlan?.(view);
      if (planMessage.dryRun) {
        reporter.onStatus?.("Dry run finished. No file was changed.");
        return;
      }
      const errors = await applyPlan(connection, target, own, plan, reporter, signal);
      await connection.send({ type: "sync-ack", ok: errors.length === 0, errors });
      const summary = summaryFor(plan, errors, 0, startedAt);
      summary.skipped = planMessage.skipped;
      summary.bytesSent = fileBytes(
        [...plan.add, ...plan.update].filter((entry) => !errors.some((error) => error.path === entry.path)),
      );
      reporter.onRoundDone?.(summary);
      own = applyRound(own, plan, new Set(errors.map((error) => error.path)));
      if (!planMessage.watch) {
        reporter.onStatus?.(errors.length ? "Sync finished with errors." : "Sync complete.");
        return;
      }
      reporter.onStatus?.("Watching for changes from the source…");
    }
  } catch (error) {
    if (signal?.aborted) throw abortError();
    throw error;
  } finally {
    signal?.removeEventListener("abort", onAbort);
    connection.close();
  }
}

async function applyPlan(
  connection: SyncConnection,
  target: SyncTarget,
  own: SyncManifest,
  plan: SyncPlan,
  reporter: SyncReporter,
  signal?: AbortSignal,
) {
  const errors: SyncFileError[] = [];
  const fail = (path: string, error: string) => {
    errors.push({ path, error });
    reporter.onFileDone?.(path, error);
  };
  const ownByPath = new Map(own.entries.map((entry) => [entry.path, entry]));
  const incoming = [...plan.add, ...plan.update];
  const conflicts = findCaseConflicts(incoming, own.entries, plan.delete);
  const reportedConflicts = new Set<string>();
  const conflict = (path: string) => {
    if (reportedConflicts.has(path)) return;
    reportedConflicts.add(path);
    fail(path, `case conflict with ${conflicts.get(path)}; not overwritten`);
  };

  for (const entry of incoming.filter((item) => item.dir).sort((a, b) => depth(a.path) - depth(b.path))) {
    if (conflicts.has(entry.path)) {
      conflict(entry.path);
      continue;
    }
    try {
      await target.mkdir(entry.path);
    } catch (error) {
      fail(entry.path, errorMessage(error));
    }
  }

  const expected = new Map(incoming.filter((entry) => !entry.dir).map((entry) => [entry.path, entry]));
  const overallTotal = fileBytes([...expected.values()]);
  let overallDone = 0;
  for (let count = expected.size; count > 0; count -= 1) {
    const { path: rawPath, data } = await connection.receiveFile(signal);
    let path: string;
    try {
      path = validateSyncPath(rawPath);
    } catch (error) {
      throw new SyncProtocolError(errorMessage(error));
    }
    const entry = expected.get(path);
    if (!entry) throw new SyncProtocolError(`Peer sent a file that is not in the plan: ${path}`);
    expected.delete(path);
    if (conflicts.has(path)) {
      conflict(path);
      continue;
    }
    if (ownByPath.get(path)?.dir) {
      fail(path, "a folder with this name exists on the target");
      continue;
    }
    reporter.onFileStart?.(path, entry.size);
    try {
      await target.writeFile(entry, data, (written) => {
        reporter.onProgress?.({
          path,
          fileDone: written,
          fileTotal: entry.size,
          overallDone: overallDone + written,
          overallTotal,
        });
      }, signal);
      reporter.onFileDone?.(path);
    } catch (error) {
      if (isAbortError(error)) throw error;
      fail(path, errorMessage(error));
    }
    overallDone += entry.size;
  }

  if (plan.delete.length) {
    const message = await expectMessage(connection, "sync-delete", signal);
    const allowed = new Set(plan.delete.map((entry) => entry.path));
    const deletes = message.paths
      .map((path) => {
        const entry = ownByPath.get(path);
        return { path, entry };
      })
      // Deepest first so folders are empty when their turn comes.
      .sort((a, b) => depth(b.path) - depth(a.path) || Number(a.entry?.dir) - Number(b.entry?.dir));
    for (const { path, entry } of deletes) {
      checkAbort(signal);
      if (!allowed.has(path)) {
        fail(path, "delete was not announced in the plan; ignored");
        continue;
      }
      if (!entry) {
        // Only delete what this side listed itself.
        fail(path, "not part of the target listing; ignored");
        continue;
      }
      if (conflicts.has(path)) {
        conflict(path);
        continue;
      }
      try {
        await target.remove(path, entry.dir);
        reporter.onFileDone?.(path);
      } catch (error) {
        fail(path, errorMessage(error));
      }
    }
  }
  return errors;
}
