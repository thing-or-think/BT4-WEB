import {
  base64ToBytes,
  bytesToBase64,
  textDecoder,
  textEncoder,
} from "./bytes";
import {
  MAX_MANIFEST_ENTRIES,
  entryToWire,
  filterToWire,
  parseRFC3339Nano,
  validateManifest,
  type SyncEntry,
  type SyncFilterSpec,
  type SyncKind,
  type SyncManifest,
  type SyncSkipReason,
  type SyncSkipped,
  type WireSyncEntry,
  type WireSyncSkipped,
} from "./sync-diff";

export const SYNC_PROTOCOL_VERSION = 1;
export const SYNC_FEATURE = "folder-sync-v1";
export const SYNC_MESSAGE_PREFIX = "sync-";
export const MAX_SYNC_BATCH_BYTES = 1 << 20;
export const MAX_MANIFEST_JSON_BYTES = 512 << 20;
export const MAX_SYNC_LIST_ITEMS = MAX_MANIFEST_ENTRIES;

const BATCH_OVERHEAD = 256;

export type SyncMessageType =
  | "sync-hello"
  | "sync-manifest-start"
  | "sync-manifest-batch"
  | "sync-manifest-end"
  | "sync-hash-request"
  | "sync-hash-result"
  | "sync-need"
  | "sync-result"
  | "sync-ping"
  | "sync-pong";

export interface SyncWireMessage {
  t: SyncMessageType | string;
  b?: Uint8Array;
}

export type SyncRole = "source" | "destination";

export interface SyncHello {
  version: number;
  role: SyncRole;
  round?: number;
  watch?: boolean;
  rejoinCode?: string;
}

export interface SyncManifestStart {
  entries: number;
  skipped: number;
  filter?: SyncFilterSpec;
  complete: boolean;
}

export interface SyncManifestBatch {
  seq: number;
  entries: SyncEntry[];
  skipped: SyncSkipped[];
}

export interface SyncManifestEnd {
  batches: number;
}

export interface SyncPathList {
  seq: number;
  paths: string[];
  last: boolean;
}

export interface SyncPathHash {
  path: string;
  hash?: Uint8Array;
  error?: string;
}

export interface SyncHashList {
  seq: number;
  hashes: SyncPathHash[];
  last: boolean;
}

export interface SyncResult {
  dryRun?: boolean;
  added: number;
  updated: number;
  touched: number;
  deleted: number;
  unchanged: number;
  conflicts: number;
  bytes: number;
  failures?: string[];
  deleteBlocked?: string;
  deletesHeld?: number;
}

export function isSyncMessageType(type: string): boolean {
  return type.startsWith(SYNC_MESSAGE_PREFIX);
}

export function isPartialSyncResult(result: SyncResult | undefined): boolean {
  return Boolean(result && result.failures && result.failures.length > 0);
}

export class SyncProtocolError extends Error {
  constructor(message: string) {
    super(`foldersync: protocol violation: ${message}`);
    this.name = "SyncProtocolError";
  }
}

export function protoErr(message: string): SyncProtocolError {
  return new SyncProtocolError(message);
}

function asObject(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw protoErr(`decode ${label}: expected JSON object`);
  }
  return value as Record<string, unknown>;
}

function assertAllowedKeys(
  obj: Record<string, unknown>,
  allowed: ReadonlySet<string>,
  label: string,
): void {
  for (const key of Object.keys(obj)) {
    if (!allowed.has(key)) {
      throw protoErr(`decode ${label}: unknown field "${key}"`);
    }
  }
}

function readInt(
  obj: Record<string, unknown>,
  key: string,
  label: string,
  defaultValue = 0,
): number {
  const value = obj[key];
  if (value === undefined) return defaultValue;
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw protoErr(`decode ${label}: field "${key}" must be an integer`);
  }
  return value;
}

function readBool(
  obj: Record<string, unknown>,
  key: string,
  label: string,
  defaultValue = false,
): boolean {
  const value = obj[key];
  if (value === undefined) return defaultValue;
  if (typeof value !== "boolean") {
    throw protoErr(`decode ${label}: field "${key}" must be a boolean`);
  }
  return value;
}

function readString(
  obj: Record<string, unknown>,
  key: string,
  label: string,
  defaultValue = "",
): string {
  const value = obj[key];
  if (value === undefined) return defaultValue;
  if (typeof value !== "string") {
    throw protoErr(`decode ${label}: field "${key}" must be a string`);
  }
  return value;
}

function readStringArray(
  obj: Record<string, unknown>,
  key: string,
  label: string,
): string[] {
  const value = obj[key];
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) {
    throw protoErr(`decode ${label}: field "${key}" must be an array`);
  }
  const out: string[] = new Array(value.length);
  for (let i = 0; i < value.length; i += 1) {
    if (typeof value[i] !== "string") {
      throw protoErr(
        `decode ${label}: field "${key}[${i}]" must be a string`,
      );
    }
    out[i] = value[i] as string;
  }
  return out;
}

const BASE64_RE = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

function readBase64Bytes(
  obj: Record<string, unknown>,
  key: string,
  label: string,
): Uint8Array | undefined {
  const value = obj[key];
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string") {
    throw protoErr(`decode ${label}: field "${key}" must be a base64 string`);
  }
  if (value === "") return new Uint8Array();
  if (!BASE64_RE.test(value)) {
    throw protoErr(`decode ${label}: field "${key}" is not valid base64`);
  }
  try {
    return base64ToBytes(value);
  } catch {
    throw protoErr(`decode ${label}: field "${key}" is not valid base64`);
  }
}

export function encodeSyncPayload(
  type: SyncMessageType,
  value: unknown,
  limit = MAX_SYNC_BATCH_BYTES,
): SyncWireMessage {
  let json: string;
  try {
    json = JSON.stringify(value);
  } catch (error) {
    throw protoErr(
      `encode ${type}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (json === undefined) {
    throw protoErr(`encode ${type}: value is not JSON-serializable`);
  }
  const bytes = textEncoder.encode(json);
  if (bytes.byteLength > limit) {
    throw protoErr(
      `${type} payload ${bytes.byteLength} bytes > ${limit}`,
    );
  }
  return { t: type, b: bytes };
}

function decodeSyncJSON(
  msg: SyncWireMessage,
  limit = MAX_SYNC_BATCH_BYTES,
): Record<string, unknown> {
  const bytes = msg.b;
  if (!bytes || bytes.byteLength === 0) {
    throw protoErr(`decode ${msg.t}: empty payload`);
  }
  if (bytes.byteLength > limit) {
    throw protoErr(`${msg.t} payload ${bytes.byteLength} bytes > ${limit}`);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(textDecoder.decode(bytes));
  } catch (error) {
    throw protoErr(
      `decode ${msg.t}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  return asObject(parsed, msg.t);
}

const HELLO_KEYS = new Set([
  "version",
  "role",
  "round",
  "watch",
  "rejoin_code",
]);

export function encodeHello(hello: SyncHello): SyncWireMessage {
  const wire: Record<string, unknown> = {
    version: hello.version,
    role: hello.role,
  };
  if (hello.round) wire.round = hello.round;
  if (hello.watch) wire.watch = true;
  if (hello.rejoinCode) wire.rejoin_code = hello.rejoinCode;
  return encodeSyncPayload("sync-hello", wire);
}

export function decodeHello(msg: SyncWireMessage): SyncHello {
  const obj = decodeSyncJSON(msg);
  assertAllowedKeys(obj, HELLO_KEYS, msg.t);
  const version = readInt(obj, "version", msg.t, 0);
  const role = readString(obj, "role", msg.t, "") as SyncRole;
  const round = readInt(obj, "round", msg.t, 0);
  const watch = readBool(obj, "watch", msg.t, false);
  const rejoinCode = readString(obj, "rejoin_code", msg.t, "");
  const out: SyncHello = { version, role };
  if (round !== 0) out.round = round;
  if (watch) out.watch = true;
  if (rejoinCode !== "") out.rejoinCode = rejoinCode;
  return out;
}

const FILTER_KEYS = new Set(["exclude", "exclude_exact", "git"]);

function decodeFilterSpec(
  raw: unknown,
  label: string,
): SyncFilterSpec | undefined {
  if (raw === undefined || raw === null) return undefined;
  const obj = asObject(raw, `${label}.filter`);
  assertAllowedKeys(obj, FILTER_KEYS, `${label}.filter`);
  const exclude = readStringArray(obj, "exclude", `${label}.filter`);
  const excludeExact = readStringArray(obj, "exclude_exact", `${label}.filter`);
  const git = readBool(obj, "git", `${label}.filter`, false);
  const filter: SyncFilterSpec = {};
  if (exclude.length > 0) filter.exclude = exclude;
  if (excludeExact.length > 0) filter.excludeExact = excludeExact;
  if (git) filter.git = true;
  return filter;
}

const MANIFEST_START_KEYS = new Set([
  "entries",
  "skipped",
  "filter",
  "complete",
]);

export function encodeManifestStart(
  start: SyncManifestStart,
  limit = MAX_SYNC_BATCH_BYTES,
): SyncWireMessage {
  const wire: Record<string, unknown> = {
    entries: start.entries,
    skipped: start.skipped,
    complete: start.complete,
  };
  const filter = filterToWire(start.filter);
  if (
    filter &&
    (filter.exclude?.length || filter.exclude_exact?.length || filter.git)
  ) {
    wire.filter = filter;
  }
  return encodeSyncPayload("sync-manifest-start", wire, limit);
}

export function decodeManifestStart(msg: SyncWireMessage): SyncManifestStart {
  const obj = decodeSyncJSON(msg);
  assertAllowedKeys(obj, MANIFEST_START_KEYS, msg.t);
  const entries = readInt(obj, "entries", msg.t, 0);
  const skipped = readInt(obj, "skipped", msg.t, 0);
  const complete = readBool(obj, "complete", msg.t, false);
  const filter = decodeFilterSpec(obj.filter, msg.t);
  const out: SyncManifestStart = { entries, skipped, complete };
  if (filter) out.filter = filter;
  return out;
}

const ENTRY_KEYS = new Set([
  "path",
  "size",
  "mod_time",
  "mode",
  "kind",
  "hash",
]);

function decodeWireEntry(raw: unknown, label: string): SyncEntry {
  const obj = asObject(raw, label);
  assertAllowedKeys(obj, ENTRY_KEYS, label);
  const path = readString(obj, "path", label, "");
  const size = readInt(obj, "size", label, 0);
  const modTime = readString(obj, "mod_time", label, "");
  if (modTime !== "") {
    try {
      parseRFC3339Nano(modTime);
    } catch {
      throw protoErr(`decode ${label}: invalid mod_time "${modTime}"`);
    }
  }
  const mode = readInt(obj, "mode", label, 0);
  const kind = readInt(obj, "kind", label, 0) as SyncKind;
  const hash = readBase64Bytes(obj, "hash", label);
  const entry: SyncEntry = { path, size, modTime, mode, kind };
  if (hash && hash.byteLength > 0) entry.hash = hash;
  return entry;
}

const SKIPPED_KEYS = new Set(["path", "reason", "detail"]);

function decodeWireSkipped(raw: unknown, label: string): SyncSkipped {
  const obj = asObject(raw, label);
  assertAllowedKeys(obj, SKIPPED_KEYS, label);
  const path = readString(obj, "path", label, "");
  const reason = readString(obj, "reason", label, "") as SyncSkipReason;
  const detail = readString(obj, "detail", label, "");
  const skipped: SyncSkipped = { path, reason };
  if (detail !== "") skipped.detail = detail;
  return skipped;
}

function skippedToStrictWire(skipped: SyncSkipped): WireSyncSkipped {
  const wire: WireSyncSkipped = {
    reason: skipped.reason,
  };
  if (skipped.path !== "") wire.path = skipped.path;
  if (skipped.detail) wire.detail = skipped.detail;
  // DiskPath is never sent on the wire (Go: json:"-").
  return wire;
}

const MANIFEST_BATCH_KEYS = new Set(["seq", "entries", "skipped"]);

export function encodeManifestBatch(
  batch: { seq: number; entries?: WireSyncEntry[]; skipped?: WireSyncSkipped[] },
  limit = MAX_SYNC_BATCH_BYTES,
): SyncWireMessage {
  const wire: Record<string, unknown> = { seq: batch.seq };
  if (batch.entries && batch.entries.length > 0) wire.entries = batch.entries;
  if (batch.skipped && batch.skipped.length > 0) wire.skipped = batch.skipped;
  return encodeSyncPayload("sync-manifest-batch", wire, limit);
}

export function decodeManifestBatch(msg: SyncWireMessage): SyncManifestBatch {
  const obj = decodeSyncJSON(msg);
  assertAllowedKeys(obj, MANIFEST_BATCH_KEYS, msg.t);
  const seq = readInt(obj, "seq", msg.t, 0);

  const entries: SyncEntry[] = [];
  if (obj.entries !== undefined && obj.entries !== null) {
    if (!Array.isArray(obj.entries)) {
      throw protoErr(`decode ${msg.t}: field "entries" must be an array`);
    }
    for (let i = 0; i < obj.entries.length; i += 1) {
      entries.push(decodeWireEntry(obj.entries[i], `${msg.t}.entries[${i}]`));
    }
  }

  const skipped: SyncSkipped[] = [];
  if (obj.skipped !== undefined && obj.skipped !== null) {
    if (!Array.isArray(obj.skipped)) {
      throw protoErr(`decode ${msg.t}: field "skipped" must be an array`);
    }
    for (let i = 0; i < obj.skipped.length; i += 1) {
      skipped.push(
        decodeWireSkipped(obj.skipped[i], `${msg.t}.skipped[${i}]`),
      );
    }
  }

  return { seq, entries, skipped };
}

const MANIFEST_END_KEYS = new Set(["batches"]);

export function encodeManifestEnd(
  end: SyncManifestEnd,
  limit = MAX_SYNC_BATCH_BYTES,
): SyncWireMessage {
  return encodeSyncPayload("sync-manifest-end", { batches: end.batches }, limit);
}

export function decodeManifestEnd(msg: SyncWireMessage): SyncManifestEnd {
  const obj = decodeSyncJSON(msg);
  assertAllowedKeys(obj, MANIFEST_END_KEYS, msg.t);
  const batches = readInt(obj, "batches", msg.t, 0);
  return { batches };
}

const PATH_LIST_KEYS = new Set(["seq", "paths", "last"]);

export function encodePathList(
  type: "sync-hash-request" | "sync-need",
  list: SyncPathList,
  limit = MAX_SYNC_BATCH_BYTES,
): SyncWireMessage {
  const wire: Record<string, unknown> = { seq: list.seq };
  if (list.paths.length > 0) wire.paths = list.paths;
  if (list.last) wire.last = true;
  return encodeSyncPayload(type, wire, limit);
}

export function decodePathList(msg: SyncWireMessage): SyncPathList {
  const obj = decodeSyncJSON(msg);
  assertAllowedKeys(obj, PATH_LIST_KEYS, msg.t);
  const seq = readInt(obj, "seq", msg.t, 0);
  const paths = readStringArray(obj, "paths", msg.t);
  const last = readBool(obj, "last", msg.t, false);
  return { seq, paths, last };
}

const PATH_HASH_KEYS = new Set(["path", "hash", "error"]);
const HASH_LIST_KEYS = new Set(["seq", "hashes", "last"]);

function pathHashToWire(item: SyncPathHash): Record<string, unknown> {
  const wire: Record<string, unknown> = { path: item.path };
  if (item.hash && item.hash.byteLength > 0) {
    wire.hash = bytesToBase64(item.hash);
  }
  if (item.error) wire.error = item.error;
  return wire;
}

export function encodeHashList(
  list: SyncHashList,
  limit = MAX_SYNC_BATCH_BYTES,
): SyncWireMessage {
  const wire: Record<string, unknown> = { seq: list.seq };
  if (list.hashes.length > 0) {
    wire.hashes = list.hashes.map(pathHashToWire);
  }
  if (list.last) wire.last = true;
  return encodeSyncPayload("sync-hash-result", wire, limit);
}

export function decodeHashList(msg: SyncWireMessage): SyncHashList {
  const obj = decodeSyncJSON(msg);
  assertAllowedKeys(obj, HASH_LIST_KEYS, msg.t);
  const seq = readInt(obj, "seq", msg.t, 0);
  const last = readBool(obj, "last", msg.t, false);
  const hashes: SyncPathHash[] = [];
  if (obj.hashes !== undefined && obj.hashes !== null) {
    if (!Array.isArray(obj.hashes)) {
      throw protoErr(`decode ${msg.t}: field "hashes" must be an array`);
    }
    for (let i = 0; i < obj.hashes.length; i += 1) {
      const label = `${msg.t}.hashes[${i}]`;
      const item = asObject(obj.hashes[i], label);
      assertAllowedKeys(item, PATH_HASH_KEYS, label);
      const path = readString(item, "path", label, "");
      const hash = readBase64Bytes(item, "hash", label);
      const error = readString(item, "error", label, "");
      const decoded: SyncPathHash = { path };
      if (hash && hash.byteLength > 0) decoded.hash = hash;
      if (error !== "") decoded.error = error;
      hashes.push(decoded);
    }
  }
  return { seq, hashes, last };
}

const RESULT_KEYS = new Set([
  "dry_run",
  "added",
  "updated",
  "touched",
  "deleted",
  "unchanged",
  "conflicts",
  "bytes",
  "failures",
  "delete_blocked",
  "deletes_held",
]);

export function encodeResult(result: SyncResult): SyncWireMessage {
  const wire: Record<string, unknown> = {
    added: result.added,
    updated: result.updated,
    touched: result.touched,
    deleted: result.deleted,
    unchanged: result.unchanged,
    conflicts: result.conflicts,
    bytes: result.bytes,
  };
  if (result.dryRun) wire.dry_run = true;
  if (result.failures && result.failures.length > 0) {
    wire.failures = result.failures;
  }
  if (result.deleteBlocked) wire.delete_blocked = result.deleteBlocked;
  if (result.deletesHeld) wire.deletes_held = result.deletesHeld;
  return encodeSyncPayload("sync-result", wire);
}

export function decodeResult(msg: SyncWireMessage): SyncResult {
  const obj = decodeSyncJSON(msg);
  assertAllowedKeys(obj, RESULT_KEYS, msg.t);
  const dryRun = readBool(obj, "dry_run", msg.t, false);
  const added = readInt(obj, "added", msg.t, 0);
  const updated = readInt(obj, "updated", msg.t, 0);
  const touched = readInt(obj, "touched", msg.t, 0);
  const deleted = readInt(obj, "deleted", msg.t, 0);
  const unchanged = readInt(obj, "unchanged", msg.t, 0);
  const conflicts = readInt(obj, "conflicts", msg.t, 0);
  const bytes = readInt(obj, "bytes", msg.t, 0);
  const failures = readStringArray(obj, "failures", msg.t);
  const deleteBlocked = readString(obj, "delete_blocked", msg.t, "");
  const deletesHeld = readInt(obj, "deletes_held", msg.t, 0);
  const out: SyncResult = {
    added,
    updated,
    touched,
    deleted,
    unchanged,
    conflicts,
    bytes,
  };
  if (dryRun) out.dryRun = true;
  if (failures.length > 0) out.failures = failures;
  if (deleteBlocked !== "") out.deleteBlocked = deleteBlocked;
  if (deletesHeld !== 0) out.deletesHeld = deletesHeld;
  return out;
}

class Batcher {
  private size = 0;
  count = 0;

  constructor(
    private limit: number,
    private overhead = BATCH_OVERHEAD,
  ) {}

  fits(n: number): boolean {
    return this.count === 0 || this.size + n + this.overhead <= this.limit;
  }

  add(n: number): void {
    this.size += n + 1;
    this.count += 1;
  }

  reset(): void {
    this.size = 0;
    this.count = 0;
  }
}

export function encodeManifest(
  manifest: SyncManifest,
  limit = MAX_SYNC_BATCH_BYTES,
): SyncWireMessage[] {
  const totalItems = manifest.entries.length + manifest.skipped.length;
  if (totalItems > MAX_MANIFEST_ENTRIES) {
    throw new Error(
      `foldersync: manifest exceeds maximum entry limit: ${totalItems} items`,
    );
  }
  const start = encodeManifestStart(
    {
      entries: manifest.entries.length,
      skipped: manifest.skipped.length,
      filter: manifest.filter,
      complete: manifest.complete,
    },
    limit,
  );
  const out: SyncWireMessage[] = [start];
  const batcher = new Batcher(limit, BATCH_OVERHEAD);
  let seq = 0;
  let curEntries: WireSyncEntry[] = [];
  let curSkipped: WireSyncSkipped[] = [];

  const flush = () => {
    if (batcher.count === 0) return;
    out.push(
      encodeManifestBatch(
        { seq, entries: curEntries, skipped: curSkipped },
        limit,
      ),
    );
    seq += 1;
    curEntries = [];
    curSkipped = [];
    batcher.reset();
  };

  let total = 0;
  for (const entry of manifest.entries) {
    const wire = entryToWire(entry);
    const rawLen = textEncoder.encode(JSON.stringify(wire)).byteLength;
    if (rawLen + BATCH_OVERHEAD > limit) {
      throw protoErr(`entry "${entry.path}" too large`);
    }
    if (!batcher.fits(rawLen)) flush();
    batcher.add(rawLen);
    total += rawLen;
    curEntries.push(wire);
  }

  for (const skipped of manifest.skipped) {
    const wire = skippedToStrictWire(skipped);
    const rawLen = textEncoder.encode(JSON.stringify(wire)).byteLength;
    if (rawLen + BATCH_OVERHEAD > limit) {
      throw protoErr(`skipped "${skipped.path}" too large`);
    }
    if (!batcher.fits(rawLen)) flush();
    batcher.add(rawLen);
    total += rawLen;
    curSkipped.push(wire);
  }

  if (total > MAX_MANIFEST_JSON_BYTES) {
    throw new Error(
      `foldersync: manifest exceeds maximum entry limit: ${total} JSON bytes`,
    );
  }

  flush();
  out.push(encodeManifestEnd({ batches: seq }, limit));
  return out;
}

export class ManifestAssembler {
  private started = false;
  private ended = false;
  private declaredEntries = 0;
  private declaredSkipped = 0;
  private nextSeq = 0;
  private jsonBytes = 0;
  private manifest: SyncManifest = {
    entries: [],
    skipped: [],
    complete: false,
  };

  constructor(private maxJsonBytes = MAX_MANIFEST_JSON_BYTES) {}

  start(msg: SyncWireMessage): void {
    if (this.started) {
      throw protoErr("duplicate manifest start");
    }
    const start = decodeManifestStart(msg);
    if (
      start.entries < 0 ||
      start.skipped < 0 ||
      start.entries > MAX_MANIFEST_ENTRIES ||
      start.skipped > MAX_MANIFEST_ENTRIES - start.entries
    ) {
      throw protoErr(
        `manifest declares ${start.entries} entries + ${start.skipped} skipped`,
      );
    }
    this.started = true;
    this.declaredEntries = start.entries;
    this.declaredSkipped = start.skipped;
    this.manifest = {
      entries: [],
      skipped: [],
      complete: start.complete,
    };
    if (start.filter) {
      this.manifest.filter = start.filter;
    }
  }

  batch(msg: SyncWireMessage): void {
    if (!this.started || this.ended) {
      throw protoErr("manifest batch outside a manifest stream");
    }
    const batch = decodeManifestBatch(msg);
    if (batch.seq !== this.nextSeq) {
      throw protoErr(
        `manifest batch seq ${batch.seq}, want ${this.nextSeq}`,
      );
    }
    if (batch.entries.length + batch.skipped.length === 0) {
      throw protoErr(`empty manifest batch ${batch.seq}`);
    }
    if (
      this.manifest.entries.length + batch.entries.length >
        this.declaredEntries ||
      this.manifest.skipped.length + batch.skipped.length >
        this.declaredSkipped
    ) {
      throw protoErr(`manifest batch ${batch.seq} exceeds declared size`);
    }
    this.jsonBytes += msg.b?.byteLength ?? 0;
    if (this.jsonBytes > this.maxJsonBytes) {
      throw protoErr(`manifest exceeds ${this.maxJsonBytes} JSON bytes`);
    }
    this.nextSeq += 1;
    for (const entry of batch.entries) this.manifest.entries.push(entry);
    for (const skipped of batch.skipped) this.manifest.skipped.push(skipped);
  }

  end(msg: SyncWireMessage): SyncManifest {
    if (!this.started || this.ended) {
      throw protoErr("manifest end outside a manifest stream");
    }
    const end = decodeManifestEnd(msg);
    if (end.batches !== this.nextSeq) {
      throw protoErr(
        `manifest end declares ${end.batches} batches, got ${this.nextSeq}`,
      );
    }
    if (
      this.manifest.entries.length !== this.declaredEntries ||
      this.manifest.skipped.length !== this.declaredSkipped
    ) {
      throw protoErr(
        `manifest incomplete: ${this.manifest.entries.length}/${this.declaredEntries} entries, ${this.manifest.skipped.length}/${this.declaredSkipped} skipped`,
      );
    }
    this.ended = true;
    validateManifest(this.manifest);
    return this.manifest;
  }
}

export function encodePathLists(
  type: "sync-hash-request" | "sync-need",
  paths: string[],
  limit = MAX_SYNC_BATCH_BYTES,
): SyncWireMessage[] {
  const out: SyncWireMessage[] = [];
  const batcher = new Batcher(limit, BATCH_OVERHEAD);
  let seq = 0;
  let current: string[] = [];

  for (const path of paths) {
    const rawLen = textEncoder.encode(JSON.stringify(path)).byteLength;
    if (rawLen + BATCH_OVERHEAD > limit) {
      throw protoErr(`path "${path}" too large`);
    }
    if (!batcher.fits(rawLen)) {
      out.push(encodePathList(type, { seq, paths: current, last: false }, limit));
      seq += 1;
      current = [];
      batcher.reset();
    }
    batcher.add(rawLen);
    current.push(path);
  }

  out.push(encodePathList(type, { seq, paths: current, last: true }, limit));
  return out;
}

export function encodeHashLists(
  hashes: SyncPathHash[],
  limit = MAX_SYNC_BATCH_BYTES,
): SyncWireMessage[] {
  const out: SyncWireMessage[] = [];
  const batcher = new Batcher(limit, BATCH_OVERHEAD);
  let seq = 0;
  let current: SyncPathHash[] = [];

  for (const item of hashes) {
    const rawLen = textEncoder.encode(
      JSON.stringify(pathHashToWire(item)),
    ).byteLength;
    if (rawLen + BATCH_OVERHEAD > limit) {
      throw protoErr(`hash entry "${item.path}" too large`);
    }
    if (!batcher.fits(rawLen)) {
      out.push(encodeHashList({ seq, hashes: current, last: false }, limit));
      seq += 1;
      current = [];
      batcher.reset();
    }
    batcher.add(rawLen);
    current.push(item);
  }

  out.push(encodeHashList({ seq, hashes: current, last: true }, limit));
  return out;
}

export class ListAssembler<T> {
  nextSeq = 0;
  done = false;
  items: T[] = [];
  private limit: number;

  constructor(limit = MAX_SYNC_LIST_ITEMS) {
    this.limit = limit;
  }

  add(kind: string, seq: number, items: T[], last: boolean): boolean {
    if (this.done) {
      throw protoErr(`${kind} after the stream ended`);
    }
    if (seq !== this.nextSeq) {
      throw protoErr(`${kind} seq ${seq}, want ${this.nextSeq}`);
    }
    const effectiveLimit = this.limit > 0 ? this.limit : MAX_SYNC_LIST_ITEMS;
    if (this.items.length + items.length > effectiveLimit) {
      throw protoErr(`${kind} exceeds ${effectiveLimit} items`);
    }
    if (items.length === 0 && !last) {
      throw protoErr(`empty non-final ${kind} batch`);
    }
    this.nextSeq += 1;
    for (const item of items) this.items.push(item);
    this.done = last;
    return last;
  }

  reset(limit = MAX_SYNC_LIST_ITEMS): void {
    this.nextSeq = 0;
    this.done = false;
    this.items = [];
    this.limit = limit;
  }
}

export class PathListAssembler extends ListAssembler<string> {
  push(msg: SyncWireMessage): boolean {
    const list = decodePathList(msg);
    return this.add(msg.t, list.seq, list.paths, list.last);
  }
}

export class HashListAssembler extends ListAssembler<SyncPathHash> {
  push(msg: SyncWireMessage): boolean {
    const list = decodeHashList(msg);
    return this.add(msg.t, list.seq, list.hashes, list.last);
  }
}
