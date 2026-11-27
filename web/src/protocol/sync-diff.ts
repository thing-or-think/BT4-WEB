import { base64ToBytes, bytesEqual, bytesToBase64 } from "./bytes";
import { collisionKey, normalizeFolder } from "./metadata";

export const MAX_MANIFEST_ENTRIES = 1_000_000;
export const HASH_SIZE = 8;
export const STAGING_DIR_PREFIX = ".croc-sync-tmp-";

export const SyncKind = {
  File: 0,
  Directory: 1,
  Symlink: 2,
} as const;

export type SyncKind = (typeof SyncKind)[keyof typeof SyncKind];

export type SyncSkipReason =
  | "filtered"
  | "symlink"
  | "invalid_name"
  | "read_error"
  | "special";

const VALID_SKIP_REASONS = new Set<string>([
  "filtered",
  "symlink",
  "invalid_name",
  "read_error",
  "special",
]);

export interface SyncFilterSpec {
  exclude?: string[];
  excludeExact?: string[];
  git?: boolean;
}

export interface SyncEntry {
  path: string;
  size: number;
  modTime: string;
  mode: number;
  kind: SyncKind;
  hash?: Uint8Array;
}

export interface SyncSkipped {
  path: string;
  diskPath?: string;
  reason: SyncSkipReason;
  detail?: string;
}

export interface SyncManifest {
  entries: SyncEntry[];
  skipped: SyncSkipped[];
  filter?: SyncFilterSpec;
  complete: boolean;
}

export interface SyncChange {
  path: string;
  src?: SyncEntry;
  dst?: SyncEntry;
  typeChanged?: boolean;
  reason?: string;
}

export interface SyncPlan {
  add: SyncChange[];
  update: SyncChange[];
  touch: SyncChange[];
  skip: SyncChange[];
  delete: SyncChange[];
  conflicts: SyncChange[];
  deleteBlockedReason: string;
  requireDeleteConfirm: boolean;
}

export interface SyncDiffOptions {
  checksum?: boolean;
  delete?: boolean;
  allowEmptySource?: boolean;
  ignoreMetadata?: boolean;
}

export type CompareResult =
  | { plan: SyncPlan; needHash?: undefined }
  | { needHash: string[]; plan?: undefined };

export interface WireSyncFilterSpec {
  exclude?: string[];
  exclude_exact?: string[];
  git?: boolean;
}

export interface WireSyncEntry {
  path: string;
  size?: number;
  mod_time?: string;
  mode?: number;
  kind: number;
  hash?: string;
}

export interface WireSyncSkipped {
  path?: string;
  disk_path?: string;
  reason: string;
  detail?: string;
}

export interface WireSyncManifest {
  entries?: WireSyncEntry[] | null;
  skipped?: WireSyncSkipped[] | null;
  filter?: WireSyncFilterSpec;
  complete?: boolean;
}

export interface WireSyncChange {
  path: string;
  src?: WireSyncEntry;
  dst?: WireSyncEntry;
  type_changed?: boolean;
  reason?: string;
}

export interface WireSyncPlan {
  add?: WireSyncChange[];
  update?: WireSyncChange[];
  touch?: WireSyncChange[];
  skip?: WireSyncChange[];
  delete?: WireSyncChange[];
  conflicts?: WireSyncChange[];
  delete_blocked_reason?: string;
  require_delete_confirm?: boolean;
}

export interface WireSyncDiffOptions {
  checksum?: boolean;
  delete?: boolean;
  allow_empty_source?: boolean;
  ignore_metadata?: boolean;
}

export function entryFromWire(wire: WireSyncEntry): SyncEntry {
  const entry: SyncEntry = {
    path: wire.path,
    size: wire.size ?? 0,
    modTime: wire.mod_time ?? "",
    mode: wire.mode ?? 0,
    kind: wire.kind as SyncKind,
  };
  if (wire.hash) {
    entry.hash = base64ToBytes(wire.hash);
  }
  return entry;
}

export function entryToWire(entry: SyncEntry): WireSyncEntry {
  const wire: WireSyncEntry = {
    path: entry.path,
    kind: entry.kind,
  };
  if (entry.size !== 0) wire.size = entry.size;
  if (entry.modTime !== "") wire.mod_time = entry.modTime;
  if (entry.mode !== 0) wire.mode = entry.mode;
  if (entry.hash && entry.hash.byteLength > 0) {
    wire.hash = bytesToBase64(entry.hash);
  }
  return wire;
}

export function skippedFromWire(wire: WireSyncSkipped): SyncSkipped {
  const skipped: SyncSkipped = {
    path: wire.path ?? "",
    reason: wire.reason as SyncSkipReason,
  };
  if (wire.disk_path !== undefined) skipped.diskPath = wire.disk_path;
  if (wire.detail !== undefined) skipped.detail = wire.detail;
  return skipped;
}

export function skippedToWire(skipped: SyncSkipped): WireSyncSkipped {
  const wire: WireSyncSkipped = {
    reason: skipped.reason,
  };
  if (skipped.path !== "") wire.path = skipped.path;
  if (skipped.diskPath) wire.disk_path = skipped.diskPath;
  if (skipped.detail) wire.detail = skipped.detail;
  return wire;
}

export function filterFromWire(
  wire?: WireSyncFilterSpec,
): SyncFilterSpec | undefined {
  if (!wire) return undefined;
  const filter: SyncFilterSpec = {};
  if (wire.exclude?.length) filter.exclude = [...wire.exclude];
  if (wire.exclude_exact?.length) filter.excludeExact = [...wire.exclude_exact];
  if (wire.git) filter.git = true;
  return filter;
}

export function filterToWire(
  filter?: SyncFilterSpec,
): WireSyncFilterSpec | undefined {
  if (!filter) return undefined;
  const wire: WireSyncFilterSpec = {};
  if (filter.exclude?.length) wire.exclude = [...filter.exclude];
  if (filter.excludeExact?.length) wire.exclude_exact = [...filter.excludeExact];
  if (filter.git) wire.git = true;
  return wire;
}

export function manifestFromWire(wire: WireSyncManifest): SyncManifest {
  const manifest: SyncManifest = {
    entries: (wire.entries ?? []).map(entryFromWire),
    skipped: (wire.skipped ?? []).map(skippedFromWire),
    complete: Boolean(wire.complete),
  };
  const filter = filterFromWire(wire.filter);
  if (filter) manifest.filter = filter;
  return manifest;
}

export function manifestToWire(manifest: SyncManifest): WireSyncManifest {
  const wire: WireSyncManifest = {
    entries: manifest.entries.map(entryToWire),
    complete: manifest.complete,
  };
  if (manifest.skipped.length > 0) {
    wire.skipped = manifest.skipped.map(skippedToWire);
  }
  const filter = filterToWire(manifest.filter);
  if (
    filter &&
    (filter.exclude?.length || filter.exclude_exact?.length || filter.git)
  ) {
    wire.filter = filter;
  }
  return wire;
}

export function diffOptionsFromWire(
  wire: WireSyncDiffOptions,
): SyncDiffOptions {
  return {
    checksum: Boolean(wire.checksum),
    delete: Boolean(wire.delete),
    allowEmptySource: Boolean(wire.allow_empty_source),
    ignoreMetadata: Boolean(wire.ignore_metadata),
  };
}

export function diffOptionsToWire(opts: SyncDiffOptions): WireSyncDiffOptions {
  return {
    checksum: Boolean(opts.checksum),
    delete: Boolean(opts.delete),
    allow_empty_source: Boolean(opts.allowEmptySource),
    ignore_metadata: Boolean(opts.ignoreMetadata),
  };
}

function changeFromWire(wire: WireSyncChange): SyncChange {
  const change: SyncChange = { path: wire.path };
  if (wire.src) change.src = entryFromWire(wire.src);
  if (wire.dst) change.dst = entryFromWire(wire.dst);
  if (wire.type_changed) change.typeChanged = true;
  if (wire.reason) change.reason = wire.reason;
  return change;
}

function changeToWire(change: SyncChange): WireSyncChange {
  const wire: WireSyncChange = { path: change.path };
  if (change.src) wire.src = entryToWire(change.src);
  if (change.dst) wire.dst = entryToWire(change.dst);
  if (change.typeChanged) wire.type_changed = true;
  if (change.reason) wire.reason = change.reason;
  return wire;
}

export function planFromWire(wire: WireSyncPlan): SyncPlan {
  return {
    add: (wire.add ?? []).map(changeFromWire),
    update: (wire.update ?? []).map(changeFromWire),
    touch: (wire.touch ?? []).map(changeFromWire),
    skip: (wire.skip ?? []).map(changeFromWire),
    delete: (wire.delete ?? []).map(changeFromWire),
    conflicts: (wire.conflicts ?? []).map(changeFromWire),
    deleteBlockedReason: wire.delete_blocked_reason ?? "",
    requireDeleteConfirm: Boolean(wire.require_delete_confirm),
  };
}

export function planToWire(plan: SyncPlan): WireSyncPlan {
  const wire: WireSyncPlan = {};
  if (plan.add.length > 0) wire.add = plan.add.map(changeToWire);
  if (plan.update.length > 0) wire.update = plan.update.map(changeToWire);
  if (plan.touch.length > 0) wire.touch = plan.touch.map(changeToWire);
  if (plan.skip.length > 0) wire.skip = plan.skip.map(changeToWire);
  if (plan.delete.length > 0) wire.delete = plan.delete.map(changeToWire);
  if (plan.conflicts.length > 0) {
    wire.conflicts = plan.conflicts.map(changeToWire);
  }
  if (plan.deleteBlockedReason) {
    wire.delete_blocked_reason = plan.deleteBlockedReason;
  }
  if (plan.requireDeleteConfirm) {
    wire.require_delete_confirm = true;
  }
  return wire;
}

export const fromWire = manifestFromWire;
export const toWire = manifestToWire;

const RFC3339_NANO_RE =
  /^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?([Zz]|([+-])(\d{2}):(\d{2}))$/;

function isLeapYear(year: number): boolean {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0;
}

function daysInMonth(year: number, month: number): number {
  switch (month) {
    case 2:
      return isLeapYear(year) ? 29 : 28;
    case 4:
    case 6:
    case 9:
    case 11:
      return 30;
    default:
      return 31;
  }
}

// Howard Hinnant's civil-from-days inverse: exact days since 1970-01-01 UTC
// across the entire proleptic Gregorian calendar (including Go's 0001-01-01).
function daysFromCivil(year: number, month: number, day: number): bigint {
  const y = BigInt(month <= 2 ? year - 1 : year);
  const era = (y >= 0n ? y : y - 399n) / 400n;
  const yoe = y - era * 400n;
  const m = BigInt(month);
  const doy = (153n * (m > 2n ? m - 3n : m + 9n) + 2n) / 5n + BigInt(day) - 1n;
  const doe = yoe * 365n + yoe / 4n - yoe / 100n + doy;
  return era * 146097n + doe - 719468n;
}

// Parse an RFC3339 / RFC3339Nano timestamp into exact UTC nanoseconds since
// the Unix epoch without using Date (which truncates sub-millisecond digits).
export function parseRFC3339Nano(value: string): bigint {
  const match = RFC3339_NANO_RE.exec(value);
  if (!match) {
    throw new Error(`foldersync: invalid RFC3339Nano timestamp: ${value}`);
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const frac = match[7];
  const tz = match[8];
  const tzSign = match[9];
  const tzHour = match[10] !== undefined ? Number(match[10]) : 0;
  const tzMinute = match[11] !== undefined ? Number(match[11]) : 0;

  if (month < 1 || month > 12) {
    throw new Error(`foldersync: invalid month in timestamp: ${value}`);
  }
  if (day < 1 || day > daysInMonth(year, month)) {
    throw new Error(`foldersync: invalid day in timestamp: ${value}`);
  }
  if (hour > 23 || minute > 59 || second > 59) {
    throw new Error(`foldersync: invalid time in timestamp: ${value}`);
  }
  if (tz !== "Z" && tz !== "z" && (tzHour > 23 || tzMinute > 59)) {
    throw new Error(`foldersync: invalid timezone offset in timestamp: ${value}`);
  }

  let offsetSeconds = 0n;
  if (tz !== "Z" && tz !== "z") {
    const magnitude = BigInt(tzHour) * 3600n + BigInt(tzMinute) * 60n;
    offsetSeconds = tzSign === "+" ? magnitude : -magnitude;
  }

  const days = daysFromCivil(year, month, day);
  const seconds =
    days * 86400n +
    BigInt(hour) * 3600n +
    BigInt(minute) * 60n +
    BigInt(second) -
    offsetSeconds;
  const nanos = frac ? BigInt(frac.padEnd(9, "0")) : 0n;
  return seconds * 1_000_000_000n + nanos;
}

const ZERO_TIME_NS = parseRFC3339Nano("0001-01-01T00:00:00Z");

function modTimeNs(value: string): bigint {
  return value === "" ? ZERO_TIME_NS : parseRFC3339Nano(value);
}

export function modTimeEqual(a: string, b: string): boolean {
  return modTimeNs(a) === modTimeNs(b);
}

const STAGING_KEY = collisionKey(STAGING_DIR_PREFIX);

export function pathKey(wirePath: string): string {
  return collisionKey(wirePath);
}

export function normalizeWirePath(raw: string): string {
  const clean = normalizeFolder(raw);
  if (clean === ".") {
    throw new Error(`foldersync: empty or root wire path: ${raw}`);
  }
  return clean;
}

export function isStagingPath(p: string): boolean {
  if (p === "") return false;
  const normalized = p.replaceAll("\\", "/");
  for (const part of normalized.split("/")) {
    if (collisionKey(part).startsWith(STAGING_KEY)) {
      return true;
    }
  }
  return false;
}

export function pathDepth(wirePath: string): number {
  if (wirePath === "" || wirePath === ".") return 0;
  let depth = 1;
  for (let i = 0; i < wirePath.length; i += 1) {
    if (wirePath.charCodeAt(i) === 47) depth += 1;
  }
  return depth;
}

function posixDir(p: string): string {
  const idx = p.lastIndexOf("/");
  if (idx < 0) return ".";
  if (idx === 0) return "/";
  return p.slice(0, idx);
}

function validateCanonicalPath(p: string): void {
  if (isStagingPath(p)) {
    throw new Error(`foldersync: reserved staging path: ${p}`);
  }
  const clean = normalizeWirePath(p);
  if (clean !== p) {
    throw new Error(
      `foldersync: non-canonical path "${p}" (want "${clean}")`,
    );
  }
}

function validateEntryShape(e: SyncEntry): void {
  if (e.kind !== SyncKind.File && e.kind !== SyncKind.Directory) {
    throw new Error(
      `foldersync: unsupported entry kind: "${e.path}" (kind=${e.kind})`,
    );
  }
  validateCanonicalPath(e.path);
  if (!Number.isSafeInteger(e.size) || e.size < 0) {
    throw new Error(
      `foldersync: invalid manifest entry: "${e.path}" has invalid size ${e.size}`,
    );
  }
  if (e.kind === SyncKind.Directory && e.size !== 0) {
    throw new Error(
      `foldersync: invalid manifest entry: directory "${e.path}" has size ${e.size}`,
    );
  }
  if (!Number.isInteger(e.mode) || e.mode < 0 || e.mode > 0o777) {
    throw new Error(
      `foldersync: invalid manifest entry: "${e.path}" mode ${e.mode} has non-permission bits`,
    );
  }
  const hashLen = e.hash?.byteLength ?? 0;
  if (hashLen !== 0 && hashLen !== HASH_SIZE) {
    throw new Error(
      `foldersync: invalid manifest entry: "${e.path}" hash length ${hashLen}, want ${HASH_SIZE}`,
    );
  }
  if (e.kind === SyncKind.Directory && hashLen !== 0) {
    throw new Error(
      `foldersync: invalid manifest entry: directory "${e.path}" has a hash`,
    );
  }
}

function validateSkipped(s: SyncSkipped): void {
  if (!VALID_SKIP_REASONS.has(s.reason)) {
    throw new Error(
      `foldersync: invalid manifest entry: skipped "${s.path}" has unknown reason "${s.reason}"`,
    );
  }
  if (s.reason === "invalid_name" && s.path === "") {
    return;
  }
  validateCanonicalPath(s.path);
}

export function validateManifest(manifest: SyncManifest | null | undefined): void {
  if (!manifest) {
    throw new Error("foldersync: nil manifest");
  }
  if (
    manifest.entries.length > MAX_MANIFEST_ENTRIES ||
    manifest.skipped.length > MAX_MANIFEST_ENTRIES - manifest.entries.length
  ) {
    throw new Error(
      `foldersync: manifest exceeds maximum entry limit: ${manifest.entries.length} entries + ${manifest.skipped.length} skipped > ${MAX_MANIFEST_ENTRIES}`,
    );
  }

  const directories = new Set<string>();
  const byKey = new Map<string, { path: string; kind: SyncKind }>();

  for (const e of manifest.entries) {
    validateEntryShape(e);
    const key = pathKey(e.path);
    const prev = byKey.get(key);
    if (prev) {
      throw new Error(
        `foldersync: receive path collision: "${e.path}" conflicts with "${prev.path}"`,
      );
    }
    byKey.set(key, { path: e.path, kind: e.kind });
    if (e.kind === SyncKind.Directory) {
      directories.add(e.path);
    }
  }

  for (const e of manifest.entries) {
    let ancestor = posixDir(e.path);
    while (ancestor !== "." && ancestor !== "/") {
      const existing = byKey.get(pathKey(ancestor));
      if (existing && existing.kind !== SyncKind.Directory) {
        throw new Error(
          `foldersync: receive path collision: "${e.path}" is beneath non-directory "${existing.path}"`,
        );
      }
      ancestor = posixDir(ancestor);
    }
  }

  for (const e of manifest.entries) {
    const parent = posixDir(e.path);
    if (parent !== "." && !directories.has(parent)) {
      throw new Error(
        `foldersync: invalid manifest entry: "${e.path}" has no parent directory entry "${parent}"`,
      );
    }
  }

  for (const s of manifest.skipped) {
    validateSkipped(s);
  }
}

interface EntryPair {
  src: SyncEntry;
  dst?: SyncEntry;
}

function matchEntries(
  src: SyncManifest,
  dst: SyncManifest,
): { pairs: EntryPair[]; matched: Set<string> } {
  const byPath = new Map<string, SyncEntry>();
  const byKey = new Map<string, SyncEntry[]>();
  for (const d of dst.entries) {
    byPath.set(d.path, d);
    const k = pathKey(d.path);
    const list = byKey.get(k);
    if (list) {
      list.push(d);
    } else {
      byKey.set(k, [d]);
    }
  }

  const matched = new Set<string>();
  const pairs: EntryPair[] = new Array(src.entries.length);
  const pending: number[] = [];

  for (let i = 0; i < src.entries.length; i += 1) {
    const s = src.entries[i];
    const exact = byPath.get(s.path);
    if (exact) {
      pairs[i] = { src: s, dst: exact };
      matched.add(exact.path);
    } else {
      pairs[i] = { src: s };
      pending.push(i);
    }
  }

  for (const i of pending) {
    const candidates = byKey.get(pathKey(pairs[i].src.path));
    if (!candidates) continue;
    for (const d of candidates) {
      if (!matched.has(d.path)) {
        pairs[i].dst = d;
        matched.add(d.path);
        break;
      }
    }
  }

  return { pairs, matched };
}

export function dstHashTargets(
  src: SyncManifest,
  dst: SyncManifest,
  needHash: string[],
): string[] {
  const want = new Set(needHash);
  const { pairs } = matchEntries(src, dst);
  const out: string[] = [];
  for (const pr of pairs) {
    if (pr.dst && want.has(pr.src.path)) {
      out.push(pr.dst.path);
    }
  }
  return out;
}

function touchReason(renamed: boolean, mode: boolean, mtime: boolean): string {
  const parts: string[] = [];
  if (renamed) parts.push("rename case");
  if (mtime) parts.push("same content, mtime differs");
  if (mode) parts.push("mode differs");
  return parts.join(", ");
}

function diskParentWirePath(diskPath: string): string | null {
  const slashPath = diskPath.replaceAll("\\", "/").replace(/\/+$/, "");
  const parent = posixDir(slashPath);
  if (parent === "." || parent === "/") return null;
  try {
    return normalizeWirePath(parent);
  } catch {
    return null;
  }
}

export function sortDeletes(changes: SyncChange[]): SyncChange[] {
  return changes.sort((a, b) => {
    const depthDiff = pathDepth(b.path) - pathDepth(a.path);
    if (depthDiff !== 0) return depthDiff;
    return a.path < b.path ? -1 : a.path > b.path ? 1 : 0;
  });
}

export function deleteCandidates(
  src: SyncManifest,
  dst: SyncManifest,
  matched: Set<string>,
): SyncChange[] {
  const protectedRoots = new Set<string>();
  for (const s of src.skipped) {
    if (s.path !== "") {
      protectedRoots.add(pathKey(s.path));
    }
  }

  const keepAncestors = new Set<string>();
  const markAncestors = (p: string) => {
    for (
      let dir = posixDir(p);
      dir !== "." && dir !== "/" && !keepAncestors.has(dir);
      dir = posixDir(dir)
    ) {
      keepAncestors.add(dir);
    }
  };

  for (const s of dst.skipped) {
    if (s.path !== "") {
      markAncestors(s.path);
    } else if (s.diskPath) {
      const wire = diskParentWirePath(s.diskPath);
      if (wire !== null) {
        keepAncestors.add(wire);
        markAncestors(wire);
      }
    }
  }

  const isProtected = (p: string): boolean => {
    for (let cur = p; cur !== "." && cur !== "/"; cur = posixDir(cur)) {
      if (protectedRoots.has(pathKey(cur))) {
        return true;
      }
    }
    return false;
  };

  const out: SyncChange[] = [];
  for (const d of dst.entries) {
    if (matched.has(d.path)) continue;
    if (isProtected(d.path)) {
      markAncestors(d.path);
      continue;
    }
    out.push({ path: d.path, dst: d, reason: "not in source" });
  }

  const filtered = out.filter((c) => !keepAncestors.has(c.path));
  return sortDeletes(filtered);
}

export function enforceDeleteRules(
  plan: SyncPlan,
  src: SyncManifest,
  dst: SyncManifest,
  opts: SyncDiffOptions,
): void {
  if (!opts.delete || plan.delete.length === 0) {
    return;
  }
  const n = plan.delete.length;
  if (!src.complete) {
    plan.deleteBlockedReason = `source scan was incomplete; ${n} deletion(s) skipped this round`;
  } else if (src.entries.length === 0 && !opts.allowEmptySource) {
    plan.deleteBlockedReason = `source is empty; refusing to delete ${n} destination item(s) without --allow-empty-source`;
  }
  if (plan.deleteBlockedReason !== "") {
    plan.delete = [];
    return;
  }

  let dstFiles = 0;
  for (const e of dst.entries) {
    if (e.kind === SyncKind.File) dstFiles += 1;
  }
  let deleteFiles = 0;
  for (const c of plan.delete) {
    if (c.dst?.kind === SyncKind.File) deleteFiles += 1;
  }
  plan.requireDeleteConfirm = deleteFiles * 2 > dstFiles;
}

export function resolveTypeChanges(
  plan: SyncPlan,
  dst: SyncManifest,
  idx: number[],
): void {
  if (idx.length === 0) return;

  const deleting = new Set(plan.delete.map((c) => c.path));
  const conflict = new Set<number>();

  for (const i of idx) {
    const dstDir = plan.update[i].dst!.path;
    const dir = `${dstDir}/`;
    for (const d of dst.entries) {
      if (d.path.startsWith(dir) && !deleting.has(d.path)) {
        conflict.add(i);
        break;
      }
    }
    if (!conflict.has(i)) {
      for (const s of dst.skipped) {
        if (s.path !== "") {
          if (s.path.startsWith(dir)) {
            conflict.add(i);
            break;
          }
        } else if (s.diskPath) {
          const parent = diskParentWirePath(s.diskPath);
          if (
            parent !== null &&
            (parent === dstDir || parent.startsWith(dir))
          ) {
            conflict.add(i);
            break;
          }
        }
      }
    }
  }

  if (conflict.size === 0) return;

  const kept: SyncChange[] = [];
  for (let i = 0; i < plan.update.length; i += 1) {
    const c = plan.update[i];
    if (conflict.has(i)) {
      c.reason =
        "conflict: destination directory is not empty (needs --delete)";
      plan.conflicts.push(c);
      continue;
    }
    kept.push(c);
  }
  plan.update = kept;
}

export function compare(
  src: SyncManifest,
  dst: SyncManifest,
  opts: SyncDiffOptions = {},
): CompareResult {
  validateManifest(src);
  if (!dst) {
    throw new Error("foldersync: nil destination manifest");
  }

  const { pairs, matched } = matchEntries(src, dst);
  const plan: SyncPlan = {
    add: [],
    update: [],
    touch: [],
    skip: [],
    delete: [],
    conflicts: [],
    deleteBlockedReason: "",
    requireDeleteConfirm: false,
  };
  const needHash: string[] = [];
  const typeChanges: number[] = [];

  const checksum = Boolean(opts.checksum);
  const allowDelete = Boolean(opts.delete);
  const ignoreMetadata = Boolean(opts.ignoreMetadata);

  for (const { src: s, dst: d } of pairs) {
    if (!d) {
      plan.add.push({ path: s.path, src: s, reason: "new" });
      continue;
    }
    const c: SyncChange = { path: s.path, src: s, dst: d };
    const renamed = d.path !== s.path;
    const modeDiffers = !ignoreMetadata && s.mode !== d.mode;

    if (s.kind !== d.kind) {
      c.typeChanged = true;
      c.reason = "type changed";
      if (d.kind === SyncKind.Directory) {
        typeChanges.push(plan.update.length);
      }
      plan.update.push(c);
    } else if (s.kind === SyncKind.Directory) {
      if (renamed || modeDiffers) {
        c.reason = touchReason(renamed, modeDiffers, false);
        plan.touch.push(c);
      } else {
        plan.skip.push(c);
      }
    } else if (s.size !== d.size) {
      c.reason = "size changed";
      plan.update.push(c);
    } else if (!checksum && modTimeEqual(s.modTime, d.modTime)) {
      if (renamed || modeDiffers) {
        c.reason = touchReason(renamed, modeDiffers, false);
        plan.touch.push(c);
      } else {
        plan.skip.push(c);
      }
    } else if (
      !s.hash ||
      s.hash.byteLength === 0 ||
      !d.hash ||
      d.hash.byteLength === 0
    ) {
      needHash.push(s.path);
    } else if (!bytesEqual(s.hash, d.hash)) {
      c.reason = "content changed";
      plan.update.push(c);
    } else {
      const mtimeDiffers = !ignoreMetadata && !modTimeEqual(s.modTime, d.modTime);
      if (renamed || modeDiffers || mtimeDiffers) {
        c.reason = touchReason(renamed, modeDiffers, mtimeDiffers);
        plan.touch.push(c);
      } else {
        plan.skip.push(c);
      }
    }
  }

  if (needHash.length > 0) {
    return { needHash };
  }

  if (allowDelete) {
    plan.delete = deleteCandidates(src, dst, matched);
  }
  enforceDeleteRules(plan, src, dst, opts);
  resolveTypeChanges(plan, dst, typeChanges);
  return { plan };
}
