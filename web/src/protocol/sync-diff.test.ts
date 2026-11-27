import { describe, expect, it } from "vitest";
import rawVectors from "../../testdata/folder-sync-vectors.json";
import {
  compare,
  diffOptionsFromWire,
  dstHashTargets,
  manifestFromWire,
  manifestToWire,
  modTimeEqual,
  parseRFC3339Nano,
  type SyncChange,
  type SyncPlan,
  type WireSyncDiffOptions,
  type WireSyncManifest,
} from "./sync-diff";

interface VectorPlan {
  add: string[];
  update: string[];
  touch: string[];
  skip: string[];
  delete: string[];
  conflicts: string[];
  type_changed: string[];
  renames: Record<string, string>;
  delete_blocked: boolean;
  require_delete_confirm: boolean;
}

interface VectorWant {
  error: boolean;
  need_hash?: string[];
  plan?: VectorPlan;
}

interface VectorCase {
  name: string;
  src: WireSyncManifest;
  dst: WireSyncManifest;
  opts: WireSyncDiffOptions;
  want: VectorWant;
}

interface VectorFile {
  version: number;
  cases: VectorCase[];
}

const vectors = rawVectors as VectorFile;

function lexSort(values: string[]): string[] {
  return [...values].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}

function summarizePlan(plan: SyncPlan): VectorPlan {
  const typeChanged: string[] = [];
  const renames: Record<string, string> = {};
  const groups: SyncChange[][] = [
    plan.add,
    plan.update,
    plan.touch,
    plan.skip,
    plan.delete,
    plan.conflicts,
  ];
  for (const group of groups) {
    for (const c of group) {
      if (c.typeChanged) {
        typeChanged.push(c.path);
      }
      if (c.src && c.dst && c.dst.path !== c.path) {
        renames[c.path] = c.dst.path;
      }
    }
  }
  return {
    add: lexSort(plan.add.map((c) => c.path)),
    update: lexSort(plan.update.map((c) => c.path)),
    touch: lexSort(plan.touch.map((c) => c.path)),
    skip: lexSort(plan.skip.map((c) => c.path)),
    delete: plan.delete.map((c) => c.path),
    conflicts: lexSort(plan.conflicts.map((c) => c.path)),
    type_changed: lexSort(typeChanged),
    renames,
    delete_blocked: plan.deleteBlockedReason !== "",
    require_delete_confirm: plan.requireDeleteConfirm,
  };
}

describe("shared folder-sync diff vectors", () => {
  it("loads version 1 with at least 35 cases", () => {
    expect(vectors.version).toBe(1);
    expect(vectors.cases.length).toBeGreaterThanOrEqual(35);
  });

  for (const tc of vectors.cases) {
    it(tc.name, () => {
      const src = manifestFromWire(tc.src);
      const dst = manifestFromWire(tc.dst);
      const opts = diffOptionsFromWire(tc.opts);

      if (tc.want.error) {
        expect(() => compare(src, dst, opts)).toThrow();
        return;
      }

      const result = compare(src, dst, opts);
      if (tc.want.need_hash) {
        expect(result.plan).toBeUndefined();
        expect(lexSort(result.needHash ?? [])).toEqual(tc.want.need_hash);
        return;
      }

      expect(result.needHash).toBeUndefined();
      expect(result.plan).toBeDefined();
      expect(summarizePlan(result.plan!)).toEqual(tc.want.plan);
    });
  }
});

describe("RFC3339Nano timestamp parser", () => {
  it("parses epoch and sub-millisecond nanoseconds exactly", () => {
    expect(parseRFC3339Nano("1970-01-01T00:00:00Z")).toBe(0n);
    const base = parseRFC3339Nano("2026-11-01T12:00:00Z");
    const plus1ns = parseRFC3339Nano("2026-11-01T12:00:00.000000001Z");
    expect(plus1ns - base).toBe(1n);
    expect(
      modTimeEqual("2026-11-01T12:00:00Z", "2026-11-01T12:00:00.000000001Z"),
    ).toBe(false);
    expect(
      modTimeEqual("2026-11-01T12:00:00.5Z", "2026-11-01T12:00:00.500000000Z"),
    ).toBe(true);
  });

  it("normalizes timezone offsets and handles pre-1970 and zero time", () => {
    expect(
      modTimeEqual("2026-11-01T12:00:00Z", "2026-11-01T14:00:00+02:00"),
    ).toBe(true);
    expect(
      modTimeEqual("2026-11-01T12:00:00z", "2026-11-01t07:00:00-05:00"),
    ).toBe(true);
    expect(parseRFC3339Nano("1969-12-31T23:59:59.5Z")).toBe(-500_000_000n);
    expect(parseRFC3339Nano("0001-01-01T00:00:00Z")).toBe(
      -62135596800_000_000_000n,
    );
    expect(modTimeEqual("", "0001-01-01T00:00:00Z")).toBe(true);
    expect(
      parseRFC3339Nano("2000-03-01T00:00:00Z") -
        parseRFC3339Nano("2000-02-29T00:00:00Z"),
    ).toBe(86400_000_000_000n);
  });

  it.each([
    "",
    "not-a-date",
    "2026-11-01T12:00:00",
    "2026-13-01T12:00:00Z",
    "2025-02-29T00:00:00Z",
    "2026-11-01T24:00:00Z",
    "2026-11-01T12:60:00Z",
    "2026-11-01T12:00:60Z",
    "2026-11-01T12:00:00.Z",
    "2026-11-01T12:00:00.0000000001Z",
    "2026-11-01T12:00:00+24:00",
  ])("rejects malformed timestamp %s", (value) => {
    expect(() => parseRFC3339Nano(value)).toThrow();
  });
});

describe("sync-diff helpers", () => {
  it("resolves destination hash targets across case-only renames", () => {
    const src = manifestFromWire({
      entries: [
        {
          path: "Readme.md",
          size: 3,
          mod_time: "2026-11-01T12:00:05Z",
          mode: 420,
          kind: 0,
        },
        {
          path: "a",
          size: 1,
          mod_time: "2026-11-01T12:00:05Z",
          mode: 420,
          kind: 0,
        },
        {
          path: "new",
          size: 1,
          mod_time: "2026-11-01T12:00:00Z",
          mode: 420,
          kind: 0,
        },
      ],
      complete: true,
    });
    const dst = manifestFromWire({
      entries: [
        {
          path: "README.md",
          size: 3,
          mod_time: "2026-11-01T12:00:00Z",
          mode: 420,
          kind: 0,
        },
        {
          path: "a",
          size: 1,
          mod_time: "2026-11-01T12:00:00Z",
          mode: 420,
          kind: 0,
        },
      ],
      complete: true,
    });
    const res = compare(src, dst);
    expect(res.needHash).toEqual(["Readme.md", "a"]);
    expect(dstHashTargets(src, dst, res.needHash!)).toEqual(["README.md", "a"]);
  });

  it("round-trips wire manifests with base64 hashes", () => {
    const wire: WireSyncManifest = {
      entries: [
        {
          path: "a",
          size: 3,
          mod_time: "2026-11-01T12:00:00Z",
          mode: 420,
          kind: 0,
          hash: "AQIDBAUGBwg=",
        },
      ],
      skipped: [{ path: "build", reason: "filtered" }],
      complete: true,
    };
    expect(manifestToWire(manifestFromWire(wire))).toEqual(wire);
  });
});
