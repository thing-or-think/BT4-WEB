import { describe, expect, it } from "vitest";
import {
  FEATURE_FOLDER_SYNC,
  ManifestAssembler,
  SyncAbortedError,
  SyncPathError,
  SyncPeerUnsupportedError,
  SyncProtocolError,
  applyExclusions,
  comparePaths,
  computeDiff,
  createLoopbackConnection,
  decodeSyncMessage,
  encodeSyncMessage,
  findCaseConflicts,
  hashBlob,
  manifestChanges,
  manifestMessages,
  planView,
  runSyncHost,
  runSyncJoin,
  sameModTime,
  scanDirectory,
  validateSyncPath,
  type SyncConnection,
  type SyncEntry,
  type SyncManifest,
  type SyncPlanView,
  type SyncReporter,
  type SyncSummary,
  type SyncWatchEvent,
  type SyncWebConfig,
} from "./sync";
import { textEncoder } from "./bytes";
import {
  MemoryDirectory,
  buildTree,
  fileAt,
  readTree,
  snapshotTree,
  writeFileAt,
  type TreeSpec,
} from "../test/memory-fs";

const ms = 1_000_000n;

function file(path: string, size: number, mtimeMs: number, hash?: string): SyncEntry {
  return { path, size, mtime: BigInt(mtimeMs) * ms, mode: 0o644, dir: false, ...(hash ? { hash } : {}) };
}

function dir(path: string): SyncEntry {
  return { path, size: 0, mtime: 0n, mode: 0x80000000 + 0o755, dir: true };
}

function manifest(...entries: SyncEntry[]): SyncManifest {
  return { hashAlgo: "xxhash", entries };
}

function paths(entries: { path: string }[]) {
  return entries.map((entry) => entry.path);
}

describe("validateSyncPath", () => {
  it.each(["a.txt", "a/b/c.txt", "deep/1/2/3/4/5/6/7/8/9/file", ".git/config", "space name.txt"])(
    "accepts %s",
    (path) => expect(validateSyncPath(path)).toBe(path),
  );

  it("normalizes to NFC like receivefs.Normalize", () => {
    expect(validateSyncPath("cafe\u0301.txt")).toBe("caf\u00e9.txt");
  });

  it.each([
    ["", "empty"],
    ["/etc/passwd", "absolute"],
    ["C:/Windows/system.ini", "drive letter"],
    ["c:evil", "drive letter"],
    ["a\\b.txt", "backslash"],
    ["..\\evil", "backslash"],
    ["../evil", "traversal"],
    ["a/../../evil", "traversal"],
    ["./a.txt", "dot segment"],
    ["a//b", "empty segment"],
    ["a/", "trailing slash"],
    [".ssh/authorized_keys", "sensitive"],
    ["home/.gnupg/pubring.kbx", "sensitive"],
    ["SSH~1/id_rsa", "8.3 alias of .ssh"],
    ["CON.txt", "device name"],
    ["file.txt:stream", "alternate data stream"],
    ["trailing.", "trailing dot"],
    ["bad\u0007bell", "control character"],
    ["x/.croc-sync-tmp-123", "sync temp file"],
  ])("rejects %j (%s)", (path) => {
    expect(() => validateSyncPath(path)).toThrow(SyncPathError);
  });
});

describe("wire messages", () => {
  it("round-trips nanosecond mtimes exactly", () => {
    const entry: SyncEntry = {
      path: "a/b.txt",
      size: 12,
      mtime: 1764230400123456789n,
      mode: 0o644,
      dir: false,
      hash: "00ff00ff00ff00ff",
    };
    const frame = encodeSyncMessage({ type: "sync-manifest-batch", index: 3, entries: [entry, dir("a")] });
    expect(frame.t).toBe("sync-manifest-batch");
    expect(frame.n).toBe(3);
    const json = new TextDecoder().decode(frame.b);
    expect(json).toContain('"mtime":1764230400123456789');
    expect(json).toContain('"path":"a/b.txt"');
    const decoded = decodeSyncMessage(frame);
    expect(decoded).toEqual({ type: "sync-manifest-batch", index: 3, entries: [entry, dir("a")] });
  });

  it("decodes Go-shaped JSON with omitted optional fields", () => {
    const frame = {
      t: "sync-manifest-batch" as const,
      b: textEncoder.encode('{"entries":[{"path":"x","size":1,"mtime":5,"mode":420,"dir":false}]}'),
    };
    expect(decodeSyncMessage(frame)).toEqual({
      type: "sync-manifest-batch",
      index: 0,
      entries: [{ path: "x", size: 1, mtime: 5n, mode: 420, dir: false }],
    });
  });

  it.each([
    ['{"entries":[{"path":"../evil","size":1,"mtime":0,"mode":0,"dir":false}]}', SyncPathError],
    ['{"entries":[{"path":"C:\\\\evil","size":1,"mtime":0,"mode":0,"dir":false}]}', SyncPathError],
    ['{"entries":[{"path":"ok","size":-1,"mtime":0,"mode":0,"dir":false}]}', SyncProtocolError],
    ['{"entries":[{"path":"ok","size":1.5,"mtime":0,"mode":0,"dir":false}]}', SyncProtocolError],
    ['{"entries":[{"path":"ok","size":9007199254740993,"mtime":0,"mode":0,"dir":false}]}', SyncProtocolError],
    ['{"entries":[{"path":"ok","size":1,"mtime":0,"mode":0,"dir":false,"hash":"XYZ"}]}', SyncProtocolError],
    ['{"entries":"nope"}', SyncProtocolError],
    ["not json", SyncProtocolError],
  ])("rejects malicious batch %s", (json, error) => {
    expect(() => decodeSyncMessage({ t: "sync-manifest-batch", b: textEncoder.encode(json) })).toThrow(error);
  });

  it("rejects unknown message types and unsafe delete paths", () => {
    expect(() => decodeSyncMessage({ t: "fileinfo" as never })).toThrow(SyncProtocolError);
    expect(() =>
      decodeSyncMessage({ t: "sync-delete", b: textEncoder.encode('{"paths":["ok","../../home"]}') }),
    ).toThrow(SyncPathError);
  });
});

describe("manifest batching", () => {
  const big = manifest(...Array.from({ length: 5 }, (_, index) => file(`f${index}`, index, 1)));

  it("splits into start, batches and end, and reassembles", () => {
    const messages = manifestMessages(big, 2);
    expect(messages.map((message) => message.type)).toEqual([
      "sync-manifest-start",
      "sync-manifest-batch",
      "sync-manifest-batch",
      "sync-manifest-batch",
      "sync-manifest-end",
    ]);
    const assembler = new ManifestAssembler();
    let result: SyncManifest | undefined;
    for (const message of messages) result = assembler.accept(decodeSyncMessage(encodeSyncMessage(message)));
    expect(result).toEqual(big);
  });

  it("sends an empty folder as start + end", () => {
    const messages = manifestMessages(manifest());
    expect(messages.map((message) => message.type)).toEqual(["sync-manifest-start", "sync-manifest-end"]);
    const assembler = new ManifestAssembler();
    assembler.accept(messages[0]);
    expect(assembler.accept(messages[1])).toEqual(manifest());
  });

  it("rejects out-of-order, duplicate, over-long and short manifests", () => {
    const [start, b0, b1] = manifestMessages(big, 2);
    const outOfOrder = new ManifestAssembler();
    outOfOrder.accept(start);
    expect(() => outOfOrder.accept(b1)).toThrow(/out of order/);

    const duplicate = new ManifestAssembler();
    duplicate.accept(start);
    duplicate.accept(b0);
    expect(() => duplicate.accept({ ...b0, index: 1 } as typeof b0)).toThrow(/Duplicate/);

    const tooMany = new ManifestAssembler();
    tooMany.accept({ type: "sync-manifest-start", total: 1, hashAlgo: "xxhash" });
    expect(() => tooMany.accept(b0)).toThrow(/more entries/);

    const short = new ManifestAssembler();
    short.accept(start);
    short.accept(b0);
    expect(() => short.accept({ type: "sync-manifest-end" })).toThrow(/announced 5/);

    expect(() => new ManifestAssembler().accept(b0)).toThrow(/before start/);
  });
});

describe("computeDiff", () => {
  it("classifies add, update, skip and delete", () => {
    const source = manifest(dir("d"), file("d/new.txt", 1, 10), file("same.txt", 3, 10), file("grown.txt", 5, 10));
    const target = manifest(dir("d"), file("same.txt", 3, 10), file("grown.txt", 4, 10), file("extra.txt", 1, 1));
    const plan = computeDiff(source, target, { delete: true });
    expect(paths(plan.add)).toEqual(["d/new.txt"]);
    expect(paths(plan.update)).toEqual(["grown.txt"]);
    expect(paths(plan.skip)).toEqual(["d", "same.txt"]);
    expect(paths(plan.delete)).toEqual(["extra.txt"]);
    expect(computeDiff(source, target).delete).toEqual([]);
  });

  it("skips everything when nothing changed", () => {
    const tree = manifest(dir("a"), file("a/b", 1, 1), file("c", 0, 2));
    const plan = computeDiff(tree, structuredClone(tree), { delete: true });
    expect(plan.add.length + plan.update.length + plan.delete.length).toBe(0);
    expect(plan.skip).toHaveLength(3);
  });

  it("detects same-size content changes with --checksum", () => {
    const source = manifest(file("a", 4, 10, "1111"));
    const target = manifest(file("a", 4, 10, "2222"));
    expect(paths(computeDiff(source, target, { checksum: true }).update)).toEqual(["a"]);
    // Without --checksum, same size + same mtime is trusted (rsync semantics).
    expect(paths(computeDiff(source, target).skip)).toEqual(["a"]);
  });

  it("uses hashes for same-size files whose mtime differs (tier 2)", () => {
    const source = manifest(file("same", 4, 10, "aaaa"), file("changed", 4, 10, "bbbb"), file("unknown", 4, 10));
    const target = manifest(file("same", 4, 99, "aaaa"), file("changed", 4, 99, "cccc"), file("unknown", 4, 99, "dddd"));
    const plan = computeDiff(source, target);
    expect(paths(plan.skip)).toEqual(["same"]);
    expect(paths(plan.update)).toEqual(["changed", "unknown"]);
  });

  it("compares mtimes at millisecond resolution", () => {
    expect(sameModTime(1_000_000_123n, 1_000_999_999n)).toBe(true);
    expect(sameModTime(1_000_000_000n, 1_001_000_000n)).toBe(false);
  });

  it("requires hashes in checksum mode", () => {
    expect(() => computeDiff(manifest(file("a", 1, 1)), manifest(file("a", 1, 1)), { checksum: true })).toThrow(
      SyncProtocolError,
    );
  });

  it("builds a plan view with byte totals", () => {
    const plan = computeDiff(manifest(file("a", 10, 1), file("b", 7, 1)), manifest(file("b", 7, 1)));
    const view = planView(plan, true);
    expect(view).toMatchObject({ dryRun: true, transferBytes: 10, savedBytes: 7, skipCount: 1 });
  });
});

describe("filters and conflicts", () => {
  it("removes excluded entries and their descendants", () => {
    const target = manifest(dir("node_modules"), file("node_modules/x/y.js", 1, 1), file("keep.txt", 1, 1), file("secret.env", 1, 1));
    const filtered = applyExclusions(target, { exclude: ["node_modules"], excludeFiles: ["./secret.env"] });
    expect(paths(filtered.entries)).toEqual(["keep.txt"]);
  });

  it("finds case-only conflicts with the target and inside the plan", () => {
    const conflicts = findCaseConflicts(
      [file("README.md", 1, 1), file("x/A.txt", 1, 1), file("x/a.txt", 1, 1), file("same.txt", 1, 1)],
      [file("Readme.md", 1, 1), file("same.txt", 1, 1)],
      [file("Readme.md", 1, 1)],
    );
    expect([...conflicts.keys()].sort()).toEqual(["README.md", "Readme.md", "x/A.txt", "x/a.txt"]);
  });

  it("sorts paths like Go (code point order)", () => {
    const sorted = ["b", "a/b", "\u{1F600}", "\uFF5E", "a"].sort(comparePaths);
    expect(sorted).toEqual(["a", "a/b", "b", "\uFF5E", "\u{1F600}"]);
  });
});

describe("scanDirectory", () => {
  it("lists nested folders, empty files and empty folders but skips temp files", async () => {
    const root = buildTree({
      "empty-dir": null,
      "a/b/c/deep.txt": "deep",
      "a/empty.txt": "",
      ".croc-sync-tmp-123": "partial",
      "a/.croc-sync-tmp-x": "partial",
    });
    const { manifest: scanned, rejected } = await scanDirectory(root, { computeHash: true });
    expect(rejected).toEqual([]);
    expect(paths(scanned.entries)).toEqual(["a", "a/b", "a/b/c", "a/b/c/deep.txt", "a/empty.txt", "empty-dir"]);
    const empty = scanned.entries.find((entry) => entry.path === "a/empty.txt")!;
    expect(empty).toMatchObject({ size: 0, dir: false, hash: "ef46db3751d8e999" });
    expect(empty.mtime % ms).toBe(0n);
  });

  it("returns an empty manifest for an empty folder", async () => {
    expect((await scanDirectory(new MemoryDirectory())).manifest).toEqual(manifest());
  });

  it("reports unsafe names instead of syncing them and applies excludes", async () => {
    const root = buildTree({ ".ssh/id_rsa": "key", "CON": "x", "ok.txt": "ok", "build/out.o": "o" });
    const { manifest: scanned, rejected } = await scanDirectory(root, { exclude: ["build"] });
    expect(paths(scanned.entries)).toEqual(["ok.txt"]);
    expect(paths(rejected).sort()).toEqual([".ssh", "CON"]);
  });

  it("hashes with XXH64 like croc's Go xxhash", async () => {
    expect(await hashBlob(new Blob(["hello"]))).toBe("26c7827d889f6da3");
  });
});

// ---------------------------------------------------------------------
// End-to-end sessions over the loopback connection.
// ---------------------------------------------------------------------

type Recorded = {
  plans: SyncPlanView[];
  summaries: SyncSummary[];
  done: Array<[string, string | undefined]>;
  events: SyncWatchEvent[];
};

function recorder(onPlan?: (plan: SyncPlanView) => boolean): [SyncReporter, Recorded] {
  const recorded: Recorded = { plans: [], summaries: [], done: [], events: [] };
  return [
    {
      onPlan(plan) {
        recorded.plans.push(plan);
        return onPlan ? onPlan(plan) : true;
      },
      onFileDone: (path, error) => recorded.done.push([path, error]),
      onRoundDone: (summary) => recorded.summaries.push(summary),
      onWatchEvent: (event) => recorded.events.push(event),
    },
    recorded,
  ];
}

async function sync(
  source: MemoryDirectory,
  target: MemoryDirectory,
  config: SyncWebConfig = {},
  onPlan?: (plan: SyncPlanView) => boolean,
) {
  const [hostConnection, joinConnection] = createLoopbackConnection();
  const [hostReporter, host] = recorder(onPlan);
  const [joinReporter, join] = recorder();
  const results = await Promise.allSettled([
    runSyncHost({ connection: hostConnection, directory: source, config, reporter: hostReporter }),
    runSyncJoin({ connection: joinConnection, directory: target, config, reporter: joinReporter }),
  ]);
  return { host, join, results };
}

describe("sync sessions", () => {
  const sourceSpec: TreeSpec = {
    "docs/readme.md": "# hi",
    "docs/empty.txt": "",
    "empty-folder": null,
    "a/b/c/d/e/deep.bin": "0123456789",
    "top.txt": "top",
  };

  it("mirrors a nested tree, then skips 100% on the second run", async () => {
    const source = buildTree(sourceSpec);
    const target = new MemoryDirectory();
    const first = await sync(source, target);
    expect(first.results.map((result) => result.status)).toEqual(["fulfilled", "fulfilled"]);
    expect(readTree(target)).toEqual(readTree(source));
    expect(first.host.summaries[0]).toMatchObject({ added: 11, updated: 0, deleted: 0, errors: [] });

    // The browser cannot restore mtimes, so every target mtime now differs
    // from the source. Target hashes still let the second run skip it all.
    const second = await sync(source, target, { delete: true });
    const plan = second.host.plans[0];
    expect(plan.add.length + plan.update.length + plan.delete.length).toBe(0);
    expect(plan.skip).toHaveLength(11);
    expect(second.host.summaries[0]).toMatchObject({ added: 0, updated: 0, deleted: 0, skipped: 11, bytesSent: 0 });
  });

  it("dry run reports the plan without touching the target", async () => {
    const source = buildTree({ "new.txt": "new", "changed.txt": "v2-longer" });
    const target = buildTree({ "changed.txt": "v1", "extra.txt": "bye", "keep/inner.txt": "x" });
    const before = snapshotTree(target);
    const { host, join, results } = await sync(source, target, { dryRun: true, delete: true });
    expect(results.map((result) => result.status)).toEqual(["fulfilled", "fulfilled"]);
    expect(snapshotTree(target)).toEqual(before);
    expect(paths(host.plans[0].add)).toEqual(["new.txt"]);
    expect(paths(host.plans[0].update)).toEqual(["changed.txt"]);
    expect(paths(host.plans[0].delete)).toEqual(["extra.txt", "keep", "keep/inner.txt"]);
    expect(join.plans[0]).toMatchObject({ dryRun: true });
    expect(paths(join.plans[0].delete)).toEqual(["extra.txt", "keep", "keep/inner.txt"]);
    expect(join.summaries).toEqual([]);
  });

  it("--delete removes only extra entries inside the target, never excluded ones", async () => {
    const source = buildTree({ "keep.txt": "k" });
    const target = buildTree({
      "keep.txt": "k",
      "old.txt": "old",
      "old-dir/nested/file.txt": "x",
      "node_modules/pkg/index.js": "excluded",
    });
    const { host, results } = await sync(source, target, { delete: true, exclude: ["node_modules"] });
    expect(results.map((result) => result.status)).toEqual(["fulfilled", "fulfilled"]);
    expect(readTree(target)).toEqual({
      "keep.txt": "k",
      node_modules: null,
      "node_modules/pkg": null,
      "node_modules/pkg/index.js": "excluded",
    });
    expect(host.summaries[0]).toMatchObject({ deleted: 4, errors: [] });
  });

  it("does not delete without --delete", async () => {
    const target = buildTree({ "extra.txt": "x" });
    await sync(buildTree({ "a.txt": "a" }), target);
    expect(readTree(target)).toEqual({ "a.txt": "a", "extra.txt": "x" });
  });

  it("aborts on both sides when the host declines the plan", async () => {
    const source = buildTree({ "a.txt": "a" });
    const target = buildTree({ "b.txt": "b" });
    const before = snapshotTree(target);
    const { results } = await sync(source, target, { delete: true }, () => false);
    expect(results[0]).toMatchObject({ status: "rejected", reason: expect.any(SyncAbortedError) });
    expect(results[1]).toMatchObject({ status: "rejected", reason: expect.any(SyncAbortedError) });
    expect(snapshotTree(target)).toEqual(before);
  });

  it("fails clearly when the peer lacks folder-sync-v1", async () => {
    const [hostConnection, joinConnection] = createLoopbackConnection({ host: ["chunked-fileinfo-v1"] });
    hostConnection.close();
    await expect(
      runSyncJoin({ connection: joinConnection, directory: new MemoryDirectory() }),
    ).rejects.toBeInstanceOf(SyncPeerUnsupportedError);
  });

  it("reports case-only conflicts instead of silently overwriting", async () => {
    const source = buildTree({ "README.md": "new" });
    const target = buildTree({ "Readme.md": "old" });
    const { host, results } = await sync(source, target, { delete: true });
    expect(results.map((result) => result.status)).toEqual(["fulfilled", "fulfilled"]);
    expect(readTree(target)).toEqual({ "Readme.md": "old" });
    expect(host.summaries[0].errors.map((error) => error.path).sort()).toEqual(["README.md", "Readme.md"]);
  });

  it("keeps other files going when one file fails verification", async () => {
    const source = buildTree({ "a.txt": "aaaa", "b.txt": "bbbb" });
    const target = new MemoryDirectory();
    const [hostConnection, joinConnection] = createLoopbackConnection();
    // Corrupt a.txt in transit: same size, different bytes.
    const corrupting: SyncConnection = {
      ...hostConnection,
      sendFile: (path, data, signal) =>
        hostConnection.sendFile(path, path === "a.txt" ? new Blob(["XXXX"]) : data, signal),
    };
    const [reporter, host] = recorder();
    await Promise.all([
      runSyncHost({ connection: corrupting, directory: source, config: { checksum: true }, reporter }),
      runSyncJoin({ connection: joinConnection, directory: target, config: { checksum: true } }),
    ]);
    expect(readTree(target)).toEqual({ "b.txt": "bbbb" });
    expect(host.summaries[0].errors).toEqual([{ path: "a.txt", error: expect.stringMatching(/hash mismatch/) }]);
    expect(host.summaries[0]).toMatchObject({ added: 1 });
  });

  it("leaves no partial file and no open writable when cancelled mid-transfer", async () => {
    const source = buildTree({ "big.bin": "x".repeat(64) });
    const target = new MemoryDirectory();
    const controller = new AbortController();
    const [hostConnection, joinConnection] = createLoopbackConnection();
    const slow = {
      ...joinConnection,
      async receiveFile(signal?: AbortSignal) {
        const received = await joinConnection.receiveFile(signal);
        controller.abort();
        return received;
      },
    };
    const results = await Promise.allSettled([
      runSyncHost({ connection: hostConnection, directory: source, signal: controller.signal }),
      runSyncJoin({ connection: slow, directory: target, signal: controller.signal }),
    ]);
    expect(results[1].status).toBe("rejected");
    expect((results[1] as PromiseRejectedResult).reason.name).toBe("AbortError");
    expect(readTree(target)).toEqual({});
  });

  it("rejects a malicious host that sends unplanned files or deletes", async () => {
    const target = buildTree({ "victim.txt": "safe", "planned.txt": "old" });
    const [hostConnection, joinConnection] = createLoopbackConnection();
    const join = runSyncJoin({ connection: joinConnection, directory: target });
    const assembler = new ManifestAssembler();
    let listing: SyncManifest | undefined;
    while (!listing) listing = assembler.accept(await hostConnection.receive());
    await hostConnection.send({
      type: "sync-plan",
      dryRun: false,
      watch: false,
      add: [],
      update: [],
      delete: [file("planned.txt", 3, 1)],
      skipped: 0,
      savedBytes: 0,
    });
    await hostConnection.send({ type: "sync-delete", paths: ["planned.txt", "victim.txt"] });
    const ack = await hostConnection.receive();
    await join;
    expect(ack).toMatchObject({ type: "sync-ack", ok: false, errors: [{ path: "victim.txt" }] });
    expect(readTree(target)).toEqual({ "victim.txt": "safe" });

    const [evilHost, evilJoin] = createLoopbackConnection();
    const second = runSyncJoin({ connection: evilJoin, directory: buildTree({}) });
    const drain = new ManifestAssembler();
    while (!drain.accept(await evilHost.receive()));
    await evilHost.send({
      type: "sync-plan",
      dryRun: false,
      watch: false,
      add: [file("a.txt", 1, 1)],
      update: [],
      delete: [],
      skipped: 0,
      savedBytes: 0,
    });
    await evilHost.sendFile("b.txt", new Blob(["b"]));
    await expect(second).rejects.toThrow(/not in the plan/);
  });

  it("--watch syncs a change in under two seconds", async () => {
    const source = buildTree({ "watched.txt": "v1" });
    const target = new MemoryDirectory();
    const controller = new AbortController();
    const [hostConnection, joinConnection] = createLoopbackConnection();
    const [joinReporter, join] = recorder();
    const options = { config: { watch: true }, signal: controller.signal, watchIntervalMs: 50, watchDebounceMs: 50 };
    const sessions = Promise.allSettled([
      runSyncHost({ ...options, connection: hostConnection, directory: source }),
      runSyncJoin({ ...options, connection: joinConnection, directory: target, reporter: joinReporter }),
    ]);
    await expect.poll(() => join.summaries.length).toBe(1);
    const changedAt = Date.now();
    writeFileAt(source, "watched.txt", "v2!");
    writeFileAt(source, "created/new.txt", "new");
    await expect.poll(() => readTree(target), { timeout: 2_000, interval: 10 }).toEqual(readTree(source));
    expect(Date.now() - changedAt).toBeLessThan(2_000);
    expect(join.events.map((event) => `${event.kind}:${event.path}`)).toEqual([
      "create:created",
      "create:created/new.txt",
      "modify:watched.txt",
    ]);
    controller.abort();
    const results = await sessions;
    expect(results.every((result) => result.status === "rejected")).toBe(true);
    expect(fileAt(target, "watched.txt")?.openWritables).toBe(0);
  });
});

describe("manifestChanges", () => {
  it("derives create, modify and delete events", () => {
    const before = manifest(file("a", 1, 1), file("b", 1, 1));
    const after = manifest(file("a", 2, 1), file("c", 1, 1));
    expect(manifestChanges(before, after, 7)).toEqual([
      { kind: "modify", path: "a", time: 7 },
      { kind: "delete", path: "b", time: 7 },
      { kind: "create", path: "c", time: 7 },
    ]);
  });
});

it("exports the feature flag expected by the Go side", () => {
  expect(FEATURE_FOLDER_SYNC).toBe("folder-sync-v1");
});
