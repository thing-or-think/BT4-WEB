import { BUNDLE_FEATURE } from "./bundle";
import { errorMessage, textDecoder } from "./bytes";
import {
  establishReceiveSession,
  receiveOfferedFiles,
  reportPeerError,
} from "./client";
import { validateSenderInfo } from "./metadata";
import { receiveControl, sendControl } from "./relay";
import {
  HASH_SIZE,
  SyncKind,
  compare,
  dstHashTargets,
  type SyncChange,
  type SyncDiffOptions,
  type SyncEntry,
  type SyncFilterSpec,
  type SyncManifest,
  type SyncPlan,
} from "./sync-diff";
import {
  SyncFsCommitter,
  applyWebDeletePolicy,
  ensureReadWritePermission,
  hashDestinationEntries,
  scanDestination,
  sortManifest,
  type DestinationScan,
} from "./sync-fs";
import type { SyncHashCache } from "./sync-hash-cache";
import {
  HashListAssembler,
  MAX_SYNC_BATCH_BYTES,
  ManifestAssembler,
  SYNC_FEATURE,
  SYNC_PROTOCOL_VERSION,
  SyncProtocolError,
  decodeHello,
  encodeHello,
  encodePathLists,
  encodeResult,
  protoErr,
  type SyncHello,
  type SyncPathHash,
  type SyncResult,
  type SyncWireMessage,
} from "./sync-wire";
import type { CrocSocket } from "./transport";
import type {
  CrocMessage,
  FileProgress,
  OfferedFile,
  ReceiveDestination,
  ReceiveSink,
  SenderInfoWire,
  TransferOffer,
  TransferSettings,
} from "./types";

export const WATCH_UNSUPPORTED_ERROR =
  "Watching (--watch) is not supported by web sync yet; run croc sync without --watch";

export const SENDER_NOT_SYNC_ERROR = "The sender is not running croc sync";

export class SyncSourceChangedError extends Error {
  constructor(paths: string[]) {
    super(`foldersync: source files changed during sync: ${paths.join(", ")}`);
    this.name = "SyncSourceChangedError";
  }
}

export class SyncWatchUnsupportedError extends Error {
  constructor() {
    super(WATCH_UNSUPPORTED_ERROR);
    this.name = "SyncWatchUnsupportedError";
  }
}

export interface SyncReceiveDiffOptions {
  delete?: boolean;
  checksum?: boolean;
  allowEmptySource?: boolean;
}

export interface SyncDecision {
  proceed: boolean;
  confirmDeletes: boolean;
}

export interface SyncReceiveCallbacks {
  onStatus?(status: string): void;
  onPlan?(plan: SyncPlan): void;
  onProgress?(progress: FileProgress): void;
  onResult?(result: SyncResult): void;
}

export interface SyncReceiveOptions {
  secret: string;
  settings: TransferSettings;
  root: FileSystemDirectoryHandle;
  diff?: SyncReceiveDiffOptions;
  decide(plan: SyncPlan): Promise<SyncDecision>;
  callbacks?: SyncReceiveCallbacks;
  signal?: AbortSignal;
  hashCache?: SyncHashCache;
}

export interface SyncDestEnv {
  scanDestination(filter?: SyncFilterSpec): Promise<SyncManifest>;
  hashDestination(dst: SyncManifest, paths: string[]): Promise<void>;
  adjustPlan?(plan: SyncPlan): void;
  decide(plan: SyncPlan): Promise<SyncDecision>;
  commit(
    plan: SyncPlan,
    src: SyncManifest,
    decision: SyncDecision,
  ): Promise<SyncResult>;
}

type DestState =
  | "waitHello"
  | "waitManifestStart"
  | "manifest"
  | "waitHashes"
  | "waitFiles"
  | "done"
  | "failed";

function transferChanges(plan: SyncPlan): SyncChange[] {
  const out: SyncChange[] = [];
  for (const group of [plan.add, plan.update]) {
    for (const change of group) {
      if (change.src && change.src.kind === SyncKind.File) {
        out.push(change);
      }
    }
  }
  return out;
}

function findEntry(manifest: SyncManifest, path: string): SyncEntry | undefined {
  let lo = 0;
  let hi = manifest.entries.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const cur = manifest.entries[mid].path;
    if (cur === path) return manifest.entries[mid];
    if (cur < path) lo = mid + 1;
    else hi = mid - 1;
  }
  return undefined;
}

export class SyncDestSession {
  private state: DestState = "waitHello";
  private started = false;
  private peerHelloValue?: SyncHello;
  private readonly asm = new ManifestAssembler();
  private src?: SyncManifest;
  private dst?: SyncManifest;
  private requested = new Set<string>();
  private readonly hashResp = new HashListAssembler();
  private afterHash?: () => Promise<SyncWireMessage[]>;
  private planValue?: SyncPlan;
  private decisionValue: SyncDecision = { proceed: false, confirmDeletes: false };
  private needSizes = new Map<string, number>();
  private needOrder: string[] = [];
  private accepted = new Set<string>();
  private resultValue?: SyncResult;
  private compares = 0;
  private readonly diffOpts: SyncDiffOptions;

  constructor(
    private readonly env: SyncDestEnv,
    opts: SyncDiffOptions = {},
  ) {
    // Web cannot set mtime or permissions: always compare with ignoreMetadata=true.
    this.diffOpts = { ...opts, ignoreMetadata: true };
  }

  peerHello(): SyncHello | undefined {
    return this.peerHelloValue;
  }

  plan(): SyncPlan | undefined {
    return this.planValue;
  }

  decision(): SyncDecision {
    return this.decisionValue;
  }

  result(): SyncResult | undefined {
    return this.resultValue;
  }

  need(): string[] {
    return [...this.needOrder];
  }

  done(): boolean {
    return this.state === "done";
  }

  waitingForFiles(): boolean {
    return this.state === "waitFiles";
  }

  private fail<T extends Error>(err: T): T {
    this.state = "failed";
    return err;
  }

  start(): SyncWireMessage[] {
    if (this.started) {
      throw this.fail(protoErr("session already started"));
    }
    this.started = true;
    try {
      return [
        encodeHello({
          version: SYNC_PROTOCOL_VERSION,
          role: "destination",
        }),
      ];
    } catch (error) {
      throw this.fail(
        error instanceof Error ? error : new Error(String(error)),
      );
    }
  }

  async handle(msg: SyncWireMessage): Promise<SyncWireMessage[]> {
    if (this.state === "failed") {
      throw protoErr(`${msg.t} after the session failed`);
    }
    if (msg.t === "sync-ping") {
      if ((msg.b?.byteLength ?? 0) > MAX_SYNC_BATCH_BYTES) {
        throw this.fail(
          protoErr(
            `${msg.t} payload ${msg.b?.byteLength ?? 0} bytes > ${MAX_SYNC_BATCH_BYTES}`,
          ),
        );
      }
      return [{ t: "sync-pong" }];
    }
    if (msg.t === "sync-pong") {
      if ((msg.b?.byteLength ?? 0) > MAX_SYNC_BATCH_BYTES) {
        throw this.fail(
          protoErr(
            `${msg.t} payload ${msg.b?.byteLength ?? 0} bytes > ${MAX_SYNC_BATCH_BYTES}`,
          ),
        );
      }
      return [];
    }
    if (this.state === "done") {
      throw this.fail(protoErr(`${msg.t} after the session finished`));
    }

    try {
      if (this.state === "waitHello") {
        if (msg.t !== "sync-hello") {
          throw protoErr(`${msg.t} before sync-hello`);
        }
        const hello = decodeHello(msg);
        if (hello.version !== SYNC_PROTOCOL_VERSION) {
          throw protoErr(
            `peer speaks sync protocol ${hello.version}, this croc speaks ${SYNC_PROTOCOL_VERSION}; update croc on both computers`,
          );
        }
        if (hello.role !== "source") {
          throw protoErr(
            `peer role "${hello.role}", want "source" (run croc sync <dir> on one computer and join from the other)`,
          );
        }
        if (hello.watch) {
          throw new SyncWatchUnsupportedError();
        }
        this.peerHelloValue = hello;
        this.state = "waitManifestStart";
        return [];
      }

      if (msg.t === "sync-manifest-start" && this.state === "waitManifestStart") {
        this.asm.start(msg);
        this.state = "manifest";
        return [];
      }

      if (msg.t === "sync-manifest-batch" && this.state === "manifest") {
        this.asm.batch(msg);
        return [];
      }

      if (msg.t === "sync-manifest-end" && this.state === "manifest") {
        const src = this.asm.end(msg);
        sortManifest(src);
        this.src = src;
        this.dst = await this.env.scanDestination(src.filter);
        return await this.runCompare();
      }

      if (msg.t === "sync-hash-result" && this.state === "waitHashes") {
        const done = this.hashResp.push(msg);
        if (!done) return [];
        this.applyHashes(this.hashResp.items);
        const next = this.afterHash;
        this.afterHash = undefined;
        if (!next) {
          throw protoErr("unexpected sync-hash-result with no pending step");
        }
        return await next();
      }

      throw protoErr(
        `unexpected ${msg.t} for the destination in state ${this.state}`,
      );
    } catch (error) {
      throw this.fail(
        error instanceof Error ? error : new Error(String(error)),
      );
    }
  }

  private applyHashes(items: SyncPathHash[]): void {
    const src = this.src!;
    const byPath = new Map<string, SyncEntry>();
    for (const entry of src.entries) {
      byPath.set(entry.path, entry);
    }
    const seen = new Set<string>();
    const changed: string[] = [];

    for (const item of items) {
      if (!this.requested.has(item.path)) {
        throw protoErr(`unrequested hash for "${item.path}"`);
      }
      if (seen.has(item.path)) {
        throw protoErr(`duplicate hash for "${item.path}"`);
      }
      seen.add(item.path);
      if (item.error) {
        changed.push(item.path);
        continue;
      }
      const len = item.hash?.byteLength ?? 0;
      if (len !== HASH_SIZE) {
        throw protoErr(`hash for "${item.path}" has ${len} bytes`);
      }
      const entry = byPath.get(item.path);
      if (!entry) {
        throw protoErr(`unrequested hash for "${item.path}"`);
      }
      entry.hash = item.hash;
    }

    if (seen.size !== this.requested.size) {
      throw protoErr(
        `got ${seen.size} hashes, requested ${this.requested.size}`,
      );
    }
    if (changed.length > 0) {
      throw new SyncSourceChangedError(changed);
    }
  }

  private requestHashes(
    paths: string[],
    next: () => Promise<SyncWireMessage[]>,
  ): SyncWireMessage[] {
    this.requested = new Set(paths);
    this.hashResp.reset(paths.length);
    this.afterHash = next;
    this.state = "waitHashes";
    return encodePathLists("sync-hash-request", paths);
  }

  private async runCompare(): Promise<SyncWireMessage[]> {
    this.compares += 1;
    if (this.compares > 3) {
      throw new Error(
        "foldersync: hashes still missing after hashing; destination changed during sync",
      );
    }
    const src = this.src!;
    const dst = this.dst!;
    const res = compare(src, dst, this.diffOpts);
    if (res.needHash && res.needHash.length > 0) {
      await this.env.hashDestination(dst, dstHashTargets(src, dst, res.needHash));
      const missing: string[] = [];
      for (const path of res.needHash) {
        const entry = findEntry(src, path);
        if (entry && (!entry.hash || entry.hash.byteLength === 0)) {
          missing.push(path);
        }
      }
      if (missing.length === 0) {
        return this.runCompare();
      }
      return this.requestHashes(missing, () => this.runCompare());
    }
    const plan = res.plan!;
    this.env.adjustPlan?.(plan);
    this.planValue = plan;
    return this.verifyTransfers();
  }

  private async verifyTransfers(): Promise<SyncWireMessage[]> {
    const missing: string[] = [];
    for (const change of transferChanges(this.planValue!)) {
      if (!change.src?.hash || change.src.hash.byteLength === 0) {
        missing.push(change.path);
      }
    }
    if (missing.length > 0) {
      return this.requestHashes(missing, () => this.verifyTransfers());
    }
    return this.runDecide();
  }

  private async runDecide(): Promise<SyncWireMessage[]> {
    const decision = await this.env.decide(this.planValue!);
    this.decisionValue = decision;
    this.needSizes = new Map();
    this.needOrder = [];
    this.accepted = new Set();
    if (decision.proceed) {
      for (const change of transferChanges(this.planValue!)) {
        this.needSizes.set(change.path, change.src!.size);
        this.needOrder.push(change.path);
      }
    }
    this.state = "waitFiles";
    return encodePathLists("sync-need", this.needOrder);
  }

  allowReceive(path: string, size: number, isDir: boolean): void {
    if (this.state !== "waitFiles") {
      throw protoErr(`file "${path}" offered outside the transfer phase`);
    }
    if (isDir) {
      throw protoErr(
        `source offered directory "${path}"; directories are created by the destination`,
      );
    }
    const want = this.needSizes.get(path);
    if (want === undefined) {
      throw protoErr(
        `source offered "${path}", which the destination did not request`,
      );
    }
    if (want !== size) {
      throw protoErr(
        `source offered "${path}" with ${size} bytes, manifest says ${want}`,
      );
    }
    if (this.accepted.has(path)) {
      throw protoErr(`source offered "${path}" more than once`);
    }
    this.accepted.add(path);
  }

  validateOffer(sender: SenderInfoWire): TransferOffer {
    if (this.state !== "waitFiles") {
      throw this.fail(
        protoErr(`fileinfo offered outside the transfer phase (${this.state})`),
      );
    }
    try {
      if (sender.SendingText) {
        throw protoErr("folder sync peer offered text instead of files");
      }
      for (const wire of sender.FilesToTransfer ?? []) {
        if (wire.sy) {
          throw protoErr(
            `folder sync peer offered symlink "${wire.n ?? ""}"`,
          );
        }
        if (wire.tf) {
          throw protoErr(
            `folder sync peer offered archive "${wire.n ?? ""}"`,
          );
        }
        if (((wire.md ?? 0) & 0x80000000) !== 0) {
          throw protoErr(
            `source offered directory "${wire.n ?? ""}"; directories are created by the destination`,
          );
        }
      }
      const offer = validateSenderInfo(sender);
      for (const folder of offer.emptyFolders) {
        this.allowReceive(folder, 0, true);
      }
      for (const file of offer.files) {
        this.allowReceive(file.path, file.size, false);
      }
      for (const path of this.needOrder) {
        if (!this.accepted.has(path)) {
          throw protoErr(`source did not offer requested file "${path}"`);
        }
      }
      return offer;
    } catch (error) {
      throw this.fail(
        error instanceof Error ? error : new Error(String(error)),
      );
    }
  }

  async finish(): Promise<SyncWireMessage[]> {
    if (this.state !== "waitFiles") {
      throw this.fail(protoErr(`finish in state ${this.state}`));
    }
    for (const path of this.needOrder) {
      if (!this.accepted.has(path)) {
        throw this.fail(
          protoErr(`source did not offer requested file "${path}"`),
        );
      }
    }
    if (this.accepted.size !== this.needSizes.size) {
      throw this.fail(
        protoErr(
          `source offered ${this.accepted.size} files, requested ${this.needSizes.size}`,
        ),
      );
    }
    try {
      const result = await this.env.commit(
        this.planValue!,
        this.src!,
        this.decisionValue,
      );
      this.resultValue = result;
      const msg = encodeResult(result);
      this.state = "done";
      return [msg];
    } catch (error) {
      throw this.fail(
        error instanceof Error ? error : new Error(String(error)),
      );
    }
  }
}

export interface SyncChannel {
  sendControl(message: SyncWireMessage | CrocMessage): Promise<void>;
  receiveControl(): Promise<CrocMessage>;
  controlSocket?: CrocSocket;
  dataSockets: CrocSocket[];
  key: Uint8Array;
  cipherHandle?: number;
  peerFeatures: string[];
}

export async function runSyncOverChannel(options: {
  channel: SyncChannel;
  root: FileSystemDirectoryHandle;
  diff?: SyncReceiveDiffOptions;
  decide(plan: SyncPlan): Promise<SyncDecision>;
  callbacks?: SyncReceiveCallbacks;
  signal?: AbortSignal;
  hashCache?: SyncHashCache;
  computeHash?: (file: File, signal?: AbortSignal) => Promise<Uint8Array>;
}): Promise<SyncResult> {
  const {
    channel,
    root,
    diff = {},
    decide,
    callbacks = {},
    signal,
    hashCache,
    computeHash,
  } = options;

  if (!channel.peerFeatures.includes(SYNC_FEATURE)) {
    throw new Error(SENDER_NOT_SYNC_ERROR);
  }

  let scan: DestinationScan | undefined;
  let committer: SyncFsCommitter | undefined;
  const changesByPath = new Map<string, SyncChange>();

  const env: SyncDestEnv = {
    async scanDestination(filter) {
      callbacks.onStatus?.("Scanning destination folder…");
      scan = await scanDestination(root, filter, signal);
      if (hashCache) {
        try {
          const seenPaths = new Set<string>();
          for (const entry of scan.manifest.entries) {
            if (entry.kind === SyncKind.File) seenPaths.add(entry.path);
          }
          await hashCache.pruneForRoot(root, seenPaths);
        } catch {
          // Best-effort cache prune.
        }
      }
      return scan.manifest;
    },

    async hashDestination(_dst, paths) {
      callbacks.onStatus?.(`Hashing ${paths.length} destination file(s)…`);
      await hashDestinationEntries(root, scan!, paths, {
        hashCache,
        computeHash,
        signal,
      });
    },

    adjustPlan(plan) {
      if (scan) {
        applyWebDeletePolicy(plan, scan, {
          delete: Boolean(diff.delete),
          checksum: Boolean(diff.checksum),
          allowEmptySource: Boolean(diff.allowEmptySource),
          ignoreMetadata: true,
        });
      }
    },

    async decide(plan) {
      callbacks.onPlan?.(plan);
      const decision = await decide(plan);
      if (decision.proceed) {
        // Check write permission before requesting files or writing anything.
        await ensureReadWritePermission(root);
        committer = new SyncFsCommitter(root, scan!, plan, {
          hashCache,
          signal,
        });
        changesByPath.clear();
        for (const ch of transferChanges(plan)) {
          changesByPath.set(ch.path, ch);
        }
      }
      return decision;
    },

    async commit(plan, _src, decision) {
      if (!decision.proceed) {
        const dryResult: SyncResult = {
          dryRun: true,
          added: 0,
          updated: 0,
          touched: 0,
          deleted: 0,
          unchanged: plan.skip.length,
          conflicts: 0,
          bytes: 0,
        };
        if (plan.deleteBlockedReason) {
          dryResult.deleteBlocked = plan.deleteBlockedReason;
        }
        return dryResult;
      }
      if (!committer) {
        committer = new SyncFsCommitter(root, scan!, plan, {
          hashCache,
          signal,
        });
      }
      return committer.finish(decision);
    },
  };

  const session = new SyncDestSession(env, {
    delete: Boolean(diff.delete),
    checksum: Boolean(diff.checksum),
    allowEmptySource: Boolean(diff.allowEmptySource),
    ignoreMetadata: true,
  });

  const sendAll = async (messages: SyncWireMessage[]) => {
    for (const msg of messages) {
      await channel.sendControl(msg);
    }
  };

  const reportErrorToPeer = async (error: unknown) => {
    try {
      await channel.sendControl({
        t: "error",
        m: errorMessage(error).slice(0, 500),
      });
    } catch {
      // The connection may already be gone.
    }
  };

  callbacks.onStatus?.("Negotiating sync…");
  await sendAll(session.start());

  try {
    while (!session.waitingForFiles()) {
      if (signal?.aborted) {
        throw new DOMException("Transfer cancelled", "AbortError");
      }
      const incoming = await channel.receiveControl();
      if (incoming.t === "error") {
        throw new Error(incoming.m || "Sender cancelled");
      }
      const outgoing = await session.handle({
        t: incoming.t as string,
        b: incoming.b,
      });
      await sendAll(outgoing);
    }

    callbacks.onStatus?.("Waiting for file offer…");
    let fileInfoMsg: CrocMessage;
    for (;;) {
      if (signal?.aborted) {
        throw new DOMException("Transfer cancelled", "AbortError");
      }
      const incoming = await channel.receiveControl();
      if ((incoming.t as string) === "sync-ping") {
        const pong = await session.handle({ t: "sync-ping", b: incoming.b });
        await sendAll(pong);
        continue;
      }
      if ((incoming.t as string) === "sync-pong") {
        await session.handle({ t: "sync-pong", b: incoming.b });
        continue;
      }
      if (incoming.t === "error") {
        throw new Error(incoming.m || "Sender cancelled");
      }
      if (incoming.t !== "fileinfo" || !incoming.b) {
        throw new SyncProtocolError(
          `expected fileinfo after sync-need, got ${incoming.t}`,
        );
      }
      fileInfoMsg = incoming;
      break;
    }

    let senderInfo: SenderInfoWire;
    try {
      senderInfo = JSON.parse(
        textDecoder.decode(fileInfoMsg.b),
      ) as SenderInfoWire;
    } catch (error) {
      throw protoErr(
        `invalid fileinfo JSON: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    const offer = session.validateOffer(senderInfo);

    if (session.decision().proceed) {
      await committer!.prepareDirectories();
      if (offer.files.length > 0) {
        const destination: ReceiveDestination = {
          async createEmptyFolder() {
            throw protoErr(
              "directories are created by the destination during sync",
            );
          },
          async openFile(file: OfferedFile): Promise<ReceiveSink> {
            const change = changesByPath.get(file.path);
            if (!change) {
              throw protoErr(
                `source offered "${file.path}", which the destination did not request`,
              );
            }
            return committer!.openSinkForChange(change, {
              swallowCommitError: true,
            });
          },
        };

        await receiveOfferedFiles({
          control: channel.controlSocket as CrocSocket,
          data: channel.dataSockets,
          key: channel.key,
          cipherHandle: channel.cipherHandle,
          peerFeatures: channel.peerFeatures,
          offer,
          destination,
          callbacks: {
            onStatus: (status) => callbacks.onStatus?.(status),
            onProgress: (progress) => callbacks.onProgress?.(progress),
          },
          signal,
          sendMessage: (msg) => channel.sendControl(msg),
          receiveMessage: () => channel.receiveControl(),
        });
      }
    }

    callbacks.onStatus?.("Finalizing sync…");
    const finishMsgs = await session.finish();
    await sendAll(finishMsgs);
    const result = session.result()!;
    callbacks.onResult?.(result);

    await channel.sendControl({ t: "finished" });
    const finishedMsg = await channel.receiveControl();
    if (finishedMsg.t !== "finished") {
      throw new Error(`Expected transfer completion, got ${finishedMsg.t}`);
    }
    callbacks.onStatus?.(
      result.dryRun ? "Dry run complete" : "Sync complete",
    );
    return result;
  } catch (error) {
    // Notify the peer with an explicit croc error before closing on deliberate
    // refusals (watch mode, unsupported protocol version, wrong role, etc.)
    // so a watching CLI source exits instead of reconnecting forever.
    if (
      error instanceof SyncWatchUnsupportedError ||
      error instanceof SyncProtocolError
    ) {
      await reportErrorToPeer(error);
    }
    throw error;
  }
}

export async function syncReceive(
  options: SyncReceiveOptions,
): Promise<SyncResult> {
  const {
    secret,
    settings,
    root,
    diff,
    decide,
    callbacks = {},
    signal,
    hashCache,
  } = options;

  const session = await establishReceiveSession({
    secret,
    settings,
    features: [BUNDLE_FEATURE, SYNC_FEATURE],
    onStatus: (status) => callbacks.onStatus?.(status),
    signal,
  });

  let peerErrorReported = false;
  try {
    if (!session.peerFeatures.includes(SYNC_FEATURE)) {
      throw new Error(SENDER_NOT_SYNC_ERROR);
    }

    const channel: SyncChannel = {
      sendControl(msg) {
        if (msg.t === "error") {
          peerErrorReported = true;
        }
        return sendControl(
          session.control,
          msg as unknown as CrocMessage,
          session.key,
        );
      },
      receiveControl() {
        return receiveControl(session.control, session.key);
      },
      controlSocket: session.control,
      dataSockets: session.data,
      key: session.key,
      cipherHandle: session.cipherHandle,
      peerFeatures: session.peerFeatures,
    };

    return await runSyncOverChannel({
      channel,
      root,
      diff,
      decide,
      callbacks,
      signal,
      hashCache,
    });
  } catch (error) {
    if (!peerErrorReported) {
      await reportPeerError(session.control, session.key, error);
    }
    throw error;
  } finally {
    await session.close();
  }
}
