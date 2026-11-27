import { useEffect, useRef, useState } from "react";
import {
  CircleStop,
  Equal,
  Eye,
  FileMinus,
  FilePen,
  FilePlus,
  FolderInput,
  FolderOpen,
  FolderOutput,
  FolderSync,
  Info,
  Play,
  Trash2,
  X,
} from "lucide-react";
import { errorMessage, formatBytes } from "./protocol/bytes";
import {
  SYNC_NETWORK_READY,
  SyncAbortedError,
  asSyncDirectory,
  connectSyncPeer,
  createLoopbackConnection,
  isAbortError,
  parseList,
  runSyncHost,
  runSyncJoin,
  supportsSyncDirectories,
  type SyncDirectoryHandle,
  type SyncFileError,
  type SyncPlanItem,
  type SyncPlanView,
  type SyncProgress,
  type SyncReporter,
  type SyncSummary,
  type SyncWatchEvent,
  type SyncWebConfig,
} from "./protocol/sync";
import type { TransferSettings } from "./protocol/types";
import "./SyncPanel.css";

export type SyncMode = "host" | "join" | "local";
type Phase = "idle" | "running" | "done" | "error";
type PlanKind = "add" | "update" | "delete" | "skip";
type ChosenFolder = { handle: SyncDirectoryHandle; name: string };

export type DirectoryPicker = (access: "read" | "readwrite") => Promise<SyncDirectoryHandle>;

type Props = {
  settings: TransferSettings;
  onActiveChange?(active: boolean): void;
  /** Injection points for tests and demos. */
  connect?: typeof connectSyncPeer;
  pickDirectory?: DirectoryPicker;
  supported?: boolean;
};

const MAX_LISTED_ITEMS = 200;
const MAX_WATCH_EVENTS = 50;

const kindMeta: Record<PlanKind, { label: string; icon: typeof FilePlus }> = {
  add: { label: "Add", icon: FilePlus },
  update: { label: "Update", icon: FilePen },
  delete: { label: "Delete", icon: FileMinus },
  skip: { label: "Unchanged", icon: Equal },
};

const modeMeta: Record<SyncMode, { label: string; hint: string }> = {
  host: { label: "Source", hint: "Send this folder's changes to another computer." },
  join: { label: "Target", hint: "Receive changes into this folder." },
  local: { label: "Two local folders", hint: "Mirror one folder into another in this browser." },
};

async function defaultPicker(access: "read" | "readwrite") {
  if (!window.showDirectoryPicker) throw new Error("Folder access is not supported in this browser");
  return asSyncDirectory(await window.showDirectoryPicker({ mode: access }));
}

type ResolvableHandle = SyncDirectoryHandle & {
  isSameEntry?(other: unknown): Promise<boolean>;
  resolve?(other: unknown): Promise<string[] | null>;
};

/** Rejects identical or nested folders, which would sync into themselves. */
async function foldersOverlap(source: SyncDirectoryHandle, target: SyncDirectoryHandle) {
  const a = source as ResolvableHandle;
  const b = target as ResolvableHandle;
  if (a === b) return true;
  if (await a.isSameEntry?.(b)) return true;
  if ((await a.resolve?.(b)) != null) return true;
  return (await b.resolve?.(a)) != null;
}

function formatDuration(ms: number) {
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} s`;
}

function summaryText(summary: SyncSummary) {
  return `${summary.added} added, ${summary.updated} updated, ${summary.deleted} deleted, ${summary.skipped} unchanged · ${formatBytes(summary.bytesSent)} in ${formatDuration(summary.durationMs)}`;
}

function PlanBadge({ kind }: { kind: PlanKind }) {
  const { label, icon: Icon } = kindMeta[kind];
  return (
    <span className={`sync-badge ${kind}`}>
      <Icon aria-hidden="true" /> {label}
    </span>
  );
}

function PlanList({ plan }: { plan: SyncPlanView }) {
  const rows: Array<{ kind: PlanKind; item: SyncPlanItem }> = [
    ...plan.add.map((item) => ({ kind: "add" as const, item })),
    ...plan.update.map((item) => ({ kind: "update" as const, item })),
    ...plan.delete.map((item) => ({ kind: "delete" as const, item })),
  ];
  const shown = rows.slice(0, MAX_LISTED_ITEMS);
  if (!rows.length)
    return <p className="sync-empty">Nothing to change: the target already matches the source.</p>;
  return (
    <>
      <ul className="sync-plan-list" aria-label="Planned changes">
        {shown.map(({ kind, item }) => (
          <li key={`${kind}:${item.path}`} data-kind={kind}>
            <PlanBadge kind={kind} />
            <span className="sync-path" title={item.path}>
              {item.path}
              {item.dir ? "/" : ""}
            </span>
            <span className="sync-size">{item.dir ? "folder" : formatBytes(item.size)}</span>
          </li>
        ))}
      </ul>
      {rows.length > shown.length && (
        <p className="sync-more">…and {rows.length - shown.length} more</p>
      )}
    </>
  );
}

function PlanSummary({ plan }: { plan: SyncPlanView }) {
  const counts: Array<[PlanKind, number]> = [
    ["add", plan.add.length],
    ["update", plan.update.length],
    ["delete", plan.delete.length],
    ["skip", plan.skipCount],
  ];
  return (
    <section className="sync-plan" aria-label="Sync plan">
      <div className="sync-plan-heading">
        <h3>{plan.dryRun ? "Dry run preview" : "Sync plan"}</h3>
        {plan.dryRun && (
          <span className="sync-dry-run">
            <Eye aria-hidden="true" /> Nothing will be changed
          </span>
        )}
      </div>
      <ul className="sync-plan-counts" aria-label="Plan totals">
        {counts.map(([kind, count]) => (
          <li key={kind} data-kind={kind}>
            <PlanBadge kind={kind} />
            <strong>{count}</strong>
          </li>
        ))}
      </ul>
      <p className="sync-bytes">
        {formatBytes(plan.transferBytes)} to send · {formatBytes(plan.savedBytes)} already up to date
      </p>
      <PlanList plan={plan} />
    </section>
  );
}

export default function SyncPanel({
  settings,
  onActiveChange,
  connect = connectSyncPeer,
  pickDirectory = defaultPicker,
  supported = supportsSyncDirectories(),
}: Props) {
  const [mode, setMode] = useState<SyncMode>(SYNC_NETWORK_READY ? "host" : "local");
  const [source, setSource] = useState<ChosenFolder>();
  const [target, setTarget] = useState<ChosenFolder>();
  const [code, setCode] = useState("");
  const [hostCode, setHostCode] = useState("");
  const [deleteExtra, setDeleteExtra] = useState(false);
  const [dryRun, setDryRun] = useState(false);
  const [watch, setWatch] = useState(false);
  const [checksum, setChecksum] = useState(false);
  const [exclude, setExclude] = useState("");
  const [excludeFiles, setExcludeFiles] = useState("");
  const [phase, setPhase] = useState<Phase>("idle");
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  const [plan, setPlan] = useState<SyncPlanView>();
  const [confirmDelete, setConfirmDelete] = useState<number>();
  const [progress, setProgress] = useState<SyncProgress>();
  const [summary, setSummary] = useState<SyncSummary>();
  const [rounds, setRounds] = useState(0);
  const [events, setEvents] = useState<SyncWatchEvent[]>([]);
  const [fileErrors, setFileErrors] = useState<SyncFileError[]>([]);

  const abort = useRef<AbortController>(undefined);
  const confirmResolver = useRef<(proceed: boolean) => void>(undefined);
  const deletesAllowed = useRef(false);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      confirmResolver.current?.(false);
      abort.current?.abort();
    };
  }, []);

  const running = phase === "running";
  const usesSource = mode !== "join";
  const usesTarget = mode !== "host";
  const ready =
    supported &&
    !running &&
    (!usesSource || !!source) &&
    (!usesTarget || !!target) &&
    (mode !== "join" || code.trim().length >= 6);

  async function choose(kind: "source" | "target") {
    setError("");
    try {
      const handle = await pickDirectory(kind === "source" ? "read" : "readwrite");
      const chosen = { handle, name: handle.name || "(selected folder)" };
      if (kind === "source") setSource(chosen);
      else setTarget(chosen);
    } catch (reason) {
      if (isAbortError(reason)) return;
      setError(errorMessage(reason));
    }
  }

  function reset() {
    setStatus("");
    setError("");
    setPlan(undefined);
    setConfirmDelete(undefined);
    setProgress(undefined);
    setSummary(undefined);
    setRounds(0);
    setEvents([]);
    setFileErrors([]);
    setHostCode("");
    deletesAllowed.current = false;
  }

  function config(): SyncWebConfig {
    return {
      delete: deleteExtra,
      dryRun,
      watch: watch && !dryRun,
      checksum,
      exclude: parseList(exclude),
      excludeFiles: parseList(excludeFiles),
    };
  }

  function guard<T extends unknown[]>(callback: (...args: T) => void) {
    return (...args: T) => {
      if (mounted.current) callback(...args);
    };
  }

  // Plan, confirmation, round summaries and watch events come from the
  // source side; progress and per-file errors from the side that writes.
  function hostReporter(): SyncReporter {
    return {
      onStatus: guard(setStatus),
      onPlan: async (next) => {
        if (!mounted.current) return false;
        setPlan(next);
        if (next.dryRun || !next.delete.length || deletesAllowed.current) return true;
        setConfirmDelete(next.delete.length);
        const proceed = await new Promise<boolean>((resolve) => {
          confirmResolver.current = resolve;
        });
        confirmResolver.current = undefined;
        if (mounted.current) setConfirmDelete(undefined);
        deletesAllowed.current = proceed;
        return proceed;
      },
      onWatchEvent: guard((event: SyncWatchEvent) =>
        setEvents((list) => [event, ...list].slice(0, MAX_WATCH_EVENTS)),
      ),
      onRoundDone: guard((next: SyncSummary) => {
        setSummary(next);
        setRounds((count) => count + 1);
        setProgress(undefined);
        setFileErrors(next.errors);
      }),
    };
  }

  function writerReporter(isRemoteSource: boolean): SyncReporter {
    return {
      onStatus: isRemoteSource ? guard(setStatus) : undefined,
      onPlan: isRemoteSource
        ? (next: SyncPlanView) => {
            if (mounted.current) setPlan(next);
            return true;
          }
        : undefined,
      onProgress: guard(setProgress),
      onWatchEvent: isRemoteSource
        ? guard((event: SyncWatchEvent) => setEvents((list) => [event, ...list].slice(0, MAX_WATCH_EVENTS)))
        : undefined,
      onRoundDone: isRemoteSource
        ? guard((next: SyncSummary) => {
            setSummary(next);
            setRounds((count) => count + 1);
            setProgress(undefined);
            setFileErrors(next.errors);
          })
        : undefined,
    };
  }

  async function start() {
    if (!ready) return;
    reset();
    setPhase("running");
    onActiveChange?.(true);
    const controller = new AbortController();
    abort.current = controller;
    const signal = controller.signal;
    try {
      if (mode === "local") {
        if (await foldersOverlap(source!.handle, target!.handle))
          throw new Error("Choose two different folders that do not contain each other.");
        const [hostConnection, joinConnection] = createLoopbackConnection();
        const settled = await Promise.allSettled([
          runSyncHost({ connection: hostConnection, directory: source!.handle, config: config(), reporter: hostReporter(), signal }),
          runSyncJoin({ connection: joinConnection, directory: target!.handle, config: config(), reporter: writerReporter(false), signal }),
        ]);
        const failure = settled.find((result): result is PromiseRejectedResult => result.status === "rejected");
        if (failure) throw failure.reason;
      } else if (mode === "host") {
        setStatus("Connecting…");
        const connection = await connect({ role: "host", code: "", settings, signal, onCode: guard(setHostCode) });
        await runSyncHost({ connection, directory: source!.handle, config: config(), reporter: hostReporter(), signal });
      } else {
        setStatus("Connecting…");
        const connection = await connect({ role: "join", code: code.trim(), settings, signal });
        await runSyncJoin({ connection, directory: target!.handle, reporter: writerReporter(true), signal });
      }
      if (mounted.current) setPhase("done");
    } catch (reason) {
      if (!mounted.current) return;
      if (isAbortError(reason)) {
        setStatus("Stopped.");
        setPhase("done");
      } else if (reason instanceof SyncAbortedError) {
        setStatus("Cancelled. No file was changed.");
        setPhase("done");
      } else {
        setError(errorMessage(reason));
        setStatus("");
        setPhase("error");
      }
    } finally {
      if (abort.current === controller) abort.current = undefined;
      if (mounted.current) {
        setProgress(undefined);
        setConfirmDelete(undefined);
        onActiveChange?.(false);
      }
    }
  }

  function stop() {
    confirmResolver.current?.(false);
    abort.current?.abort();
  }

  const startLabel = dryRun ? "Preview changes" : watch ? "Sync and watch" : "Sync now";
  const percent = progress && progress.overallTotal > 0
    ? Math.min(100, Math.round((progress.overallDone / progress.overallTotal) * 100))
    : 0;

  return (
    <section className="sync-workspace" aria-label="Folder sync">
      <article className="panel sync-panel">
        <div className="panel-heading">
          <span className="step">
            <FolderSync aria-hidden="true" />
          </span>
          <div>
            <h2>Sync folders</h2>
            <p>
              One-way sync, end-to-end encrypted. Only new or changed files are sent from the
              source to the target. Same as <code>croc sync</code> on the command line.
            </p>
          </div>
        </div>

        {!supported && (
          <p className="status-message error sync-unsupported" role="alert">
            <Info aria-hidden="true" />
            <span>
              Folder sync needs access to folders on your disk, which only Chrome and Edge on
              desktop provide. Firefox and Safari can still send and receive files, or you can run{" "}
              <code>croc sync &lt;folder&gt;</code> in a terminal.
            </span>
          </p>
        )}

        <fieldset className="sync-mode" disabled={running}>
          <legend>This browser is the</legend>
          {(Object.keys(modeMeta) as SyncMode[]).map((value) => (
            <label key={value} className={mode === value ? "selected" : ""}>
              <input
                type="radio"
                name="sync-mode"
                value={value}
                checked={mode === value}
                onChange={() => {
                  setMode(value);
                  reset();
                  setPhase("idle");
                }}
              />
              <span>
                <strong>{modeMeta[value].label}</strong>
                <small>{modeMeta[value].hint}</small>
              </span>
            </label>
          ))}
        </fieldset>

        {mode !== "local" && !SYNC_NETWORK_READY && (
          <p className="status-message sync-notice">
            <Info aria-hidden="true" />
            <span>
              Syncing with another computer is still being built. Until then you can try it with
              two local folders.
            </span>
          </p>
        )}

        <div className="sync-folders">
          {usesSource && (
            <div className="sync-folder">
              <span className="sync-folder-label">Source folder</span>
              <button
                type="button"
                className="secondary-button"
                disabled={!supported || running}
                onClick={() => void choose("source")}
              >
                <FolderOutput aria-hidden="true" /> Choose source folder
              </button>
              <span className="sync-folder-name" aria-label="Selected source folder">
                {source ? (
                  <>
                    <FolderOpen aria-hidden="true" /> {source.name}
                  </>
                ) : (
                  "No folder selected"
                )}
              </span>
            </div>
          )}
          {usesTarget && (
            <div className="sync-folder">
              <span className="sync-folder-label">Target folder</span>
              <button
                type="button"
                className="secondary-button"
                disabled={!supported || running}
                onClick={() => void choose("target")}
              >
                <FolderInput aria-hidden="true" /> Choose target folder
              </button>
              <span className="sync-folder-name" aria-label="Selected target folder">
                {target ? (
                  <>
                    <FolderOpen aria-hidden="true" /> {target.name}
                  </>
                ) : (
                  "No folder selected"
                )}
              </span>
            </div>
          )}
        </div>

        {mode === "join" && (
          <div className="sync-field">
            <label htmlFor="croc-sync-code">Croc code from the source</label>
            <input
              id="croc-sync-code"
              type="text"
              autoComplete="off"
              autoCapitalize="none"
              autoCorrect="off"
              spellCheck={false}
              value={code}
              disabled={running}
              placeholder="code-phrase-words"
              onChange={(event) => setCode(event.target.value)}
            />
          </div>
        )}

        {mode !== "join" && (
          <fieldset className="sync-options" disabled={running}>
            <legend>Options</legend>
            <label>
              <input type="checkbox" checked={deleteExtra} onChange={(event) => setDeleteExtra(event.target.checked)} />
              Delete files on the target that are not in the source <code>--delete</code>
            </label>
            <label>
              <input type="checkbox" checked={dryRun} onChange={(event) => setDryRun(event.target.checked)} />
              Dry run: only show what would change <code>--dry-run</code>
            </label>
            <label>
              <input
                type="checkbox"
                checked={watch && !dryRun}
                disabled={dryRun}
                onChange={(event) => setWatch(event.target.checked)}
              />
              Keep watching and sync changes as they happen <code>--watch</code>
            </label>
            <label>
              <input type="checkbox" checked={checksum} onChange={(event) => setChecksum(event.target.checked)} />
              Compare file contents by hash <code>--checksum</code>
            </label>
            <div className="sync-field">
              <label htmlFor="croc-sync-exclude">Exclude paths containing (comma separated)</label>
              <input
                id="croc-sync-exclude"
                type="text"
                value={exclude}
                placeholder="node_modules, .cache"
                onChange={(event) => setExclude(event.target.value)}
              />
            </div>
            <div className="sync-field">
              <label htmlFor="croc-sync-exclude-files">Exclude exact files (comma separated)</label>
              <input
                id="croc-sync-exclude-files"
                type="text"
                value={excludeFiles}
                placeholder="secrets.env, build/output.log"
                onChange={(event) => setExcludeFiles(event.target.value)}
              />
            </div>
          </fieldset>
        )}

        {hostCode && (
          <div className="sync-code" aria-live="polite">
            <span>Code for the target</span>
            <code>{hostCode}</code>
          </div>
        )}

        {status && (
          <div className="ssh-status-row sync-status" aria-live="polite">
            <span className={`status-dot${running ? " connected" : ""}`} />
            <span>{status}</span>
          </div>
        )}
        {error && (
          <p className="status-message error" role="alert">
            {error}
          </p>
        )}

        {confirmDelete !== undefined && (
          <div className="sync-confirm" role="alertdialog" aria-labelledby="sync-confirm-title">
            <p id="sync-confirm-title">
              <Trash2 aria-hidden="true" /> Delete {confirmDelete} item{confirmDelete === 1 ? "" : "s"} from the
              target folder?
              {watch && " Later deletes in this watch session are applied automatically."}
            </p>
            <div className="sync-confirm-actions">
              <button type="button" className="primary-button" onClick={() => confirmResolver.current?.(true)}>
                <Trash2 aria-hidden="true" /> Delete and sync
              </button>
              <button type="button" className="secondary-button" onClick={() => confirmResolver.current?.(false)}>
                <X aria-hidden="true" /> Cancel
              </button>
            </div>
          </div>
        )}

        {plan && <PlanSummary plan={plan} />}

        {progress && (
          <div className="progress-block" aria-label="Sync progress">
            <div className="progress-copy">
              <span>{progress.path}</span>
              <span>{percent}%</span>
            </div>
            <div
              className="progress-track"
              role="progressbar"
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={percent}
            >
              <span style={{ width: `${percent}%` }} />
            </div>
            <div className="progress-file">
              <span>
                {formatBytes(progress.overallDone)} of {formatBytes(progress.overallTotal)}
              </span>
            </div>
          </div>
        )}

        {summary && (
          <p className="status-message done sync-summary" aria-live="polite">
            {rounds > 1 ? `Round ${rounds}: ` : ""}
            {plan?.dryRun ? "Dry run: no file was changed." : summaryText(summary)}
          </p>
        )}

        {fileErrors.length > 0 && (
          <section className="sync-errors" aria-label="Files with errors">
            <h3>
              {fileErrors.length} item{fileErrors.length === 1 ? "" : "s"} could not be synced
            </h3>
            <ul>
              {fileErrors.map((item) => (
                <li key={item.path}>
                  <span className="sync-path">{item.path}</span>
                  <span>{item.error}</span>
                </li>
              ))}
            </ul>
          </section>
        )}

        {events.length > 0 && (
          <section className="sync-watch-log" aria-label="Watch log">
            <h3>Recent changes</h3>
            <ol>
              {events.map((event, index) => (
                <li key={`${event.time}:${event.kind}:${event.path}:${index}`}>
                  <time dateTime={new Date(event.time).toISOString()}>
                    {new Date(event.time).toLocaleTimeString()}
                  </time>
                  <span className="sync-event-kind">{event.kind}</span>
                  <span className="sync-path">{event.path}</span>
                </li>
              ))}
            </ol>
          </section>
        )}

        <div className="sync-actions">
          {running ? (
            <button type="button" className="primary-button inverted" onClick={stop}>
              <CircleStop aria-hidden="true" /> Stop
            </button>
          ) : (
            <button type="button" className="primary-button" disabled={!ready} onClick={() => void start()}>
              <Play aria-hidden="true" /> {startLabel}
            </button>
          )}
        </div>
      </article>
    </section>
  );
}
