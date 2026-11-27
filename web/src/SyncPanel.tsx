import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import {
  AlertTriangle,
  Check,
  FolderOpen,
  RefreshCw,
  ShieldAlert,
  X,
} from "lucide-react";
import { errorMessage, formatBytes } from "./protocol/bytes";
import { supportsDirectoryDestination } from "./protocol/storage";
import {
  SyncWatchUnsupportedError,
  WATCH_UNSUPPORTED_ERROR,
  syncReceive,
  type SyncDecision,
} from "./protocol/sync";
import { SyncKind, type SyncChange, type SyncPlan } from "./protocol/sync-diff";
import { SyncHashCache } from "./protocol/sync-hash-cache";
import type { SyncResult } from "./protocol/sync-wire";
import type { FileProgress, TransferSettings } from "./protocol/types";

export const MAX_PLAN_ROWS_PER_GROUP = 50;

export const MTIME_CACHE_NOTE =
  "Browsers cannot keep file modification times; the next sync compares contents and is fast thanks to a local hash cache.";

type PermissionStateLike = "granted" | "denied" | "prompt";

type DirectoryHandleWithPermission = FileSystemDirectoryHandle & {
  queryPermission?(descriptor?: {
    mode?: "read" | "readwrite";
  }): Promise<PermissionStateLike>;
  requestPermission?(descriptor?: {
    mode?: "read" | "readwrite";
  }): Promise<PermissionStateLike>;
};

export type SyncPanelProps = {
  initialCode?: string;
  settings: TransferSettings;
  onActiveChange?(active: boolean): void;
};

type PendingDeleteConfirm = {
  plan: SyncPlan;
  deleteCount: number;
  deleteFileCount: number;
  requireTypedYes: boolean;
  resolve: (decision: SyncDecision) => void;
};

function syncCodeFromURL(search = window.location.search): string {
  return new URLSearchParams(search).get("sync")?.trim() ?? "";
}

function isOpfsTestEnabled(search = window.location.search): boolean {
  return new URLSearchParams(search).get("opfs") === "1";
}

function supportsOpfs(): boolean {
  return (
    typeof navigator !== "undefined" &&
    typeof navigator.storage?.getDirectory === "function"
  );
}

export function supportsWebSync(): boolean {
  return supportsDirectoryDestination() && Boolean(window.isSecureContext);
}

export function validateSyncCode(raw: string): string | null {
  const trimmed = raw.trim();
  if (trimmed.length === 0) return null;
  if (trimmed.length < 6) {
    return "Code must be at least 6 characters";
  }
  if (!/^[\x20-\x7e]+$/.test(trimmed)) {
    return "Code must use printable ASCII characters";
  }
  return null;
}

function formatCliSyncCommand(code: string): string {
  const value = code.trim() || "<code>";
  const escaped = value.replaceAll("'", "'\\''");
  return `CROC_SECRET='${escaped}' croc sync <folder>`;
}

function isPermissionError(error: unknown): boolean {
  if (error instanceof DOMException) {
    return error.name === "NotAllowedError" || error.name === "SecurityError";
  }
  if (error instanceof Error) {
    if (error.name === "NotAllowedError" || error.name === "SecurityError") {
      return true;
    }
    return /\bpermission\b/i.test(error.message);
  }
  return false;
}

function isWatchUnsupportedError(error: unknown): boolean {
  if (error instanceof SyncWatchUnsupportedError) return true;
  if (error instanceof Error) {
    return (
      error.name === "SyncWatchUnsupportedError" ||
      error.message.includes(WATCH_UNSUPPORTED_ERROR) ||
      error.message.includes("--watch")
    );
  }
  return false;
}

function formatChangePath(change: SyncChange): string {
  const isDir =
    change.src?.kind === SyncKind.Directory ||
    change.dst?.kind === SyncKind.Directory;
  if (!change.typeChanged && isDir && !change.path.endsWith("/")) {
    return `${change.path}/`;
  }
  return change.path;
}

function formatChangeDetail(change: SyncChange, marker: string): string {
  if (marker === "!") {
    return change.reason || "conflict";
  }
  if (change.typeChanged) {
    if (
      change.src?.kind === SyncKind.Directory ||
      change.dst?.kind === SyncKind.File
    ) {
      return "file -> folder";
    }
    if (
      change.src?.kind === SyncKind.File ||
      change.dst?.kind === SyncKind.Directory
    ) {
      return "folder -> file";
    }
  }
  const entry = change.src ?? change.dst;
  if (!entry) return "";
  if (entry.kind === SyncKind.Directory) return "folder";
  return formatBytes(entry.size);
}

function countDeleteFiles(deletes: SyncChange[]): number {
  let count = 0;
  for (const item of deletes) {
    if (item.dst?.kind === SyncKind.Directory) continue;
    count += 1;
  }
  return count > 0 ? count : deletes.length;
}

export default function SyncPanel({
  initialCode,
  settings,
  onActiveChange,
}: SyncPanelProps) {
  const opfsOptionAvailable = isOpfsTestEnabled() && supportsOpfs();
  const browserSupported = supportsWebSync();
  const [code, setCode] = useState(() => initialCode ?? syncCodeFromURL());
  const [root, setRoot] = useState<FileSystemDirectoryHandle | null>(null);
  const [folderLabel, setFolderLabel] = useState("");
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [allowEmptySource, setAllowEmptySource] = useState(false);
  const [checksum, setChecksum] = useState(false);
  const [dryRun, setDryRun] = useState(false);

  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState("");
  const [statusLines, setStatusLines] = useState<string[]>([]);
  const [error, setError] = useState("");
  const [permissionLost, setPermissionLost] = useState(false);
  const [watchUnsupported, setWatchUnsupported] = useState(false);
  const [plan, setPlan] = useState<SyncPlan>();
  const [progress, setProgress] = useState<FileProgress>();
  const [result, setResult] = useState<SyncResult>();
  const [cancelledDecision, setCancelledDecision] = useState(false);
  const [pendingConfirm, setPendingConfirm] = useState<PendingDeleteConfirm>();
  const [confirmText, setConfirmText] = useState("");

  const hashCache = useMemo(() => new SyncHashCache(), []);
  const abortRef = useRef<AbortController | undefined>(undefined);
  const pendingResolveRef = useRef<
    ((decision: SyncDecision) => void) | undefined
  >(undefined);
  const cancelledDecisionRef = useRef(false);
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      abortRef.current?.abort();
      pendingResolveRef.current?.({ proceed: false, confirmDeletes: false });
      hashCache.close();
    };
  }, [hashCache]);

  const trimmedCode = code.trim();
  const codeValidationError = validateSyncCode(code);
  const canStart =
    !busy && root !== null && trimmedCode.length >= 6 && !codeValidationError;

  if (!browserSupported && !opfsOptionAvailable) {
    return (
      <section className="sync-workspace" aria-label="Sync folder">
        <article className="panel sync-panel sync-unsupported">
          <div className="panel-heading">
            <span className="step">
              <RefreshCw aria-hidden="true" />
            </span>
            <div>
              <h2>Sync folder</h2>
              <p>
                Web Sync needs Chrome or Edge on desktop (File System Access API
                in a secure context).
              </p>
            </div>
          </div>
          <p className="sync-unsupported-copy">
            This browser cannot write directly to a local folder. Use the croc
            CLI instead:
          </p>
          <pre className="sync-cli-command" aria-label="CLI sync command">
            <code>{formatCliSyncCommand(trimmedCode)}</code>
          </pre>
        </article>
      </section>
    );
  }

  async function chooseFolder() {
    if (!window.showDirectoryPicker) return;
    try {
      const handle = await window.showDirectoryPicker({ mode: "readwrite" });
      if (!mountedRef.current) return;
      setRoot(handle);
      setFolderLabel(handle.name || "Selected folder");
      setPermissionLost(false);
      setError("");
    } catch (reason) {
      if (reason instanceof DOMException && reason.name === "AbortError") {
        return;
      }
      if (!mountedRef.current) return;
      setPermissionLost(isPermissionError(reason));
      setError(errorMessage(reason));
    }
  }

  async function chooseOpfsFolder() {
    if (!supportsOpfs()) {
      setError("Browser test storage (OPFS) is unavailable.");
      return;
    }
    try {
      const handle = await navigator.storage.getDirectory();
      if (!mountedRef.current) return;
      setRoot(handle);
      setFolderLabel(
        handle.name
          ? `${handle.name} (OPFS test storage)`
          : "Browser test storage (OPFS)",
      );
      setPermissionLost(false);
      setError("");
    } catch (reason) {
      if (!mountedRef.current) return;
      setError(errorMessage(reason));
    }
  }

  async function regrantPermission() {
    if (!root) return;
    const handle = root as DirectoryHandleWithPermission;
    if (typeof handle.requestPermission !== "function") {
      await chooseFolder();
      return;
    }
    try {
      const state = await handle.requestPermission({ mode: "readwrite" });
      if (!mountedRef.current) return;
      if (state === "granted") {
        setPermissionLost(false);
        setError("");
        setStatus("Folder access granted. Start sync to try again.");
      } else {
        setPermissionLost(true);
        setError(
          "Folder write permission is still denied. Grant read/write access to continue.",
        );
      }
    } catch (reason) {
      if (!mountedRef.current) return;
      setPermissionLost(true);
      setError(errorMessage(reason));
    }
  }

  async function startSync() {
    if (!canStart || !root) return;

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    cancelledDecisionRef.current = false;

    setBusy(true);
    setStatus("Connecting…");
    setStatusLines(["Connecting…"]);
    setError("");
    setPermissionLost(false);
    setWatchUnsupported(false);
    setPlan(undefined);
    setProgress(undefined);
    setResult(undefined);
    setCancelledDecision(false);
    setPendingConfirm(undefined);
    setConfirmText("");
    onActiveChange?.(true);

    const dryRunNow = dryRun;
    const activeSecret = trimmedCode;

    const decide = (incomingPlan: SyncPlan): Promise<SyncDecision> => {
      if (mountedRef.current) {
        setPlan(incomingPlan);
      }
      if (dryRunNow) {
        return Promise.resolve({ proceed: false, confirmDeletes: false });
      }
      if (incomingPlan.delete.length === 0) {
        return Promise.resolve({ proceed: true, confirmDeletes: false });
      }
      return new Promise<SyncDecision>((resolve) => {
        if (controller.signal.aborted || !mountedRef.current) {
          resolve({ proceed: false, confirmDeletes: false });
          return;
        }
        const finish = (decision: SyncDecision) => {
          controller.signal.removeEventListener("abort", onAbort);
          pendingResolveRef.current = undefined;
          if (mountedRef.current) {
            setPendingConfirm(undefined);
            setConfirmText("");
          }
          resolve(decision);
        };
        const onAbort = () => finish({ proceed: false, confirmDeletes: false });
        controller.signal.addEventListener("abort", onAbort, { once: true });
        pendingResolveRef.current = finish;
        setConfirmText("");
        setPendingConfirm({
          plan: incomingPlan,
          deleteCount: incomingPlan.delete.length,
          deleteFileCount: countDeleteFiles(incomingPlan.delete),
          requireTypedYes: Boolean(incomingPlan.requireDeleteConfirm),
          resolve: finish,
        });
      });
    };

    try {
      const outcome = await syncReceive({
        secret: activeSecret,
        settings,
        root,
        diff: {
          delete: deleteFiles,
          checksum,
          allowEmptySource: deleteFiles && allowEmptySource,
        },
        decide,
        hashCache,
        signal: controller.signal,
        callbacks: {
          onStatus: (nextStatus) => {
            if (!mountedRef.current || abortRef.current !== controller) return;
            setStatus(nextStatus);
            setStatusLines((current) =>
              current[current.length - 1] === nextStatus
                ? current
                : [...current.slice(-4), nextStatus],
            );
          },
          onPlan: (incomingPlan) => {
            if (!mountedRef.current || abortRef.current !== controller) return;
            setPlan(incomingPlan);
          },
          onProgress: (nextProgress) => {
            if (!mountedRef.current || abortRef.current !== controller) return;
            setProgress(nextProgress);
          },
          onResult: (nextResult) => {
            if (!mountedRef.current || abortRef.current !== controller) return;
            setResult(nextResult);
          },
        },
      });
      if (!mountedRef.current || abortRef.current !== controller) return;
      setResult(outcome);
      setCancelledDecision(cancelledDecisionRef.current);
      if (cancelledDecisionRef.current) {
        setStatus("Sync cancelled; nothing was changed.");
      } else if (outcome.dryRun) {
        setStatus("Dry run complete.");
      } else {
        setStatus("Sync complete.");
      }
    } catch (reason) {
      if (!mountedRef.current || abortRef.current !== controller) return;
      if (
        controller.signal.aborted ||
        (reason instanceof DOMException && reason.name === "AbortError")
      ) {
        setStatus("Sync cancelled");
        setError("");
      } else {
        setWatchUnsupported(isWatchUnsupportedError(reason));
        setPermissionLost(isPermissionError(reason));
        setError(errorMessage(reason));
        setStatus("");
      }
    } finally {
      if (abortRef.current === controller) {
        abortRef.current = undefined;
      }
      pendingResolveRef.current = undefined;
      if (mountedRef.current) {
        setPendingConfirm(undefined);
        setBusy(false);
        onActiveChange?.(false);
      }
    }
  }

  function cancelSync() {
    abortRef.current?.abort();
    pendingResolveRef.current?.({ proceed: false, confirmDeletes: false });
  }

  const planGroups = plan
    ? [
        {
          key: "add",
          marker: "+" as const,
          label: "Add",
          className: "sync-row-add",
          items: plan.add,
        },
        {
          key: "update",
          marker: "~" as const,
          label: "Update",
          className: "sync-row-update",
          items: plan.update,
        },
        ...(plan.touch.length > 0
          ? [
              {
                key: "touch",
                marker: "*" as const,
                label: "Touch",
                className: "sync-row-touch",
                items: plan.touch,
              },
            ]
          : []),
        {
          key: "delete",
          marker: "-" as const,
          label: "Delete",
          className: "sync-row-delete",
          items: plan.delete,
        },
        {
          key: "conflicts",
          marker: "!" as const,
          label: "Conflict",
          className: "sync-row-conflict",
          items: plan.conflicts,
        },
      ]
    : [];

  const hasPlanRows = planGroups.some((group) => group.items.length > 0);
  const progressPercent =
    progress && progress.totalSize > 0
      ? Math.min(100, (progress.totalBytes / progress.totalSize) * 100)
      : 0;
  const confirmDeleteDisabled = Boolean(
    pendingConfirm?.requireTypedYes &&
      confirmText.trim().toLowerCase() !== "yes",
  );

  return (
    <section className="sync-workspace" aria-label="Sync folder">
      <article className="panel sync-panel">
        <div className="panel-heading">
          <span className="step">
            <RefreshCw aria-hidden="true" />
          </span>
          <div>
            <h2>Sync folder</h2>
            <p>
              Sync one-way from a <code>croc sync</code> source into a local
              folder on this computer.
            </p>
          </div>
        </div>

        <form
          className="sync-form"
          autoComplete="off"
          onSubmit={(event) => {
            event.preventDefault();
            void startSync();
          }}
        >
          <label className="field-label" htmlFor="sync-code">
            Sync code
          </label>
          <input
            id="sync-code"
            name="sync-code"
            value={code}
            disabled={busy}
            placeholder="word-word-word or six-word code"
            spellCheck={false}
            autoComplete="off"
            autoCapitalize="none"
            autoCorrect="off"
            aria-invalid={Boolean(codeValidationError)}
            onChange={(event) => setCode(event.target.value)}
          />
          {codeValidationError && (
            <p className="field-help sync-field-error" role="alert">
              {codeValidationError}
            </p>
          )}

          <div className="sync-folder-block">
            <span className="field-label">Destination folder</span>
            <div className="sync-folder-row">
              {browserSupported && (
                <button
                  type="button"
                  className="secondary-button sync-folder-button"
                  disabled={busy}
                  onClick={() => void chooseFolder()}
                >
                  <FolderOpen aria-hidden="true" /> Choose folder
                </button>
              )}
              <span className="sync-folder-name" aria-live="polite">
                {root ? folderLabel : "No folder selected"}
              </span>
            </div>
            {opfsOptionAvailable && (
              <div className="sync-opfs-option">
                <button
                  type="button"
                  className="secondary-button"
                  disabled={busy}
                  onClick={() => void chooseOpfsFolder()}
                >
                  Use browser test storage (OPFS)
                </button>
                <small>
                  Test option: writes to Origin Private File System instead of a
                  local folder.
                </small>
              </div>
            )}
          </div>

          <fieldset className="sync-options" disabled={busy}>
            <legend className="field-label">Sync options</legend>
            <label className="checkbox-label sync-checkbox">
              <input
                type="checkbox"
                checked={deleteFiles}
                onChange={(event) => {
                  const next = event.target.checked;
                  setDeleteFiles(next);
                  if (!next) setAllowEmptySource(false);
                }}
              />
              <span>Delete files that are not in the source</span>
            </label>
            {deleteFiles && (
              <label className="checkbox-label sync-checkbox sync-checkbox-nested">
                <input
                  type="checkbox"
                  checked={allowEmptySource}
                  onChange={(event) =>
                    setAllowEmptySource(event.target.checked)
                  }
                />
                <span>Allow an empty source to delete everything</span>
              </label>
            )}
            <label className="checkbox-label sync-checkbox">
              <input
                type="checkbox"
                checked={checksum}
                onChange={(event) => setChecksum(event.target.checked)}
              />
              <span>Compare contents (slower)</span>
            </label>
            <label className="checkbox-label sync-checkbox">
              <input
                type="checkbox"
                checked={dryRun}
                onChange={(event) => setDryRun(event.target.checked)}
              />
              <span>Preview only (dry run)</span>
            </label>
          </fieldset>

          <div className="sync-actions">
            {busy ? (
              <button
                type="button"
                className="primary-button inverted"
                onClick={cancelSync}
              >
                <X aria-hidden="true" /> Cancel sync
              </button>
            ) : (
              <button
                type="submit"
                className="primary-button"
                disabled={!canStart}
              >
                <RefreshCw aria-hidden="true" />{" "}
                {dryRun ? "Preview sync" : "Start sync"}
              </button>
            )}
          </div>
        </form>

        {(status || statusLines.length > 0) && !error && !progress && (
          <div className="sync-status-log" aria-live="polite">
            <p className={`status-message ${result && !busy ? "done" : "working"}`} role="status">
              {result && !busy ? <Check aria-hidden="true" /> : null}
              <span>{status}</span>
            </p>
            {statusLines.length > 1 && (
              <ul className="sync-status-history" aria-label="Sync status history">
                {statusLines.slice(0, -1).map((line, index) => (
                  <li key={`${index}-${line}`}>{line}</li>
                ))}
              </ul>
            )}
          </div>
        )}

        {progress && (
          <div className="progress-block" aria-live="polite">
            <div className="progress-copy">
              <span>{status || "Transferring files…"}</span>
              <span>
                {formatBytes(progress.totalBytes)} /{" "}
                {formatBytes(progress.totalSize)}
              </span>
            </div>
            <div
              className="progress-track"
              role="progressbar"
              aria-label="Sync transfer progress"
              aria-valuemin={0}
              aria-valuemax={100}
              aria-valuenow={Math.round(progressPercent)}
            >
              <span style={{ width: `${progressPercent}%` }} />
            </div>
            <div className="progress-file">
              <span>
                {progress.fileIndex + 1}/{progress.fileCount}{" "}
                {progress.fileName}
              </span>
              <span>{Math.round(progressPercent)}%</span>
            </div>
          </div>
        )}

        {error && (
          <div className="sync-error-box" role="alert">
            <p className="status-message error">
              <AlertTriangle aria-hidden="true" />
              <span>{error}</span>
            </p>
            {permissionLost && root && (
              <div className="sync-permission-recovery">
                <p>
                  Browser permission to access this folder was denied or lost.
                  Grant read/write access again to continue.
                </p>
                <button
                  type="button"
                  className="secondary-button"
                  disabled={busy}
                  onClick={() => void regrantPermission()}
                >
                  Grant access again
                </button>
              </div>
            )}
            {watchUnsupported && (
              <div className="sync-watch-alternative">
                <p>
                  To sync with a watching source, use the croc CLI instead:
                </p>
                <pre className="sync-cli-command" aria-label="CLI sync alternative">
                  <code>{formatCliSyncCommand(trimmedCode)}</code>
                </pre>
              </div>
            )}
          </div>
        )}

        {plan && (
          <section className="sync-plan" aria-label="Sync plan">
            <p className="sync-plan-counts">
              <strong>Plan:</strong> {plan.add.length} to add,{" "}
              {plan.update.length} to update
              {plan.touch.length > 0 ? `, ${plan.touch.length} to touch` : ""},{" "}
              {plan.delete.length} to delete, {plan.conflicts.length} conflicts,{" "}
              {plan.skip.length} unchanged
            </p>
            {hasPlanRows ? (
              <div className="sync-plan-table-wrap">
                <table className="sync-plan-table" aria-label="Planned changes">
                  <thead>
                    <tr>
                      <th scope="col">Action</th>
                      <th scope="col">Path</th>
                      <th scope="col">Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {planGroups.map((group) => {
                      if (group.items.length === 0) return null;
                      const visible = group.items.slice(
                        0,
                        MAX_PLAN_ROWS_PER_GROUP,
                      );
                      const hidden = group.items.length - visible.length;
                      return (
                        <Fragment key={group.key}>
                          {visible.map((change) => (
                            <tr
                              key={`${group.key}:${change.path}`}
                              className={group.className}
                            >
                              <td
                                className="sync-plan-marker"
                                aria-label={group.label}
                              >
                                {group.marker}
                              </td>
                              <td className="sync-plan-path">
                                {formatChangePath(change)}
                              </td>
                              <td className="sync-plan-detail">
                                {formatChangeDetail(change, group.marker)}
                              </td>
                            </tr>
                          ))}
                          {hidden > 0 && (
                            <tr
                              key={`${group.key}:more`}
                              className="sync-plan-more"
                            >
                              <td />
                              <td colSpan={2}>…and {hidden} more</td>
                            </tr>
                          )}
                        </Fragment>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="sync-plan-empty">Already in sync.</p>
            )}
            {plan.deleteBlockedReason && (
              <p className="sync-notice" role="status">
                <AlertTriangle aria-hidden="true" />
                <span>Deletions skipped: {plan.deleteBlockedReason}</span>
              </p>
            )}
          </section>
        )}

        {pendingConfirm && (
          <section
            className="sync-confirm"
            role="region"
            aria-label="Confirm deletions"
          >
            <div className="sync-confirm-header">
              <ShieldAlert aria-hidden="true" />
              <strong>Confirm deletions</strong>
            </div>
            {pendingConfirm.requireTypedYes ? (
              <>
                <p>
                  This deletes {pendingConfirm.deleteFileCount} of the
                  destination files (more than half). Type <code>yes</code> to
                  allow deleting them, or apply without deleting.
                </p>
                <label
                  className="field-label"
                  htmlFor="sync-confirm-delete-input"
                >
                  Type yes to confirm deletions
                </label>
                <input
                  id="sync-confirm-delete-input"
                  className="sync-confirm-input"
                  value={confirmText}
                  autoComplete="off"
                  spellCheck={false}
                  placeholder="yes"
                  onChange={(event) => setConfirmText(event.target.value)}
                />
              </>
            ) : (
              <p>
                Apply these changes, including {pendingConfirm.deleteCount}{" "}
                {pendingConfirm.deleteCount === 1 ? "deletion" : "deletions"}?
              </p>
            )}
            <div className="sync-confirm-actions">
              <button
                type="button"
                className="primary-button"
                disabled={confirmDeleteDisabled}
                onClick={() =>
                  pendingConfirm.resolve({
                    proceed: true,
                    confirmDeletes: true,
                  })
                }
              >
                Apply, including {pendingConfirm.deleteCount}{" "}
                {pendingConfirm.deleteCount === 1 ? "deletion" : "deletions"}
              </button>
              <button
                type="button"
                className="secondary-button"
                onClick={() => {
                  if (!pendingConfirm.plan.requireDeleteConfirm) {
                    try {
                      pendingConfirm.plan.requireDeleteConfirm = true;
                    } catch {
                      // Ignore if plan is frozen in tests.
                    }
                  }
                  pendingConfirm.resolve({
                    proceed: true,
                    confirmDeletes: false,
                  });
                }}
              >
                Apply without deleting
              </button>
              <button
                type="button"
                className="secondary-button"
                onClick={() => {
                  cancelledDecisionRef.current = true;
                  pendingConfirm.resolve({
                    proceed: false,
                    confirmDeletes: false,
                  });
                }}
              >
                Cancel
              </button>
            </div>
          </section>
        )}

        {result && (
          <section className="sync-result" aria-label="Sync result">
            {result.dryRun ? (
              <p className="sync-result-summary">
                <strong>
                  {cancelledDecision
                    ? "Sync cancelled — nothing was changed."
                    : "Dry run complete — nothing was changed."}
                </strong>
              </p>
            ) : (
              <p className="sync-result-summary">
                <strong>Synced:</strong> {result.added} added, {result.updated}{" "}
                updated, {result.deleted} deleted, {result.unchanged} unchanged
                {result.conflicts > 0 ? `, ${result.conflicts} conflicts` : ""},{" "}
                {formatBytes(result.bytes)} transferred
              </p>
            )}

            {result.deletesHeld && result.deletesHeld > 0 ? (
              <p className="sync-notice" role="status">
                <AlertTriangle aria-hidden="true" />
                <span>
                  {result.deletesHeld} deletion
                  {result.deletesHeld === 1 ? "" : "s"} held
                  {result.deleteBlocked ? ` (${result.deleteBlocked})` : ""}
                </span>
              </p>
            ) : !result.dryRun && result.deleteBlocked ? (
              <p className="sync-notice" role="status">
                <AlertTriangle aria-hidden="true" />
                <span>Deletions skipped: {result.deleteBlocked}</span>
              </p>
            ) : null}

            {result.failures && result.failures.length > 0 && (
              <div className="sync-result-failures">
                <strong>
                  {result.failures.length} item
                  {result.failures.length === 1 ? "" : "s"} failed:
                </strong>
                <ul>
                  {result.failures.map((failure, index) => (
                    <li key={`${index}-${failure}`}>{failure}</li>
                  ))}
                </ul>
              </div>
            )}

            <p className="sync-result-note">{MTIME_CACHE_NOTE}</p>
          </section>
        )}
      </article>
    </section>
  );
}
