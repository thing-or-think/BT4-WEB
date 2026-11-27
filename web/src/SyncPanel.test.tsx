import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import SyncPanel, { MTIME_CACHE_NOTE } from "./SyncPanel";
import { SyncKind, type SyncChange, type SyncPlan } from "./protocol/sync-diff";
import type { SyncDecision, SyncReceiveOptions } from "./protocol/sync";
import type { SyncResult } from "./protocol/sync-wire";
import type { TransferSettings } from "./protocol/types";

const mocks = vi.hoisted(() => ({
  syncReceive: vi.fn<(options: SyncReceiveOptions) => Promise<SyncResult>>(),
}));

vi.mock("./protocol/sync", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./protocol/sync")>();
  return {
    ...actual,
    syncReceive: mocks.syncReceive,
  };
});

const defaultSettings: TransferSettings = {
  gatewayURL: "/ws",
  relayAddresses: ["relay.example:9009"],
  relayPassword: "pass123",
  storeAPI: "/api/v1/store",
};

type FakeDirHandle = FileSystemDirectoryHandle & {
  requestPermission: ReturnType<typeof vi.fn>;
  queryPermission: ReturnType<typeof vi.fn>;
};

function makeFakeDirHandle(name = "project-folder"): FakeDirHandle {
  return {
    kind: "directory",
    name,
    isSameEntry: vi.fn(async () => false),
    getDirectoryHandle: vi.fn(),
    getFileHandle: vi.fn(),
    removeEntry: vi.fn(),
    resolve: vi.fn(),
    queryPermission: vi.fn(async () => "granted"),
    requestPermission: vi.fn(async () => "granted"),
  } as unknown as FakeDirHandle;
}

function makePlan(overrides: Partial<SyncPlan> = {}): SyncPlan {
  return {
    add: [],
    update: [],
    touch: [],
    skip: [],
    delete: [],
    conflicts: [],
    deleteBlockedReason: "",
    requireDeleteConfirm: false,
    ...overrides,
  };
}

function makeFileChange(
  path: string,
  opts: { src?: boolean; dst?: boolean; size?: number; reason?: string } = {},
): SyncChange {
  const size = opts.size ?? 128;
  const entry = {
    path,
    size,
    modTime: "2026-11-01T12:00:00Z",
    mode: 0o644,
    kind: SyncKind.File,
  };
  return {
    path,
    ...(opts.src !== false ? { src: entry } : {}),
    ...(opts.dst ? { dst: entry } : {}),
    ...(opts.reason ? { reason: opts.reason } : {}),
  };
}

function setSecureContext(value: boolean) {
  Object.defineProperty(window, "isSecureContext", {
    value,
    configurable: true,
    writable: true,
  });
}

let originalShowDirectoryPicker: typeof window.showDirectoryPicker;
let originalStorageDescriptor: PropertyDescriptor | undefined;
let originalSecureContextDescriptor: PropertyDescriptor | undefined;

beforeEach(() => {
  vi.clearAllMocks();
  window.history.replaceState({}, "", "/#sync");
  originalShowDirectoryPicker = window.showDirectoryPicker;
  originalStorageDescriptor = Object.getOwnPropertyDescriptor(
    navigator,
    "storage",
  );
  originalSecureContextDescriptor = Object.getOwnPropertyDescriptor(
    window,
    "isSecureContext",
  );
  setSecureContext(true);
  window.showDirectoryPicker = vi.fn(async () => makeFakeDirHandle());
});

afterEach(() => {
  cleanup();
  window.showDirectoryPicker = originalShowDirectoryPicker;
  if (originalStorageDescriptor) {
    Object.defineProperty(navigator, "storage", originalStorageDescriptor);
  } else {
    Reflect.deleteProperty(navigator, "storage");
  }
  if (originalSecureContextDescriptor) {
    Object.defineProperty(
      window,
      "isSecureContext",
      originalSecureContextDescriptor,
    );
  } else {
    Reflect.deleteProperty(window, "isSecureContext");
  }
});

describe("SyncPanel", () => {
  it("shows Chrome/Edge guidance and CLI alternative when showDirectoryPicker is unavailable", () => {
    window.showDirectoryPicker = undefined;
    window.history.replaceState({}, "", "/?sync=alpha-beta-gamma#sync");

    render(<SyncPanel settings={defaultSettings} />);

    expect(screen.getByText(/Chrome or Edge on desktop/i)).toBeVisible();
    expect(
      screen.getByText("CROC_SECRET='alpha-beta-gamma' croc sync <folder>"),
    ).toBeVisible();
    expect(screen.queryByLabelText("Sync code")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /Choose folder/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /Start sync/i }),
    ).not.toBeInTheDocument();
  });

  it("shows unsupported message when not in a secure context", () => {
    setSecureContext(false);

    render(<SyncPanel settings={defaultSettings} />);

    expect(screen.getByText(/Chrome or Edge on desktop/i)).toBeVisible();
    expect(
      screen.getByText("CROC_SECRET='<code>' croc sync <folder>"),
    ).toBeVisible();
    expect(screen.queryByLabelText("Sync code")).not.toBeInTheDocument();
  });

  it("fills the code from ?sync= without auto-connecting", () => {
    window.history.replaceState({}, "", "/?sync=acid-acorn-acre#sync");

    render(<SyncPanel settings={defaultSettings} />);

    expect(screen.getByLabelText("Sync code")).toHaveValue("acid-acorn-acre");
    expect(mocks.syncReceive).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: /Start sync/i })).toBeDisabled();
  });

  it("requires a folder and valid code before Start, and toggles empty-source option with delete", async () => {
    const picker = vi.fn(async () => makeFakeDirHandle("my-photos"));
    window.showDirectoryPicker = picker;

    render(<SyncPanel settings={defaultSettings} />);

    const codeInput = screen.getByLabelText("Sync code");
    const startButton = screen.getByRole("button", { name: /Start sync/i });

    expect(startButton).toBeDisabled();
    fireEvent.change(codeInput, { target: { value: "short" } });
    expect(screen.getByRole("alert")).toHaveTextContent(/at least 6/i);
    expect(startButton).toBeDisabled();

    fireEvent.change(codeInput, { target: { value: "alpha-beta-gamma" } });
    expect(startButton).toBeDisabled();

    expect(
      screen.queryByLabelText("Allow an empty source to delete everything"),
    ).not.toBeInTheDocument();
    fireEvent.click(
      screen.getByLabelText("Delete files that are not in the source"),
    );
    expect(
      screen.getByLabelText("Allow an empty source to delete everything"),
    ).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() => expect(screen.getByText("my-photos")).toBeVisible());
    expect(picker).toHaveBeenCalledWith({ mode: "readwrite" });
    expect(startButton).toBeEnabled();
  });

  it("renders grouped plan table with markers, 50-row truncation, and deletion-blocked notice", async () => {
    const adds = Array.from({ length: 55 }, (_, i) =>
      makeFileChange(`add-${String(i + 1).padStart(2, "0")}.txt`, {
        src: true,
        size: 10,
      }),
    );
    const plan = makePlan({
      add: adds,
      update: [makeFileChange("edited.txt", { src: true, dst: true, size: 64 })],
      delete: [
        {
          path: "obsolete.txt",
          dst: {
            path: "obsolete.txt",
            size: 12,
            modTime: "2026-11-01T12:00:00Z",
            mode: 0o644,
            kind: SyncKind.File,
          },
        },
      ],
      conflicts: [
        makeFileChange("clash", {
          src: true,
          dst: true,
          reason: "destination directory is not empty",
        }),
      ],
      skip: [makeFileChange("same.txt", { src: true, dst: true })],
      deleteBlockedReason:
        "the source respects .gitignore, which web sync cannot evaluate; deletions are disabled",
    });

    mocks.syncReceive.mockImplementation(async (options) => {
      options.callbacks?.onStatus?.("Comparing folders…");
      options.callbacks?.onPlan?.(plan);
      await options.decide(plan);
      return {
        dryRun: true,
        added: 0,
        updated: 0,
        touched: 0,
        deleted: 0,
        unchanged: 1,
        conflicts: 1,
        bytes: 0,
        deleteBlocked: plan.deleteBlockedReason,
      };
    });

    render(
      <SyncPanel
        initialCode="alpha-beta-gamma"
        settings={defaultSettings}
      />,
    );

    fireEvent.click(screen.getByLabelText("Preview only (dry run)"));
    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() =>
      expect(screen.getByText("project-folder")).toBeVisible(),
    );

    fireEvent.click(screen.getByRole("button", { name: /Preview sync/i }));

    await waitFor(() =>
      expect(screen.getByRole("table", { name: "Planned changes" })).toBeVisible(),
    );

    expect(
      screen.getByText(
        /55 to add, 1 to update, 1 to delete, 1 conflicts, 1 unchanged/,
      ),
    ).toBeVisible();

    const table = screen.getByRole("table", { name: "Planned changes" });
    expect(within(table).getByText("add-01.txt")).toBeVisible();
    expect(within(table).getByText("add-50.txt")).toBeVisible();
    expect(within(table).queryByText("add-51.txt")).not.toBeInTheDocument();
    expect(within(table).getByText("…and 5 more")).toBeVisible();
    expect(within(table).getByText("edited.txt")).toBeVisible();
    expect(within(table).getByText("obsolete.txt")).toBeVisible();
    expect(within(table).getByText("clash")).toBeVisible();
    expect(
      within(table).getByText("destination directory is not empty"),
    ).toBeVisible();
    expect(within(table).getAllByLabelText("Add")).toHaveLength(50);
    expect(within(table).getByLabelText("Update")).toHaveTextContent("~");
    expect(within(table).getByLabelText("Delete")).toHaveTextContent("-");
    expect(within(table).getByLabelText("Conflict")).toHaveTextContent("!");

    expect(
      screen.getByText(/Deletions skipped: the source respects \.gitignore/i),
    ).toBeVisible();
  });

  it("proceeds automatically without prompting when plan has no deletes", async () => {
    let capturedDecision: SyncDecision | undefined;
    const plan = makePlan({
      add: [makeFileChange("new.txt")],
    });

    mocks.syncReceive.mockImplementation(async (options) => {
      options.callbacks?.onPlan?.(plan);
      capturedDecision = await options.decide(plan);
      return {
        added: 1,
        updated: 0,
        touched: 0,
        deleted: 0,
        unchanged: 0,
        conflicts: 0,
        bytes: 128,
      };
    });

    render(
      <SyncPanel initialCode="alpha-beta-gamma" settings={defaultSettings} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() =>
      expect(screen.getByText("project-folder")).toBeVisible(),
    );
    fireEvent.click(screen.getByRole("button", { name: /Start sync/i }));

    await waitFor(() =>
      expect(capturedDecision).toEqual({
        proceed: true,
        confirmDeletes: false,
      }),
    );
    expect(
      screen.queryByRole("region", { name: "Confirm deletions" }),
    ).not.toBeInTheDocument();
  });

  it("prompts on normal deletes and proceeds with deletions when confirmed", async () => {
    let capturedDecision: SyncDecision | undefined;
    const plan = makePlan({
      delete: [
        makeFileChange("old1.txt", { src: false, dst: true }),
        makeFileChange("old2.txt", { src: false, dst: true }),
      ],
      requireDeleteConfirm: false,
    });

    mocks.syncReceive.mockImplementation(async (options) => {
      options.callbacks?.onPlan?.(plan);
      capturedDecision = await options.decide(plan);
      return {
        added: 0,
        updated: 0,
        touched: 0,
        deleted: 2,
        unchanged: 5,
        conflicts: 0,
        bytes: 0,
      };
    });

    render(
      <SyncPanel initialCode="alpha-beta-gamma" settings={defaultSettings} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() =>
      expect(screen.getByText("project-folder")).toBeVisible(),
    );
    fireEvent.click(screen.getByRole("button", { name: /Start sync/i }));

    const applyBtn = await screen.findByRole("button", {
      name: "Apply, including 2 deletions",
    });
    expect(applyBtn).toBeEnabled();
    fireEvent.click(applyBtn);

    await waitFor(() =>
      expect(capturedDecision).toEqual({
        proceed: true,
        confirmDeletes: true,
      }),
    );
  });

  it("requires typing yes when deleting more than half of destination files", async () => {
    let capturedDecision: SyncDecision | undefined;
    const plan = makePlan({
      delete: [
        makeFileChange("a.txt", { src: false, dst: true }),
        makeFileChange("b.txt", { src: false, dst: true }),
        makeFileChange("c.txt", { src: false, dst: true }),
      ],
      requireDeleteConfirm: true,
    });

    mocks.syncReceive.mockImplementation(async (options) => {
      options.callbacks?.onPlan?.(plan);
      capturedDecision = await options.decide(plan);
      return {
        added: 0,
        updated: 0,
        touched: 0,
        deleted: 3,
        unchanged: 1,
        conflicts: 0,
        bytes: 0,
      };
    });

    render(
      <SyncPanel initialCode="alpha-beta-gamma" settings={defaultSettings} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() =>
      expect(screen.getByText("project-folder")).toBeVisible(),
    );
    fireEvent.click(screen.getByRole("button", { name: /Start sync/i }));

    const applyBtn = await screen.findByRole("button", {
      name: "Apply, including 3 deletions",
    });
    const confirmInput = screen.getByLabelText("Type yes to confirm deletions");
    expect(applyBtn).toBeDisabled();

    fireEvent.change(confirmInput, { target: { value: "y" } });
    expect(applyBtn).toBeDisabled();

    fireEvent.change(confirmInput, { target: { value: "yes" } });
    expect(applyBtn).toBeEnabled();

    fireEvent.click(applyBtn);
    await waitFor(() =>
      expect(capturedDecision).toEqual({
        proceed: true,
        confirmDeletes: true,
      }),
    );
  });

  it("supports Apply without deleting (proceed true, confirmDeletes false)", async () => {
    let capturedDecision: SyncDecision | undefined;
    const plan = makePlan({
      add: [makeFileChange("keep-new.txt")],
      delete: [makeFileChange("held.txt", { src: false, dst: true })],
      requireDeleteConfirm: true,
    });

    mocks.syncReceive.mockImplementation(async (options) => {
      options.callbacks?.onPlan?.(plan);
      capturedDecision = await options.decide(plan);
      return {
        added: 1,
        updated: 0,
        touched: 0,
        deleted: 0,
        unchanged: 0,
        conflicts: 0,
        bytes: 128,
        deletesHeld: 1,
      };
    });

    render(
      <SyncPanel initialCode="alpha-beta-gamma" settings={defaultSettings} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() =>
      expect(screen.getByText("project-folder")).toBeVisible(),
    );
    fireEvent.click(screen.getByRole("button", { name: /Start sync/i }));

    const withoutDeletesBtn = await screen.findByRole("button", {
      name: "Apply without deleting",
    });
    fireEvent.click(withoutDeletesBtn);

    await waitFor(() =>
      expect(capturedDecision).toEqual({
        proceed: true,
        confirmDeletes: false,
      }),
    );
    expect(await screen.findByText(/1 deletion held/i)).toBeVisible();
  });

  it("supports Cancel in delete confirmation (proceed false, nothing written)", async () => {
    let capturedDecision: SyncDecision | undefined;
    const plan = makePlan({
      delete: [makeFileChange("a.txt", { src: false, dst: true })],
      requireDeleteConfirm: false,
    });

    mocks.syncReceive.mockImplementation(async (options) => {
      options.callbacks?.onPlan?.(plan);
      capturedDecision = await options.decide(plan);
      return {
        dryRun: true,
        added: 0,
        updated: 0,
        touched: 0,
        deleted: 0,
        unchanged: 1,
        conflicts: 0,
        bytes: 0,
      };
    });

    render(
      <SyncPanel initialCode="alpha-beta-gamma" settings={defaultSettings} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() =>
      expect(screen.getByText("project-folder")).toBeVisible(),
    );
    fireEvent.click(screen.getByRole("button", { name: /Start sync/i }));

    const cancelBtn = await screen.findByRole("button", { name: "Cancel" });
    fireEvent.click(cancelBtn);

    await waitFor(() =>
      expect(capturedDecision).toEqual({
        proceed: false,
        confirmDeletes: false,
      }),
    );
    expect(
      await screen.findByText(/Sync cancelled — nothing was changed/i),
    ).toBeVisible();
  });

  it("runs dry run without asking even when deletes are planned", async () => {
    let capturedDecision: SyncDecision | undefined;
    const plan = makePlan({
      add: [makeFileChange("a.txt")],
      delete: [makeFileChange("b.txt", { src: false, dst: true })],
      requireDeleteConfirm: true,
    });

    mocks.syncReceive.mockImplementation(async (options) => {
      options.callbacks?.onPlan?.(plan);
      capturedDecision = await options.decide(plan);
      return {
        dryRun: true,
        added: 0,
        updated: 0,
        touched: 0,
        deleted: 0,
        unchanged: 2,
        conflicts: 0,
        bytes: 0,
      };
    });

    render(
      <SyncPanel initialCode="alpha-beta-gamma" settings={defaultSettings} />,
    );
    fireEvent.click(screen.getByLabelText("Preview only (dry run)"));
    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() =>
      expect(screen.getByText("project-folder")).toBeVisible(),
    );
    fireEvent.click(screen.getByRole("button", { name: /Preview sync/i }));

    await waitFor(() =>
      expect(capturedDecision).toEqual({
        proceed: false,
        confirmDeletes: false,
      }),
    );
    expect(
      screen.queryByRole("region", { name: "Confirm deletions" }),
    ).not.toBeInTheDocument();
    expect(
      await screen.findByText(/Dry run complete — nothing was changed/i),
    ).toBeVisible();
  });

  it("shows permission error and re-grants access via requestPermission", async () => {
    const dirHandle = makeFakeDirHandle("secured-dir");
    dirHandle.requestPermission.mockResolvedValue("granted");
    window.showDirectoryPicker = vi.fn(async () => dirHandle);

    mocks.syncReceive.mockRejectedValue(
      new Error("Write permission for the destination folder was denied"),
    );

    render(
      <SyncPanel initialCode="alpha-beta-gamma" settings={defaultSettings} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() => expect(screen.getByText("secured-dir")).toBeVisible());
    fireEvent.click(screen.getByRole("button", { name: /Start sync/i }));

    const regrantBtn = await screen.findByRole("button", {
      name: "Grant access again",
    });
    expect(
      screen.getByText(/Write permission for the destination folder was denied/),
    ).toBeVisible();

    fireEvent.click(regrantBtn);
    await waitFor(() =>
      expect(dirHandle.requestPermission).toHaveBeenCalledWith({
        mode: "readwrite",
      }),
    );
    await waitFor(() =>
      expect(
        screen.queryByRole("button", { name: "Grant access again" }),
      ).not.toBeInTheDocument(),
    );
    expect(screen.getByText(/Folder access granted/i)).toBeVisible();
  });

  it("shows watch-unsupported error with CLI alternative", async () => {
    const { SyncWatchUnsupportedError } = await import("./protocol/sync");
    mocks.syncReceive.mockRejectedValue(new SyncWatchUnsupportedError());

    render(
      <SyncPanel initialCode="alpha-beta-gamma" settings={defaultSettings} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() =>
      expect(screen.getByText("project-folder")).toBeVisible(),
    );
    fireEvent.click(screen.getByRole("button", { name: /Start sync/i }));

    expect(
      await screen.findByText(/Watching \(--watch\) is not supported/i),
    ).toBeVisible();
    expect(
      screen.getByText("CROC_SECRET='alpha-beta-gamma' croc sync <folder>"),
    ).toBeVisible();
  });

  it("renders progress during transfer, full result summary with failures and held deletes, and mtime cache note", async () => {
    let finishTransfer!: () => void;
    const transferGate = new Promise<void>((resolve) => {
      finishTransfer = resolve;
    });

    mocks.syncReceive.mockImplementation(async (options) => {
      options.callbacks?.onProgress?.({
        fileIndex: 0,
        fileCount: 2,
        fileName: "video.bin",
        fileBytes: 512,
        fileSize: 1024,
        totalBytes: 512,
        totalSize: 2048,
      });
      await transferGate;
      return {
        added: 2,
        updated: 1,
        touched: 0,
        deleted: 1,
        unchanged: 4,
        conflicts: 0,
        bytes: 2048,
        failures: ["locked.txt: changed since the scan"],
        deletesHeld: 2,
        deleteBlocked: "some changes failed to commit",
      };
    });

    render(
      <SyncPanel initialCode="alpha-beta-gamma" settings={defaultSettings} />,
    );
    fireEvent.click(screen.getByRole("button", { name: /Choose folder/i }));
    await waitFor(() =>
      expect(screen.getByText("project-folder")).toBeVisible(),
    );
    fireEvent.click(screen.getByRole("button", { name: /Start sync/i }));

    const progressbar = await screen.findByRole("progressbar", {
      name: "Sync transfer progress",
    });
    expect(progressbar).toHaveAttribute("aria-valuenow", "25");
    expect(screen.getByText(/1\/2 video\.bin/)).toBeVisible();

    act(() => finishTransfer());

    expect(
      await screen.findByText(
        /2 added, 1 updated, 1 deleted, 4 unchanged, 2\.00 kB transferred/,
      ),
    ).toBeVisible();
    expect(
      screen.getByText("locked.txt: changed since the scan"),
    ).toBeVisible();
    expect(
      screen.getByText(/2 deletions held \(some changes failed to commit\)/),
    ).toBeVisible();
    expect(screen.getByText(MTIME_CACHE_NOTE)).toBeVisible();
  });

  it("offers OPFS browser test storage when opfs=1 is in the URL", async () => {
    window.history.replaceState({}, "", "/?opfs=1#sync");
    const opfsDir = makeFakeDirHandle("");
    const getDirectory = vi.fn(async () => opfsDir);
    Object.defineProperty(navigator, "storage", {
      value: { getDirectory },
      configurable: true,
    });

    render(
      <SyncPanel initialCode="alpha-beta-gamma" settings={defaultSettings} />,
    );

    const opfsBtn = screen.getByRole("button", {
      name: "Use browser test storage (OPFS)",
    });
    expect(opfsBtn).toBeVisible();
    fireEvent.click(opfsBtn);

    await waitFor(() => expect(getDirectory).toHaveBeenCalledOnce());
    expect(screen.getByText("Browser test storage (OPFS)")).toBeVisible();
    expect(screen.getByRole("button", { name: /Start sync/i })).toBeEnabled();
  });
});
