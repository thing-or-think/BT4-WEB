import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import SyncPanel, { type DirectoryPicker } from "./SyncPanel";
import { TaskLauncher, taskFromHash } from "./TaskLauncher";
import type { TransferSettings } from "./protocol/types";
import { buildTree, readTree, snapshotTree, type MemoryDirectory } from "./test/memory-fs";

const settings: TransferSettings = {
  gatewayURL: "ws://127.0.0.1:9014/ws",
  relayAddresses: ["127.0.0.1:9009"],
  relayPassword: "pass123",
  storeAPI: "",
};

afterEach(cleanup);

function picker(...folders: MemoryDirectory[]): DirectoryPicker {
  const queue = [...folders];
  return vi.fn(async () => {
    const next = queue.shift();
    if (!next) throw new DOMException("cancelled", "AbortError");
    return next;
  });
}

function named(root: MemoryDirectory, name: string) {
  Object.defineProperty(root, "name", { value: name });
  return root;
}

async function chooseLocalFolders(source: MemoryDirectory, target: MemoryDirectory, onActiveChange = vi.fn()) {
  render(
    <SyncPanel settings={settings} supported pickDirectory={picker(source, target)} onActiveChange={onActiveChange} />,
  );
  fireEvent.click(screen.getByRole("radio", { name: /Two local folders/ }));
  fireEvent.click(screen.getByRole("button", { name: "Choose source folder" }));
  await screen.findByText(source.name);
  fireEvent.click(screen.getByRole("button", { name: "Choose target folder" }));
  await screen.findByText(target.name);
  return onActiveChange;
}

describe("SyncPanel", () => {
  it("explains politely when the browser cannot open folders", () => {
    render(<SyncPanel settings={settings} supported={false} />);
    expect(screen.getByRole("alert")).toHaveTextContent(/only Chrome and Edge on desktop/);
    expect(screen.getByRole("alert")).toHaveTextContent("croc sync <folder>");
    for (const button of screen.getAllByRole("button")) expect(button).toBeDisabled();
  });

  it("previews a dry run with labelled kinds and leaves the target untouched", async () => {
    const source = named(buildTree({ "new.txt": "new", "same.txt": "same", "grown.txt": "v2-longer" }), "project");
    const target = named(buildTree({ "same.txt": "same", "grown.txt": "v1", "extra.txt": "x" }), "backup");
    const before = snapshotTree(target);
    await chooseLocalFolders(source, target);
    fireEvent.click(screen.getByLabelText(/Delete files on the target/));
    fireEvent.click(screen.getByLabelText(/Dry run/));
    fireEvent.click(screen.getByRole("button", { name: "Preview changes" }));

    const plan = await screen.findByRole("region", { name: "Sync plan" });
    expect(within(plan).getByText("Dry run preview")).toBeVisible();
    expect(within(plan).getByText("Nothing will be changed")).toBeVisible();
    const totals = within(plan).getByRole("list", { name: "Plan totals" });
    expect(totals).toHaveTextContent(/Add\s*1/);
    expect(totals).toHaveTextContent(/Update\s*1/);
    expect(totals).toHaveTextContent(/Delete\s*1/);
    expect(totals).toHaveTextContent(/Unchanged\s*0|Unchanged\s*1/);
    const rows = within(within(plan).getByRole("list", { name: "Planned changes" })).getAllByRole("listitem");
    expect(rows.map((row) => row.textContent)).toEqual([
      expect.stringMatching(/^ Add\s*new\.txt/),
      expect.stringMatching(/^ Update\s*grown\.txt/),
      expect.stringMatching(/^ Delete\s*extra\.txt/),
    ]);
    await screen.findByText("Dry run finished. No file was changed.");
    expect(screen.queryByRole("alertdialog")).not.toBeInTheDocument();
    expect(snapshotTree(target)).toEqual(before);
  });

  it("asks before deleting, cancels safely, then syncs after confirmation", async () => {
    const source = named(buildTree({ "keep.txt": "k", "docs/a.md": "a" }), "src");
    const target = named(buildTree({ "keep.txt": "k", "old.txt": "old" }), "dst");
    const onActiveChange = await chooseLocalFolders(source, target);
    fireEvent.click(screen.getByLabelText(/Delete files on the target/));

    fireEvent.click(screen.getByRole("button", { name: "Sync now" }));
    const dialog = await screen.findByRole("alertdialog");
    expect(dialog).toHaveTextContent("Delete 1 item from the target folder?");
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await screen.findByText("Cancelled. No file was changed.");
    expect(readTree(target)).toEqual({ "keep.txt": "k", "old.txt": "old" });

    fireEvent.click(screen.getByRole("button", { name: "Sync now" }));
    fireEvent.click(within(await screen.findByRole("alertdialog")).getByRole("button", { name: "Delete and sync" }));
    await screen.findByText(/2 added, 0 updated, 1 deleted, 1 unchanged/);
    expect(readTree(target)).toEqual({ "keep.txt": "k", docs: null, "docs/a.md": "a" });
    expect(onActiveChange.mock.calls.map(([active]) => active)).toEqual([true, false, true, false]);
  });

  it("refuses to sync a folder into itself", async () => {
    const folder = named(buildTree({ "a.txt": "a" }), "same");
    render(<SyncPanel settings={settings} supported pickDirectory={picker(folder, folder)} />);
    fireEvent.click(screen.getByRole("radio", { name: /Two local folders/ }));
    fireEvent.click(screen.getByRole("button", { name: "Choose source folder" }));
    fireEvent.click(screen.getByRole("button", { name: "Choose target folder" }));
    await waitFor(() => expect(screen.getAllByText("same")).toHaveLength(2));
    fireEvent.click(screen.getByRole("button", { name: "Sync now" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(/two different folders/);
  });

  it("reports that CLI sync is not available until the wire protocol lands", async () => {
    const source = named(buildTree({ "a.txt": "a" }), "src");
    render(<SyncPanel settings={settings} supported pickDirectory={picker(source)} />);
    fireEvent.click(screen.getByRole("radio", { name: /Source/ }));
    expect(screen.getByText(/still being built/)).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Choose source folder" }));
    await screen.findByText("src");
    fireEvent.click(screen.getByRole("button", { name: "Sync now" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(/folder-sync-v1/);
  });

  it("needs a code and a target folder to join", async () => {
    const target = named(buildTree({}), "dst");
    const connect = vi.fn(async () => {
      throw new Error("peer offline");
    });
    render(<SyncPanel settings={settings} supported pickDirectory={picker(target)} connect={connect} />);
    fireEvent.click(screen.getByRole("radio", { name: /Target/ }));
    expect(screen.queryByRole("group", { name: "Options" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Choose target folder" }));
    await screen.findByText("dst");
    const start = screen.getByRole("button", { name: "Sync now" });
    expect(start).toBeDisabled();
    fireEvent.change(screen.getByLabelText("Croc code from the source"), { target: { value: "  7-alpha-beta  " } });
    expect(start).toBeEnabled();
    fireEvent.click(start);
    expect(await screen.findByRole("alert")).toHaveTextContent("peer offline");
    expect(connect).toHaveBeenCalledWith(expect.objectContaining({ role: "join", code: "7-alpha-beta" }));
  });
});

describe("sync task routing", () => {
  it("opens from #sync and the task launcher", () => {
    expect(taskFromHash("#sync")).toBe("sync");
    const onSelect = vi.fn();
    render(<TaskLauncher storeEnabled={false} onSelect={onSelect} />);
    fireEvent.click(screen.getByRole("button", { name: "Sync folders" }));
    expect(onSelect).toHaveBeenCalledWith("sync");
  });
});
