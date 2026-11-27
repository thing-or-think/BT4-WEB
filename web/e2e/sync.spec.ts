import { expect, test, type Page } from "@playwright/test";

// Folder sync E2E. Real FileSystemDirectoryHandle objects come from the
// Origin Private File System (OPFS), and window.showDirectoryPicker is
// replaced by a queue of OPFS folders so no native dialog opens.

type Tree = Record<string, string | null>;

const folderTests = "folder sync needs the File System Access API (Chromium only)";

async function installPicker(page: Page) {
  await page.addInitScript(() => {
    const queue: string[] = [];
    Object.assign(window, { __syncPickQueue: queue });
    Object.defineProperty(window, "showDirectoryPicker", {
      configurable: true,
      value: async () => {
        const name = queue.shift();
        if (!name) throw new DOMException("No folder queued", "AbortError");
        const root = await navigator.storage.getDirectory();
        return root.getDirectoryHandle(name, { create: true });
      },
    });
  });
}

async function queueFolders(page: Page, ...names: string[]) {
  await page.evaluate((list) => {
    (window as unknown as { __syncPickQueue: string[] }).__syncPickQueue.push(...list);
  }, names);
}

/** Replaces an OPFS folder with the given tree (null = folder). */
async function writeTree(page: Page, folder: string, tree: Tree) {
  await page.evaluate(
    async ({ folder, tree }) => {
      const root = await navigator.storage.getDirectory();
      await root.removeEntry(folder, { recursive: true }).catch(() => undefined);
      const base = await root.getDirectoryHandle(folder, { create: true });
      for (const [path, content] of Object.entries(tree)) {
        const segments = path.split("/");
        const last = content === null ? segments.length : segments.length - 1;
        let directory = base;
        for (const segment of segments.slice(0, last))
          directory = await directory.getDirectoryHandle(segment, { create: true });
        if (content === null) continue;
        const file = await directory.getFileHandle(segments[segments.length - 1], { create: true });
        const writable = await file.createWritable();
        await writable.write(content);
        await writable.close();
      }
    },
    { folder, tree },
  );
}

/** Reads an OPFS folder back as {path: content | null}. */
async function readTree(page: Page, folder: string): Promise<Tree> {
  return page.evaluate(async (folder) => {
    type Directory = FileSystemDirectoryHandle & {
      values(): AsyncIterable<FileSystemFileHandle | FileSystemDirectoryHandle>;
    };
    const root = await navigator.storage.getDirectory();
    const out: Record<string, string | null> = {};
    async function walk(directory: Directory, prefix: string) {
      for await (const child of directory.values()) {
        const path = prefix ? `${prefix}/${child.name}` : child.name;
        if (child.kind === "directory") {
          out[path] = null;
          await walk(child as Directory, path);
        } else {
          out[path] = await (await (child as FileSystemFileHandle).getFile()).text();
        }
      }
    }
    await walk((await root.getDirectoryHandle(folder, { create: true })) as Directory, "");
    return Object.fromEntries(Object.entries(out).sort(([a], [b]) => (a < b ? -1 : 1)));
  }, folder);
}

async function lastModified(page: Page, folder: string) {
  return page.evaluate(async (folder) => {
    const root = await navigator.storage.getDirectory();
    const base = await root.getDirectoryHandle(folder);
    const times: Record<string, number> = {};
    for await (const child of (base as unknown as { values(): AsyncIterable<FileSystemHandle> }).values()) {
      if (child.kind === "file") times[child.name] = (await (child as FileSystemFileHandle).getFile()).lastModified;
    }
    return times;
  }, folder);
}

async function openSyncPanel(page: Page) {
  await page.goto("/");
  await page.getByRole("button", { name: "Sync folders", exact: true }).click();
  await expect(page).toHaveURL(/#sync$/);
  const panel = page.getByRole("region", { name: "Folder sync" });
  await expect(panel.getByRole("heading", { name: "Sync folders" })).toBeVisible();
  return panel;
}

async function chooseLocal(page: Page, source: string, target: string) {
  const panel = await openSyncPanel(page);
  await panel.getByRole("radio", { name: /Two local folders/ }).check();
  await queueFolders(page, source, target);
  await panel.getByRole("button", { name: "Choose source folder" }).click();
  await expect(panel.getByLabel("Selected source folder")).toContainText(source);
  await panel.getByRole("button", { name: "Choose target folder" }).click();
  await expect(panel.getByLabel("Selected target folder")).toContainText(target);
  return panel;
}

test("Sync folders opens from the task switcher and #sync", async ({ page }) => {
  await openSyncPanel(page);
  await page.goto("/#sync");
  await expect(page.getByRole("heading", { name: "Sync folders, secured end-to-end." })).toBeVisible();
});

test("explains the fallback when the browser cannot open folders", async ({ page }) => {
  await page.addInitScript(() => {
    // The picker lives on Window.prototype in Chromium; shadow it.
    Object.defineProperty(window, "showDirectoryPicker", { configurable: true, value: undefined });
  });
  const panel = await openSyncPanel(page);
  await expect(panel.getByRole("alert")).toContainText("only Chrome and Edge on desktop");
  await expect(panel.getByRole("button", { name: "Choose source folder" })).toBeDisabled();
  expect(await page.evaluate(() => document.documentElement.scrollWidth - innerWidth)).toBeLessThanOrEqual(0);
});

test.describe("local folder sync", () => {
  test.beforeEach(async ({ page, browserName }) => {
    test.skip(browserName !== "chromium", folderTests);
    await installPicker(page);
    await page.goto("/");
  });

  test("dry run previews Add / Update / Delete without changing the target", async ({ page }) => {
    await writeTree(page, "dry-src", { "new.txt": "new", "grown.txt": "v2-longer", "same.txt": "same" });
    await writeTree(page, "dry-dst", { "grown.txt": "v1", "same.txt": "same", "extra.txt": "x" });
    const before = { tree: await readTree(page, "dry-dst"), times: await lastModified(page, "dry-dst") };

    const panel = await chooseLocal(page, "dry-src", "dry-dst");
    await panel.getByLabel(/Delete files on the target/).check();
    await panel.getByLabel(/Dry run/).check();
    await panel.getByRole("button", { name: "Preview changes" }).click();

    const plan = panel.getByRole("region", { name: "Sync plan" });
    await expect(plan.getByText("Nothing will be changed")).toBeVisible();
    const rows = plan.getByRole("list", { name: "Planned changes" }).getByRole("listitem");
    await expect(rows).toHaveText([/Add\s*new\.txt/, /Update\s*grown\.txt/, /Delete\s*extra\.txt/]);
    await expect(panel.getByText("Dry run finished. No file was changed.")).toBeVisible();
    expect(await readTree(page, "dry-dst")).toEqual(before.tree);
    expect(await lastModified(page, "dry-dst")).toEqual(before.times);
  });

  test("mirrors a nested tree, confirms deletes and skips everything on the second run", async ({ page }) => {
    const source: Tree = {
      "top.txt": "top",
      "docs/readme.md": "# hi",
      "docs/empty.txt": "",
      "empty-folder": null,
      "a/b/c/d/deep.txt": "deep",
    };
    await writeTree(page, "mirror-src", source);
    await writeTree(page, "mirror-dst", { "stale.txt": "old", "top.txt": "outdated" });

    const panel = await chooseLocal(page, "mirror-src", "mirror-dst");
    await panel.getByLabel(/Delete files on the target/).check();
    await panel.getByRole("button", { name: "Sync now" }).click();
    const dialog = panel.getByRole("alertdialog");
    await expect(dialog).toContainText("Delete 1 item from the target folder?");
    await dialog.getByRole("button", { name: "Delete and sync" }).click();
    await expect(panel.getByText(/9 added, 1 updated, 1 deleted/)).toBeVisible();
    expect(await readTree(page, "mirror-dst")).toEqual(await readTree(page, "mirror-src"));

    await panel.getByRole("button", { name: "Sync now" }).click();
    await expect(panel.getByText("Nothing to change: the target already matches the source.")).toBeVisible();
    await expect(panel.getByText(/0 added, 0 updated, 0 deleted, 10 unchanged/)).toBeVisible();
    await expect(panel.getByRole("alertdialog")).toHaveCount(0);
  });

  test("watch mode syncs a change in under two seconds", async ({ page }) => {
    await writeTree(page, "watch-src", { "watched.txt": "v1" });
    await writeTree(page, "watch-dst", {});
    const panel = await chooseLocal(page, "watch-src", "watch-dst");
    await panel.getByLabel(/Keep watching/).check();
    await panel.getByRole("button", { name: "Sync and watch" }).click();
    await expect(panel.getByText("Watching for changes…")).toBeVisible();
    expect(await readTree(page, "watch-dst")).toEqual({ "watched.txt": "v1" });

    const started = Date.now();
    await page.evaluate(async () => {
      const root = await navigator.storage.getDirectory();
      const folder = await root.getDirectoryHandle("watch-src");
      const file = await folder.getFileHandle("watched.txt");
      const writable = await file.createWritable();
      await writable.write("v2 changed");
      await writable.close();
    });
    await expect
      .poll(() => readTree(page, "watch-dst"), { timeout: 2_000, intervals: [50] })
      .toEqual({ "watched.txt": "v2 changed" });
    expect(Date.now() - started).toBeLessThan(2_000);
    await expect(panel.getByRole("region", { name: "Watch log" })).toContainText("watched.txt");

    await panel.getByRole("button", { name: "Stop" }).click();
    await expect(panel.getByText("Stopped.")).toBeVisible();
    await expect(page.getByRole("button", { name: "Send files", exact: true })).toBeEnabled();
  });

  test("never syncs a folder into itself", async ({ page }) => {
    await writeTree(page, "self", { "a.txt": "a" });
    const panel = await chooseLocal(page, "self", "self");
    await panel.getByRole("button", { name: "Sync now" }).click();
    await expect(panel.getByRole("alert")).toContainText("two different folders");
    expect(await readTree(page, "self")).toEqual({ "a.txt": "a" });
  });
});

// Cross Web <-> CLI sync needs the folder-sync-v1 wire protocol (Member 2)
// and connectSyncPeer() in web/src/protocol/sync.ts. Enable when both land.
test.fixme("CLI croc sync → Web target folder", async () => {});
test.fixme("Web source folder → CLI croc sync", async () => {});
