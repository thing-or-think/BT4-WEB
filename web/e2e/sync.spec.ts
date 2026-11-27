import { spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { promises as fs } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, type Page, type TestInfo } from "@playwright/test";

const webDirectory = dirname(dirname(fileURLToPath(import.meta.url)));
const crocBinary = join(
  webDirectory,
  ".e2e",
  process.env.CROC_E2E_BINARY_NAME ?? "croc",
);
const relayPorts = process.env.CROC_E2E_RELAY_PORTS?.split(",") ?? [];
const relayAddress =
  process.env.CROC_E2E_RELAY_ADDRESS ?? `127.0.0.1:${relayPorts[0]}`;
const relayPassword = "pass123";
const syncTimeout = 60_000;

const TEST_OPFS_PREFIX = "croc-sync-e2e-";

let codeSequence = 0;

function uniqueSyncCode(label: string): string {
  codeSequence += 1;
  return `sync-${label}-${process.pid}-${codeSequence}-${randomBytes(4).toString("hex")}`;
}

type TreeSnapshot = {
  dirs: string[];
  files: Record<string, { size: number; sha256: string }>;
};

type RunningSyncCLI = {
  child: ReturnType<typeof spawn>;
  done: Promise<void>;
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  output(): string;
  stop(): void;
};

type OpfsDirectoryHandle = FileSystemDirectoryHandle & {
  entries(): AsyncIterableIterator<[string, FileSystemHandle]>;
};

async function createSyncSourceFixture(rootDir: string): Promise<void> {
  await fs.mkdir(join(rootDir, `${TEST_OPFS_PREFIX}nested`, "level1", "level2"), {
    recursive: true,
  });
  await fs.mkdir(join(rootDir, `${TEST_OPFS_PREFIX}empty`), {
    recursive: true,
  });
  await fs.mkdir(join(rootDir, `${TEST_OPFS_PREFIX}small`), {
    recursive: true,
  });

  await fs.writeFile(
    join(rootDir, `${TEST_OPFS_PREFIX}nested`, "readme.txt"),
    Buffer.from("initial nested readme content\n"),
  );
  await fs.writeFile(
    join(rootDir, `${TEST_OPFS_PREFIX}nested`, "level1", "level2", "deep.txt"),
    Buffer.from("deeply nested file payload\n"),
  );
  await fs.writeFile(
    join(rootDir, `${TEST_OPFS_PREFIX}large.bin`),
    randomBytes(2 * 1024 * 1024 + 137),
  );

  for (let i = 0; i < 20; i += 1) {
    const name = `small-${String(i).padStart(2, "0")}.txt`;
    await fs.writeFile(
      join(rootDir, `${TEST_OPFS_PREFIX}small`, name),
      Buffer.from(`small fixture file #${i} payload\n`),
    );
  }
}

async function snapshotDiskTree(rootDir: string): Promise<TreeSnapshot> {
  const dirs: string[] = [];
  const files: Record<string, { size: number; sha256: string }> = {};

  async function walk(currentDir: string, relPrefix: string): Promise<void> {
    const entries = await fs.readdir(currentDir, { withFileTypes: true });
    entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    for (const entry of entries) {
      const fullPath = join(currentDir, entry.name);
      const wirePath = relPrefix ? `${relPrefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        dirs.push(wirePath);
        await walk(fullPath, wirePath);
      } else if (entry.isFile()) {
        const data = await fs.readFile(fullPath);
        files[wirePath] = {
          size: data.byteLength,
          sha256: createHash("sha256").update(data).digest("hex"),
        };
      }
    }
  }

  await walk(rootDir, "");
  dirs.sort();
  return { dirs, files };
}

async function clearTestOpfs(page: Page): Promise<void> {
  await page.evaluate(async (prefix: string) => {
    if (typeof navigator.storage?.getDirectory !== "function") return;
    const root = (await navigator.storage.getDirectory()) as OpfsDirectoryHandle;
    const toRemove: string[] = [];
    for await (const [name] of root.entries()) {
      if (name.startsWith(prefix)) {
        toRemove.push(name);
      }
    }
    for (const name of toRemove) {
      await root.removeEntry(name, { recursive: true });
    }
  }, TEST_OPFS_PREFIX);
}

async function snapshotOpfsTree(
  page: Page,
  subdirName?: string,
): Promise<TreeSnapshot> {
  return page.evaluate(
    async ({
      prefix,
      subdir,
    }: {
      prefix: string;
      subdir: string | undefined;
    }): Promise<TreeSnapshot> => {
      const opfsRoot =
        (await navigator.storage.getDirectory()) as OpfsDirectoryHandle;
      const startDir = subdir
        ? ((await opfsRoot.getDirectoryHandle(subdir, {
            create: false,
          })) as OpfsDirectoryHandle)
        : opfsRoot;

      const dirs: string[] = [];
      const files: Record<string, { size: number; sha256: string }> = {};

      async function walk(
        dir: OpfsDirectoryHandle,
        relPrefix: string,
        isTopLevelOpfs: boolean,
      ): Promise<void> {
        const entries: Array<[string, FileSystemHandle]> = [];
        for await (const entry of dir.entries()) {
          entries.push(entry);
        }
        entries.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));

        for (const [name, handle] of entries) {
          if (isTopLevelOpfs && !name.startsWith(prefix)) continue;
          const wirePath = relPrefix ? `${relPrefix}/${name}` : name;
          if (handle.kind === "directory") {
            dirs.push(wirePath);
            await walk(handle as OpfsDirectoryHandle, wirePath, false);
          } else if (handle.kind === "file") {
            const file = await (handle as FileSystemFileHandle).getFile();
            const buffer = await file.arrayBuffer();
            const digest = await crypto.subtle.digest("SHA-256", buffer);
            const hex = Array.from(new Uint8Array(digest), (byte) =>
              byte.toString(16).padStart(2, "0"),
            ).join("");
            files[wirePath] = {
              size: buffer.byteLength,
              sha256: hex,
            };
          }
        }
      }

      await walk(startDir, "", !subdir);
      dirs.sort();
      return { dirs, files };
    },
    { prefix: TEST_OPFS_PREFIX, subdir: subdirName },
  );
}

async function spawnSyncSource(
  testInfo: TestInfo,
  sourceDir: string,
  code: string,
  extraSyncArgs: string[] = [],
): Promise<RunningSyncCLI> {
  const configDir = testInfo.outputPath("croc-config");
  await fs.mkdir(configDir, { recursive: true });
  // Passing --code on UNIX requires classic mode in this isolated test config dir.
  await fs.writeFile(join(configDir, "classic_enabled"), "enabled");

  const env: NodeJS.ProcessEnv = {
    ...process.env,
    CROC_CONFIG_DIR: configDir,
  };
  delete env.CROC_SECRET;

  const args = [
    "--relay",
    relayAddress,
    "--pass",
    relayPassword,
    "--no-update-check",
    "--disable-clipboard",
    "sync",
    "--code",
    code,
    "--no-local",
    "--transport",
    "relay",
    ...extraSyncArgs,
    sourceDir,
  ];

  let output = "";
  const child = spawn(crocBinary, args, {
    cwd: webDirectory,
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout?.on("data", (chunk: Buffer) => {
    output += chunk.toString();
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    output += chunk.toString();
  });

  const exited = new Promise<{
    code: number | null;
    signal: NodeJS.Signals | null;
  }>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (exitCode, signal) => {
      resolve({ code: exitCode, signal });
    });
  });

  const done = exited.then(({ code: exitCode, signal }) => {
    if (exitCode !== 0) {
      throw new Error(
        `croc sync exited with ${signal ?? exitCode ?? "unknown"}\n${output.trim()}`,
      );
    }
  });
  // Prevent unhandledRejection when a test expects a non-zero exit via `exited`.
  done.catch(() => undefined);

  return {
    child,
    done,
    exited,
    output() {
      return output;
    },
    stop() {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill("SIGTERM");
      }
    },
  };
}

test.beforeEach(async ({ page, browserName }) => {
  if (browserName !== "chromium") return;
  await page.goto("/");
  await clearTestOpfs(page);
});

test("web sync CLI → Web full round into OPFS", async ({
  page,
  browserName,
}, testInfo) => {
  test.skip(browserName !== "chromium", "Web Sync OPFS tests run on Chromium");
  test.setTimeout(60_000);

  const sourceDir = testInfo.outputPath("sync-src-full");
  await createSyncSourceFixture(sourceDir);
  const code = uniqueSyncCode("full");
  const cli = await spawnSyncSource(testInfo, sourceDir, code);

  try {
    await page.goto(`/?sync=${encodeURIComponent(code)}&opfs=1#sync`);
    const panel = page.locator(".sync-panel");
    await expect(panel).toBeVisible();
    await panel
      .getByRole("button", { name: "Use browser test storage (OPFS)" })
      .click();
    await panel.getByRole("button", { name: "Start sync" }).click();

    await expect(panel.locator(".sync-result-summary")).toContainText(
      "Synced:",
      { timeout: syncTimeout },
    );
    await cli.done;

    const [expected, actual] = await Promise.all([
      snapshotDiskTree(sourceDir),
      snapshotOpfsTree(page),
    ]);
    expect(actual.dirs).toEqual(expected.dirs);
    expect(actual.files).toEqual(expected.files);
  } finally {
    cli.stop();
    await cli.done.catch(() => undefined);
  }
});

test("web sync second round applies edit, addition, and confirmed deletions", async ({
  page,
  browserName,
}, testInfo) => {
  test.skip(browserName !== "chromium", "Web Sync OPFS tests run on Chromium");
  test.setTimeout(90_000);

  const sourceDir = testInfo.outputPath("sync-src-round2");
  await createSyncSourceFixture(sourceDir);

  const code1 = uniqueSyncCode("r1");
  const cli1 = await spawnSyncSource(testInfo, sourceDir, code1);
  try {
    await page.goto(`/?sync=${encodeURIComponent(code1)}&opfs=1#sync`);
    const panel = page.locator(".sync-panel");
    await panel
      .getByRole("button", { name: "Use browser test storage (OPFS)" })
      .click();
    await panel.getByRole("button", { name: "Start sync" }).click();
    await expect(panel.locator(".sync-result-summary")).toContainText(
      "Synced:",
      { timeout: syncTimeout },
    );
    await cli1.done;
  } finally {
    cli1.stop();
    await cli1.done.catch(() => undefined);
  }

  // Mutate source: 1 edited file (300 B), 1 added file (200 B), 3 removed files.
  const updatedBytes = Buffer.alloc(300, "U");
  const addedBytes = Buffer.alloc(200, "A");
  await fs.writeFile(
    join(sourceDir, `${TEST_OPFS_PREFIX}nested`, "readme.txt"),
    updatedBytes,
  );
  await fs.writeFile(
    join(sourceDir, `${TEST_OPFS_PREFIX}small`, "added-file.txt"),
    addedBytes,
  );
  await Promise.all([
    fs.rm(join(sourceDir, `${TEST_OPFS_PREFIX}small`, "small-17.txt")),
    fs.rm(join(sourceDir, `${TEST_OPFS_PREFIX}small`, "small-18.txt")),
    fs.rm(join(sourceDir, `${TEST_OPFS_PREFIX}small`, "small-19.txt")),
  ]);

  const code2 = uniqueSyncCode("r2");
  const cli2 = await spawnSyncSource(testInfo, sourceDir, code2);
  try {
    const panel = page.locator(".sync-panel");
    await panel.getByLabel("Sync code").fill(code2);
    await panel.getByLabel("Delete files that are not in the source").check();
    await panel.getByRole("button", { name: "Start sync" }).click();

    await expect(panel.locator(".sync-plan-counts")).toContainText(
      "1 to add, 1 to update, 3 to delete",
      { timeout: syncTimeout },
    );

    const confirmButton = panel.getByRole("button", {
      name: "Apply, including 3 deletions",
    });
    await expect(confirmButton).toBeEnabled();
    await confirmButton.click();

    const summary = panel.locator(".sync-result-summary");
    await expect(summary).toContainText("Synced:", { timeout: syncTimeout });
    await expect(summary).toContainText("1 added, 1 updated, 3 deleted");
    await expect(summary).toContainText("500 B transferred");
    await cli2.done;

    const [expected, actual] = await Promise.all([
      snapshotDiskTree(sourceDir),
      snapshotOpfsTree(page),
    ]);
    expect(actual.dirs).toEqual(expected.dirs);
    expect(actual.files).toEqual(expected.files);
  } finally {
    cli2.stop();
    await cli2.done.catch(() => undefined);
  }
});

test("web sync dry run with delete leaves OPFS byte-identical", async ({
  page,
  browserName,
}, testInfo) => {
  test.skip(browserName !== "chromium", "Web Sync OPFS tests run on Chromium");
  test.setTimeout(90_000);

  const sourceDir = testInfo.outputPath("sync-src-dryrun");
  await createSyncSourceFixture(sourceDir);

  const code1 = uniqueSyncCode("dry1");
  const cli1 = await spawnSyncSource(testInfo, sourceDir, code1);
  try {
    await page.goto(`/?sync=${encodeURIComponent(code1)}&opfs=1#sync`);
    const panel = page.locator(".sync-panel");
    await panel
      .getByRole("button", { name: "Use browser test storage (OPFS)" })
      .click();
    await panel.getByRole("button", { name: "Start sync" }).click();
    await expect(panel.locator(".sync-result-summary")).toContainText(
      "Synced:",
      { timeout: syncTimeout },
    );
    await cli1.done;
  } finally {
    cli1.stop();
    await cli1.done.catch(() => undefined);
  }

  const beforeDryRun = await snapshotOpfsTree(page);

  await fs.writeFile(
    join(sourceDir, `${TEST_OPFS_PREFIX}nested`, "readme.txt"),
    Buffer.from("edited during dry run test\n"),
  );
  await fs.writeFile(
    join(sourceDir, `${TEST_OPFS_PREFIX}small`, "dry-run-added.txt"),
    Buffer.from("new file for dry run\n"),
  );
  await fs.rm(join(sourceDir, `${TEST_OPFS_PREFIX}small`, "small-00.txt"));

  const code2 = uniqueSyncCode("dry2");
  const cli2 = await spawnSyncSource(testInfo, sourceDir, code2);
  try {
    const panel = page.locator(".sync-panel");
    await panel.getByLabel("Sync code").fill(code2);
    await panel.getByLabel("Delete files that are not in the source").check();
    await panel.getByLabel("Preview only (dry run)").check();
    await panel.getByRole("button", { name: "Preview sync" }).click();

    await expect(panel.locator(".sync-result-summary")).toContainText(
      "Dry run complete — nothing was changed.",
      { timeout: syncTimeout },
    );
    await expect(panel.locator(".sync-plan-counts")).toContainText(
      "1 to add, 1 to update, 1 to delete",
    );
    await cli2.done;

    const afterDryRun = await snapshotOpfsTree(page);
    expect(afterDryRun).toEqual(beforeDryRun);
  } finally {
    cli2.stop();
    await cli2.done.catch(() => undefined);
  }
});

test("web sync idle round reports Already in sync and 0 B", async ({
  page,
  browserName,
}, testInfo) => {
  test.skip(browserName !== "chromium", "Web Sync OPFS tests run on Chromium");
  test.setTimeout(90_000);

  const sourceDir = testInfo.outputPath("sync-src-idle");
  await createSyncSourceFixture(sourceDir);

  const code1 = uniqueSyncCode("idle1");
  const cli1 = await spawnSyncSource(testInfo, sourceDir, code1);
  try {
    await page.goto(`/?sync=${encodeURIComponent(code1)}&opfs=1#sync`);
    const panel = page.locator(".sync-panel");
    await panel
      .getByRole("button", { name: "Use browser test storage (OPFS)" })
      .click();
    await panel.getByRole("button", { name: "Start sync" }).click();
    await expect(panel.locator(".sync-result-summary")).toContainText(
      "Synced:",
      { timeout: syncTimeout },
    );
    await cli1.done;
  } finally {
    cli1.stop();
    await cli1.done.catch(() => undefined);
  }

  const code2 = uniqueSyncCode("idle2");
  const cli2 = await spawnSyncSource(testInfo, sourceDir, code2);
  try {
    const panel = page.locator(".sync-panel");
    await panel.getByLabel("Sync code").fill(code2);
    await panel.getByRole("button", { name: "Start sync" }).click();

    await expect(panel.locator(".sync-plan-empty")).toHaveText(
      "Already in sync.",
      { timeout: syncTimeout },
    );
    const summary = panel.locator(".sync-result-summary");
    await expect(summary).toContainText("Synced:", { timeout: syncTimeout });
    await expect(summary).toContainText("0 B transferred");
    await cli2.done;
  } finally {
    cli2.stop();
    await cli2.done.catch(() => undefined);
  }
});

test("web sync refuses watching source and exits CLI promptly", async ({
  page,
  browserName,
}, testInfo) => {
  test.skip(browserName !== "chromium", "Web Sync OPFS tests run on Chromium");
  test.setTimeout(60_000);

  const sourceDir = testInfo.outputPath("sync-src-watch");
  await fs.mkdir(sourceDir, { recursive: true });
  await fs.writeFile(
    join(sourceDir, `${TEST_OPFS_PREFIX}watched.txt`),
    Buffer.from("watched content\n"),
  );

  const code = uniqueSyncCode("watch");
  const cli = await spawnSyncSource(testInfo, sourceDir, code, ["--watch"]);

  try {
    await page.goto(`/?sync=${encodeURIComponent(code)}&opfs=1#sync`);
    const panel = page.locator(".sync-panel");
    await panel
      .getByRole("button", { name: "Use browser test storage (OPFS)" })
      .click();
    await panel.getByRole("button", { name: "Start sync" }).click();

    await expect(panel.locator(".sync-error-box")).toContainText(
      "Watching (--watch) is not supported by web sync yet",
      { timeout: syncTimeout },
    );

    let timer: ReturnType<typeof setTimeout> | undefined;
    const exitResult = await Promise.race([
      cli.exited,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(
            new Error(
              `croc sync --watch did not exit within 15s\n${cli.output()}`,
            ),
          );
        }, 15_000);
      }),
    ]).finally(() => {
      if (timer) clearTimeout(timer);
    });

    expect(exitResult.code, cli.output()).not.toBe(0);
    expect(exitResult.code, cli.output()).not.toBeNull();
  } finally {
    cli.stop();
    await cli.exited.catch(() => undefined);
  }
});

test("web sync folder picker completes a round into a picked directory", async ({
  page,
  browserName,
}, testInfo) => {
  test.skip(browserName !== "chromium", "Web Sync picker test runs on Chromium");
  test.setTimeout(60_000);

  const pickerSubdir = `${TEST_OPFS_PREFIX}picker-root`;
  await page.addInitScript((subdirName: string) => {
    Object.defineProperty(window, "showDirectoryPicker", {
      configurable: true,
      value: async () => {
        const root = await navigator.storage.getDirectory();
        return root.getDirectoryHandle(subdirName, { create: true });
      },
    });
  }, pickerSubdir);

  const sourceDir = testInfo.outputPath("sync-src-picker");
  await fs.mkdir(join(sourceDir, "docs", "nested"), { recursive: true });
  await fs.mkdir(join(sourceDir, "empty-subdir"), { recursive: true });
  await fs.writeFile(
    join(sourceDir, "docs", "nested", "note.txt"),
    Buffer.from("synced via mocked folder picker\n"),
  );
  await fs.writeFile(
    join(sourceDir, "payload.bin"),
    randomBytes(128 * 1024 + 53),
  );

  const code = uniqueSyncCode("picker");
  const cli = await spawnSyncSource(testInfo, sourceDir, code);

  try {
    await page.goto(`/?sync=${encodeURIComponent(code)}#sync`);
    const panel = page.locator(".sync-panel");
    await expect(panel).toBeVisible();
    await panel.getByRole("button", { name: /Choose folder/i }).click();
    await expect(panel.locator(".sync-folder-name")).toHaveText(pickerSubdir);
    await panel.getByRole("button", { name: "Start sync" }).click();

    await expect(panel.locator(".sync-result-summary")).toContainText(
      "Synced:",
      { timeout: syncTimeout },
    );
    await cli.done;

    const [expected, actual] = await Promise.all([
      snapshotDiskTree(sourceDir),
      snapshotOpfsTree(page, pickerSubdir),
    ]);
    expect(actual.dirs).toEqual(expected.dirs);
    expect(actual.files).toEqual(expected.files);
  } finally {
    cli.stop();
    await cli.done.catch(() => undefined);
  }
});

test("web sync shows Chrome or Edge requirement in unsupported browsers", async ({
  page,
  browserName,
}) => {
  test.skip(
    browserName === "chromium",
    "Chromium supports the File System Access API",
  );
  test.setTimeout(60_000);

  await page.goto("/");
  await page.getByRole("button", { name: "Sync folder", exact: true }).click();
  const panel = page.locator(".sync-panel");
  await expect(panel).toBeVisible();
  await expect(panel).toContainText("needs Chrome or Edge on desktop");
  await expect(panel.getByRole("button", { name: /Start sync/i })).toHaveCount(
    0,
  );
});
