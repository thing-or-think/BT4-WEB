import { describe, expect, it, vi } from "vitest";
import { bytesEqual, textEncoder } from "./bytes";
import {
  hashDestinationEntries,
  hashFileXXH64,
  scanDestination,
} from "./sync-fs";
import { MemorySyncHashCacheStore, SyncHashCache } from "./sync-hash-cache";

interface MemFileNode {
  kind: "file";
  content: Uint8Array<ArrayBuffer>;
  lastModified: number;
}

interface MemDirNode {
  kind: "directory";
  entries: Map<string, MemNode>;
}

type MemNode = MemFileNode | MemDirNode;

class MemFileHandle {
  readonly kind = "file" as const;

  constructor(
    readonly name: string,
    readonly node: MemFileNode,
  ) {}

  async isSameEntry(other: unknown): Promise<boolean> {
    return other instanceof MemFileHandle && other.node === this.node;
  }

  async getFile(): Promise<File> {
    const bytes = this.node.content.slice();
    const file = new File([bytes], this.name, {
      lastModified: this.node.lastModified,
    });
    Object.defineProperty(file, "arrayBuffer", {
      value: async () => bytes.buffer.slice(0),
    });
    Object.defineProperty(file, "stream", {
      value: () =>
        new ReadableStream<Uint8Array<ArrayBuffer>>({
          start(controller) {
            if (bytes.byteLength > 0) controller.enqueue(bytes.slice());
            controller.close();
          },
        }),
    });
    return file;
  }
}

class MemDirHandle {
  readonly kind = "directory" as const;

  constructor(
    readonly name: string,
    readonly node: MemDirNode = { kind: "directory", entries: new Map() },
  ) {}

  async isSameEntry(other: unknown): Promise<boolean> {
    return other instanceof MemDirHandle && other.node === this.node;
  }

  async removeEntry(name: string): Promise<void> {
    if (!this.node.entries.has(name)) {
      throw new DOMException("Not found", "NotFoundError");
    }
    this.node.entries.delete(name);
  }

  async *entries(): AsyncIterableIterator<[string, FileSystemHandle]> {
    for (const [name, child] of this.node.entries.entries()) {
      if (child.kind === "directory") {
        yield [name, new MemDirHandle(name, child) as unknown as FileSystemHandle];
      } else {
        yield [name, new MemFileHandle(name, child) as unknown as FileSystemHandle];
      }
    }
  }

  setFile(path: string, content: string, lastModified = 1_700_000_000_000): void {
    const parts = path.split("/").filter(Boolean);
    const name = parts.pop()!;
    let cur = this.node;
    for (const seg of parts) {
      let next = cur.entries.get(seg);
      if (!next) {
        next = { kind: "directory", entries: new Map() };
        cur.entries.set(seg, next);
      }
      if (next.kind !== "directory") throw new Error(`${seg} is not a dir`);
      cur = next;
    }
    const bytes = textEncoder.encode(content).slice();
    cur.entries.set(name, { kind: "file", content: bytes, lastModified });
  }

  asHandle(): FileSystemDirectoryHandle {
    return this as unknown as FileSystemDirectoryHandle;
  }
}

describe("SyncHashCache", () => {
  it("performs zero hash computations on a second scan of an unchanged tree", async () => {
    const root = new MemDirHandle("project");
    root.setFile("a.txt", "alpha", 1_700_000_001_000);
    root.setFile("sub/b.txt", "bravo-content", 1_700_000_002_000);

    const store = new MemorySyncHashCacheStore();
    const cache = new SyncHashCache(store);
    const computeSpy = vi.fn(hashFileXXH64);

    // Round 1: cache is cold, hashes both files locally.
    const scan1 = await scanDestination(root.asHandle());
    await hashDestinationEntries(root.asHandle(), scan1, ["a.txt", "sub/b.txt"], {
      hashCache: cache,
      computeHash: computeSpy,
    });
    expect(computeSpy).toHaveBeenCalledTimes(2);
    const hashA1 = scan1.manifest.entries.find((e) => e.path === "a.txt")!.hash!;
    const hashB1 = scan1.manifest.entries.find((e) => e.path === "sub/b.txt")!.hash!;

    // Round 2: new handle wrapper pointing to the same directory (matched via isSameEntry).
    const sameRootNewHandle = new MemDirHandle("project", root.node);
    computeSpy.mockClear();

    const scan2 = await scanDestination(sameRootNewHandle.asHandle());
    await hashDestinationEntries(
      sameRootNewHandle.asHandle(),
      scan2,
      ["a.txt", "sub/b.txt"],
      {
        hashCache: cache,
        computeHash: computeSpy,
      },
    );
    expect(computeSpy).toHaveBeenCalledTimes(0);

    const hashA2 = scan2.manifest.entries.find((e) => e.path === "a.txt")!.hash!;
    const hashB2 = scan2.manifest.entries.find((e) => e.path === "sub/b.txt")!.hash!;
    expect(bytesEqual(hashA1, hashA2)).toBe(true);
    expect(bytesEqual(hashB1, hashB2)).toBe(true);
  });

  it("recomputes when size or lastModified changes and prunes deleted paths", async () => {
    const root = new MemDirHandle("dirA");
    root.setFile("keep.txt", "same", 1_700_000_000_000);
    root.setFile("edit.txt", "v1", 1_700_000_000_000);
    root.setFile("gone.txt", "bye", 1_700_000_000_000);

    const otherRoot = new MemDirHandle("dirB");
    otherRoot.setFile("keep.txt", "different-dir", 1_700_000_000_000);

    const store = new MemorySyncHashCacheStore();
    const cache = new SyncHashCache(store);
    const computeSpy = vi.fn(hashFileXXH64);

    const scan1 = await scanDestination(root.asHandle());
    await hashDestinationEntries(
      root.asHandle(),
      scan1,
      ["keep.txt", "edit.txt", "gone.txt"],
      { hashCache: cache, computeHash: computeSpy },
    );
    const otherScan = await scanDestination(otherRoot.asHandle());
    await hashDestinationEntries(otherRoot.asHandle(), otherScan, ["keep.txt"], {
      hashCache: cache,
      computeHash: computeSpy,
    });
    expect(computeSpy).toHaveBeenCalledTimes(4);

    const dirIdA = await cache.resolveDirId(root.asHandle());
    const dirIdB = await cache.resolveDirId(otherRoot.asHandle());
    expect(dirIdA).not.toBe(dirIdB);
    expect(store.sizeForDir(dirIdA)).toBe(3);

    // Edit edit.txt and remove gone.txt, then prune.
    root.setFile("edit.txt", "v2-longer", 1_700_000_500_000);
    await root.removeEntry("gone.txt");

    const scan2 = await scanDestination(root.asHandle());
    await cache.pruneForRoot(
      root.asHandle(),
      new Set(scan2.manifest.entries.map((e) => e.path)),
    );
    expect(store.sizeForDir(dirIdA)).toBe(2);
    expect(store.sizeForDir(dirIdB)).toBe(1);

    computeSpy.mockClear();
    await hashDestinationEntries(root.asHandle(), scan2, ["keep.txt", "edit.txt"], {
      hashCache: cache,
      computeHash: computeSpy,
    });
    // Only edit.txt recomputed; keep.txt came from cache.
    expect(computeSpy).toHaveBeenCalledTimes(1);
  });
});
