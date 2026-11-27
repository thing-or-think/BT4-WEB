// In-memory File System Access API double for folder sync tests.
import type {
  SyncDirectoryHandle,
  SyncFileHandle,
  SyncWritable,
} from "../protocol/sync";

let clock = Date.UTC(2026, 0, 1);
/** Deterministic, strictly increasing lastModified values (ms). */
export function tick(step = 1_000) {
  clock += step;
  return clock;
}

function domError(name: string, message = name) {
  return new DOMException(message, name);
}

function concat(chunks: Uint8Array[]) {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const bytes = new Uint8Array(total);
  let at = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, at);
    at += chunk.byteLength;
  }
  return bytes;
}

export class MemoryFile implements SyncFileHandle {
  readonly kind = "file" as const;
  bytes = new Uint8Array();
  lastModified = tick();
  /** Writables that were opened but neither closed nor aborted. */
  openWritables = 0;
  failWrites?: Error;

  constructor(readonly name: string) {}

  async getFile() {
    return new File([this.bytes.slice()], this.name, { lastModified: this.lastModified });
  }

  async createWritable(): Promise<SyncWritable> {
    const chunks: Uint8Array[] = [];
    let settled = false;
    this.openWritables += 1;
    const settle = () => {
      if (!settled) this.openWritables -= 1;
      settled = true;
    };
    return {
      write: async (data) => {
        if (this.failWrites) throw this.failWrites;
        chunks.push(data.slice());
      },
      close: async () => {
        settle();
        this.bytes = concat(chunks);
        this.lastModified = tick();
      },
      abort: async () => {
        settle();
      },
    };
  }
}

export class MemoryDirectory implements SyncDirectoryHandle {
  readonly kind = "directory" as const;
  readonly children = new Map<string, MemoryDirectory | MemoryFile>();

  constructor(readonly name = "") {}

  async *values() {
    for (const child of [...this.children.values()]) yield child;
  }

  async getDirectoryHandle(name: string, options: { create?: boolean } = {}) {
    const child = this.children.get(name);
    if (child?.kind === "directory") return child;
    if (child) throw domError("TypeMismatchError");
    if (!options.create) throw domError("NotFoundError");
    const directory = new MemoryDirectory(name);
    this.children.set(name, directory);
    return directory;
  }

  async getFileHandle(name: string, options: { create?: boolean } = {}) {
    const child = this.children.get(name);
    if (child?.kind === "file") return child;
    if (child) throw domError("TypeMismatchError");
    if (!options.create) throw domError("NotFoundError");
    const file = new MemoryFile(name);
    this.children.set(name, file);
    return file;
  }

  async removeEntry(name: string, options: { recursive?: boolean } = {}) {
    const child = this.children.get(name);
    if (!child) throw domError("NotFoundError");
    if (child.kind === "directory" && child.children.size && !options.recursive)
      throw domError("InvalidModificationError", "directory is not empty");
    this.children.delete(name);
  }
}

export type TreeSpec = Record<string, string | null>;

/** Creates files (string content) and folders (null) from POSIX paths. */
export function buildTree(spec: TreeSpec, root = new MemoryDirectory()) {
  for (const [path, content] of Object.entries(spec)) {
    const segments = path.split("/");
    let directory = root;
    const last = content === null ? segments.length : segments.length - 1;
    for (const segment of segments.slice(0, last)) {
      let next = directory.children.get(segment);
      if (!next) {
        next = new MemoryDirectory(segment);
        directory.children.set(segment, next);
      }
      directory = next as MemoryDirectory;
    }
    if (content !== null) {
      const file = new MemoryFile(segments[segments.length - 1]);
      file.bytes = new TextEncoder().encode(content);
      directory.children.set(file.name, file);
    }
  }
  return root;
}

/** Flattens a tree to {path: content | null}. */
export function readTree(root: MemoryDirectory, prefix = ""): TreeSpec {
  const tree: TreeSpec = {};
  for (const [name, child] of root.children) {
    const path = prefix ? `${prefix}/${name}` : name;
    if (child.kind === "directory") {
      tree[path] = null;
      Object.assign(tree, readTree(child, path));
    } else {
      tree[path] = new TextDecoder().decode(child.bytes);
    }
  }
  return tree;
}

/** Content plus mtimes, to prove that a dry run touched nothing. */
export function snapshotTree(root: MemoryDirectory, prefix = ""): Record<string, string> {
  const tree: Record<string, string> = {};
  for (const [name, child] of root.children) {
    const path = prefix ? `${prefix}/${name}` : name;
    if (child.kind === "directory") {
      tree[path] = "<dir>";
      Object.assign(tree, snapshotTree(child, path));
    } else {
      tree[path] = `${child.lastModified}:${new TextDecoder().decode(child.bytes)}`;
    }
  }
  return tree;
}

export function fileAt(root: MemoryDirectory, path: string) {
  let node: MemoryDirectory | MemoryFile | undefined = root;
  for (const segment of path.split("/")) {
    if (node?.kind !== "directory") return undefined;
    node = node.children.get(segment);
  }
  return node?.kind === "file" ? node : undefined;
}

export function writeFileAt(root: MemoryDirectory, path: string, content: string) {
  buildTree({ [path]: content }, root);
  const file = fileAt(root, path)!;
  file.lastModified = tick();
  return file;
}
