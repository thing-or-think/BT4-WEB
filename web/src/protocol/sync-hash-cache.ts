import { HASH_SIZE } from "./sync-diff";

export interface SyncHashCacheEntry {
  dirId: string;
  path: string;
  size: number;
  lastModified: number;
  hash: Uint8Array;
}

export interface SyncHashCacheStore {
  resolveDirId(root: FileSystemDirectoryHandle): Promise<string>;
  get(
    dirId: string,
    path: string,
    size: number,
    lastModified: number,
  ): Promise<Uint8Array | undefined>;
  put(entry: SyncHashCacheEntry): Promise<void>;
  prune(dirId: string, seenPaths: ReadonlySet<string>): Promise<void>;
  close?(): void;
}

interface StoredDirRecord {
  id: string;
  handle: FileSystemDirectoryHandle;
  lastUsed: number;
}

interface StoredHashRecord {
  dirId: string;
  path: string;
  size: number;
  lastModified: number;
  hash: Uint8Array;
}

function generateId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  const bytes = new Uint8Array(16);
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    crypto.getRandomValues(bytes);
  } else {
    for (let i = 0; i < bytes.length; i += 1) {
      bytes[i] = Math.floor(Math.random() * 256);
    }
  }
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

async function handlesMatch(
  a: FileSystemDirectoryHandle,
  b: FileSystemDirectoryHandle,
): Promise<boolean> {
  if (a === b) return true;
  if (typeof a.isSameEntry === "function") {
    try {
      return await a.isSameEntry(b);
    } catch {
      return false;
    }
  }
  return false;
}

function copyHash(hash: Uint8Array): Uint8Array {
  return hash.slice();
}

export class MemorySyncHashCacheStore implements SyncHashCacheStore {
  private dirs: StoredDirRecord[] = [];
  private handleIds = new WeakMap<FileSystemDirectoryHandle, string>();
  private entries = new Map<string, Map<string, StoredHashRecord>>();

  async resolveDirId(root: FileSystemDirectoryHandle): Promise<string> {
    const cached = this.handleIds.get(root);
    if (cached) return cached;
    for (const record of this.dirs) {
      if (await handlesMatch(root, record.handle)) {
        record.lastUsed = Date.now();
        this.handleIds.set(root, record.id);
        return record.id;
      }
    }
    const id = generateId();
    this.dirs.push({ id, handle: root, lastUsed: Date.now() });
    this.handleIds.set(root, id);
    return id;
  }

  async get(
    dirId: string,
    path: string,
    size: number,
    lastModified: number,
  ): Promise<Uint8Array | undefined> {
    const record = this.entries.get(dirId)?.get(path);
    if (!record) return undefined;
    if (record.size !== size || record.lastModified !== lastModified) {
      return undefined;
    }
    if (record.hash.byteLength !== HASH_SIZE) return undefined;
    return copyHash(record.hash);
  }

  async put(entry: SyncHashCacheEntry): Promise<void> {
    let dirMap = this.entries.get(entry.dirId);
    if (!dirMap) {
      dirMap = new Map();
      this.entries.set(entry.dirId, dirMap);
    }
    dirMap.set(entry.path, {
      dirId: entry.dirId,
      path: entry.path,
      size: entry.size,
      lastModified: entry.lastModified,
      hash: copyHash(entry.hash),
    });
  }

  async prune(dirId: string, seenPaths: ReadonlySet<string>): Promise<void> {
    const dirMap = this.entries.get(dirId);
    if (!dirMap) return;
    for (const path of dirMap.keys()) {
      if (!seenPaths.has(path)) {
        dirMap.delete(path);
      }
    }
  }

  sizeForDir(dirId: string): number {
    return this.entries.get(dirId)?.size ?? 0;
  }
}

const DB_NAME = "croc-sync-hash-cache";
const DB_VERSION = 1;
const STORE_DIRS = "directories";
const STORE_HASHES = "hashes";
const INDEX_BY_DIR = "by_dir";

function reqToPromise<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () =>
      reject(request.error ?? new Error("IndexedDB request failed"));
  });
}

function txDone(tx: IDBTransaction): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () =>
      reject(tx.error ?? new Error("IndexedDB transaction failed"));
    tx.onabort = () =>
      reject(tx.error ?? new Error("IndexedDB transaction aborted"));
  });
}

export class IndexedDBSyncHashCacheStore implements SyncHashCacheStore {
  private dbPromise?: Promise<IDBDatabase>;
  private handleIds = new WeakMap<FileSystemDirectoryHandle, string>();

  constructor(private dbName = DB_NAME) {}

  private open(): Promise<IDBDatabase> {
    if (this.dbPromise) return this.dbPromise;
    this.dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
      if (typeof indexedDB === "undefined") {
        reject(new Error("IndexedDB is not available"));
        return;
      }
      const req = indexedDB.open(this.dbName, DB_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(STORE_DIRS)) {
          db.createObjectStore(STORE_DIRS, { keyPath: "id" });
        }
        if (!db.objectStoreNames.contains(STORE_HASHES)) {
          const store = db.createObjectStore(STORE_HASHES, {
            keyPath: ["dirId", "path"],
          });
          store.createIndex(INDEX_BY_DIR, "dirId", { unique: false });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () =>
        reject(req.error ?? new Error("Failed to open hash cache database"));
    });
    return this.dbPromise;
  }

  async resolveDirId(root: FileSystemDirectoryHandle): Promise<string> {
    const cached = this.handleIds.get(root);
    if (cached) return cached;

    const db = await this.open();
    const readTx = db.transaction(STORE_DIRS, "readonly");
    const records = (await reqToPromise(
      readTx.objectStore(STORE_DIRS).getAll(),
    )) as StoredDirRecord[];

    for (const record of records) {
      if (record.handle && (await handlesMatch(root, record.handle))) {
        this.handleIds.set(root, record.id);
        return record.id;
      }
    }

    const id = generateId();
    const writeTx = db.transaction(STORE_DIRS, "readwrite");
    writeTx.objectStore(STORE_DIRS).put({
      id,
      handle: root,
      lastUsed: Date.now(),
    } satisfies StoredDirRecord);
    await txDone(writeTx);
    this.handleIds.set(root, id);
    return id;
  }

  async get(
    dirId: string,
    path: string,
    size: number,
    lastModified: number,
  ): Promise<Uint8Array | undefined> {
    const db = await this.open();
    const tx = db.transaction(STORE_HASHES, "readonly");
    const record = (await reqToPromise(
      tx.objectStore(STORE_HASHES).get([dirId, path]),
    )) as StoredHashRecord | undefined;
    if (!record) return undefined;
    if (record.size !== size || record.lastModified !== lastModified) {
      return undefined;
    }
    const raw =
      record.hash instanceof Uint8Array
        ? record.hash
        : new Uint8Array(record.hash);
    if (raw.byteLength !== HASH_SIZE) return undefined;
    return copyHash(raw);
  }

  async put(entry: SyncHashCacheEntry): Promise<void> {
    const db = await this.open();
    const tx = db.transaction(STORE_HASHES, "readwrite");
    tx.objectStore(STORE_HASHES).put({
      dirId: entry.dirId,
      path: entry.path,
      size: entry.size,
      lastModified: entry.lastModified,
      hash: copyHash(entry.hash),
    } satisfies StoredHashRecord);
    await txDone(tx);
  }

  async prune(dirId: string, seenPaths: ReadonlySet<string>): Promise<void> {
    const db = await this.open();
    const tx = db.transaction(STORE_HASHES, "readwrite");
    const index = tx.objectStore(STORE_HASHES).index(INDEX_BY_DIR);
    await new Promise<void>((resolve, reject) => {
      const req = index.openCursor(IDBKeyRange.only(dirId));
      req.onsuccess = () => {
        const cursor = req.result;
        if (!cursor) {
          resolve();
          return;
        }
        const record = cursor.value as StoredHashRecord;
        if (!seenPaths.has(record.path)) {
          cursor.delete();
        }
        cursor.continue();
      };
      req.onerror = () =>
        reject(req.error ?? new Error("Failed to prune hash cache"));
    });
    await txDone(tx);
  }

  close(): void {
    void this.dbPromise?.then((db) => db.close()).catch(() => undefined);
    this.dbPromise = undefined;
  }
}

export class SyncHashCache {
  private readonly store: SyncHashCacheStore;

  constructor(store?: SyncHashCacheStore) {
    if (store) {
      this.store = store;
    } else if (typeof indexedDB !== "undefined") {
      this.store = new IndexedDBSyncHashCacheStore();
    } else {
      this.store = new MemorySyncHashCacheStore();
    }
  }

  resolveDirId(root: FileSystemDirectoryHandle): Promise<string> {
    return this.store.resolveDirId(root);
  }

  get(
    dirId: string,
    path: string,
    size: number,
    lastModified: number,
  ): Promise<Uint8Array | undefined> {
    return this.store.get(dirId, path, size, lastModified);
  }

  put(
    dirId: string,
    path: string,
    size: number,
    lastModified: number,
    hash: Uint8Array,
  ): Promise<void> {
    return this.store.put({ dirId, path, size, lastModified, hash });
  }

  prune(dirId: string, seenPaths: ReadonlySet<string>): Promise<void> {
    return this.store.prune(dirId, seenPaths);
  }

  async getForRoot(
    root: FileSystemDirectoryHandle,
    path: string,
    size: number,
    lastModified: number,
  ): Promise<Uint8Array | undefined> {
    const dirId = await this.resolveDirId(root);
    return this.get(dirId, path, size, lastModified);
  }

  async putForRoot(
    root: FileSystemDirectoryHandle,
    path: string,
    size: number,
    lastModified: number,
    hash: Uint8Array,
  ): Promise<void> {
    const dirId = await this.resolveDirId(root);
    await this.put(dirId, path, size, lastModified, hash);
  }

  async pruneForRoot(
    root: FileSystemDirectoryHandle,
    seenPaths: ReadonlySet<string>,
  ): Promise<void> {
    const dirId = await this.resolveDirId(root);
    await this.prune(dirId, seenPaths);
  }

  async hashFile(
    root: FileSystemDirectoryHandle,
    path: string,
    file: File,
    computeHash: (file: File, signal?: AbortSignal) => Promise<Uint8Array>,
    signal?: AbortSignal,
  ): Promise<Uint8Array> {
    const dirId = await this.resolveDirId(root);
    const cached = await this.get(dirId, path, file.size, file.lastModified);
    if (cached) return cached;
    const computed = await computeHash(file, signal);
    await this.put(dirId, path, file.size, file.lastModified, computed);
    return computed;
  }

  close(): void {
    this.store.close?.();
  }
}
