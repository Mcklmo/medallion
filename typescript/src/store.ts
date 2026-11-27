/** Where cached outputs live; implement it on your own storage. Paths use `/`. */
export interface Store {
  exists(path: string): Promise<boolean>
  /** Replaces any file at `path`; readers must never see it half written. */
  upload(path: string, content: Uint8Array): Promise<void>
  download(path: string): Promise<Uint8Array>
  /** Every path starting with `prefix`, which ends in `/`. */
  list(prefix: string): Promise<string[]>
  delete(path: string): Promise<void>
}

/** A store that is gone when the process is. */
export class MemoryStore implements Store {
  #files = new Map<string, Uint8Array>()

  async exists(path: string) {
    return this.#files.has(path)
  }

  async upload(path: string, content: Uint8Array) {
    this.#files.set(path, new Uint8Array(content))
  }

  async download(path: string) {
    const content = this.#files.get(path)
    if (!content) throw new Error(`no file at "${path}"`)
    return new Uint8Array(content)
  }

  async list(prefix: string) {
    return [...this.#files.keys()].filter((path) => path.startsWith(prefix))
  }

  async delete(path: string) {
    this.#files.delete(path)
  }
}
