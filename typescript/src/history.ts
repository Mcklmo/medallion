import type { OutputVersion } from './cache.js'
import type { Store } from './store.js'
import { decode, encode, errorMessage, toHex } from './util.js'

export interface Run {
  /** The record is the store's file `runs/<id>`. */
  id: string
  /** ISO 8601, like `finishedAt`. */
  startedAt: string
  finishedAt: string
  status: 'succeeded' | 'failed' | 'cancelled'
  /** Why the run failed or was cancelled. */
  error?: string
  /** The stored output each cached step used or wrote, one per item it was called with. */
  outputs: OutputVersion[]
}

/** Every recorded run in the store, oldest first. */
export async function listRuns(store: Store): Promise<Run[]> {
  const runs: Run[] = []
  for (const path of await store.list('runs/')) {
    const run = parse(decode(await store.download(path)))
    if (run && `runs/${run.id}` === path) runs.push(run)
  }
  return runs.sort((a, b) => a.startedAt.localeCompare(b.startedAt))
}

function parse(text: string): Run | undefined {
  try {
    return JSON.parse(text)
  } catch {
    return undefined
  }
}

export async function recorded<T>(
  store: Store,
  signal: AbortSignal,
  outputs: OutputVersion[],
  run: () => Promise<T>,
): Promise<T> {
  // not randomUUID, which browsers only provide in secure contexts
  const id = toHex(crypto.getRandomValues(new Uint8Array(16)))
  const startedAt = new Date().toISOString()

  async function record(status: Run['status'], error?: string) {
    const finishedAt = new Date().toISOString()
    const entry: Run = { id, startedAt, finishedAt, status, error, outputs }
    await store.upload(`runs/${id}`, encode(JSON.stringify(entry)))
  }

  let result: T
  try {
    result = await run()
  } catch (error) {
    // why the run stopped matters more than a history that cannot be written
    await record(signal.aborted ? 'cancelled' : 'failed', errorMessage(error)).catch(() => {})
    throw error
  }
  await record('succeeded')
  return result
}
