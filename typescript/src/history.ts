import type { OutputVersion } from './cache.js'
import type { Store } from './store.js'

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
    runs.push(JSON.parse(new TextDecoder().decode(await store.download(path))))
  }
  return runs.sort((a, b) => a.startedAt.localeCompare(b.startedAt))
}

export async function recorded<T>(
  store: Store,
  signal: AbortSignal,
  // filled while `run` goes
  outputs: OutputVersion[],
  run: () => Promise<T>,
): Promise<T> {
  const id = crypto.randomUUID()
  const startedAt = new Date().toISOString()

  async function record(status: Run['status'], error?: string) {
    const finishedAt = new Date().toISOString()
    const entry: Run = { id, startedAt, finishedAt, status, error, outputs }
    await store.upload(`runs/${id}`, new TextEncoder().encode(JSON.stringify(entry)))
  }

  let result: T
  try {
    result = await run()
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error)
    // why the run stopped matters more than a history that cannot be written
    await record(signal.aborted ? 'cancelled' : 'failed', reason).catch(() => {})
    throw error
  }
  await record('succeeded')
  return result
}
