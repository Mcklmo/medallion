import type { Extractor, Transformer } from './index.js'
import type { Store } from './store.js'

/** How a cached step's output items become bytes and back; the default is JSON. */
export interface Serializer<T> {
  serialize(item: T): Uint8Array | Promise<Uint8Array>
  deserialize(content: Uint8Array): T | Promise<T>
}

const encode = (text: string) => new TextEncoder().encode(text)
const decode = (content: Uint8Array) => new TextDecoder().decode(content)

const json: Serializer<any> = {
  serialize: (item) => encode(toJson(item)),
  deserialize: (content) => JSON.parse(decode(content)),
}

function toJson(value: unknown): string {
  const text: string | undefined = JSON.stringify(value)
  if (text === undefined) throw new Error(`${typeof value} has no JSON form`)
  return text
}

/** One stored output of a step: the items it gave for one input. */
export interface OutputVersion {
  step: string
  stepVersion: string
  /** `extract`, or the input's hash. */
  key: string
  /** Counts up from 1; the highest is the latest. */
  number: number
}

/** How a run uses one cached step. */
export interface Cache {
  store: Store
  /** Run the step even if it has a stored output. */
  force: boolean
  used(output: OutputVersion): void
}

// An output version is the folder <key>/<number>: one file per item, then `count`.
// Without `count` the write never finished and the version does not exist.

/** Every stored output of this step at this version, by key, then number. */
export async function listVersions(
  store: Store,
  step: { name: string; version: string },
): Promise<OutputVersion[]> {
  const prefix = cacheFolder(step, '')
  const found: OutputVersion[] = []
  for (const path of await store.list(prefix)) {
    const [key, number, file] = path.slice(prefix.length).split('/')
    if (file === 'count') found.push(outputVersion(step, key!, Number(number)))
  }
  return found.sort((a, b) => a.key.localeCompare(b.key) || a.number - b.number)
}

/** Writes a copy of `output` as the latest version for its key, and returns that copy. */
export async function restore(store: Store, output: OutputVersion): Promise<OutputVersion> {
  const step = { name: output.step, version: output.stepVersion }
  const folder = cacheFolder(step, output.key)
  const from = `${folder}/${output.number}`
  if (!(await store.exists(`${from}/count`))) {
    throw new Error(`step "${step.name}" has no output version ${output.number} for that key`)
  }
  const { next } = await versions(store, folder)
  const count = await store.download(`${from}/count`)
  for (let i = 0; i < Number(decode(count)); i++) {
    await store.upload(`${folder}/${next}/${i}`, await store.download(`${from}/${i}`))
  }
  await store.upload(`${folder}/${next}/count`, count)
  return outputVersion(step, output.key, next)
}

export async function* cachedExtract<Out>(
  { store, force, used }: Cache,
  step: Extractor<Out>,
  // lazy: iterating it is what runs the extractor
  items: AsyncIterable<Out>,
  signal: AbortSignal,
): AsyncIterable<Out> {
  const key = 'extract'
  const folder = cacheFolder(step, key)
  const serializer: Serializer<Out> = step.serializer ?? json
  let { latest, next } = await versions(store, folder)
  if (force || latest === undefined) {
    latest = next
    // all of it is stored before the first item goes on, so a later failure loses none
    await write(store, `${folder}/${latest}`, items, serializer, signal)
  }
  used(outputVersion(step, key, latest))
  yield* read(store, `${folder}/${latest}`, serializer)
}

export async function cachedTransform<In, Out>(
  { store, force, used }: Cache,
  step: Transformer<In, Out>,
  input: In,
  signal: AbortSignal,
  run: () => Promise<Out[]>,
): Promise<Out[]> {
  const identity = step.hashInput ? await step.hashInput(input) : toJson(input)
  const key = await sha256(identity)
  const folder = cacheFolder(step, key)
  const serializer: Serializer<Out> = step.serializer ?? json
  const { latest, next } = await versions(store, folder)

  if (!force && latest !== undefined) {
    const items: Out[] = []
    for await (const item of read(store, `${folder}/${latest}`, serializer)) items.push(item)
    used(outputVersion(step, key, latest))
    return items
  }

  const output = await run()
  await write(store, `${folder}/${next}`, output, serializer, signal)
  used(outputVersion(step, key, next))
  return output
}

function cacheFolder(step: { name: string; version: string }, key: string) {
  const [name, version] = [step.name, step.version].map(encodeURIComponent)
  return `cache/${name}/${version}/${key}`
}

function outputVersion(
  step: { name: string; version: string },
  key: string,
  number: number,
): OutputVersion {
  return { step: step.name, stepVersion: step.version, key, number }
}

async function versions(store: Store, folder: string) {
  let latest: number | undefined
  let last = 0
  for (const path of await store.list(`${folder}/`)) {
    const [name, file] = path.slice(folder.length + 1).split('/')
    const number = Number(name)
    last = Math.max(last, number)
    if (file === 'count') latest = Math.max(latest ?? 0, number)
  }
  // `next` skips unfinished versions, so their leftovers never mix into a new one
  // ponytail: two runs writing one key at once can pick the same number; lock in the host if needed
  return { latest, next: last + 1 }
}

async function write<T>(
  store: Store,
  folder: string,
  items: Iterable<T> | AsyncIterable<T>,
  serializer: Serializer<T>,
  signal: AbortSignal,
) {
  let count = 0
  for await (const item of items) {
    signal.throwIfAborted()
    await store.upload(`${folder}/${count++}`, await serializer.serialize(item))
  }
  // a cancelled step may return early with partial output
  signal.throwIfAborted()
  await store.upload(`${folder}/count`, encode(String(count)))
}

async function* read<T>(store: Store, folder: string, serializer: Serializer<T>) {
  const count = Number(decode(await store.download(`${folder}/count`)))
  for (let i = 0; i < count; i++) {
    yield await serializer.deserialize(await store.download(`${folder}/${i}`))
  }
}

async function sha256(text: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', encode(text))
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('')
}
