import type { Extractor, Transformer } from './index.js'
import type { Store } from './store.js'
import { decode, encode, toHex } from './util.js'

/** How a cached step's output items become bytes and back; the default is JSON. */
export interface Serializer<T> {
  serialize(item: T): Uint8Array | Promise<Uint8Array>
  deserialize(content: Uint8Array): T | Promise<T>
}

const json: Serializer<any> = {
  serialize: (item) => encode(toJson(item)),
  deserialize: (content) => JSON.parse(decode(content)),
}

function toJson(value: unknown): string {
  const text: string | undefined = JSON.stringify(value, (_, part) => {
    // they would all become {}, so different inputs would share a cache key
    if (part instanceof Map || part instanceof Set) {
      throw new Error(`${part.constructor.name} has no JSON form`)
    }
    return part
  })
  if (text === undefined) throw new Error(`${typeof value} has no JSON form`)
  return text
}

/** What a step's cached output is filed under. */
export interface StepId {
  name: string
  version: string
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

export interface Cache {
  store: Store
  /** Run the step even if it has a stored output. */
  force: boolean
  recordUse(output: OutputVersion): void
}

// An output version is the folder <key>/<number>: one file per item, then `count`.
// Without `count` the write never finished and the version does not exist.

/** Every stored output of this step at this version, by key, then number. */
export async function listVersions(store: Store, step: StepId): Promise<OutputVersion[]> {
  const prefix = cacheFolder(step, '')
  const found: OutputVersion[] = []
  for (const path of await store.list(prefix)) {
    const [key, number, file] = path.slice(prefix.length).split('/')
    if (file === 'count' && isVersionNumber(number!)) {
      found.push(outputVersion(step, key!, Number(number)))
    }
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
  let count = 0
  for await (const content of contents(store, from)) {
    await store.upload(`${folder}/${next}/${count++}`, content)
  }
  await store.upload(`${folder}/${next}/count`, encode(String(count)))
  return outputVersion(step, output.key, next)
}

export async function* cachedExtract<Out>(
  { store, force, recordUse }: Cache,
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
  recordUse(outputVersion(step, key, latest))
  yield* read(store, `${folder}/${latest}`, serializer)
}

export async function cachedTransform<In, Out>(
  { store, force, recordUse }: Cache,
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

  let number = latest
  if (force || number === undefined) {
    number = next
    await write(store, `${folder}/${number}`, await run(), serializer, signal)
  }
  recordUse(outputVersion(step, key, number))
  // read back even when fresh, so a run gets the same items with or without a cache hit
  const items: Out[] = []
  for await (const item of read(store, `${folder}/${number}`, serializer)) items.push(item)
  return items
}

function cacheFolder(step: StepId, key: string) {
  const [name, version] = [step.name, step.version].map((part) => {
    if (part === '.' || part === '..') {
      throw new Error(`a cached step's name or version cannot be "${part}"`)
    }
    return encodeURIComponent(part)
  })
  return `cache/${name}/${version}/${key}`
}

function outputVersion(step: StepId, key: string, number: number): OutputVersion {
  return { step: step.name, stepVersion: step.version, key, number }
}

// anything else, such as .DS_Store, is not ours
const isVersionNumber = (name: string) => /^\d+$/.test(name)

async function versions(store: Store, folder: string) {
  let latest: number | undefined
  let last = 0
  for (const path of await store.list(`${folder}/`)) {
    const [name, file] = path.slice(folder.length + 1).split('/')
    if (!isVersionNumber(name!)) continue
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
  for await (const content of contents(store, folder)) {
    yield await serializer.deserialize(content)
  }
}

async function* contents(store: Store, folder: string) {
  const count = Number(decode(await store.download(`${folder}/count`)))
  for (let i = 0; i < count; i++) {
    yield await store.download(`${folder}/${i}`)
  }
}

async function sha256(text: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', encode(text))
  return toHex(new Uint8Array(digest))
}
