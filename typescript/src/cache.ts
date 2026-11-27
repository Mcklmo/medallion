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

// An output version is the folder <key>/<number>: one file per item, then `count`.
// Without `count` the write never finished and the version does not exist.

export async function* cachedExtract<Out>(
  store: Store,
  step: Extractor<Out>,
  signal: AbortSignal,
): AsyncIterable<Out> {
  const key = cacheKey(step, 'extract')
  const serializer: Serializer<Out> = step.serializer ?? json
  let { latest, next } = await versions(store, key)
  if (latest === undefined) {
    latest = next
    // all of it is stored before the first item goes on, so a later failure loses none
    await write(store, `${key}/${latest}`, step.extract(signal), serializer, signal)
  }
  yield* read(store, `${key}/${latest}`, serializer)
}

export async function cachedTransform<In, Out>(
  store: Store,
  step: Transformer<In, Out>,
  input: In,
  signal: AbortSignal,
): Promise<Out | Out[]> {
  const identity = step.hashInput ? await step.hashInput(input) : toJson(input)
  const key = cacheKey(step, await sha256(identity))
  const serializer: Serializer<Out> = step.serializer ?? json
  const { latest, next } = await versions(store, key)

  if (latest !== undefined) {
    const items: Out[] = []
    for await (const item of read(store, `${key}/${latest}`, serializer)) items.push(item)
    return items
  }

  const output = await step.transform(input, signal)
  const items = Array.isArray(output) ? output : [output]
  await write(store, `${key}/${next}`, items, serializer, signal)
  return output
}

function cacheKey(step: { name: string; version: string }, input: string) {
  const [name, version] = [step.name, step.version].map(encodeURIComponent)
  return `cache/${name}/${version}/${input}`
}

async function versions(store: Store, key: string) {
  let latest: number | undefined
  let last = 0
  for (const path of await store.list(`${key}/`)) {
    const [folder, file] = path.slice(key.length + 1).split('/')
    const number = Number(folder)
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
