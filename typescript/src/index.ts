import { cachedExtract, cachedTransform, type Serializer } from './cache.js'
import type { Store } from './store.js'

export type { Serializer } from './cache.js'
export { MemoryStore, type Store } from './store.js'

export interface Extractor<Out> {
  name: string
  /** Bump when the output for the same input changes. */
  version: string
  extract(signal: AbortSignal): AsyncIterable<Out>
  serializer?: Serializer<Out>
}

export interface Transformer<In, Out> {
  name: string
  /** Bump when the output for the same input changes. */
  version: string
  // a property, not a method: TypeScript checks method parameters too loosely to type a chain
  /** An array is several items. */
  transform: (item: In, signal: AbortSignal) => Promise<Out | Out[]>
  serializer?: Serializer<Out>
  /** Identifies an input in the cache key; the default is its JSON. */
  hashInput?(item: In): string | Promise<string>
}

export interface StepOptions {
  /** Reuse this step's stored output instead of running it again. */
  cache?: boolean
}

export interface RunOptions {
  /** Required if any step is cached. */
  store?: Store
}

export interface Pipeline<Out> {
  pipe<Next>(transformer: Transformer<Out, Next>, options?: StepOptions): Pipeline<Next>
  /** Resolves to the last step's items. */
  run(options?: RunOptions): Promise<Out[]>
}

/** Starts a pipeline at an extractor; add transformers with `pipe`. */
export function pipeline<Out>(
  extractor: Extractor<Out>,
  options: StepOptions = {},
): Pipeline<Out> {
  return chain(extractor, [], [options])
}

function chain<Out>(
  extractor: Extractor<unknown>,
  transformers: Transformer<any, unknown>[],
  // the extractor's, then one per transformer
  stepOptions: StepOptions[],
): Pipeline<Out> {
  return {
    pipe: <Next>(transformer: Transformer<Out, Next>, options: StepOptions = {}) =>
      chain<Next>(extractor, [...transformers, transformer], [...stepOptions, options]),

    async run({ store }: RunOptions = {}) {
      const stores = [extractor, ...transformers].map(({ name }, i) => {
        if (!stepOptions[i]?.cache) return undefined
        if (!store) throw new Error(`step "${name}" is cached, but run got no store`)
        return store
      })
      const { signal } = new AbortController()
      const results: Out[] = []

      async function push(item: unknown, index: number): Promise<void> {
        const step = transformers[index]
        if (!step) {
          results.push(item as Out)
          return
        }
        let output: unknown
        try {
          const cache = stores[index + 1]
          output = cache
            ? await cachedTransform(cache, step, item, signal)
            : await step.transform(item, signal)
        } catch (error) {
          throw stepError(step.name, error)
        }
        for (const next of Array.isArray(output) ? output : [output]) {
          await push(next, index + 1)
        }
      }

      for await (const item of extract(extractor, signal, stores[0])) {
        await push(item, 0)
      }
      return results
    },
  }
}

// errors from the loop consuming this generator never reach its catch
async function* extract<Out>(step: Extractor<Out>, signal: AbortSignal, cache?: Store) {
  try {
    yield* cache ? cachedExtract(cache, step, signal) : step.extract(signal)
  } catch (error) {
    throw stepError(step.name, error)
  }
}

function stepError(name: string, cause: unknown): Error {
  const reason = cause instanceof Error ? cause.message : String(cause)
  return new Error(`step "${name}" failed: ${reason}`, { cause })
}
