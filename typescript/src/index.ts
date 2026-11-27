import {
  cachedExtract,
  cachedTransform,
  type Cache,
  type OutputVersion,
  type Serializer,
} from './cache.js'
import { recorded } from './history.js'
import type { Store } from './store.js'
import { errorMessage } from './util.js'

export { listVersions, restore } from './cache.js'
export type { OutputVersion, Serializer, StepId } from './cache.js'
export { listRuns, type Run } from './history.js'
export { MemoryStore, type Store } from './store.js'

export interface Extractor<Out> {
  name: string
  /** Bump when the output for the same input changes. */
  version: string
  /** Checked against each item `extract` yields. */
  outputSchema?: StandardSchema
  extract(signal: AbortSignal): AsyncIterable<Out>
  serializer?: Serializer<Out>
}

export interface Transformer<In, Out> {
  name: string
  /** Bump when the output for the same input changes. */
  version: string
  /** Checked against each item before `transform`. */
  inputSchema?: StandardSchema
  /** Checked against each item `transform` returns. */
  outputSchema?: StandardSchema
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

/** The part of https://standardschema.dev the runner uses; items pass through unchanged. */
export interface StandardSchema {
  readonly '~standard': {
    readonly version: 1
    readonly vendor: string
    readonly validate: (value: unknown) => SchemaResult | Promise<SchemaResult>
  }
}

type SchemaResult =
  | { readonly value: unknown; readonly issues?: undefined }
  | { readonly issues: ReadonlyArray<SchemaIssue> }

interface SchemaIssue {
  readonly message: string
  readonly path?: ReadonlyArray<PropertyKey | { readonly key: PropertyKey }> | undefined
}

export interface Pipeline<Out> {
  pipe<Next>(transformer: Transformer<Out, Next>, options?: StepOptions): Pipeline<Next>
  /** Resolves to the last step's items. */
  run(options?: RunOptions): Promise<Out[]>
}

export interface RunOptions {
  /** Aborting it rejects the run with the signal's reason, once the running step returns. */
  signal?: AbortSignal
  /** Called each time a step finishes an item. */
  onProgress?: (progress: Progress) => void
  /** Records the run; required if any step is cached. */
  store?: Store
  /** A step's name: it and all steps after it run, even if cached. */
  forceFrom?: string
}

export interface Progress {
  step: string
  /** How many items this step has finished in this run, counting from 1. */
  item: number
}

/** Starts a pipeline at an extractor; add transformers with `pipe`. */
export function pipeline<Out>(
  extractor: Extractor<Out>,
  options: StepOptions = {},
): Pipeline<Out> {
  return chain({ step: extractor, options }, [])
}

interface Configured<Step> {
  step: Step
  options: StepOptions
}

interface Stage<Step> {
  step: Step
  cache: Cache | undefined
  finished: number
}

function chain<Out>(
  first: Configured<Extractor<unknown>>,
  rest: Configured<Transformer<any, unknown>>[],
): Pipeline<Out> {
  return {
    pipe: <Next>(transformer: Transformer<Out, Next>, options: StepOptions = {}) =>
      chain<Next>(first, [...rest, { step: transformer, options }]),

    async run({
      signal = new AbortController().signal,
      onProgress,
      store,
      forceFrom,
    }: RunOptions = {}) {
      const steps = [first, ...rest]
      const forced =
        forceFrom === undefined
          ? steps.length
          : steps.findIndex(({ step }) => step.name === forceFrom)
      if (forced < 0) throw new Error(`no step "${forceFrom}" to force from`)
      const used: OutputVersion[] = []

      function stage<Step extends { name: string }>(
        { step, options }: Configured<Step>,
        position: number,
      ): Stage<Step> {
        if (!options.cache) return { step, cache: undefined, finished: 0 }
        if (!store) throw new Error(`step "${step.name}" is cached, but run got no store`)
        const recordUse = (output: OutputVersion) => used.push(output)
        return { step, cache: { store, force: position >= forced, recordUse }, finished: 0 }
      }

      const source = stage(first, 0)
      const stages = rest.map((configured, i) => stage(configured, i + 1))
      const results: Out[] = []

      function report(done: Stage<{ name: string }>): void {
        onProgress?.({ step: done.step.name, item: ++done.finished })
      }

      async function push(item: unknown, index: number): Promise<void> {
        const current = stages[index]
        if (!current) {
          results.push(item as Out)
          return
        }
        const { step, cache } = current
        signal.throwIfAborted()
        let outputs: unknown[]
        try {
          await check(step.inputSchema, item, 'input')
          const call = () => transform(step, item, signal)
          if (cache) {
            outputs = await cachedTransform(cache, step, item, signal, call)
            // a stored output may predate the schema
            await checkOutputs(step.outputSchema, outputs)
          } else {
            outputs = await call()
          }
        } catch (error) {
          throw stepError(step.name, error)
        }
        report(current)
        for (const next of outputs) {
          await push(next, index + 1)
        }
      }

      async function execute() {
        try {
          signal.throwIfAborted()
          for await (const item of extract(source.step, signal, source.cache)) {
            report(source)
            await push(item, 0)
            // before the extractor is resumed
            signal.throwIfAborted()
          }
          signal.throwIfAborted()
        } catch (error) {
          // a cancelled run is no step's failure, whatever the step threw
          signal.throwIfAborted()
          throw error
        }
        return results
      }

      return store ? recorded(store, signal, used, execute) : execute()
    },
  }
}

// errors from the loop consuming this generator never reach its catch
async function* extract<Out>(step: Extractor<Out>, signal: AbortSignal, cache?: Cache) {
  try {
    const fresh = checked(step.outputSchema, () => step.extract(signal))
    // a stored output may predate the schema
    yield* cache
      ? checked(step.outputSchema, () => cachedExtract(cache, step, fresh, signal))
      : fresh
  } catch (error) {
    throw stepError(step.name, error)
  }
}

async function transform<In, Out>(step: Transformer<In, Out>, item: In, signal: AbortSignal) {
  signal.throwIfAborted()
  const output = await step.transform(item, signal)
  const outputs = Array.isArray(output) ? output : [output]
  await checkOutputs(step.outputSchema, outputs)
  return outputs
}

async function checkOutputs(schema: StandardSchema | undefined, outputs: unknown[]) {
  for (const output of outputs) {
    await check(schema, output, 'output')
  }
}

async function* checked<Out>(schema: StandardSchema | undefined, items: () => AsyncIterable<Out>) {
  for await (const item of items()) {
    await check(schema, item, 'output')
    yield item
  }
}

async function check(schema: StandardSchema | undefined, value: unknown, side: 'input' | 'output') {
  const issues = (await schema?.['~standard'].validate(value))?.issues
  if (!issues) return
  const details = issues.map(({ message, path }) => {
    const at = path?.map((part) => String(typeof part === 'object' ? part.key : part)).join('.')
    return at ? `${at}: ${message}` : message
  })
  throw new Error(`invalid ${side}: ${details.join('; ')}`, { cause: issues })
}

function stepError(name: string, cause: unknown): Error {
  return new Error(`step "${name}" failed: ${errorMessage(cause)}`, { cause })
}
