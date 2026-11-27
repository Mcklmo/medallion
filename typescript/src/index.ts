export interface Extractor<Out> {
  name: string
  /** Bump when the output for the same input changes. */
  version: string
  /** Checked against each item `extract` yields. */
  outputSchema?: StandardSchema
  extract(signal: AbortSignal): AsyncIterable<Out>
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
  pipe<Next>(transformer: Transformer<Out, Next>): Pipeline<Next>
  /** Resolves to the last step's items. */
  run(options?: RunOptions): Promise<Out[]>
}

export interface RunOptions {
  /** Aborting it rejects the run with the signal's reason, once the running step returns. */
  signal?: AbortSignal
  /** Called each time a step finishes an item. */
  onProgress?: (progress: Progress) => void
}

export interface Progress {
  step: string
  /** How many items this step has finished in this run, counting from 1. */
  item: number
}

/** Starts a pipeline at an extractor; add transformers with `pipe`. */
export function pipeline<Out>(extractor: Extractor<Out>): Pipeline<Out> {
  return chain(extractor, [])
}

function chain<Out>(
  extractor: Extractor<unknown>,
  transformers: Transformer<any, unknown>[],
): Pipeline<Out> {
  return {
    pipe: <Next>(transformer: Transformer<Out, Next>) =>
      chain<Next>(extractor, [...transformers, transformer]),

    async run({ signal = new AbortController().signal, onProgress }: RunOptions = {}) {
      const results: Out[] = []
      // by position, as two steps may share a name; 0 is the extractor
      const finished: number[] = []

      function report({ name }: { name: string }, position: number): void {
        const item = (finished[position] = (finished[position] ?? 0) + 1)
        onProgress?.({ step: name, item })
      }

      async function push(item: unknown, index: number): Promise<void> {
        const step = transformers[index]
        if (!step) {
          results.push(item as Out)
          return
        }
        signal.throwIfAborted()
        let outputs: unknown[]
        try {
          await check(step.inputSchema, item, 'input')
          signal.throwIfAborted()
          const output: unknown = await step.transform(item, signal)
          outputs = Array.isArray(output) ? output : [output]
          for (const next of outputs) {
            await check(step.outputSchema, next, 'output')
          }
        } catch (error) {
          throw stepError(step.name, error)
        }
        report(step, index + 1)
        for (const next of outputs) {
          await push(next, index + 1)
        }
      }

      try {
        signal.throwIfAborted()
        for await (const item of extract(extractor, signal)) {
          report(extractor, 0)
          await push(item, 0)
        }
        signal.throwIfAborted()
      } catch (error) {
        // a cancelled run is no step's failure, whatever the step threw
        signal.throwIfAborted()
        throw error
      }
      return results
    },
  }
}

// errors from the loop consuming this generator never reach its catch
async function* extract<Out>(step: Extractor<Out>, signal: AbortSignal) {
  try {
    for await (const item of step.extract(signal)) {
      await check(step.outputSchema, item, 'output')
      yield item
    }
  } catch (error) {
    throw stepError(step.name, error)
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
  const reason = cause instanceof Error ? cause.message : String(cause)
  return new Error(`step "${name}" failed: ${reason}`, { cause })
}
