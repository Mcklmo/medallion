export interface Extractor<Out> {
  name: string
  /** Bump when the output for the same input changes. */
  version: string
  extract(signal: AbortSignal): AsyncIterable<Out>
}

export interface Transformer<In, Out> {
  name: string
  /** Bump when the output for the same input changes. */
  version: string
  // a property, not a method: TypeScript checks method parameters too loosely to type a chain
  /** An array is several items. */
  transform: (item: In, signal: AbortSignal) => Promise<Out | Out[]>
}

export interface Pipeline<Out> {
  pipe<Next>(transformer: Transformer<Out, Next>): Pipeline<Next>
  /** Resolves to the last step's items. */
  run(): Promise<Out[]>
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

    async run() {
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
          output = await step.transform(item, signal)
        } catch (error) {
          throw stepError(step.name, error)
        }
        for (const next of Array.isArray(output) ? output : [output]) {
          await push(next, index + 1)
        }
      }

      for await (const item of extract(extractor, signal)) {
        await push(item, 0)
      }
      return results
    },
  }
}

// errors from the loop consuming this generator never reach its catch
async function* extract<Out>(step: Extractor<Out>, signal: AbortSignal) {
  try {
    yield* step.extract(signal)
  } catch (error) {
    throw stepError(step.name, error)
  }
}

function stepError(name: string, cause: unknown): Error {
  const reason = cause instanceof Error ? cause.message : String(cause)
  return new Error(`step "${name}" failed: ${reason}`, { cause })
}
