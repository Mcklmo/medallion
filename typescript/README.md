# medallion-pipeline

Runs an extractor and transformers in order, inside your app. Works in Node and browser WebViews, from TypeScript or JavaScript.

```sh
npm install medallion-pipeline
```

## Usage

A step is a class or object with a `name`, a `version` (bump it when the output for the same input changes) and `extract` or `transform`.

```ts
import { pipeline, type Extractor, type Transformer } from 'medallion-pipeline'

const lines: Extractor<string> = {
  name: 'lines',
  version: '1',
  async *extract(signal) {
    yield 'a b'
    yield 'c'
  },
}

class Words implements Transformer<string, string> {
  name = 'words'
  version = '1'
  async transform(line: string) {
    return line.split(' ') // an array is several items
  }
}

const words = await pipeline(lines).pipe(new Words()).run() // ['a', 'b', 'c']
```

Items go one at a time through all steps, in order. `run` resolves to the last step's items. If a step throws, the run stops with `step "<name>" failed: <reason>`, and the original error as `cause`.

### Cancel and progress

```ts
const controller = new AbortController()
await pipeline(lines)
  .pipe(new Words())
  .run({
    signal: controller.signal,
    onProgress: ({ step, item }) => console.log(step, item),
  })
```

Steps receive the signal as their second argument (an extractor as its only one). After `controller.abort()`, no further step is called and `run` rejects with the signal's reason once the running step returns, so pass the signal on to slow work such as `fetch`.

`onProgress` is called each time a step finishes an item; `item` counts that step's items from 1.

### Schemas

A step may have an `inputSchema` (transformers) and an `outputSchema`: any [Standard Schema](https://standardschema.dev), such as Zod or Valibot.

```ts
import { z } from 'zod'

const shout: Transformer<string, string> = {
  name: 'shout',
  version: '1',
  inputSchema: z.string().min(1),
  outputSchema: z.string(),
  transform: async (word) => word.toUpperCase(),
}
```

Each item is checked, and an invalid one stops the run: `step "shout" failed: invalid input: <issues>`. Schemas only check; items pass through unchanged, so coercions and defaults are not applied.

## Develop

Needs Node 22.18 or later.

```sh
npm ci
npm test
```
