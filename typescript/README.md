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

## Cache

Off by default. Turn it on per step and give `run` a store; finished work is then reused, also after a failed run.

```ts
import { MemoryStore } from 'medallion-pipeline'

const store = new MemoryStore() // or your own `Store`, on your own storage

const drafting = pipeline(topics, { cache: true }).pipe(write, { cache: true })
const drafts = await drafting.run({ store })
```

- The key is the step's `name`, `version` and a hash of the input. Configuration, such as a model name, is not part of it.
- A cached extractor replays its latest output. It finishes extracting before the first item goes on.
- Outputs and inputs are stored and hashed as JSON. For anything else (bytes, images, `Map`), give the step a `serializer: { serialize, deserialize }` for its output items, and a transformer a `hashInput(item)` returning a string that identifies the input.

A human review is two runs:

```ts
const approved = await review(drafts) // your UI; returns an extractor of the approved drafts
await pipeline(approved).pipe(publish).run()
```

## Develop

Needs Node 22.18 or later.

```sh
npm ci
npm test
```
