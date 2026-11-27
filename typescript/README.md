# medallion-pipeline

Runs an extractor and transformers in order, inside your app, with a cache and run history. Works in Node and browser WebViews, from TypeScript or JavaScript.

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

### Cache

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

### Force, history and restore

Force a step after changing something the cache key does not see: earlier steps use their cache, that step (a cached extractor too) and all after it run. If two steps share the name, the first counts.

```ts
await drafting.run({ store, forceFrom: 'write' })
```

Every run that gets a store is recorded when it ends. Each cache write adds an output version; old ones stay.

```ts
import { listRuns, listVersions, restore } from 'medallion-pipeline'

const runs = await listRuns(store) // oldest first
// [{ id, startedAt, finishedAt, status, error?, outputs }]
// status: 'succeeded' | 'failed' | 'cancelled'

const versions = await listVersions(store, write)
// [{ step: 'write', stepVersion: '1', key, number }]
```

`key` stands for one input; the highest `number` of a key is its latest. A run's `outputs` lists the version each cached step used or wrote, one per input.

`restore` writes a copy of an earlier version as the latest, without running anything, and returns the copy:

```ts
await restore(store, versions[0])
```

On the next run, later steps look up their output for the restored items.

Nothing is deleted for you. Delete with your store:

- a run: `runs/<id>`
- an output version: the files under `cache/<name>/<version>/<key>/<number>/`, `count` first (name and version URI-encoded). Deleting a key's highest number frees it for reuse, so older run records then point at the new output.

## Develop

Needs Node 22.18 or later.

```sh
npm ci
npm test
```
