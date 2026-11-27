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

## Develop

Needs Node 22.18 or later.

```sh
npm ci
npm test
```
