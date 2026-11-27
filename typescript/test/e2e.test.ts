import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  pipeline,
  type Extractor,
  type StandardSchema,
  type Transformer,
} from 'medallion-pipeline'

test('a host app runs a pipeline', async () => {
  const log: string[] = []

  const lines: Extractor<string> = {
    name: 'lines',
    version: '1',
    async *extract(signal) {
      assert.equal(signal.aborted, false)
      for (const line of ['a b', 'c']) {
        log.push(`extract ${line}`)
        yield line
      }
    },
  }

  class Words implements Transformer<string, string> {
    name = 'words'
    version = '1'
    async transform(line: string) {
      return line.split(' ')
    }
  }

  const shout: Transformer<string, { word: string }> = {
    name: 'shout',
    version: '1',
    async transform(word, signal) {
      assert.equal(signal.aborted, false)
      log.push(`shout ${word}`)
      return { word: word.toUpperCase() }
    },
  }

  const words = pipeline(lines).pipe(new Words())
  const out: { word: string }[] = await words.pipe(shout).run()

  assert.deepEqual(out, [{ word: 'A' }, { word: 'B' }, { word: 'C' }])
  assert.deepEqual(log, ['extract a b', 'shout a', 'shout b', 'extract c', 'shout c'])

  const double: Transformer<number, number> = {
    name: 'double',
    version: '1',
    transform: async (n) => n * 2,
  }
  // @ts-expect-error a step must accept what the previous one returns
  words.pipe(double)

  const boom = new Error('no network')
  const upload: Transformer<string, string> = {
    name: 'upload',
    version: '1',
    async transform() {
      throw boom
    },
  }
  log.length = 0
  await assert.rejects(words.pipe(upload).run(), {
    message: 'step "upload" failed: no network',
    cause: boom,
  })
  assert.deepEqual(log, ['extract a b'])

  const download: Extractor<string> = {
    name: 'download',
    version: '1',
    async *extract() {
      throw boom
    },
  }
  await assert.rejects(pipeline(download).pipe(upload).run(), {
    message: 'step "download" failed: no network',
    cause: boom,
  })

  // cancel: the step sees the abort, and run rejects with its reason, not a step error
  const reason = new Error('user left')
  const cancelled = (error: unknown) => error === reason
  const controller = new AbortController()
  const slow: Transformer<string, string> = {
    name: 'slow',
    version: '1',
    async transform(word, signal) {
      log.push(`slow ${word}`)
      controller.abort(reason)
      assert.equal(signal.aborted, true)
      throw new Error('request aborted')
    },
  }
  log.length = 0
  await assert.rejects(words.pipe(slow).run({ signal: controller.signal }), cancelled)
  assert.deepEqual(log, ['extract a b', 'slow a'])

  // a step that ignores the signal is the last to run
  const stop = new AbortController()
  const deaf: Transformer<string, string> = {
    name: 'deaf',
    version: '1',
    async transform(word) {
      log.push(`deaf ${word}`)
      stop.abort(reason)
      return word
    },
  }
  log.length = 0
  await assert.rejects(words.pipe(deaf).pipe(slow).run({ signal: stop.signal }), cancelled)
  assert.deepEqual(log, ['extract a b', 'deaf a'])

  log.length = 0
  await assert.rejects(words.run({ signal: stop.signal }), cancelled)
  assert.deepEqual(log, [])

  // an extractor that just returns on abort does not make a partial run succeed
  const quit = new AbortController()
  const quitter: Extractor<string> = {
    name: 'quitter',
    version: '1',
    async *extract() {
      quit.abort(reason)
    },
  }
  await assert.rejects(pipeline(quitter).run({ signal: quit.signal }), cancelled)

  // progress: one report each time a step finishes an item, counted per step from 1
  const progress: string[] = []
  await words.pipe(shout).run({
    onProgress: ({ step, item }) => progress.push(`${step} ${item}`),
  })
  assert.deepEqual(progress, [
    'lines 1',
    'words 1',
    'shout 1',
    'shout 2',
    'lines 2',
    'words 2',
    'shout 3',
  ])

  progress.length = 0
  await assert.rejects(
    words.pipe(upload).run({ onProgress: ({ step, item }) => progress.push(`${step} ${item}`) }),
  )
  assert.deepEqual(progress, ['lines 1', 'words 1'])

  // schemas: any Standard Schema, sync or async, checks each item and leaves it unchanged
  const text: StandardSchema = {
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: (value) =>
        typeof value === 'string'
          ? { value: 'replaced' }
          : { issues: [{ message: 'expected a string', path: [{ key: 'words' }, 0] }] },
    },
  }
  const asyncText: StandardSchema = {
    '~standard': { ...text['~standard'], validate: async (v) => text['~standard'].validate(v) },
  }
  const checked = pipeline({ ...lines, outputSchema: text }).pipe({
    name: 'words',
    version: '1',
    inputSchema: asyncText,
    outputSchema: text,
    transform: new Words().transform,
  })
  assert.deepEqual(await checked.run(), ['a', 'b', 'c'])

  // an extractor's bad item never reaches the next step
  const numbers: Extractor<any> = {
    name: 'numbers',
    version: '1',
    outputSchema: asyncText,
    async *extract() {
      yield 1
    },
  }
  log.length = 0
  await assert.rejects(pipeline(numbers).pipe(slow).run(), {
    message: 'step "numbers" failed: invalid output: words.0: expected a string',
  })
  assert.deepEqual(log, [])

  // a bad input stops the step from being called
  const reject: StandardSchema = {
    '~standard': { ...text['~standard'], validate: () => ({ issues: [{ message: 'no' }] }) },
  }
  await assert.rejects(pipeline(lines).pipe({ ...slow, inputSchema: reject }).run(), {
    message: 'step "slow" failed: invalid input: no',
  })
  assert.deepEqual(log, ['extract a b'])

  // one bad item in an array output stops all of them
  const mixed: Transformer<string, any> = {
    name: 'mixed',
    version: '1',
    outputSchema: text,
    transform: async (line) => [line, 2],
  }
  log.length = 0
  await assert.rejects(pipeline(lines).pipe(mixed).pipe(slow).run(), {
    message: 'step "mixed" failed: invalid output: words.0: expected a string',
  })
  assert.deepEqual(log, ['extract a b'])
})
