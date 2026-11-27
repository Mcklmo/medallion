import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  MemoryStore,
  pipeline,
  type Extractor,
  type Serializer,
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

  {
  const calls: string[] = []
  const topics: Extractor<string> = {
    name: 'topics',
    version: '1',
    async *extract() {
      calls.push('topics')
      yield 'tea'
      yield 'rain'
    },
  }
  let offline = false
  const write: Transformer<string, { text: string }> = {
    name: 'write',
    version: '1',
    async transform(topic) {
      if (offline && topic === 'rain') throw boom
      calls.push(`write ${topic}`)
      return { text: `on ${topic}` }
    },
  }
  const store = new MemoryStore()
  const drafting = pipeline(topics, { cache: true }).pipe(write, { cache: true })

  // a failed run keeps what it finished
  offline = true
  await assert.rejects(drafting.run({ store }), { cause: boom })
  assert.deepEqual(calls, ['topics', 'write tea'])

  offline = false
  calls.length = 0
  const drafts = await drafting.run({ store })
  assert.deepEqual(drafts, [{ text: 'on tea' }, { text: 'on rain' }])
  assert.deepEqual(calls, ['write rain'])

  calls.length = 0
  assert.deepEqual(await drafting.run({ store }), drafts)
  assert.equal(calls.length, 0)

  // the second run of a review takes the approved draft
  const approved: Extractor<{ text: string }> = {
    name: 'approved',
    version: '1',
    async *extract() {
      yield { text: 'on tea, edited' }
    },
  }
  const publish: Transformer<{ text: string }, string> = {
    name: 'publish',
    version: '1',
    async transform({ text }) {
      calls.push(`publish ${text}`)
      return text
    },
  }
  const publishing = pipeline(approved).pipe(publish, { cache: true })
  assert.deepEqual(await publishing.run({ store }), ['on tea, edited'])
  assert.deepEqual(await publishing.run({ store }), ['on tea, edited'])
  assert.deepEqual(calls, ['publish on tea, edited'])

  // a new version misses the cache, a step without `cache` never reads it
  calls.length = 0
  await pipeline(topics, { cache: true })
    .pipe({ ...write, version: '2' }, { cache: true })
    .run({ store })
  assert.deepEqual(calls, ['write tea', 'write rain'])
  calls.length = 0
  await pipeline(topics).pipe(write).run({ store })
  assert.deepEqual(calls, ['topics', 'write tea', 'write rain'])

  await assert.rejects(drafting.run(), {
    message: 'step "topics" is cached, but run got no store',
  })

  // half an extraction is not an output
  let broken = true
  const pages: Extractor<number> = {
    name: 'pages',
    version: '1',
    async *extract() {
      yield 1
      if (broken) throw boom
      yield 2
    },
  }
  const paging = pipeline(pages, { cache: true })
  await assert.rejects(paging.run({ store }), { message: 'step "pages" failed: no network' })
  broken = false
  assert.deepEqual(await paging.run({ store }), [1, 2])

  // a step brings its own serializing and input hashing
  type Frame = { id: string; pixels: Uint8Array }
  const raw: Serializer<Frame> = {
    serialize: ({ id, pixels }) => new Uint8Array([id.charCodeAt(0), ...pixels]),
    deserialize: ([id, ...pixels]) => ({
      id: String.fromCharCode(id!),
      pixels: new Uint8Array(pixels),
    }),
  }
  const camera = (id: string, pixel: number): Extractor<Frame> => ({
    name: 'camera',
    version: pixel.toString(),
    serializer: raw,
    async *extract() {
      yield { id, pixels: new Uint8Array([pixel]) }
    },
  })
  const invert: Transformer<Frame, Frame> = {
    name: 'invert',
    version: '1',
    serializer: raw,
    hashInput: (frame) => frame.id,
    async transform({ id, pixels }) {
      calls.push(`invert ${id}`)
      return { id, pixels: pixels.map((p) => 255 - p) }
    },
  }
  calls.length = 0
  const inverted = [{ id: 'a', pixels: new Uint8Array([254]) }]
  for (const pixel of [1, 2]) {
    const film = pipeline(camera('a', pixel), { cache: true }).pipe(invert, { cache: true })
    assert.deepEqual(await film.run({ store }), inverted)
  }
  assert.deepEqual(calls, ['invert a'])

  // neither invalid nor cancelled output is stored
  const strings: StandardSchema = { '~standard': { ...text['~standard'] } }
  let answer: unknown = 1
  const ask: Transformer<string, any> = {
    name: 'ask',
    version: '1',
    outputSchema: strings,
    transform: async () => answer,
  }
  const asking = pipeline(topics, { cache: true }).pipe(ask, { cache: true })
  await assert.rejects(asking.run({ store }), /step "ask" failed: invalid output/)
  answer = 'yes'
  assert.deepEqual(await asking.run({ store }), ['yes', 'yes'])

  const controller = new AbortController()
  const feed: Extractor<number> = {
    name: 'feed',
    version: '1',
    async *extract(signal) {
      yield 1
      if (signal === controller.signal) controller.abort(boom)
      yield 2
    },
  }
  const feeding = pipeline(feed, { cache: true })
  await assert.rejects(
    feeding.run({ store, signal: controller.signal }),
    (error) => error === boom,
  )
  assert.deepEqual(await feeding.run({ store }), [1, 2])
  }
})
