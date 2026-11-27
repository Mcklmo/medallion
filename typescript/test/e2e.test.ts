import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  listRuns,
  listVersions,
  MemoryStore,
  pipeline,
  restore,
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

  {
  // each real call answers differently, so versions can be told apart
  let call = 0
  const calls: string[] = []
  const subjects: Extractor<string> = {
    name: 'subjects',
    version: '1',
    async *extract() {
      calls.push('subjects')
      yield 'sun'
    },
  }
  const draft: Transformer<string, string> = {
    name: 'draft',
    version: '1',
    async transform(subject) {
      calls.push('draft')
      return `${subject} ${++call}`
    },
  }
  let offline = false
  const polish: Transformer<string, string> = {
    name: 'polish',
    version: '1',
    async transform(text) {
      if (offline) throw boom
      calls.push('polish')
      return `${text}!`
    },
  }
  const store = new MemoryStore()
  const writing = pipeline(subjects, { cache: true })
    .pipe(draft, { cache: true })
    .pipe(polish, { cache: true })
  assert.deepEqual(await writing.run({ store }), ['sun 1!'])

  // force from a step: earlier steps use their cache, it and all after it run
  calls.length = 0
  assert.deepEqual(await writing.run({ store, forceFrom: 'draft' }), ['sun 2!'])
  assert.deepEqual(calls, ['draft', 'polish'])
  calls.length = 0
  assert.deepEqual(await writing.run({ store }), ['sun 2!'])
  assert.deepEqual(calls, [])

  // a cached extractor can be forced too
  assert.deepEqual(await writing.run({ store, forceFrom: 'subjects' }), ['sun 3!'])
  assert.deepEqual(calls, ['subjects', 'draft', 'polish'])

  await assert.rejects(writing.run({ store, forceFrom: 'drafts' }), {
    message: 'no step "drafts" to force from',
  })

  // history: every run that got a store, oldest first
  const runs = await listRuns(store)
  assert.deepEqual(
    runs.map(({ status }) => status),
    ['succeeded', 'succeeded', 'succeeded', 'succeeded'],
  )
  const [first, second, cached] = runs.map(({ outputs }) =>
    outputs.map(({ step, number }) => `${step} ${number}`),
  )
  assert.deepEqual(first, ['subjects 1', 'draft 1', 'polish 1'])
  // a forced step with a new input starts that input's versions at 1
  assert.deepEqual(second, ['subjects 1', 'draft 2', 'polish 1'])
  assert.deepEqual(cached, second)
  for (const { startedAt, finishedAt, error } of runs) {
    assert.ok(Date.parse(startedAt) <= Date.parse(finishedAt))
    assert.equal(error, undefined)
  }

  // a failed run records its error and what it finished
  offline = true
  await assert.rejects(writing.run({ store, forceFrom: 'draft' }), { cause: boom })
  const failed = (await listRuns(store)).at(-1)!
  assert.equal(failed.status, 'failed')
  assert.equal(failed.error, 'step "polish" failed: no network')
  assert.deepEqual(
    failed.outputs.map(({ step, number }) => `${step} ${number}`),
    ['subjects 2', 'draft 4'],
  )
  // and the next run reuses that
  offline = false
  calls.length = 0
  assert.deepEqual(await writing.run({ store }), ['sun 4!'])
  assert.deepEqual(calls, ['polish'])

  const leave = new AbortController()
  leave.abort(new Error('user left'))
  await assert.rejects(writing.run({ store, signal: leave.signal }))
  const left = (await listRuns(store)).at(-1)!
  assert.deepEqual([left.status, left.error, left.outputs], ['cancelled', 'user left', []])

  // a history that cannot be written does not hide why the run failed
  class ReadOnly extends MemoryStore {
    override async upload(): Promise<void> {
      throw new Error('disk full')
    }
  }
  await assert.rejects(pipeline(subjects).run({ store: new ReadOnly() }), {
    message: 'disk full',
  })
  await assert.rejects(pipeline(download).run({ store: new ReadOnly() }), { cause: boom })

  // restore: an earlier version is the latest again, without running anything
  const drafted = await listVersions(store, draft)
  assert.deepEqual(
    drafted.map(({ number }) => number),
    [1, 2, 3, 4],
  )
  assert.deepEqual(drafted[0], (await listRuns(store))[0]!.outputs[1])
  calls.length = 0
  const restored = await restore(store, drafted[0]!)
  assert.deepEqual(restored, { ...drafted[0], number: 5 })
  assert.deepEqual(calls, [])
  assert.deepEqual(await listVersions(store, draft), [...drafted, restored])
  assert.deepEqual(await writing.run({ store }), ['sun 1!'])
  assert.deepEqual(calls, [])
  assert.deepEqual((await listRuns(store)).at(-1)!.outputs[1], restored)

  await assert.rejects(restore(store, { ...restored, number: 9 }), {
    message: 'step "draft" has no output version 9 for that key',
  })
  // a version of one step is not another's
  assert.deepEqual(await listVersions(store, { ...draft, version: '2' }), [])
  }
})
