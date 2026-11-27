import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  MemoryStore,
  pipeline,
  type Extractor,
  type Serializer,
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
})
