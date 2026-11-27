import assert from 'node:assert/strict'
import { test } from 'node:test'
import { pipeline, type Extractor, type Transformer } from 'medallion-pipeline'

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
})
