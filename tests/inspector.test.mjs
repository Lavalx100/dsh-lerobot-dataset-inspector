import assert from 'node:assert/strict'
import { mkdtemp, mkdir, readFile, readdir, realpath, rm, stat, symlink, writeFile, copyFile, open } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'
import { apply } from '../src/index.mjs'
import { inspectDataset, validateToolArgs } from '../src/inspect.mjs'

const fixtureDir = fileURLToPath(new URL('./fixtures/', import.meta.url))
const hasHyparquet = await import('hyparquet').then(() => true, () => false)

const v2Episodes = [
  { episode_index: 0, length: 2, tasks: ['pick object'], dataset_from_index: 0, dataset_to_index: 2 },
  { episode_index: 1, length: 3, tasks: ['place object'], dataset_from_index: 2, dataset_to_index: 5 },
]

const v2Tasks = [
  { task_index: 0, task: 'pick object' },
  { task_index: 1, task: 'place object' },
]

function defaultInfo(version = 'v2.1') {
  return {
    codebase_version: version,
    robot_type: 'so101',
    fps: 30,
    total_episodes: 2,
    total_frames: 5,
    total_tasks: 2,
    total_videos: 1,
    features: {
      'observation.state': { dtype: 'float32', shape: [3] },
      'observation.images.front': { dtype: 'video', shape: [3, 480, 640] },
    },
  }
}

async function temporaryDataset(t) {
  const parent = await mkdtemp(path.join(tmpdir(), 'dsh-lerobot-test-'))
  const root = path.join(parent, 'dataset')
  await mkdir(root)
  t.after(() => rm(parent, { recursive: true, force: true }))
  return { parent, root }
}

async function writeJson(filePath, value) {
  await mkdir(path.dirname(filePath), { recursive: true })
  await writeFile(filePath, JSON.stringify(value))
}

async function writeJsonl(filePath, rows) {
  await mkdir(path.dirname(filePath), { recursive: true })
  await writeFile(filePath, rows.map(row => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : ''))
}

async function createV2(root, options = {}) {
  await writeJson(path.join(root, 'meta/info.json'), options.info ?? defaultInfo())
  if (options.episodesText !== undefined) {
    await mkdir(path.join(root, 'meta'), { recursive: true })
    await writeFile(path.join(root, 'meta/episodes.jsonl'), options.episodesText)
  } else {
    await writeJsonl(path.join(root, 'meta/episodes.jsonl'), options.episodes ?? v2Episodes)
  }
  if (options.tasks !== false) await writeJsonl(path.join(root, 'meta/tasks.jsonl'), options.tasks ?? v2Tasks)
  if (options.stats !== false) {
    await writeJson(path.join(root, 'meta/stats.json'), {
      'observation.state': { min: [-1, -1, -1], mean: [0, 0, 0], std: [1, 1, 1], max: [1, 1, 1] },
    })
  }
  await mkdir(path.join(root, 'data/chunk-000'), { recursive: true })
  await writeFile(path.join(root, 'data/chunk-000/file-000.parquet'), Buffer.from([0, 1, 2, 3]))
  await mkdir(path.join(root, 'videos/observation.images.front/chunk-000'), { recursive: true })
  await writeFile(path.join(root, 'videos/observation.images.front/chunk-000/file-000.mp4'), Buffer.from([4, 5, 6]))
}

function createTestFs() {
  const calls = { readBytes: [], readRanges: [], listDirs: [] }

  function targetFor(targetPath) {
    const absolute = path.resolve(targetPath)
    return { targetKey: absolute, displayPath: absolute, path: absolute }
  }

  const fs = {
    calls,
    async resolve(input, options) {
      options?.signal?.throwIfAborted()
      try {
        return targetFor(await realpath(input))
      } catch (error) {
        if (error.code === 'ENOENT') return targetFor(input)
        throw error
      }
    },
    async stat(target, signal) {
      signal?.throwIfAborted()
      try {
        const entry = await stat(target.path)
        return { type: entry.isFile() ? 'file' : entry.isDirectory() ? 'directory' : 'other', size: entry.size }
      } catch (error) {
        if (error.code === 'ENOENT') return undefined
        throw error
      }
    },
    contains(parent, child) {
      const relative = path.relative(parent.path, child.path)
      return relative === '' || (!relative.startsWith(`..${path.sep}`) && relative !== '..' && !path.isAbsolute(relative))
    },
    async listDir(target, signal) {
      signal?.throwIfAborted()
      calls.listDirs.push(target.path)
      const names = (await readdir(target.path)).sort((a, b) => a.localeCompare(b))
      const entries = []
      for (const name of names) {
        const entryPath = path.join(target.path, name)
        let resolvedPath
        try {
          resolvedPath = await realpath(entryPath)
        } catch {
          resolvedPath = entryPath
        }
        let info
        try {
          info = await stat(entryPath)
        } catch {
          info = undefined
        }
        entries.push({
          name,
          type: info?.isFile() ? 'file' : info?.isDirectory() ? 'directory' : 'other',
          size: info?.isFile() ? info.size : undefined,
          target: targetFor(resolvedPath),
        })
      }
      return entries
    },
    async readBytes(target, signal, maxBytes) {
      signal?.throwIfAborted()
      calls.readBytes.push(target.path)
      const bytes = await readFile(target.path)
      if (bytes.byteLength > maxBytes) throw new Error('test filesystem byte cap exceeded')
      return bytes
    },
    async readByteRange(target, range, signal) {
      signal?.throwIfAborted()
      calls.readRanges.push({ path: target.path, ...range })
      const handle = await open(target.path, 'r')
      try {
        const bytes = Buffer.alloc(range.length)
        const { bytesRead } = await handle.read(bytes, 0, range.length, range.offset)
        return bytes.subarray(0, bytesRead)
      } finally {
        await handle.close()
      }
    },
  }
  return fs
}

async function inspect(root, episodeIndex) {
  const fs = createTestFs()
  const value = await inspectDataset(fs, root, episodeIndex)
  return { fs, value }
}

function schemaMatches(schema, value) {
  if (schema.oneOf) return schema.oneOf.filter(branch => schemaMatches(branch, value)).length === 1
  if (schema.enum && !schema.enum.includes(value)) return false
  if (!schema.type) return true
  if (schema.type === 'null') return value === null
  if (schema.type === 'object') {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) return false
    if (schema.required?.some(key => !Object.hasOwn(value, key))) return false
    if (schema.additionalProperties === false && Object.keys(value).some(key => !Object.hasOwn(schema.properties ?? {}, key))) return false
    return Object.entries(schema.properties ?? {}).every(([key, child]) => !Object.hasOwn(value, key) || schemaMatches(child, value[key]))
  }
  if (schema.type === 'array') return Array.isArray(value) && value.every(item => schema.items === undefined || schemaMatches(schema.items, item))
  if (schema.type === 'integer') return Number.isSafeInteger(value)
  if (schema.type === 'number') return typeof value === 'number' && Number.isFinite(value)
  return typeof value === schema.type
}

test('v2.1 JSONL inspection summarizes features, tasks, episodes, stats and assets without opening payloads', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root)
  const { fs, value } = await inspect(root, 1)

  assert.equal(value.status, 'ok')
  assert.equal(value.metadata.codebase_version, 'v2.1')
  assert.deepEqual(value.episodes.requested_episode, {
    episode_index: 1,
    length: 3,
    tasks: ['place object'],
    dataset_from_index: 2,
    dataset_to_index: 5,
    episode_statistics_available: false,
    episode_statistics_complete: false,
    episode_statistics: null,
  })
  assert.deepEqual(value.episodes.task_counts, [
    { task: 'pick object', episode_count: 1 },
    { task: 'place object', episode_count: 1 },
  ])
  assert.equal(value.statistics.available, true)
  assert.equal(value.statistics.complete, true)
  assert.equal(value.storage.data_parquet_files, 1)
  assert.equal(value.storage.video_files, 1)
  assert.equal(value.storage.data_parquet_bytes, 4)
  assert.equal(value.storage.video_bytes, 3)
  assert.equal(value.read_stats.bytes_read, value.read_stats.text_bytes_read + value.read_stats.parquet_bytes_read)
  assert.ok(fs.calls.readRanges.every(({ path: readPath }) => readPath.includes('/meta/')))
  assert.ok(fs.calls.readBytes.every(readPath => readPath.endsWith('.json')))
  assert.equal(fs.calls.readRanges.some(({ path: readPath }) => readPath.includes('/data/')), false)
  assert.equal(fs.calls.readBytes.some(readPath => readPath.endsWith('.mp4')), false)
})

test('v2.0 accepts CRLF manifests and a final record without a newline', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root, { info: defaultInfo('v2.0') })
  const episodes = [
    { episode_index: 0, length: 2, tasks: [0], dataset_from_index: 0, dataset_to_index: 2 },
    { episode_index: 1, length: 3, tasks: [1], dataset_from_index: 2, dataset_to_index: 5 },
  ]
  await writeFile(path.join(root, 'meta/episodes.jsonl'), episodes.map(JSON.stringify).join('\r\n'))
  await writeFile(path.join(root, 'meta/tasks.jsonl'), v2Tasks.map(JSON.stringify).join('\r\n'))

  const { value } = await inspect(root)
  assert.equal(value.status, 'ok')
  assert.equal(value.episodes.manifest_complete, true)
  assert.deepEqual(value.episodes.task_counts, [
    { task: 'pick object', episode_count: 1 },
    { task: 'place object', episode_count: 1 },
  ])
})

test('long feature shapes and statistics vectors are returned as incomplete', async t => {
  const { root } = await temporaryDataset(t)
  const info = defaultInfo()
  info.features['observation.state'].shape = Array.from({ length: 20 }, (_, index) => index + 1)
  await createV2(root, { info })
  await writeJson(path.join(root, 'meta/stats.json'), {
    'observation.state': {
      min: Array.from({ length: 20 }, (_, index) => -index),
      mean: Array.from({ length: 20 }, (_, index) => index),
      std: Array.from({ length: 20 }, () => 1),
      max: Array.from({ length: 20 }, (_, index) => index + 1),
    },
  })

  const { value } = await inspect(root)
  assert.equal(value.status, 'partial')
  assert.equal(value.features_complete, false)
  assert.equal(value.features[0].shape.length, 16)
  assert.equal(value.statistics.complete, false)
  assert.equal(value.statistics.features[0].min.length, 16)
  assert.ok(value.findings.some(finding => finding.code === 'feature_shapes_truncated'))
  assert.ok(value.findings.some(finding => finding.code === 'statistics_values_truncated'))
})

test('v3.0 inspection reads actual Parquet task and episode manifests', { skip: !hasHyparquet && 'install hyparquet to run the v3 integration test' }, async t => {
  const { root } = await temporaryDataset(t)
  await writeJson(path.join(root, 'meta/info.json'), defaultInfo('v3.0'))
  await mkdir(path.join(root, 'meta/episodes/chunk-000'), { recursive: true })
  await copyFile(path.join(fixtureDir, 'episodes-v3.parquet'), path.join(root, 'meta/episodes/chunk-000/file-000.parquet'))
  await copyFile(path.join(fixtureDir, 'tasks-v3.parquet'), path.join(root, 'meta/tasks.parquet'))
  await mkdir(path.join(root, 'data/chunk-000'), { recursive: true })
  await writeFile(path.join(root, 'data/chunk-000/file-000.parquet'), Buffer.from([1, 2, 3]))
  await mkdir(path.join(root, 'videos/camera/chunk-000'), { recursive: true })
  await writeFile(path.join(root, 'videos/camera/chunk-000/file-000.mp4'), Buffer.from([4]))

  const { fs, value } = await inspect(root, 0)
  assert.equal(value.status, 'ok')
  assert.deepEqual(value.tasks.names, [
    { task_index: 0, task: 'pick object' },
    { task_index: 1, task: 'place object' },
  ])
  assert.equal(value.episodes.manifest_row_count, 2)
  assert.equal(value.episodes.frame_count_observed, 5)
  assert.deepEqual(value.episodes.requested_episode, {
    episode_index: 0,
    length: 2,
    tasks: ['pick object'],
    dataset_from_index: 0,
    dataset_to_index: 2,
    episode_statistics_available: true,
    episode_statistics_complete: true,
    episode_statistics: [{
      name: 'observation.state',
      min: [-1, -2],
      mean: [0, 1],
      std: [1, 1.5],
      max: [1, 2],
      count: [2, 2],
    }],
  })
  assert.equal(value.statistics.available, false)
  assert.ok(fs.calls.readRanges.some(({ path: readPath }) => readPath.endsWith('meta/tasks.parquet')))
  assert.ok(fs.calls.readRanges.some(({ path: readPath }) => readPath.endsWith('meta/episodes/chunk-000/file-000.parquet')))
  assert.ok(fs.calls.readRanges.every(({ path: readPath }) => readPath.includes('/meta/')))
  assert.ok(fs.calls.readRanges.every(({ length }) => length <= 8 * 1024 * 1024))
})

test('depth-limited asset and episode scans are explicitly incomplete', async t => {
  const assetDataset = await temporaryDataset(t)
  await createV2(assetDataset.root)
  await mkdir(path.join(assetDataset.root, 'data/chunk-000/deep/further'), { recursive: true })
  await writeFile(path.join(assetDataset.root, 'data/chunk-000/deep/further/hidden.parquet'), Buffer.from([8]))
  const assetResult = await inspect(assetDataset.root)
  assert.equal(assetResult.value.storage.data_parquet_files, 1)
  assert.equal(assetResult.value.storage.inventory_complete, false)
  assert.ok(assetResult.value.findings.some(finding => finding.code === 'inventory_depth_limit'))

  if (!hasHyparquet) return
  const episodeDataset = await temporaryDataset(t)
  await writeJson(path.join(episodeDataset.root, 'meta/info.json'), defaultInfo('v3.0'))
  await mkdir(path.join(episodeDataset.root, 'meta/episodes/chunk-000/deep/nested/deeper'), { recursive: true })
  await copyFile(path.join(fixtureDir, 'episodes-v3.parquet'), path.join(episodeDataset.root, 'meta/episodes/chunk-000/file-000.parquet'))
  await copyFile(path.join(fixtureDir, 'episodes-v3.parquet'), path.join(episodeDataset.root, 'meta/episodes/chunk-000/deep/nested/deeper/file-001.parquet'))
  await copyFile(path.join(fixtureDir, 'tasks-v3.parquet'), path.join(episodeDataset.root, 'meta/tasks.parquet'))
  await mkdir(path.join(episodeDataset.root, 'data/chunk-000'), { recursive: true })
  await writeFile(path.join(episodeDataset.root, 'data/chunk-000/file-000.parquet'), Buffer.from([1]))
  const episodeResult = await inspect(episodeDataset.root)
  assert.equal(episodeResult.value.episodes.records_scanned, 2)
  assert.equal(episodeResult.value.episodes.manifest_complete, false)
  assert.ok(episodeResult.value.findings.some(finding => finding.code === 'inventory_depth_limit'))
})

test('v3.0 reports partial results when its declared Parquet reader is unavailable', { skip: hasHyparquet }, async t => {
  const { root } = await temporaryDataset(t)
  await writeJson(path.join(root, 'meta/info.json'), defaultInfo('v3.0'))
  await mkdir(path.join(root, 'meta/episodes/chunk-000'), { recursive: true })
  await copyFile(path.join(fixtureDir, 'episodes-v3.parquet'), path.join(root, 'meta/episodes/chunk-000/file-000.parquet'))
  await copyFile(path.join(fixtureDir, 'tasks-v3.parquet'), path.join(root, 'meta/tasks.parquet'))
  const { value } = await inspect(root)
  assert.equal(value.status, 'partial')
  assert.ok(value.findings.some(finding => finding.code === 'episodes_parquet_unreadable'))
  assert.ok(value.findings.some(finding => finding.code === 'tasks_manifest_unreadable'))
})

test('unsupported versions are reported and their layout is not guessed', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root, { info: defaultInfo('v9.0') })
  const { value } = await inspect(root)
  assert.equal(value.status, 'partial')
  assert.equal(value.episodes.manifest_complete, false)
  assert.ok(value.findings.some(finding => finding.code === 'version_unsupported'))
})

test('a missing requested episode is distinguished from a bounded search', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root)
  const { value } = await inspect(root, 99)
  assert.equal(value.status, 'issues')
  assert.ok(value.findings.some(finding => finding.code === 'episode_not_found'))
})

test('malformed JSONL remains partial and preserves valid rows scanned before the error', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root, { episodesText: '{"episode_index":0,"length":2}\n{bad}\n' })
  const { value } = await inspect(root)
  assert.equal(value.status, 'partial')
  assert.equal(value.episodes.records_valid, 1)
  assert.equal(value.episodes.manifest_complete, false)
  assert.ok(value.findings.some(finding => finding.code === 'jsonl_row_invalid'))
})

test('semantically invalid episode rows do not count as a complete manifest', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root, { episodes: [{ episode_index: 0, length: -1 }] })
  const { value } = await inspect(root)
  assert.equal(value.episodes.records_scanned, 1)
  assert.equal(value.episodes.records_valid, 0)
  assert.equal(value.episodes.manifest_complete, false)
  assert.ok(value.findings.some(finding => finding.code === 'episode_row_invalid'))
})

test('invalid UTF-8 and missing manifests produce explicit partial results', async t => {
  const invalidUtf8 = await temporaryDataset(t)
  await createV2(invalidUtf8.root)
  await writeFile(path.join(invalidUtf8.root, 'meta/episodes.jsonl'), Buffer.from([0xff, 0xfe]))
  const invalidResult = await inspect(invalidUtf8.root)
  assert.equal(invalidResult.value.episodes.manifest_complete, false)
  assert.ok(invalidResult.value.findings.some(finding => finding.code === 'jsonl_read_failed'))

  const missing = await temporaryDataset(t)
  await createV2(missing.root)
  await rm(path.join(missing.root, 'meta/episodes.jsonl'))
  const missingResult = await inspect(missing.root, 0)
  assert.equal(missingResult.value.status, 'partial')
  assert.ok(missingResult.value.findings.some(finding => finding.code === 'episode_search_incomplete'))
})

test('JSONL reads stop at the per-manifest byte budget', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root, { tasks: false, stats: false })
  const filePath = path.join(root, 'meta/episodes.jsonl')
  const largeManifest = Buffer.alloc(8 * 1024 * 1024 + 1, 10)
  await writeFile(filePath, largeManifest)
  const canonicalFilePath = await realpath(filePath)
  const { fs, value } = await inspect(root)
  const bytesRead = fs.calls.readRanges
    .filter(call => call.path === canonicalFilePath)
    .reduce((sum, call) => sum + call.length, 0)
  assert.equal(bytesRead, 8 * 1024 * 1024)
  assert.equal(value.episodes.manifest_complete, false)
  assert.equal(value.status, 'partial')
})

test('overlong JSONL rows are rejected without parsing the rest of the file', async t => {
  const { root } = await temporaryDataset(t)
  const hugeString = 'x'.repeat(1024 * 1024 + 20)
  await createV2(root, { episodesText: `${JSON.stringify({ payload: hugeString })}\n` })
  const { fs, value } = await inspect(root)
  assert.ok(value.findings.some(finding => finding.code === 'jsonl_line_too_large'))
  assert.equal(value.episodes.manifest_complete, false)
  assert.ok(fs.calls.readRanges.filter(call => call.path.endsWith('episodes.jsonl')).length < 32)
})

test('missing and invalid info files produce structured invalid results', async t => {
  const missing = await temporaryDataset(t)
  await mkdir(path.join(missing.root, 'meta'))
  const missingResult = await inspect(missing.root)
  assert.equal(missingResult.value.status, 'invalid')
  assert.ok(missingResult.value.findings.some(finding => finding.code === 'info_missing'))

  const invalid = await temporaryDataset(t)
  await mkdir(path.join(invalid.root, 'meta'))
  await writeFile(path.join(invalid.root, 'meta/info.json'), '{not json')
  const invalidResult = await inspect(invalid.root)
  assert.equal(invalidResult.value.status, 'invalid')
  assert.ok(invalidResult.value.findings.some(finding => finding.code === 'text_file_invalid'))
})

test('oversized JSON metadata is rejected before its content is read', async t => {
  const { root } = await temporaryDataset(t)
  await mkdir(path.join(root, 'meta'))
  const infoPath = path.join(root, 'meta/info.json')
  await writeFile(infoPath, Buffer.alloc(2 * 1024 * 1024 + 1, 32))
  const { fs, value } = await inspect(root)
  assert.equal(value.status, 'invalid')
  assert.equal(fs.calls.readBytes.includes(await realpath(infoPath)), false)
  assert.ok(value.findings.some(finding => finding.code === 'text_file_too_large'))
})

test('outside-root symbolic links are skipped before any file read', async t => {
  const { parent, root } = await temporaryDataset(t)
  await mkdir(path.join(root, 'meta'))
  const outside = path.join(parent, 'outside.json')
  await writeFile(outside, JSON.stringify(defaultInfo()))
  await symlink(outside, path.join(root, 'meta/info.json'))
  const { fs, value } = await inspect(root)
  assert.equal(value.status, 'invalid')
  assert.equal(fs.calls.readBytes.includes(outside), false)
  assert.ok(value.findings.some(finding => finding.code === 'outside_root_entry_skipped'))
})

test('missing asset sizes make byte totals explicitly incomplete', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root)
  const fs = createTestFs()
  const original = fs.listDir
  fs.listDir = async target => (await original(target)).map(entry => entry.name.endsWith('.mp4') ? { ...entry, size: undefined } : entry)
  const value = await inspectDataset(fs, root)
  assert.equal(value.storage.video_files, 1)
  assert.equal(value.storage.sizes_complete, false)
  assert.equal(value.status, 'partial')
  assert.ok(value.findings.some(finding => finding.code === 'storage_sizes_incomplete'))
})

test('truncated directory inventory reports lower-bound file counts', async t => {
  const { root } = await temporaryDataset(t)
  await writeJson(path.join(root, 'meta/info.json'), { ...defaultInfo(), total_episodes: 0, total_frames: 0 })
  await writeJsonl(path.join(root, 'meta/episodes.jsonl'), [])
  await writeJsonl(path.join(root, 'meta/tasks.jsonl'), v2Tasks)
  await mkdir(path.join(root, 'data/chunk-000'), { recursive: true })
  for (let index = 0; index < 257; index += 1) {
    await writeFile(path.join(root, 'data/chunk-000', `file-${String(index).padStart(3, '0')}.parquet`), Buffer.alloc(0))
  }
  const { value } = await inspect(root)
  assert.equal(value.storage.inventory_complete, false)
  assert.equal(value.status, 'partial')
  assert.ok(value.findings.some(finding => finding.code === 'directory_listing_truncated'))
})

test('integer overflow in observed frame and asset totals is capped and marked incomplete', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root, {
    episodes: [
      { episode_index: 0, length: Number.MAX_SAFE_INTEGER, task_index: 0 },
      { episode_index: 1, length: 1, task_index: 1 },
    ],
  })
  await writeFile(path.join(root, 'data/chunk-000/file-001.parquet'), Buffer.from([1]))
  const fs = createTestFs()
  const original = fs.listDir
  fs.listDir = async (target, signal) => (await original(target, signal)).map(entry => entry.name.endsWith('.parquet')
    ? { ...entry, size: Number.MAX_SAFE_INTEGER }
    : entry)
  const value = await inspectDataset(fs, root)
  assert.equal(value.episodes.frame_count_observed, Number.MAX_SAFE_INTEGER)
  assert.equal(value.episodes.manifest_complete, false)
  assert.ok(value.findings.some(finding => finding.code === 'episode_frame_count_overflow'))
  assert.equal(value.storage.data_parquet_bytes, Number.MAX_SAFE_INTEGER)
  assert.equal(value.storage.sizes_complete, false)
})

test('path and argument guards reject invalid inputs and honor cancellation', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root)
  assert.throws(() => validateToolArgs(null), /Arguments must be an object/)
  assert.throws(() => validateToolArgs({ dataset_path: 'x', extra: true }), /Only dataset_path and episode_index/)
  assert.throws(() => validateToolArgs({ dataset_path: ' ' }), /non-empty path/)
  assert.throws(() => validateToolArgs({ dataset_path: 'x\0y' }), /NUL/)
  assert.throws(() => validateToolArgs({ dataset_path: 'x', episode_index: -1 }), /non-negative/)
  assert.throws(() => validateToolArgs({ dataset_path: 'x', episode_index: Number.MAX_SAFE_INTEGER + 1 }), /non-negative/)
  assert.throws(() => validateToolArgs({ dataset_path: 'x'.repeat(4097) }), /4096/)
  const controller = new AbortController()
  controller.abort()
  await assert.rejects(inspectDataset(createTestFs(), root, undefined, controller.signal), { name: 'AbortError' })
  const midReadController = new AbortController()
  const midReadFs = createTestFs()
  const readRange = midReadFs.readByteRange
  midReadFs.readByteRange = async (target, range, signal) => {
    const bytes = await readRange(target, range, signal)
    midReadController.abort()
    return bytes
  }
  await assert.rejects(inspectDataset(midReadFs, root, undefined, midReadController.signal), { name: 'AbortError' })
  const fs = createTestFs()
  await assert.rejects(inspectDataset(fs, path.join(root, 'missing')), /does not exist/)
  await assert.rejects(inspectDataset(fs, path.join(root, 'meta/info.json')), /must name a directory/)
})

test('registered tool has a strict output contract and returns a JSON render', async t => {
  const { root } = await temporaryDataset(t)
  await createV2(root)
  let definition
  apply({ fs: createTestFs(), tools: { register(value) { definition = value; return () => {} } } })
  assert.equal(definition.name, 'lerobot_inspect_dataset')
  assert.deepEqual(definition.parameters.required, ['dataset_path'])
  const args = { dataset_path: root, episode_index: 0 }
  const value = await definition.execute(args, { signal: undefined })
  assert.equal(schemaMatches(definition.output.schema, value), true)
  const [rendered] = definition.output.render(args, value)
  assert.equal(rendered.type, 'text')
  assert.deepEqual(JSON.parse(rendered.text), value)
  assert.equal(schemaMatches(definition.parameters, { dataset_path: root }), true)
  assert.equal(schemaMatches(definition.parameters, { dataset_path: root, unexpected: true }), false)
})

test('package metadata points to a standalone DSH bundle entry point', async () => {
  const manifest = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'))
  const patch = await readFile(new URL('../cordis.patch.yml', import.meta.url), 'utf8')
  assert.equal(manifest.name, 'dsh-lerobot-dataset-inspector')
  assert.equal(manifest.main, './src/index.mjs')
  assert.ok(manifest.files.includes('src'))
  assert.ok(manifest.files.includes('README.zh.md'))
  assert.equal(manifest.dsh.bundle.patch, './cordis.patch.yml')
  assert.match(patch, /name: dsh-lerobot-dataset-inspector/)
})
