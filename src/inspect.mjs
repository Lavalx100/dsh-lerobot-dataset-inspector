const MAX_TEXT_FILE_BYTES = 2 * 1024 * 1024
const MAX_JSONL_SCAN_BYTES = 8 * 1024 * 1024
const MAX_JSONL_CHUNK_BYTES = 64 * 1024
const MAX_JSONL_ROWS = 50_000
const MAX_PARQUET_FILES = 128
const MAX_PARQUET_FILE_BYTES = 8 * 1024 * 1024
const MAX_PARQUET_RANGE_BYTES = 8 * 1024 * 1024
const MAX_TOTAL_READ_BYTES = 32 * 1024 * 1024
const MAX_EPISODE_ROWS = 50_000
const MAX_TASK_ROWS = 5_000
const MAX_DIRECTORY_ENTRIES = 256
const MAX_INVENTORY_ENTRIES = 8_192
const MAX_INVENTORY_DEPTH = 3
const MAX_FEATURES = 256
const MAX_TASK_NAMES = 256
const MAX_STATS_VALUES = 16
const MAX_TASKS_PER_EPISODE = 64
const MAX_JSONL_LINE_CHARS = 1024 * 1024
const MAX_OUTPUT_STRING_CHARS = 512

const EPISODE_COLUMNS = [
  'episode_index',
  'length',
  'tasks',
  'task_index',
  'dataset_from_index',
  'dataset_to_index',
]
const EPISODE_STAT_COLUMNS = ['min', 'mean', 'std', 'max', 'count']
const TASK_COLUMNS = ['task_index', 'task', '__index_level_0__', 'text']

class ReadBudgetExceeded extends Error {
  constructor(message) {
    super(message)
    this.name = 'ReadBudgetExceeded'
  }
}

class Inspection {
  constructor(fs, root, signal) {
    this.fs = fs
    this.root = root
    this.signal = signal
    this.findings = []
    this.metadata = undefined
    this.truncatedListings = 0
    this.skippedEntries = 0
    this.budget = {
      reservedBytes: 0,
      bytesRead: 0,
      textBytesRead: 0,
      parquetBytesRead: 0,
      rangeRequests: 0,
    }
    this.taskNames = new Map()
    this.taskNamesComplete = false
    this.episode = {
      declared_count: null,
      manifest_row_count: null,
      records_scanned: 0,
      records_valid: 0,
      frame_count_observed: 0,
      frame_count_overflow: false,
      min_length: null,
      max_length: null,
      task_counts: new Map(),
      manifest_complete: false,
      requested_episode: null,
      requested_episode_found: false,
      requested_episode_index: null,
    }
    this.statistics = { available: false, features: [], complete: false }
    this.storage = {
      data_parquet_files: 0,
      data_parquet_bytes: 0,
      video_files: 0,
      video_bytes: 0,
      inventory_complete: true,
      sizes_complete: true,
    }
    this.features = []
    this.featuresComplete = true
    this.info = null
  }

  addFinding(severity, code, message) {
    if (this.findings.length < 100 && !this.findings.some(finding => finding.code === code && finding.message === message)) {
      this.findings.push({ severity, code, message })
    }
  }

  async run(episodeIndex) {
    this.episode.requested_episode_index = episodeIndex ?? null

    const meta = await this.findChild(this.root, 'meta')
    if (meta !== undefined && meta.type !== 'directory') {
      this.addFinding('error', 'meta_not_directory', 'The dataset meta entry is not a directory.')
    }
    const infoEntry = meta?.type === 'directory' ? await this.findChild(meta.target, 'info.json') : undefined
    if (infoEntry === undefined) {
      this.addFinding('error', 'info_missing', 'The dataset is missing meta/info.json.')
    } else {
      this.info = await this.readJson(infoEntry, 'info.json', true)
      if (this.info !== null) this.readInfoSummary()
    }

    if (meta?.type === 'directory' && this.info !== null) {
      await this.readTasks(meta.target)
      await this.readEpisodes(meta.target, episodeIndex)
      await this.readStatistics(meta.target)
    } else if (this.info !== null) {
      await this.readStatistics(meta?.target)
    }

    await this.readStorageInventory()
    this.checkConsistency()
    return this.toValue()
  }

  async findChild(parent, name) {
    const entries = await this.fs.listDir(parent.target ?? parent, this.signal)
    const entry = entries.slice(0, MAX_DIRECTORY_ENTRIES).find(candidate => candidate.name === name)
    if (entries.length > MAX_DIRECTORY_ENTRIES) {
      this.truncatedListings += 1
      this.storage.inventory_complete = false
      this.addFinding('warning', 'directory_listing_truncated', 'A directory contains more entries than the inspection limit; file counts may be incomplete.')
    }
    if (entry === undefined) return undefined
    if (!this.fs.contains(this.root, entry.target)) {
      this.skippedEntries += 1
      this.storage.inventory_complete = false
      this.addFinding('warning', 'outside_root_entry_skipped', 'A listed path resolved outside the selected dataset directory and was skipped.')
      return undefined
    }
    return entry
  }

  async listChildren(target) {
    const entries = await this.fs.listDir(target, this.signal)
    const contained = []
    for (const entry of entries.slice(0, MAX_DIRECTORY_ENTRIES)) {
      if (!this.fs.contains(this.root, entry.target)) {
        this.skippedEntries += 1
        this.storage.inventory_complete = false
        this.addFinding('warning', 'outside_root_entry_skipped', 'A listed path resolved outside the selected dataset directory and was skipped.')
        continue
      }
      contained.push(entry)
    }
    if (entries.length > MAX_DIRECTORY_ENTRIES) {
      this.truncatedListings += 1
      this.storage.inventory_complete = false
      this.addFinding('warning', 'directory_listing_truncated', 'A directory contains more entries than the inspection limit; file counts may be incomplete.')
    }
    return contained
  }

  async getFileSize(entry, label) {
    if (entry.type !== 'file') {
      this.addFinding('warning', 'file_not_regular', `${label} is not a regular file.`)
      return undefined
    }
    const info = await this.fs.stat(entry.target, this.signal)
    if (info?.type !== 'file') {
      this.addFinding('warning', 'file_unavailable', `${label} is not a readable regular file.`)
      return undefined
    }
    const size = safeInteger(info.size ?? entry.size)
    if (size === null || size < 0) {
      this.addFinding('warning', 'file_size_unknown', `The size of ${label} is unavailable, so its contents were skipped.`)
      return undefined
    }
    return size
  }

  async readJson(entry, label, required = false) {
    if (entry.type !== 'file') {
      this.addFinding(required ? 'error' : 'warning', 'file_not_regular', `${label} is not a regular file.`)
      return null
    }
    const size = await this.getFileSize(entry, label)
    if (size === undefined) return null
    if (size > MAX_TEXT_FILE_BYTES) {
      this.addFinding(required ? 'error' : 'warning', 'text_file_too_large', `${label} exceeds the ${MAX_TEXT_FILE_BYTES}-byte metadata limit.`)
      return null
    }
    try {
      this.reserveBytes(size)
    } catch {
      this.addFinding(required ? 'error' : 'warning', 'read_budget_exceeded', `${label} was skipped because the total metadata read limit was reached.`)
      return null
    }
    let bytes
    try {
      bytes = await this.fs.readBytes(entry.target, this.signal, MAX_TEXT_FILE_BYTES)
    } catch {
      this.signal?.throwIfAborted()
      this.addFinding(required ? 'error' : 'warning', 'text_file_unreadable', `${label} could not be read.`)
      return null
    }
    this.recordBytes(bytes.byteLength, 'text')
    try {
      const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes)
      const value = JSON.parse(text)
      if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new TypeError('expected a JSON object')
      return value
    } catch {
      this.addFinding(required ? 'error' : 'warning', 'text_file_invalid', `${label} is not a valid UTF-8 JSON object.`)
      return null
    }
  }

  reserveBytes(length) {
    if (this.budget.reservedBytes + length > MAX_TOTAL_READ_BYTES) {
      throw new ReadBudgetExceeded('The dataset metadata read budget was reached.')
    }
    this.budget.reservedBytes += length
  }

  recordBytes(length, kind) {
    this.budget.bytesRead += length
    if (kind === 'text') this.budget.textBytesRead += length
    else this.budget.parquetBytesRead += length
  }

  readInfoSummary() {
    const info = this.info
    const version = stringOrNull(info.codebase_version)
    const features = info.features
    if (features === null || typeof features !== 'object' || Array.isArray(features)) {
      this.addFinding('warning', 'features_missing', 'meta/info.json does not contain a feature map.')
    } else {
      const entries = Object.entries(features)
      this.featuresComplete = entries.length <= MAX_FEATURES
      this.features = entries.slice(0, MAX_FEATURES).map(([name, feature]) => ({
        name: name.slice(0, MAX_OUTPUT_STRING_CHARS),
        dtype: stringOrNull(feature?.dtype),
        shape: Array.isArray(feature?.shape)
          ? feature.shape.slice(0, MAX_STATS_VALUES).map(value => nonNegativeIntegerOrNull(value)).filter(value => value !== null)
          : [],
        video: feature?.dtype === 'video',
      }))
      if (entries.length > MAX_FEATURES) this.addFinding('warning', 'features_truncated', `Only the first ${MAX_FEATURES} feature definitions are included.`)
      if (entries.some(([, feature]) => Array.isArray(feature?.shape) && feature.shape.length > MAX_STATS_VALUES)) {
        this.featuresComplete = false
        this.addFinding('warning', 'feature_shapes_truncated', `Feature shapes include at most ${MAX_STATS_VALUES} dimensions.`)
      }
    }
    this.metadata = {
      codebase_version: version,
      robot_type: stringOrNull(info.robot_type),
      fps: safeNumber(info.fps),
      total_episodes: nonNegativeIntegerOrNull(info.total_episodes),
      total_frames: nonNegativeIntegerOrNull(info.total_frames),
      total_tasks: nonNegativeIntegerOrNull(info.total_tasks),
      total_videos: nonNegativeIntegerOrNull(info.total_videos),
    }
    this.episode.declared_count = this.metadata.total_episodes
    if (!isSupportedVersion(version)) {
      this.addFinding('warning', 'version_unsupported', `Dataset version ${version ?? '(missing)'} is outside the supported v2.0, v2.1, and v3.0 formats.`)
    }
  }

  async readTasks(metaTarget) {
    const tasksParquet = await this.findChild(metaTarget, 'tasks.parquet')
    if (tasksParquet !== undefined) {
      await this.readParquetTasks(tasksParquet)
      return
    }
    const tasksJsonl = await this.findChild(metaTarget, 'tasks.jsonl')
    if (tasksJsonl !== undefined) {
      await this.readJsonlTasks(tasksJsonl)
      return
    }
    if ((this.metadata.total_tasks ?? 0) > 0) {
      this.addFinding('warning', 'tasks_manifest_missing', 'meta/tasks.parquet or meta/tasks.jsonl is missing.')
    } else {
      this.taskNamesComplete = true
    }
  }

  async readJsonlTasks(entry) {
    const size = await this.getFileSize(entry, 'meta/tasks.jsonl')
    if (size === undefined) return
    let validRows = 0
    const state = await this.scanJsonLines(entry, size, MAX_TASK_ROWS, 'tasks', row => {
      const task = firstString(row, ['task', 'text', '__index_level_0__'])
      const index = nonNegativeIntegerOrNull(row.task_index ?? row.__index_level_0__)
      if (task !== null && index !== null) {
        validRows += 1
        this.taskNames.set(index, task)
      }
    })
    this.taskNamesComplete = state.complete && validRows === state.rowsScanned
    if (!this.taskNamesComplete) this.addFinding('warning', 'tasks_manifest_incomplete', 'The task manifest exceeded a scan limit or contains invalid rows.')
  }

  async readParquetTasks(entry) {
    try {
      const size = await this.getFileSize(entry, 'meta/tasks.parquet')
      if (size === undefined) return
      const result = await this.readParquet(entry, size, TASK_COLUMNS, MAX_TASK_ROWS)
      if (result === undefined) return
      let validRows = 0
      for (const row of result.rows) {
        const task = firstString(row, ['task', 'text', '__index_level_0__'])
        const index = nonNegativeIntegerOrNull(row.task_index ?? row.__index_level_0__)
        if (task !== null && index !== null) {
          validRows += 1
          this.taskNames.set(index, task)
        }
      }
      this.taskNamesComplete = result.rows.length === result.rowCount && validRows === result.rowCount
      if (!this.taskNamesComplete) this.addFinding('warning', 'tasks_manifest_incomplete', 'The task parquet table exceeded a row or read limit.')
    } catch {
      this.signal?.throwIfAborted()
      this.addFinding('warning', 'tasks_manifest_unreadable', 'meta/tasks.parquet could not be inspected.')
    }
  }

  async readEpisodes(metaTarget, episodeIndex) {
    const version = this.metadata.codebase_version
    if (!isSupportedVersion(version)) {
      this.finishEpisodeScan(episodeIndex)
      return
    }
    if (version?.startsWith('v3.')) {
      await this.readParquetEpisodes(metaTarget, episodeIndex)
      return
    }
    const episodes = await this.findChild(metaTarget, 'episodes.jsonl')
    if (episodes === undefined) {
      if ((this.metadata.total_episodes ?? 0) > 0) {
        this.addFinding('warning', 'episodes_manifest_missing', 'meta/episodes.jsonl is missing; episode details could not be checked.')
      }
      this.finishEpisodeScan(episodeIndex)
      return
    }
    const size = await this.getFileSize(episodes, 'meta/episodes.jsonl')
    if (size === undefined) {
      this.addFinding('warning', 'episodes_manifest_unreadable', 'meta/episodes.jsonl could not be inspected.')
      this.finishEpisodeScan(episodeIndex)
      return
    }
    const state = await this.scanJsonLines(episodes, size, MAX_JSONL_ROWS, 'episodes', row => this.addEpisode(row, episodeIndex))
    this.episode.manifest_row_count = state.rowsScanned
    this.episode.manifest_complete = state.complete && this.episode.records_valid === this.episode.records_scanned
      && !this.episode.frame_count_overflow
    if (!this.episode.manifest_complete) this.addFinding('warning', 'episodes_manifest_incomplete', 'The episode JSONL manifest exceeded a scan limit or contains invalid rows.')
    this.finishEpisodeScan(episodeIndex)
  }

  async scanJsonLines(entry, size, maxRows, kind, onRow) {
    const state = { rowsScanned: 0, complete: false }
    const perFile = { reservedBytes: 0 }
    const decoder = new TextDecoder('utf-8', { fatal: true })
    let offset = 0
    let pending = ''
    let lineNumber = 0
    let malformed = false
    try {
      while (offset < size && state.rowsScanned < maxRows) {
        const remainingBudget = MAX_JSONL_SCAN_BYTES - perFile.reservedBytes
        if (remainingBudget <= 0) break
        const length = Math.min(MAX_JSONL_CHUNK_BYTES, size - offset, remainingBudget)
        const bytes = await this.readRange(entry.target, offset, length, 'text', perFile, MAX_JSONL_SCAN_BYTES)
        if (bytes.byteLength === 0) break
        offset += bytes.byteLength
        pending += decoder.decode(bytes, { stream: offset < size })
        if (pending.length > MAX_JSONL_LINE_CHARS && !pending.includes('\n')) {
          malformed = true
          this.addFinding('warning', 'jsonl_line_too_large', `A line in meta/${kind}.jsonl exceeded the line-length limit.`)
          break
        }
        let newline = pending.indexOf('\n')
        while (newline >= 0 && state.rowsScanned < maxRows) {
          const line = pending.slice(0, newline).replace(/\r$/, '')
          pending = pending.slice(newline + 1)
          lineNumber += 1
          if (line.trim() !== '') {
            if (line.length > MAX_JSONL_LINE_CHARS) {
              malformed = true
              this.addFinding('warning', 'jsonl_line_too_large', `A line in meta/${kind}.jsonl exceeded the line-length limit.`)
            } else {
              try {
                const row = JSON.parse(line)
                if (row === null || typeof row !== 'object' || Array.isArray(row)) throw new TypeError('expected an object')
                onRow(row)
                state.rowsScanned += 1
              } catch {
                malformed = true
                this.addFinding('warning', 'jsonl_row_invalid', `Line ${lineNumber} in meta/${kind}.jsonl is not a valid JSON object.`)
              }
            }
          }
          newline = pending.indexOf('\n')
        }
        if (state.rowsScanned >= maxRows && (offset < size || pending.length > 0)) break
      }
      if (offset === size && state.rowsScanned < maxRows && !malformed) {
        pending += decoder.decode()
        const line = pending.replace(/\r$/, '')
        if (line.trim() !== '') {
          lineNumber += 1
          const row = JSON.parse(line)
          if (row === null || typeof row !== 'object' || Array.isArray(row)) throw new TypeError('expected an object')
          onRow(row)
          state.rowsScanned += 1
        }
        pending = ''
      }
      if (offset === size && pending.trim() === '') pending = ''
      state.complete = offset === size && pending.length === 0 && !malformed
    } catch (error) {
      this.signal?.throwIfAborted()
      if (error instanceof ReadBudgetExceeded) {
        state.complete = false
      } else {
        malformed = true
        this.addFinding('warning', 'jsonl_read_failed', `meta/${kind}.jsonl could not be fully read.`)
      }
    }
    if (offset < size || state.rowsScanned >= maxRows && pending.length > 0) state.complete = false
    if (malformed) state.complete = false
    return state
  }

  async readParquetEpisodes(metaTarget, episodeIndex) {
    const episodesRoot = await this.findChild(metaTarget, 'episodes')
    if (episodesRoot === undefined || episodesRoot.type !== 'directory') {
      if ((this.metadata.total_episodes ?? 0) > 0) this.addFinding('warning', 'episodes_manifest_missing', 'meta/episodes does not contain a readable episode manifest directory.')
      this.finishEpisodeScan(episodeIndex)
      return
    }
    const files = await this.collectFiles(episodesRoot, '.parquet', MAX_INVENTORY_DEPTH)
    if (!files.complete) this.addFinding('warning', 'episodes_manifest_file_limit', 'Episode manifest scanning reached a file, entry, or directory-depth limit.')
    let rowsKnown = 0
    let allFilesRead = files.complete
    let remainingRows = MAX_EPISODE_ROWS
    for (const entry of files.entries) {
      this.signal?.throwIfAborted()
      if (remainingRows <= 0) {
        allFilesRead = false
        break
      }
      try {
        const size = await this.getFileSize(entry, 'episode metadata parquet')
        if (size === undefined) {
          allFilesRead = false
          continue
        }
        const rowLimit = Math.min(remainingRows, MAX_EPISODE_ROWS)
        const wantedColumns = episodeIndex === undefined
          ? EPISODE_COLUMNS
          : [...EPISODE_COLUMNS, ...this.episodeStatisticsColumns()]
        const result = await this.readParquet(entry, size, wantedColumns, rowLimit)
        if (result === undefined) {
          allFilesRead = false
          continue
        }
        rowsKnown += result.rowCount
        for (const row of result.rows) this.addEpisode(row, episodeIndex)
        remainingRows -= result.rows.length
        if (result.rows.length < result.rowCount) allFilesRead = false
      } catch {
        this.signal?.throwIfAborted()
        allFilesRead = false
        this.addFinding('warning', 'episodes_parquet_unreadable', 'An episode metadata parquet file could not be inspected.')
      }
    }
    this.episode.manifest_row_count = rowsKnown
    this.episode.manifest_complete = allFilesRead && rowsKnown === this.episode.records_scanned
      && this.episode.records_valid === this.episode.records_scanned
      && !this.episode.frame_count_overflow
    if (!this.episode.manifest_complete) {
      this.addFinding('warning', 'episodes_manifest_incomplete', 'The episode parquet manifest exceeded a file, row, or byte limit, or contains unreadable data.')
    }
    this.finishEpisodeScan(episodeIndex)
  }

  episodeStatisticsColumns() {
    const featureNames = Object.keys(this.info?.features ?? {}).slice(0, MAX_FEATURES)
    return featureNames.flatMap(name => EPISODE_STAT_COLUMNS.map(stat => `stats/${name}/${stat}`))
  }

  async readParquet(entry, size, wantedColumns, rowLimit) {
    if (size <= 0) throw new Error('the parquet file is empty')
    const { parquetMetadataAsync, parquetReadObjects, parquetSchema } = await import('hyparquet')
    const fileBudget = { reservedBytes: 0 }
    const file = {
      byteLength: size,
      slice: async (start, end = size) => {
        if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end < start || end > size) {
          throw new RangeError('the parquet reader requested an invalid byte range')
        }
        const bytes = await this.readRange(entry.target, start, end - start, 'parquet', fileBudget, MAX_PARQUET_FILE_BYTES, MAX_PARQUET_RANGE_BYTES)
        return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)
      },
    }
    const metadata = await parquetMetadataAsync(file)
    const rowCount = safeInteger(metadata.num_rows)
    if (rowCount === null || rowCount < 0) throw new Error('the parquet row count is invalid')
    const schema = parquetSchema(metadata)
    const columns = Array.isArray(schema.children)
      ? schema.children.map(child => child.element?.name).filter(name => typeof name === 'string')
      : []
    const wanted = wantedColumns.filter(column => columns.includes(column))
    if (rowCount === 0 || rowLimit <= 0 || wanted.length === 0) return { rowCount, columns, rows: [] }
    const rows = await parquetReadObjects({
      file,
      columns: wanted,
      rowStart: 0,
      rowEnd: Math.min(rowCount, rowLimit),
    })
    return { rowCount, columns, rows }
  }

  async readRange(target, offset, length, kind, fileBudget, perFileLimit, requestLimit = MAX_JSONL_CHUNK_BYTES) {
    this.signal?.throwIfAborted()
    if (!Number.isSafeInteger(offset) || !Number.isSafeInteger(length) || offset < 0 || length < 0) {
      throw new RangeError('the filesystem reader received an invalid byte range')
    }
    if (length === 0) return new Uint8Array()
    if (length > requestLimit) throw new ReadBudgetExceeded('One metadata read exceeded the per-request byte limit.')
    if (fileBudget.reservedBytes + length > perFileLimit) throw new ReadBudgetExceeded('A metadata file exceeded its read limit.')
    this.reserveBytes(length)
    fileBudget.reservedBytes += length
    this.budget.rangeRequests += 1
    const bytes = await this.fs.readByteRange(target, { offset, length }, this.signal)
    this.recordBytes(bytes.byteLength, kind)
    if (bytes.byteLength !== length) throw new Error('the filesystem returned a short byte range')
    return bytes
  }

  async readStatistics(metaTarget) {
    if (metaTarget === undefined) return
    const statsEntry = await this.findChild(metaTarget, 'stats.json')
    if (statsEntry === undefined) return
    const stats = await this.readJson(statsEntry, 'stats.json')
    if (stats === null) {
      this.statistics.complete = false
      return
    }
    this.statistics.available = true
    const entries = Object.entries(stats)
    this.statistics.complete = entries.length <= MAX_FEATURES
    let truncatedValues = false
    let invalidValues = false
    this.statistics.features = entries.slice(0, MAX_FEATURES).map(([name, featureStats]) => {
      const values = Object.fromEntries(['min', 'mean', 'std', 'max'].map(stat => {
        const result = numericValues(featureStats?.[stat])
        truncatedValues ||= result.truncated
        invalidValues ||= result.invalid
        return [stat, result.values]
      }))
      return { name: name.slice(0, MAX_OUTPUT_STRING_CHARS), ...values }
    })
    if (entries.length > MAX_FEATURES) {
      this.statistics.complete = false
      this.addFinding('warning', 'statistics_truncated', `Only the first ${MAX_FEATURES} feature statistics are included.`)
    }
    if (truncatedValues) {
      this.statistics.complete = false
      this.addFinding('warning', 'statistics_values_truncated', `Statistic vectors include at most ${MAX_STATS_VALUES} values.`)
    }
    if (invalidValues) {
      this.statistics.complete = false
      this.addFinding('warning', 'statistics_values_invalid', 'One or more statistic vectors contain non-numeric values.')
    }
  }

  async collectFiles(rootEntry, extension, maxDepth) {
    const result = { entries: [], complete: true }
    const queue = [{ target: rootEntry.target, depth: 0 }]
    const seen = new Set()
    let visitedEntries = 0
    while (queue.length > 0) {
      this.signal?.throwIfAborted()
      const current = queue.shift()
      if (seen.has(current.target.targetKey)) continue
      seen.add(current.target.targetKey)
      const truncationsBefore = this.truncatedListings
      const skippedBefore = this.skippedEntries
      const children = await this.listChildren(current.target)
      if (this.truncatedListings > truncationsBefore) result.complete = false
      if (this.skippedEntries > skippedBefore) result.complete = false
      for (const entry of children) {
        visitedEntries += 1
        if (visitedEntries > MAX_INVENTORY_ENTRIES) {
          result.complete = false
          return result
        }
        if (entry.type === 'file' && entry.name.endsWith(extension)) {
          result.entries.push(entry)
          if (result.entries.length > MAX_PARQUET_FILES) {
            result.entries.pop()
            result.complete = false
            return result
          }
        } else if (entry.type === 'directory' && current.depth < maxDepth) {
          queue.push({ target: entry.target, depth: current.depth + 1 })
        } else if (entry.type === 'directory') {
          result.complete = false
          this.addFinding('warning', 'inventory_depth_limit', 'A directory scan reached its depth limit; nested files may be missing from the results.')
        }
      }
    }
    result.entries.sort((a, b) => a.name.localeCompare(b.name))
    return result
  }

  addEpisode(row, requestedIndex) {
    this.episode.records_scanned += 1
    const index = nonNegativeIntegerOrNull(row.episode_index)
    const length = nonNegativeIntegerOrNull(row.length)
    if (index === null || length === null) {
      this.addFinding('warning', 'episode_row_invalid', 'An episode manifest row lacks a non-negative episode_index or length.')
      return
    }
    this.episode.records_valid += 1
    if (this.episode.frame_count_observed > Number.MAX_SAFE_INTEGER - length) {
      this.episode.frame_count_observed = Number.MAX_SAFE_INTEGER
      this.episode.frame_count_overflow = true
      this.addFinding('warning', 'episode_frame_count_overflow', 'The observed frame total exceeded the safe integer range and was capped.')
    } else {
      this.episode.frame_count_observed += length
    }
    this.episode.min_length = this.episode.min_length === null ? length : Math.min(this.episode.min_length, length)
    this.episode.max_length = this.episode.max_length === null ? length : Math.max(this.episode.max_length, length)
    const tasks = taskNamesForRow(row, this.taskNames)
    for (const task of tasks) this.episode.task_counts.set(task, (this.episode.task_counts.get(task) ?? 0) + 1)
    if (requestedIndex !== undefined && index === requestedIndex) {
      this.episode.requested_episode_found = true
      this.episode.requested_episode = {
        episode_index: index,
        length,
        tasks,
        dataset_from_index: nonNegativeIntegerOrNull(row.dataset_from_index),
        dataset_to_index: nonNegativeIntegerOrNull(row.dataset_to_index),
        ...this.readEpisodeStatistics(row),
      }
    }
  }

  readEpisodeStatistics(row) {
    const features = []
    let available = false
    let complete = true
    let truncatedValues = false
    let invalidValues = false
    const featureNames = Object.keys(this.info?.features ?? {}).slice(0, MAX_FEATURES)
    for (const name of featureNames) {
      const present = EPISODE_STAT_COLUMNS.filter(stat => Object.hasOwn(row, `stats/${name}/${stat}`))
      if (present.length === 0) continue
      available = true
      if (present.length !== EPISODE_STAT_COLUMNS.length) complete = false
      const values = Object.fromEntries(EPISODE_STAT_COLUMNS.map(stat => {
        const result = Object.hasOwn(row, `stats/${name}/${stat}`)
          ? numericValues(row[`stats/${name}/${stat}`])
          : { values: [], complete: false, truncated: false, invalid: false }
        complete &&= result.complete
        truncatedValues ||= result.truncated
        invalidValues ||= result.invalid
        return [stat, result.values]
      }))
      features.push({ name: name.slice(0, MAX_OUTPUT_STRING_CHARS), ...values })
    }
    if (available && !complete) {
      this.addFinding('warning', 'episode_statistics_incomplete', 'The requested episode statistics are missing fields or exceed a value limit.')
    }
    if (truncatedValues) {
      this.addFinding('warning', 'episode_statistics_values_truncated', `Episode statistic vectors include at most ${MAX_STATS_VALUES} values.`)
    }
    if (invalidValues) {
      this.addFinding('warning', 'episode_statistics_values_invalid', 'One or more episode statistic vectors contain non-numeric values.')
    }
    return {
      episode_statistics_available: available,
      episode_statistics_complete: available && complete,
      episode_statistics: available ? features : null,
    }
  }

  finishEpisodeScan(requestedIndex) {
    if (requestedIndex === undefined || this.episode.requested_episode_found) return
    if (this.episode.manifest_complete) {
      this.addFinding('warning', 'episode_not_found', `Episode ${requestedIndex} is not present in the inspected manifest.`)
    } else {
      this.addFinding('warning', 'episode_search_incomplete', `Episode ${requestedIndex} was not found before a manifest scan limit was reached.`)
    }
  }

  async readStorageInventory() {
    const data = await this.findChild(this.root, 'data')
    if (data !== undefined && data.type === 'directory') {
      const inventory = await this.scanAssetTree(data, '.parquet', 2)
      this.storage.data_parquet_files = inventory.count
      this.storage.data_parquet_bytes = inventory.bytes
      this.storage.inventory_complete &&= inventory.complete
      this.storage.sizes_complete &&= inventory.sizesComplete
    } else if (data !== undefined) {
      this.storage.inventory_complete = false
      this.addFinding('warning', 'data_not_directory', 'The dataset data entry is not a directory.')
    }
    const videos = await this.findChild(this.root, 'videos')
    if (videos !== undefined && videos.type === 'directory') {
      const inventory = await this.scanAssetTree(videos, '.mp4', MAX_INVENTORY_DEPTH)
      this.storage.video_files = inventory.count
      this.storage.video_bytes = inventory.bytes
      this.storage.inventory_complete &&= inventory.complete
      this.storage.sizes_complete &&= inventory.sizesComplete
    } else if (videos !== undefined) {
      this.storage.inventory_complete = false
      this.addFinding('warning', 'videos_not_directory', 'The dataset videos entry is not a directory.')
    }
    if (!this.storage.inventory_complete) {
      this.addFinding('warning', 'storage_inventory_incomplete', 'Storage file counts are lower bounds because the inventory scan reached a limit.')
    }
    if (!this.storage.sizes_complete) {
      this.addFinding('warning', 'storage_sizes_incomplete', 'Some asset sizes were unavailable, so storage byte totals are lower bounds.')
    }
  }

  async scanAssetTree(rootEntry, extension, maxDepth) {
    const queue = [{ target: rootEntry.target, depth: 0 }]
    const seen = new Set()
    let visitedEntries = 0
    let count = 0
    let bytes = 0
    let complete = true
    let sizesComplete = true
    while (queue.length > 0) {
      this.signal?.throwIfAborted()
      const current = queue.shift()
      if (seen.has(current.target.targetKey)) continue
      seen.add(current.target.targetKey)
      const allChildren = await this.fs.listDir(current.target, this.signal)
      const children = allChildren.slice(0, MAX_DIRECTORY_ENTRIES)
      if (allChildren.length > children.length) {
        complete = false
        this.addFinding('warning', 'directory_listing_truncated', 'A directory contains more entries than the inspection limit; file counts may be incomplete.')
      }
      for (const entry of children) {
        visitedEntries += 1
        if (visitedEntries > MAX_INVENTORY_ENTRIES) return { count, bytes, complete: false, sizesComplete }
        if (!this.fs.contains(this.root, entry.target)) {
          this.skippedEntries += 1
          complete = false
          this.storage.inventory_complete = false
          this.addFinding('warning', 'outside_root_entry_skipped', 'A listed path resolved outside the selected dataset directory and was skipped.')
          continue
        }
        if (entry.type === 'file' && entry.name.endsWith(extension)) {
          count += 1
          const size = nonNegativeIntegerOrNull(entry.size)
          if (size === null) sizesComplete = false
          else if (bytes > Number.MAX_SAFE_INTEGER - size) {
            sizesComplete = false
            bytes = Number.MAX_SAFE_INTEGER
          } else bytes += size
        } else if (entry.type === 'directory' && current.depth < maxDepth) {
          queue.push({ target: entry.target, depth: current.depth + 1 })
        } else if (entry.type === 'directory') {
          complete = false
          this.addFinding('warning', 'inventory_depth_limit', 'A directory scan reached its depth limit; nested files may be missing from the results.')
        }
      }
    }
    return { count, bytes, complete, sizesComplete }
  }

  checkConsistency() {
    if (this.metadata === undefined || this.metadata === null) return
    const declaredEpisodes = this.metadata.total_episodes
    const declaredFrames = this.metadata.total_frames
    const manifestRows = this.episode.manifest_row_count
    if (this.episode.manifest_complete && declaredEpisodes !== null && manifestRows !== null && declaredEpisodes !== manifestRows) {
      this.addFinding('warning', 'episode_count_mismatch', `meta/info.json declares ${declaredEpisodes} episodes, but the episode manifest contains ${manifestRows}.`)
    }
    if (this.episode.manifest_complete && declaredFrames !== null && this.episode.frame_count_observed !== declaredFrames) {
      this.addFinding('warning', 'frame_count_mismatch', `meta/info.json declares ${declaredFrames} frames, but episode lengths sum to ${this.episode.frame_count_observed}.`)
    }
    if (this.storage.inventory_complete && (declaredEpisodes ?? 0) > 0 && this.storage.data_parquet_files === 0) {
      this.addFinding('warning', 'data_files_missing', 'The dataset declares episodes but no data parquet files were found.')
    }
    if (this.storage.inventory_complete && ((this.metadata.total_videos ?? 0) > 0 || this.features.some(feature => feature.video)) && this.storage.video_files === 0) {
      this.addFinding('warning', 'video_files_missing', 'The feature map includes video streams but no MP4 files were found.')
    }
    const declaredTasks = this.metadata.total_tasks
    if (this.taskNamesComplete && declaredTasks !== null && this.taskNames.size !== declaredTasks) {
      this.addFinding('warning', 'task_count_mismatch', `meta/info.json declares ${declaredTasks} tasks, but the task manifest contains ${this.taskNames.size}.`)
    }
  }

  toValue() {
    const infoValid = this.info !== null && this.metadata !== undefined
    const tasksComplete = (this.metadata?.total_tasks ?? 0) === 0 || this.taskNamesComplete
    const complete = infoValid && this.episode.manifest_complete && tasksComplete
      && this.storage.inventory_complete && this.storage.sizes_complete && this.featuresComplete
      && (!this.statistics.available || this.statistics.complete)
      && (!this.episode.requested_episode?.episode_statistics_available || this.episode.requested_episode.episode_statistics_complete)
    const hasError = this.findings.some(finding => finding.severity === 'error')
    const status = !infoValid || hasError ? 'invalid'
      : !complete ? 'partial'
        : this.findings.length > 0 ? 'issues' : 'ok'
    return {
      dataset_path: this.root.displayPath,
      status,
      metadata: this.metadata ?? null,
      features: this.features,
      features_complete: this.featuresComplete,
      statistics: this.statistics,
      tasks: {
        declared_count: this.metadata?.total_tasks ?? null,
        manifest_count: this.taskNames.size,
        names: [...this.taskNames.entries()]
          .sort((a, b) => a[0] - b[0])
          .slice(0, MAX_TASK_NAMES)
          .map(([task_index, task]) => ({ task_index, task })),
        complete: this.taskNamesComplete,
      },
      episodes: {
        declared_count: this.episode.declared_count,
        manifest_row_count: this.episode.manifest_row_count,
        records_scanned: this.episode.records_scanned,
        records_valid: this.episode.records_valid,
        frame_count_observed: this.episode.frame_count_observed,
        min_length: this.episode.min_length,
        max_length: this.episode.max_length,
        task_counts: [...this.episode.task_counts.entries()]
          .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
          .slice(0, MAX_TASK_NAMES)
          .map(([task, episode_count]) => ({ task, episode_count })),
        manifest_complete: this.episode.manifest_complete,
        requested_episode_index: this.episode.requested_episode_index,
        requested_episode: this.episode.requested_episode,
      },
      storage: this.storage,
      findings: this.findings.slice(0, 100),
      read_stats: {
        bytes_read: this.budget.bytesRead,
        text_bytes_read: this.budget.textBytesRead,
        parquet_bytes_read: this.budget.parquetBytesRead,
        range_requests: this.budget.rangeRequests,
      },
    }
  }
}

/** Inspect a local LeRobot dataset using only the supplied DSH filesystem service. */
export async function inspectDataset(fs, datasetPath, episodeIndex, signal) {
  signal?.throwIfAborted()
  const root = await fs.resolve(datasetPath, { signal })
  const info = await fs.stat(root, signal)
  if (info === undefined) throw new Error('The dataset path does not exist in the current DSH filesystem.')
  if (info.type !== 'directory') throw new Error('The dataset path must name a directory.')
  return new Inspection(fs, root, signal).run(episodeIndex)
}

export function validateToolArgs(args) {
  if (args === null || typeof args !== 'object' || Array.isArray(args)) throw new TypeError('Arguments must be an object.')
  const keys = Object.keys(args)
  if (keys.some(key => key !== 'dataset_path' && key !== 'episode_index')) throw new TypeError('Only dataset_path and episode_index are accepted.')
  if (typeof args.dataset_path !== 'string' || args.dataset_path.trim() === '' || args.dataset_path.length > 4096) {
    throw new TypeError('dataset_path must be a non-empty path of at most 4096 characters.')
  }
  if (args.dataset_path.includes('\0')) throw new TypeError('dataset_path cannot contain a NUL character.')
  if (args.episode_index !== undefined && (!Number.isSafeInteger(args.episode_index) || args.episode_index < 0)) {
    throw new TypeError('episode_index must be a non-negative safe integer.')
  }
  return { dataset_path: args.dataset_path, episode_index: args.episode_index }
}

function safeInteger(value) {
  if (typeof value === 'bigint') {
    if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER)) return null
    return Number(value)
  }
  return typeof value === 'number' && Number.isSafeInteger(value) ? value : null
}

function safeNumber(value) {
  if (typeof value === 'bigint') {
    if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER)) return null
    return Number(value)
  }
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function nonNegativeIntegerOrNull(value) {
  const number = safeInteger(value)
  return number !== null && number >= 0 ? number : null
}

function stringOrNull(value) {
  return typeof value === 'string' && value.trim() !== '' ? value.slice(0, MAX_OUTPUT_STRING_CHARS) : null
}

function firstString(value, keys) {
  for (const key of keys) {
    const candidate = stringOrNull(value?.[key])
    if (candidate !== null) return candidate
  }
  return null
}

function isSupportedVersion(version) {
  return version === 'v2.0' || version === 'v2.1' || version === 'v3.0'
}

function numericValues(value) {
  const values = Array.isArray(value) ? value : [value]
  const selected = values.slice(0, MAX_STATS_VALUES)
  const converted = selected.map(safeNumber)
  return {
    values: converted.filter(number => number !== null),
    complete: values.length <= MAX_STATS_VALUES && converted.every(number => number !== null),
    truncated: values.length > MAX_STATS_VALUES,
    invalid: converted.some(number => number === null),
  }
}

function taskNamesForRow(row, taskMap) {
  const values = row.tasks
  const names = []
  if (Array.isArray(values)) {
    for (const value of values.slice(0, MAX_TASKS_PER_EPISODE)) {
      const direct = stringOrNull(value)
      if (direct !== null) names.push(direct)
      else {
        const index = nonNegativeIntegerOrNull(value)
        if (index !== null && taskMap.has(index)) names.push(taskMap.get(index))
      }
    }
  } else if (typeof values === 'string') {
    const direct = stringOrNull(values)
    if (direct !== null) names.push(direct)
  }
  if (names.length > 0) return [...new Set(names)]
  const taskIndex = nonNegativeIntegerOrNull(row.task_index)
  if (taskIndex !== null && taskMap.has(taskIndex)) return [taskMap.get(taskIndex)]
  return []
}
