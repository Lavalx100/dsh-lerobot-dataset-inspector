import { inspectDataset, validateToolArgs } from './inspect.mjs'

const scalar = type => ({ type })
const nullable = type => ({ oneOf: [{ type }, { type: 'null' }] })
const arrayOf = items => ({ type: 'array', items })
const objectOf = properties => ({
  type: 'object',
  properties,
  required: Object.keys(properties),
  additionalProperties: false,
})

const OUTPUT_SCHEMA = objectOf({
  dataset_path: scalar('string'),
  status: { type: 'string', enum: ['ok', 'issues', 'partial', 'invalid'] },
  metadata: { oneOf: [objectOf({
    codebase_version: nullable('string'),
    robot_type: nullable('string'),
    fps: nullable('number'),
    total_episodes: nullable('integer'),
    total_frames: nullable('integer'),
    total_tasks: nullable('integer'),
    total_videos: nullable('integer'),
  }), scalar('null')] },
  features: arrayOf(objectOf({
    name: scalar('string'),
    dtype: nullable('string'),
    shape: arrayOf(scalar('integer')),
    video: scalar('boolean'),
  })),
  features_complete: scalar('boolean'),
  statistics: objectOf({
    available: scalar('boolean'),
    features: arrayOf(objectOf({
      name: scalar('string'),
      min: arrayOf(scalar('number')),
      mean: arrayOf(scalar('number')),
      std: arrayOf(scalar('number')),
      max: arrayOf(scalar('number')),
    })),
    complete: scalar('boolean'),
  }),
  tasks: objectOf({
    declared_count: nullable('integer'),
    manifest_count: scalar('integer'),
    names: arrayOf(objectOf({ task_index: scalar('integer'), task: scalar('string') })),
    complete: scalar('boolean'),
  }),
  episodes: objectOf({
    declared_count: nullable('integer'),
    manifest_row_count: nullable('integer'),
    records_scanned: scalar('integer'),
    records_valid: scalar('integer'),
    frame_count_observed: scalar('integer'),
    min_length: nullable('integer'),
    max_length: nullable('integer'),
    task_counts: arrayOf(objectOf({ task: scalar('string'), episode_count: scalar('integer') })),
    manifest_complete: scalar('boolean'),
    requested_episode_index: nullable('integer'),
    requested_episode: { oneOf: [objectOf({
      episode_index: scalar('integer'),
      length: scalar('integer'),
      tasks: arrayOf(scalar('string')),
      dataset_from_index: nullable('integer'),
      dataset_to_index: nullable('integer'),
      episode_statistics_available: scalar('boolean'),
      episode_statistics_complete: scalar('boolean'),
      episode_statistics: { oneOf: [arrayOf(objectOf({
        name: scalar('string'),
        min: arrayOf(scalar('number')),
        mean: arrayOf(scalar('number')),
        std: arrayOf(scalar('number')),
        max: arrayOf(scalar('number')),
        count: arrayOf(scalar('number')),
      })), scalar('null')] },
    }), scalar('null')] },
  }),
  storage: objectOf({
    data_parquet_files: scalar('integer'),
    data_parquet_bytes: scalar('integer'),
    video_files: scalar('integer'),
    video_bytes: scalar('integer'),
    inventory_complete: scalar('boolean'),
    sizes_complete: scalar('boolean'),
  }),
  findings: arrayOf(objectOf({
    severity: { type: 'string', enum: ['info', 'warning', 'error'] },
    code: scalar('string'),
    message: scalar('string'),
  })),
  read_stats: objectOf({
    bytes_read: scalar('integer'),
    text_bytes_read: scalar('integer'),
    parquet_bytes_read: scalar('integer'),
    range_requests: scalar('integer'),
  }),
})

/** Cordis plugin name. */
export const name = 'lerobot-dataset-inspector'

/** DSH services used by the read-only dataset tool. */
export const inject = ['fs', 'tools']

/** Register the bounded, read-only LeRobot metadata inspection tool. */
export function apply(ctx) {
  ctx.tools.register({
    name: 'lerobot_inspect_dataset',
    description: 'Inspect a local LeRobot v2.0, v2.1, or v3.0 dataset. Reads bounded metadata and episode manifests, reports feature/task/statistics summaries and data/video file counts, and can return one episode record. It never reads frame payloads or decodes videos.',
    parameters: {
      type: 'object',
      properties: {
        dataset_path: { type: 'string', description: 'Path to the dataset directory, resolved by the active DSH filesystem.' },
        episode_index: { type: 'integer', description: 'Optional non-negative episode index to include in the summary.' },
      },
      required: ['dataset_path'],
      additionalProperties: false,
    },
    output: {
      schema: OUTPUT_SCHEMA,
      render: (_args, value) => [{ type: 'text', text: JSON.stringify(value, null, 2) }],
    },
    timeoutMs: 120_000,
    async execute(args, exec) {
      const parsed = validateToolArgs(args)
      return inspectDataset(ctx.fs, parsed.dataset_path, parsed.episode_index, exec.signal)
    },
  })
}
