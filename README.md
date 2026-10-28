---
description: "Install this profile layer to inspect local LeRobot v2.0, v2.1, and v3.0 metadata, manifests, and bounded asset inventories."
kind: "package-bundle"
---

# DSH LeRobot Dataset Inspector

English | [中文](README.zh.md)

## Summary

Add `lerobot_inspect_dataset` to a profile to inspect a local LeRobot dataset. It summarizes metadata, features, statistics, tasks, episodes, and asset counts, with optional details for one episode. The plugin reads through the active DSH filesystem and does not open robot data Parquet files or decode video frames.

## Table of Contents

- [Use this package](#use-this-package)
- [Understand the implementation](#understand-the-implementation)
- [Further Exploration](#further-exploration)
- [Model Experience](#model-experience)
- [Known Limitations and Deferred Work](#known-limitations-and-deferred-work)
- [Dev Note](#dev-note)

-----

<a id="use-this-package"></a>
## Use this package

### Install into a profile

From a DSH checkout or installed DSH CLI, add this local bundle to a profile:

```sh
dsh plugin --profile <profile> add /absolute/path/to/lerobot-dataset-inspector
dsh plugin --profile <profile> remove dsh-lerobot-dataset-inspector
```

Start DSH with that profile, then call `lerobot_inspect_dataset` with a dataset directory visible to its filesystem. Removing the package removes its profile layer.

### What you get

The tool returns LeRobot version and counts, feature shapes, available aggregate statistics, task names, episode-length summaries, and data/video file counts. Pass `episode_index` to include that episode's metadata and, for v3.0, its per-episode statistics.

The tool reads metadata and episode/task manifests through `ctx.fs`. It does not open data Parquet files or read or decode MP4 contents. Every path remains subject to the active profile's filesystem permissions.

### Run the tests

The tests use small metadata files and Parquet fixtures. They do not access robot datasets or network services.

```sh
pnpm install --ignore-workspace --ignore-scripts
node --test tests/inspector.test.mjs
node --experimental-test-coverage --test tests/inspector.test.mjs
```

The package requires Node.js 24.9 or newer and DSH `>=0.2.2-alpha.2 <0.3.0`.

-----

<a id="understand-the-implementation"></a>
## Understand the implementation

<details>
<summary>Implementation details — click to expand</summary>

The bundle patch inserts one Cordis plugin row. The plugin resolves dataset paths through the profile filesystem, reads only metadata and episode/task manifests, and applies bounded file, row, byte, and directory-depth scans. When a scan reaches a limit, the result reports incomplete data and lower-bound counts.

The patch and source map are [`cordis.patch.yml`](cordis.patch.yml), [`src/index.mjs`](src/index.mjs), and [`src/inspect.mjs`](src/inspect.mjs). Parquet parsing uses the pinned `hyparquet` dependency.

</details>

-----

<a id="further-exploration"></a>
## Further Exploration

See the [DeepSeek Harness contribution guide](https://github.com/deepseek-ai/deepseek-harness/blob/master/CONTRIBUTING.md) for ways to share independent plugins and participate in the community.

-----

<a id="model-experience"></a>
## Model Experience

The tool returns structured metadata and bounded summaries. It does not expose robot frame contents or decoded video to the model; one requested v3.0 episode may include its stored per-episode statistics.

#### KV Cache impact

The result size is bounded by feature, task, row, and output limits, though larger metadata summaries use more context than smaller datasets.

-----

## Known Limitations and Deferred Work

The inspector does not validate robot frame values or decode video. It reports inventory as incomplete when file, entry, or depth limits prevent a full scan.

- **Metadata reads are bounded** — JSON files, JSONL rows, Parquet files, ranges, and total bytes have fixed limits; findings identify incomplete results.
- **Vector output is capped** — feature shapes and statistics include at most 16 values, and the result marks truncation as incomplete.
- **Episode statistics are per-episode** — v3.0 episode statistics appear only when `episode_index` selects a row that contains them.

<a id="dev-note"></a>
### Dev Note

<details>
<summary>Maintainer context — click to expand</summary>

None.

</details>
