"""Regenerate the tiny v3 metadata tables with PyArrow for the Node tests."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


OUTPUT = Path(__file__).parent

tasks = pa.table({
    "task_index": pa.array([0, 1], type=pa.int64()),
    "task": pa.array(["pick object", "place object"], type=pa.string()),
})
episodes = pa.table({
    "episode_index": pa.array([0, 1], type=pa.int64()),
    "length": pa.array([2, 3], type=pa.int64()),
    "tasks": pa.array([["pick object"], ["place object"]], type=pa.list_(pa.string())),
    "data/chunk_index": pa.array([0, 0], type=pa.int64()),
    "data/file_index": pa.array([0, 0], type=pa.int64()),
    "dataset_from_index": pa.array([0, 2], type=pa.int64()),
    "dataset_to_index": pa.array([2, 5], type=pa.int64()),
    "stats/observation.state/min": pa.array([[-1.0, -2.0], [-3.0, -4.0]], type=pa.list_(pa.float32())),
    "stats/observation.state/mean": pa.array([[0.0, 1.0], [2.0, 3.0]], type=pa.list_(pa.float32())),
    "stats/observation.state/std": pa.array([[1.0, 1.5], [2.0, 2.5]], type=pa.list_(pa.float32())),
    "stats/observation.state/max": pa.array([[1.0, 2.0], [3.0, 4.0]], type=pa.list_(pa.float32())),
    "stats/observation.state/count": pa.array([[2.0, 2.0], [3.0, 3.0]], type=pa.list_(pa.float32())),
})

pq.write_table(tasks, OUTPUT / "tasks-v3.parquet", compression="NONE", use_dictionary=False)
pq.write_table(episodes, OUTPUT / "episodes-v3.parquet", compression="NONE", use_dictionary=False)
