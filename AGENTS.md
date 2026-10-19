# LakeForge project instructions

- Implement a local-first lakehouse control plane in Scala 2.13 and a Svelte 5/TypeScript UI.
- SQLite owns persistent dataset and job metadata. Parquet, logs and artifacts live in a configurable data directory, never in disposable worker containers.
- One submitted job gets one dedicated, unprivileged, resource-limited container using a versioned engine image. Podman is preferred; Docker is a development fallback.
- The first vertical slice is a DuckDB SELECT against explicitly registered Parquet data, producing validated Parquet output. Spark, DataFusion, benchmarks, telemetry, and GPU support are later milestones; never imply they work before verifying them.
- Do not accept arbitrary host commands or paths in the job API. Reject unsupported engines and modes explicitly. Keep worker inputs and outputs isolated.
- Keep transitions and recovery explicit; preserve diagnostics after containers exit. Tests must exercise state transitions and the worker contract.
- Pin dependencies and images. Do not hardcode machine paths or log secrets. Do not commit unless requested.
- Milestone 1 acceptance: start control plane; register Parquet; submit via UI; run actual dedicated worker; publish valid output; inspect persisted status/logs; cancel running work; recover across restart; run tests and document limitations.
