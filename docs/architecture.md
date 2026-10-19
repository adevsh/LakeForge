# Architecture and decisions

## ADR 001: Small Scala HTTP service

Use JDK `HttpServer` for the first local control plane: Scala 2.13, SQLite JDBC and ujson are the only backend dependencies. It keeps process supervision and persistence visible and avoids a framework until routing/middleware complexity justifies one. It is not intended as an internet-facing service: bind to loopback, and add authentication before remote access. Scala CLI pins the Scala/tooling inputs; use JDK 21 (this macOS verification host only had JDK 27). The Svelte 5 UI runs behind a Vite development proxy.

## ADR 002: Worker isolation and protocol

The control plane prefers the `podman` executable on PATH and otherwise invokes `docker`; `LAKEFORGE_RUNTIME` overrides detection. Shell aliases are not visible to JVM subprocesses. Each job uses `run -d`, a versioned `lakeforge-duckdb:1.0.0` image resolved to a content-addressed digest at submission, `--network none`, no-new-privileges, read-only root, pids/CPU/memory limits, one read-only input file mount and one writable per-job staging mount. No runtime socket or secrets enter the worker. The immutable request is `/work/request.json`; `/work/result.json` is the response; `/work/output.parquet` is the candidate output. Worker stdout/stderr are captured to durable logs before the terminal transition and container deletion. A nonzero exit, missing/invalid result, missing/empty output, or excess output fails the job. Mount security assumes a trusted local user controls the data directory; do not run this service on an untrusted multi-user host. Disk quotas and guaranteed cleanup of orphan staging files remain future work.

Worker contract v1: request `{version:1, jobId, sql}` where SQL is a restricted single `SELECT` over a table named `input`; result `{version:1, jobId, rowCount, columns:[{name,type}], engineVersion}`. Worker materializes the input into a private per-job DuckDB database before disabling external access during submitted SQL. It closes that connection and opens a new worker-controlled connection to export Parquet, then removes the private database on success. This bounds filesystem reads during user SQL but duplicates input on disk; large inputs require a streaming-safe policy and disk quota in a later milestone. Control plane validates metadata and output before publication. Additional engines must implement this contract, with an explicit capability registry and separate image; no silent CPU/GPU or engine fallback.

## ADR 003: Persistence and publication

SQLite migrations are versioned SQL files; WAL mode and foreign keys are enabled. Job statuses are `QUEUED -> STARTING -> RUNNING -> {SUCCEEDED,FAILED,CANCELLED,TIMED_OUT}`; nonterminal jobs can fail on infrastructure loss or restart. Terminal status and container exit reason are separate fields. At startup, remaining runtime containers for nonterminal jobs are stopped and jobs become FAILED with restart diagnostics, avoiding indefinitely RUNNING records; this intentionally errs on the side of no duplicate writes. Each output is unique to a job, staged under `staging/<job-id>`, then moved into `outputs/<job-id>` on the same filesystem. Atomic rename makes the file visible at once, but SQLite metadata and filesystem publication are not one transaction; orphan outputs after a crash require cleanup. Concurrent writes to existing datasets and transactional table updates are not offered. Iceberg catalog/snapshots would be needed for concurrent mutable tables. A later retention sweeper may delete old terminal artifacts; retention is not yet implemented.

## API v1

- `GET /api/health` readiness.
- `GET /api/datasets` list registered datasets.
- `POST /api/datasets` body `{name,file}` with a basename referring to an existing `.parquet` inside `data/inputs`.
- `POST /api/jobs` body `{datasetId,sql,engine:"duckdb",mode:"cpu"}`. SQL must be one read-only SELECT; unsupported engines/modes return 400.
- `GET /api/jobs`, `GET /api/jobs/{id}`, `GET /api/jobs/{id}/logs`; `POST /api/jobs/{id}/cancel`.

All responses are JSON except logs (text). Jobs include wall time, timestamps, image, limits, dataset IDs, container ID, exit code/reason, log and output paths, failure and correlation ID. Worker CPU/memory measurements, traces, plans, and Spark event logs are explicitly unavailable in this milestone, not measured as zero. The API is local-only and unauthenticated.

## Layout and interoperability

`data/inputs` holds user-supplied Parquet, `data/staging` private in-flight artifacts, `data/outputs` published results, `data/logs` durable logs, `data/lakeforge.sqlite` metadata. Registration probes a file through a read-only inspection container and persists schema, row count and byte size. CrossQL may share explicitly copied Parquet into inputs; no shared metadata or unsafe implicit catalog sync. A remote worker later can consume the same versioned request/result schema through a different transport.
