# LakeForge

A local-first lakehouse workbench. The currently implemented vertical slice uses Scala 2.13, SQLite, Svelte 5 and one DuckDB container per query. Spark, DataFusion, GPU acceleration, benchmarks and OpenTelemetry are **not implemented yet**.

## Requirements

- Java 21 (Java 27 may work but is not the supported target), Scala CLI 1.9.1+, Node.js 20+, npm.
- Podman (preferred on Bazzite) or Docker with an active daemon. This application binds to loopback only and is not an authenticated network service.
- At least 1 GiB RAM per worker. Defaults: two concurrent workers, 1 CPU, 1 GiB memory, 120 seconds and a 256 MiB output cap. The versioned worker image is resolved to a local image digest at submission and that digest is used for execution.

## Start

```sh
docker build -t lakeforge-duckdb:1.0.0 worker
mkdir -p data/inputs
docker run --rm --entrypoint python -v "$PWD/data/inputs:/inputs:rw" lakeforge-duckdb:1.0.0 /app/seed.py
scala-cli run backend --server=false --main-class lakeforge.Main
```

The control plane prefers a Podman executable on PATH and otherwise runs Docker. Shell aliases are not visible to the control plane; `LAKEFORGE_RUNTIME=docker` explicitly selects Docker even when Podman is installed. On Bazzite, replace the image-build and seed commands with `podman`. In another terminal:

```sh
cd frontend
npm ci
npm run dev
```

Visit `http://127.0.0.1:5173`, register `sample.parquet` under a dataset name, then submit:

```sql
SELECT category, COUNT(*) AS count, SUM(amount) AS total FROM input GROUP BY category ORDER BY category
```

The UI shows status, persisted logs, output location and row count. Outputs are under `data/outputs`; `data/lakeforge.sqlite` preserves metadata across server restarts. You may use your own `.parquet` file in `data/inputs`; only a filename inside that directory can be registered. This is a local trusted-user workflow, not a public SQL endpoint.

Registration reads schema, row count and byte size in an isolated inspection container. Datasets registered before the schema migration have `null` inspection fields; new registrations include inspection metadata. Names and files are unique. Change defaults through `LAKEFORGE_DATA`, `LAKEFORGE_PORT`, `LAKEFORGE_RUNTIME`, `LAKEFORGE_DUCKDB_IMAGE`, `LAKEFORGE_CPUS`, `LAKEFORGE_MEMORY`, `LAKEFORGE_TIMEOUT_SECONDS`, `LAKEFORGE_CONCURRENCY` and `LAKEFORGE_MAX_OUTPUT_BYTES`. Invalid limits fail at startup. The output cap is checked after worker execution, not enforced as a filesystem quota.

For direct API access, `POST /api/datasets` with `{"name":"Sample","file":"sample.parquet"}`, then `POST /api/jobs` with `{"datasetId":"<id>","sql":"SELECT * FROM input","engine":"duckdb","mode":"cpu"}`. `GET /api/jobs/<id>/logs` returns durable worker logs; `POST /api/jobs/<id>/cancel` stops a running worker.

## Verify

```sh
scala-cli test backend --server=false
(cd frontend && npm run check && npm run build)
```

With the control plane and sample data running, the repeatable container smoke test requires `jq`:

```sh
bash scripts/smoke.sh
```

It checks actual Parquet contents, failure logs and container cancellation. Stop and restart the backend to verify durable history and restart reconciliation. The current DuckDB path passed backend tests, frontend checks, browser submission, Docker smoke tests and restart recovery. Native Podman/Bazzite, Linux-specific permissions and JDK 21 have not been verified yet. See [architecture](docs/architecture.md) for the API, state machine, publication limitations and isolation assumptions.
