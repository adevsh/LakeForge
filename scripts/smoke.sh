#!/usr/bin/env bash
# Run against an already-started control plane and a seeded sample.parquet.
set -euo pipefail

base="${LAKEFORGE_URL:-http://127.0.0.1:8080}"
runtime="${LAKEFORGE_RUNTIME:-}"
if [[ -z "$runtime" ]]; then
  if command -v podman >/dev/null 2>&1; then runtime="podman"; else runtime="docker"; fi
fi
root="${LAKEFORGE_DATA:-data}"
name="Smoke-$RANDOM-$$"

submit() {
  curl -fsS -X POST "$base/api/jobs" -H 'Content-Type: application/json' \
    -d "$(jq -n --arg dataset "$dataset" --arg sql "$1" \
      '{datasetId:$dataset,sql:$sql,engine:"duckdb",mode:"cpu"}')" | jq -r '.id'
}
status() { curl -fsS "$base/api/jobs/$1" | jq -r '.status'; }
await_status() {
  local id="$1" expected="$2" current
  for _ in {1..100}; do
    current="$(status "$id")"
    [[ "$current" == "$expected" ]] && return 0
    if [[ "$current" == "SUCCEEDED" || "$current" == "FAILED" ||
          "$current" == "CANCELLED" || "$current" == "TIMED_OUT" ]]; then
      printf 'Job %s ended unexpectedly: %s\n' "$id" "$current" >&2
      return 1
    fi
    sleep 0.2
  done
  printf 'Job %s did not reach %s\n' "$id" "$expected" >&2
  return 1
}

curl -fsS "$base/api/health" | jq -e '.ok == true' >/dev/null
dataset="$(curl -fsS "$base/api/datasets" | jq -r '[.[] | select(.file == "sample.parquet")][0].id // empty')"
if [[ -z "$dataset" ]]; then
  dataset="$(curl -fsS -X POST "$base/api/datasets" -H 'Content-Type: application/json' \
    -d "$(jq -n --arg name "$name" '{name:$name,file:"sample.parquet"}')" | jq -r '.id')"
fi
job="$(submit 'SELECT category, COUNT(*) AS count FROM input GROUP BY category')"
await_status "$job" SUCCEEDED
curl -fsS "$base/api/jobs/$job" | jq -e '.result.rowCount == 5 and .exitCode == 0' >/dev/null
test -s "$root/outputs/$job.parquet"
"$runtime" run --rm --entrypoint python \
  -v "$(realpath "$root/outputs"):/outputs:ro" lakeforge-duckdb:1.0.0 \
  -c "import duckdb; assert duckdb.sql(\"SELECT sum(count) FROM read_parquet('/outputs/$job.parquet')\").fetchone()[0] == 1000"
echo "Success path: $job"

failed="$(submit 'SELECT missing_column FROM input')"
await_status "$failed" FAILED
curl -fsS "$base/api/jobs/$failed/logs" | grep -q 'missing_column'
echo "Failure diagnostics: $failed"

cancelled="$(submit 'SELECT SUM(i) FROM input CROSS JOIN range(1000000000) t(i)')"
await_status "$cancelled" RUNNING
curl -fsS -X POST "$base/api/jobs/$cancelled/cancel" | jq -e '.status == "CANCELLED"' >/dev/null
for _ in {1..50}; do
  if [[ -z "$("$runtime" ps -a --filter "name=lakeforge-$cancelled" --format '{{.ID}}')" ]]; then
    break
  fi
  sleep 0.2
done
test -z "$("$runtime" ps -a --filter "name=lakeforge-$cancelled" --format '{{.ID}}')"
curl -fsS "$base/api/jobs/$cancelled" | jq -e '.status == "CANCELLED" and .exitReason == "USER_CANCELLED"' >/dev/null
echo "Cancellation path: $cancelled"
