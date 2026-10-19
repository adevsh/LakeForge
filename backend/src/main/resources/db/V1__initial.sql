CREATE TABLE IF NOT EXISTS datasets (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  file TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  dataset_id TEXT NOT NULL REFERENCES datasets(id),
  sql_text TEXT NOT NULL,
  engine TEXT NOT NULL,
  mode TEXT NOT NULL,
  image TEXT NOT NULL,
  submitted_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  container_id TEXT,
  exit_code INTEGER,
  exit_reason TEXT,
  failure TEXT,
  output_file TEXT,
  result_json TEXT,
  trace_id TEXT NOT NULL,
  cpu_limit REAL NOT NULL,
  memory_limit TEXT NOT NULL,
  timeout_seconds INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs(status);
