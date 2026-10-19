"""Create a deterministic starter dataset with the same versioned worker image."""

import pathlib
import duckdb

destination = pathlib.Path("/inputs/sample.parquet")
duckdb.sql(
    "COPY (SELECT i AS id, i % 5 AS category, i * 1.25 AS amount "
    "FROM range(1000) t(i)) TO '/inputs/sample.parquet' (FORMAT PARQUET)"
)
print(f"Created {destination}")
