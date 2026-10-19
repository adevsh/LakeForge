"""Read schema and size of an explicitly mounted Parquet file."""

import json
import pathlib

import duckdb

input_file = pathlib.Path("/inputs/input.parquet")
connection = duckdb.connect()
columns = connection.execute(
    "DESCRIBE SELECT * FROM read_parquet('/inputs/input.parquet')"
).fetchall()
rows = connection.execute(
    "SELECT count(*) FROM read_parquet('/inputs/input.parquet')"
).fetchone()[0]
print(
    json.dumps(
        {
            "columns": [{"name": row[0], "type": row[1]} for row in columns],
            "rowCount": rows,
            "byteSize": input_file.stat().st_size,
        }
    )
)
