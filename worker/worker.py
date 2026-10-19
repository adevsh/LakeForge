"""Version 1 of the dedicated DuckDB worker protocol."""

import json
import pathlib
import re
import sys

import duckdb


def main():
    work = pathlib.Path("/work")
    request = json.loads((work / "request.json").read_text())
    if request["version"] != 1 or not re.match(r"^[0-9a-f-]{36}$", request["jobId"]):
        raise ValueError("invalid worker request")
    sql = request["sql"].strip()
    if (
        not re.match(r"(?is)^select\b", sql)
        or not re.search(r"(?is)\bfrom\s+input\b", sql)
        or any(token in sql for token in (";", "--", "/*"))
    ):
        raise ValueError("only one SELECT against input is supported")
    database = work / "worker.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute("CREATE TABLE input AS SELECT * FROM read_parquet('/inputs/input.parquet')")
    connection.execute("SET enable_external_access = false")
    connection.execute(f"CREATE TABLE result AS {sql}")
    connection.close()
    connection = duckdb.connect(str(database))
    connection.execute("COPY result TO '/work/output.parquet' (FORMAT PARQUET)")
    result = connection.execute("DESCRIBE SELECT * FROM read_parquet('/work/output.parquet')").fetchall()
    rows = connection.execute("SELECT count(*) FROM read_parquet('/work/output.parquet')").fetchone()[0]
    response = {
        "version": 1,
        "jobId": request["jobId"],
        "rowCount": rows,
        "columns": [{"name": row[0], "type": row[1]} for row in result],
        "engineVersion": duckdb.__version__,
    }
    (work / "result.json").write_text(json.dumps(response))
    connection.close()
    database.unlink()
    print(f"Completed {rows} rows with DuckDB {duckdb.__version__}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Worker failed: {error}", file=sys.stderr, flush=True)
        raise
