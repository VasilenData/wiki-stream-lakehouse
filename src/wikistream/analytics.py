"""Query the lake with DuckDB (no cluster needed). Works on local paths or s3:// with httpfs."""
from __future__ import annotations

import argparse
import sys

import duckdb

QUERIES = {
    "top_wikis": """
        SELECT wiki, sum(edits) AS edits, round(100.0*sum(bot_edits)/sum(edits),1) AS bot_pct,
               sum(bytes_changed) AS bytes_changed
        FROM silver GROUP BY wiki ORDER BY edits DESC LIMIT 10""",
    "edits_per_minute": """
        SELECT window_start, sum(edits) AS edits FROM silver GROUP BY 1 ORDER BY 1""",
    "event_types": """
        SELECT type, count(*) AS n, round(100.0*count(*)/sum(count(*)) OVER (),1) AS pct
        FROM bronze GROUP BY type ORDER BY n DESC""",
    "end_to_end_check": """
        SELECT (SELECT count(*) FROM bronze) AS bronze_events,
               (SELECT sum(edits) FROM silver) AS silver_edits_in_closed_windows""",
}


def connect(lake: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"CREATE VIEW bronze AS SELECT * FROM read_parquet('{lake}/bronze_events/**/*.parquet', hive_partitioning=true)")
    con.execute(f"CREATE VIEW silver AS SELECT * FROM read_parquet('{lake}/silver_wiki_minute/*.parquet')")
    return con


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("query", choices=QUERIES)
    ap.add_argument("--lake", default="lake")
    a = ap.parse_args()
    df = connect(a.lake).execute(QUERIES[a.query]).df()
    df.to_string(sys.stdout, index=False)
    print()


if __name__ == "__main__":
    main()
