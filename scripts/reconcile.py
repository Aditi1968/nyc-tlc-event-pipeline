"""Cross-layer reconciliation after replaying the given source files.

    python scripts/reconcile.py data/yellow_tripdata_2024-01.parquet [--limit N]

Checks (exit 1 if any fails):
  source rows        == curated rows + distinct quarantined IDs
  staging distinct   == curated rows
  duplicate stable_ids in curated == 0
  source fare (cents) == curated fare + quarantined fare
"""
import argparse
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from merge import connect

SQL = r"""
WITH q AS (
  SELECT DISTINCT ON (stable_id) stable_id,
    CASE WHEN raw_value IS JSON OBJECT
          AND (raw_value::jsonb ->> 'fare_amount') ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN (raw_value::jsonb ->> 'fare_amount')::numeric END AS fare
  FROM quarantine_trips ORDER BY stable_id
)
SELECT
  (SELECT count(*) FROM staging_trips) AS staging_rows,
  (SELECT count(DISTINCT stable_id) FROM staging_trips) AS staging_distinct,
  (SELECT count(*) FROM curated_trips) AS curated_rows,
  (SELECT count(*) - count(DISTINCT stable_id) FROM curated_trips) AS curated_duplicates,
  (SELECT coalesce(sum(round(fare_amount * 100)), 0)::bigint FROM curated_trips) AS curated_fare_cents,
  (SELECT count(*) FROM q) AS quarantine_distinct,
  (SELECT coalesce(sum(round(fare * 100)), 0)::bigint FROM q) AS quarantine_fare_cents
"""


def source_totals(paths, limit=None):
    rows = cents = 0
    for path in paths:
        fare = pq.read_table(path, columns=["fare_amount"]).column("fare_amount")
        if limit is not None:
            fare = fare.slice(0, limit)
        rows += len(fare)
        cents += pc.sum(pc.cast(pc.round(pc.multiply(fare, 100)), pa.int64())).as_py() or 0
    return rows, cents


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--limit", type=int, help="rows per file, if the producer used --limit")
    a = ap.parse_args()

    src_rows, src_cents = source_totals(a.files, a.limit)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(SQL)
        r = dict(zip([d[0] for d in cur.description], cur.fetchone()))
    print(f"source_rows={src_rows} source_fare_cents={src_cents}")
    for k, v in r.items():
        print(f"{k}={v}")

    checks = {
        "source == curated + quarantine": src_rows == r["curated_rows"] + r["quarantine_distinct"],
        "staging distinct == curated": r["staging_distinct"] == r["curated_rows"],
        "zero duplicate curated facts": r["curated_duplicates"] == 0,
        "fare aggregates match": src_cents == r["curated_fare_cents"] + r["quarantine_fare_cents"],
    }
    for name, ok in checks.items():
        print("PASS" if ok else "FAIL", name)
    sys.exit(0 if all(checks.values()) else 1)


if __name__ == "__main__":
    main()
