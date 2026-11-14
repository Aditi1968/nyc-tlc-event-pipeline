"""PostgreSQL tables and the idempotent staging -> curated upsert.

    python scripts/merge.py --init
    python scripts/merge.py --dates 2024-01-01 2024-01-02

Used by the stream job (per micro-batch) and by the Airflow backfill DAG (one
event date at a time). Connection comes from the standard PG* environment
variables; defaults match docker-compose.yml.
"""
import argparse
import os

import psycopg2

TRIP_COLS = """
  stable_id text {pk}, source_file text, row_pos bigint,
  pickup_ts timestamp, dropoff_ts timestamp, passenger_count bigint,
  trip_distance double precision, pu_location_id bigint, do_location_id bigint,
  payment_type bigint, fare_amount numeric(12,2), tip_amount numeric(12,2),
  total_amount numeric(12,2), event_date date, ingested_at timestamp"""

DDL = f"""
CREATE TABLE IF NOT EXISTS staging_trips ({TRIP_COLS.format(pk="NOT NULL")});
CREATE INDEX IF NOT EXISTS staging_trips_event_date ON staging_trips (event_date);
CREATE TABLE IF NOT EXISTS curated_trips ({TRIP_COLS.format(pk="PRIMARY KEY")});
CREATE INDEX IF NOT EXISTS curated_trips_event_date ON curated_trips (event_date);
CREATE TABLE IF NOT EXISTS quarantine_trips (
  stable_id text, raw_value text, rejection_reason text NOT NULL,
  event_date date, ingested_at timestamp);
"""

# Staging can hold the same stable_id more than once after a replay, so the
# source side is deduplicated (DISTINCT ON) before the upsert. The primary key
# on curated makes a duplicate fact impossible at the database level.
UPSERT_SQL = """
INSERT INTO curated_trips
SELECT DISTINCT ON (stable_id) *
FROM staging_trips
WHERE event_date = ANY(%s::date[])
ORDER BY stable_id, ingested_at DESC
ON CONFLICT (stable_id) DO UPDATE SET
  source_file = EXCLUDED.source_file, row_pos = EXCLUDED.row_pos,
  pickup_ts = EXCLUDED.pickup_ts, dropoff_ts = EXCLUDED.dropoff_ts,
  passenger_count = EXCLUDED.passenger_count, trip_distance = EXCLUDED.trip_distance,
  pu_location_id = EXCLUDED.pu_location_id, do_location_id = EXCLUDED.do_location_id,
  payment_type = EXCLUDED.payment_type, fare_amount = EXCLUDED.fare_amount,
  tip_amount = EXCLUDED.tip_amount, total_amount = EXCLUDED.total_amount,
  event_date = EXCLUDED.event_date, ingested_at = EXCLUDED.ingested_at
"""


def pg_env():
    e = os.environ.get
    return {
        "host": e("PGHOST", "localhost"), "port": e("PGPORT", "5432"),
        "dbname": e("PGDATABASE", "tlc"), "user": e("PGUSER", "tlc"),
        "password": e("PGPASSWORD", "tlc"),
    }


def connect():
    return psycopg2.connect(**pg_env())


def init():
    with connect() as conn, conn.cursor() as cur:
        cur.execute(DDL)


def merge_dates(dates):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(UPSERT_SQL, ([str(d) for d in dates],))
        return cur.rowcount


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", action="store_true")
    ap.add_argument("--dates", nargs="*", default=[])
    a = ap.parse_args()
    if a.init:
        init()
    if a.dates:
        print("rows affected:", merge_dates(a.dates))
