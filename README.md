# NYC TLC Event Pipeline

Replay recovery and metric CI on NYC Yellow Taxi trip data. Runs entirely on a laptop with Docker: no cloud account, no cost.

Historical TLC Parquet files are replayed as events through Kafka, validated by Spark Structured Streaming, and landed in PostgreSQL. The project exists to test the things that usually go wrong: replays, bad records, a stream dying mid-run, backfills, and SQL changes that silently move a metric.

```
TLC Parquet -> producer/replay.py -> Kafka -> Spark Structured Streaming (checkpointed)
                 stable_id key                    | parse + validate
                                      invalid ----+---- valid
                                         |               |
                              Postgres quarantine   Postgres staging -> upsert on stable_id -> curated
                              (+ rejection reason)                                              |
                                                                                    dbt -> daily metrics
```

Stack: Python, PySpark (Structured Streaming and batch), Kafka, PostgreSQL, Airflow, dbt, GitHub Actions, Docker Compose.

## How correctness under replay works

| Mechanism | What it does |
|---|---|
| Stable ID = SHA-256(source file + row position) | Same source record gets the same ID on every replay. A Kafka offset is only a position in one partition and changes on replay; the stable ID does not. |
| Kafka retention | Events can be re-read after a failure. |
| Spark checkpoint | Restores stream progress, so a restarted job continues from the last committed batch. Delivery to staging is at-least-once. |
| Upsert on stable ID (`INSERT ... ON CONFLICT DO UPDATE`, primary key on curated) | Makes curated idempotent: re-delivered rows update instead of duplicating. |
| Airflow backfill DAG | Re-runs the upsert one event date at a time. |
| `scripts/reconcile.py` | Source vs staging vs curated counts and fare totals, duplicate check. |

Rejected records (missing required field, unparseable timestamp, dropoff before pickup, negative fare, negative distance, invalid numeric type) go to `quarantine_trips` with the raw payload and a rejection reason.

## Layout

| Path | Purpose |
|---|---|
| `producer/replay.py` | Parquet -> Kafka (UTF-8 JSON, key = stable ID, original timestamps kept) |
| `streaming/validate.py` | Validation rules |
| `streaming/stream_job.py` | Structured Streaming job |
| `scripts/merge.py` | Table DDL and the upsert |
| `scripts/reconcile.py` | Cross-layer reconciliation |
| `batch/enrich_zones.py` | Zone-lookup enrichment with the repartition optimization |
| `airflow/dags/backfill_dag.py` | Per-date backfill |
| `dbt/` | Metric models and deterministic fixtures |
| `ci/`, `.github/workflows/metric_ci.yml` | Metric regression CI |

## Run it

Needs Docker and Python 3.12. Git Bash on Windows: prefix `docker compose run/exec` commands with `MSYS_NO_PATHCONV=1`.

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt -r requirements-dbt.txt   # Linux/macOS: .venv/bin/pip
mkdir -p data
curl -o data/yellow_tripdata_2024-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
curl -o data/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

docker compose up -d kafka postgres
.venv/Scripts/python producer/replay.py data/yellow_tripdata_2024-01.parquet --limit 500000
docker compose --profile local run --rm spark          # kill it mid-run and start it again to test recovery
PYTHONPATH=scripts .venv/Scripts/python scripts/reconcile.py data/yellow_tripdata_2024-01.parquet --limit 500000
```

Backfill (one upsert task per event date):

```bash
docker compose --profile airflow run --rm --entrypoint bash airflow -c \
  'airflow db migrate && airflow dags test tlc_backfill --conf "{\"start_date\": \"2024-01-01\", \"end_date\": \"2024-01-06\"}"'
```

Metrics: `cd dbt && DBT_PROFILES_DIR=. ../.venv/Scripts/dbt build`

Enrichment, without and with the optimization:

```bash
docker compose --profile local run --rm --entrypoint /opt/spark/bin/spark-submit spark --driver-memory 2g \
  batch/enrich_zones.py --trips /app/data/yellow_tripdata_2024-01.parquet --zones /app/data/taxi_zone_lookup.csv \
  --out /out/enriched            # add --repartition for the optimized run
```

Tests (inside the Spark image, so no local Java/PySpark setup is needed):

```bash
docker compose --profile local run --rm spark 'pip install -q pytest pyarrow && PYTHONPATH=/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip) python3 -m pytest -q -p no:cacheprovider'
```

## Metric CI

On a PR that touches `dbt/` or `ci/`, GitHub Actions compares dbt state against the base branch, builds the modified models and everything downstream of them on seed fixtures (duplicate IDs, a midnight boundary, zero and negative fares) in an isolated `ci_pr_<number>` schema of a throwaway Postgres service container, and compares the resulting metrics exactly against `ci/baseline.json`. Any unexpected difference fails the PR. An intended metric change updates `baseline.json` in the same PR, which makes the change visible in review. No secrets are needed.

## Results

All measured on one laptop (Docker Desktop, 8 GB VM, Spark local mode), 14 Nov 2026. Source: the first 500,000 rows of `yellow_tripdata_2024-01.parquet` for the streaming tests, the full file (2.96M rows) for the enrichment job.

**Streaming, failure and replay**

| Measure | Value |
|---|---|
| Events replayed into Kafka | 500,000 (0 delivery failures, about 31 s) |
| Curated trips / quarantined | 493,031 / 6,969 (1.4%: 6,965 negative fare, 4 dropoff before pickup) |
| Kill test | stream killed mid-batch after about 300,000 events, restarted from its checkpoint, finished the remaining events |
| Replay test | 100,000 already-processed events replayed: staging grew to 591,450 rows (98,419 re-delivered), curated stayed at 493,031 |
| Duplicate curated facts | 0 |
| Reconciliation | source rows = curated + quarantined; fare totals match to the cent |
| Backfill | one event date (79,344 trips) deleted from curated and rebuilt by the Airflow DAG; reconciliation passes |

**Enrichment job (2.96M trips, two runs each)**

| | Shuffles of trip data | Shuffle read | Runtime |
|---|---|---|---|
| Before | 2 | 311 MB | 25.5 to 26.3 s |
| After `repartition("PULocationID")` | 1 | 151 MB | 21.1 to 22.9 s |

**Metric CI (same steps as the workflow, run locally; not yet run on GitHub Actions)**

| Case | Outcome |
|---|---|
| Edit that does not change metrics | passes, about 41 s for state compare + build + compare |
| Edit counting unpaid trips as paid | fails, reporting `paid_trip_count: got 3, expected 2` for both fixture dates |
