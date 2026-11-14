"""Historical backfill: re-run the idempotent upsert one event date at a time.

"Partition" here means an event-date slice of the warehouse, not a Kafka partition.
Trigger with config: {"start_date": "2024-01-01", "end_date": "2024-01-07"}
"""
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"start_date": "2024-01-01", "end_date": "2024-01-07"},
)
def tlc_backfill():
    @task
    def event_dates(**context):
        start = date.fromisoformat(context["params"]["start_date"])
        end = date.fromisoformat(context["params"]["end_date"])
        if end < start:
            raise ValueError("end_date is before start_date")
        return [str(start + timedelta(days=i)) for i in range((end - start).days + 1)]

    @task(max_active_tis_per_dag=1, retries=2)  # one date at a time
    def merge_partition(event_date: str):
        from merge import merge_dates  # scripts/ is on PYTHONPATH (docker-compose.yml)

        return merge_dates([event_date])

    merge_partition.expand(event_date=event_dates())


tlc_backfill()
