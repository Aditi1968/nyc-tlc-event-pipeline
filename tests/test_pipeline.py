import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from compare_metrics import diff
from replay import stable_id
from validate import parse_and_validate

GOOD = {
    "stable_id": "s1", "source_file": "f.parquet", "row_pos": 0,
    "pickup_ts": "2024-01-01T10:00:00", "dropoff_ts": "2024-01-01T10:20:00",
    "passenger_count": 1.0, "trip_distance": 2.5, "pu_location_id": 132,
    "do_location_id": 7, "payment_type": 1, "fare_amount": 12.5,
    "tip_amount": 2.0, "total_amount": 16.0,
}


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def run(spark, value, key="k"):
    # lit() keeps this JVM-only: no Python workers needed
    df = spark.range(1).select(F.lit(key).alias("key"), F.lit(value).alias("value"))
    return parse_and_validate(df).collect()[0]


def test_stable_id_is_deterministic_and_distinct():
    assert stable_id("f.parquet", 7) == stable_id("f.parquet", 7)
    assert stable_id("f.parquet", 7) != stable_id("f.parquet", 8)
    assert stable_id("f.parquet", 7) != stable_id("g.parquet", 7)


def test_valid_event(spark):
    r = run(spark, json.dumps(GOOD))
    assert r.rejection_reason is None
    assert str(r.event_date) == "2024-01-01"
    assert float(r.fare_amount) == 12.5
    assert r.passenger_count == 1 and r.pu_location_id == 132


@pytest.mark.parametrize(
    "change, reason",
    [
        ({"fare_amount": None}, "missing_required_field"),
        ({"pickup_ts": "not a time"}, "unparseable_timestamp"),
        ({"fare_amount": "abc"}, "invalid_numeric_type"),
        ({"dropoff_ts": "2024-01-01T09:00:00"}, "dropoff_before_pickup"),
        ({"fare_amount": -3.0}, "negative_fare"),
        ({"trip_distance": -1.0}, "negative_distance"),
    ],
)
def test_rejections(spark, change, reason):
    r = run(spark, json.dumps({**GOOD, **change}))
    assert r.rejection_reason == reason
    assert r.raw_value is not None


def test_garbage_payload_keeps_kafka_key(spark):
    r = run(spark, "{not json", key="the-key")
    assert r.rejection_reason == "missing_required_field"
    assert r.stable_id == "the-key"


def test_metric_diff():
    base = [{"event_date": "2024-01-01", "total_fare": "19.5"}]
    assert diff(base, [{"event_date": "2024-01-01", "total_fare": "19.50"}]) == []
    assert diff(base, [{"event_date": "2024-01-01", "total_fare": "19.51"}])
    assert diff(base, [])


def test_repartition_removes_an_exchange(spark, tmp_path):
    from enrich_zones import build

    trips = tmp_path / "trips"
    zones = tmp_path / "zones.csv"
    spark.sql(
        "select timestamp'2024-01-01 10:00:00' as tpep_pickup_datetime, 132 as PULocationID, 10.0D as fare_amount"
    ).write.parquet(str(trips))
    zones.write_text("LocationID,Borough,Zone\n132,Queens,JFK Airport\n")

    def exchanges(repartition):
        plan = build(spark, str(trips), str(zones), repartition)._jdf.queryExecution().executedPlan().toString()
        return plan.count("Exchange hashpartitioning")

    assert exchanges(True) < exchanges(False)
