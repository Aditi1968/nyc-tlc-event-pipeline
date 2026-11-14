"""Parse Kafka JSON trip events and attach a rejection reason (NULL = valid).

Every field is first read as a string so that a wrong numeric type is detected
here instead of silently becoming NULL inside from_json.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

FIELDS = [
    "stable_id", "source_file", "row_pos", "pickup_ts", "dropoff_ts",
    "passenger_count", "trip_distance", "pu_location_id", "do_location_id",
    "payment_type", "fare_amount", "tip_amount", "total_amount",
]
REQUIRED = ["stable_id", "pickup_ts", "dropoff_ts", "trip_distance", "fare_amount", "pu_location_id"]
RAW_SCHEMA = StructType([StructField(f, StringType()) for f in FIELDS])


def _int(field):  # via double: the source stores some integer columns as 1.0
    return F.expr(f"cast(try_cast(j.{field} as double) as bigint)")


def _money(field):
    return F.expr(f"try_cast(j.{field} as decimal(12,2))")


def parse_and_validate(df: DataFrame) -> DataFrame:
    """df needs string columns `key` and `value`."""
    typed = df.withColumn("j", F.from_json("value", RAW_SCHEMA)).select(
        F.coalesce(F.col("j.stable_id"), F.col("key")).alias("stable_id"),
        F.col("j.source_file").alias("source_file"),
        _int("row_pos").alias("row_pos"),
        F.expr("try_to_timestamp(j.pickup_ts)").alias("pickup_ts"),
        F.expr("try_to_timestamp(j.dropoff_ts)").alias("dropoff_ts"),
        _int("passenger_count").alias("passenger_count"),
        F.expr("try_cast(j.trip_distance as double)").alias("trip_distance"),
        _int("pu_location_id").alias("pu_location_id"),
        _int("do_location_id").alias("do_location_id"),
        _int("payment_type").alias("payment_type"),
        _money("fare_amount").alias("fare_amount"),
        _money("tip_amount").alias("tip_amount"),
        _money("total_amount").alias("total_amount"),
        F.col("value").alias("raw_value"),
        F.col("j").alias("j"),
    )
    missing = F.lit(False)
    for f in REQUIRED:
        missing = missing | F.col(f"j.{f}").isNull()
    reason = (
        F.when(missing, "missing_required_field")
        .when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), "unparseable_timestamp")
        .when(
            F.col("fare_amount").isNull() | F.col("trip_distance").isNull() | F.col("pu_location_id").isNull(),
            "invalid_numeric_type",
        )
        .when(F.col("dropoff_ts") < F.col("pickup_ts"), "dropoff_before_pickup")
        .when(F.col("fare_amount") < 0, "negative_fare")
        .when(F.col("trip_distance") < 0, "negative_distance")
    )
    return (
        typed.withColumn("rejection_reason", reason)
        .withColumn("event_date", F.to_date("pickup_ts"))
        .drop("j")
    )
