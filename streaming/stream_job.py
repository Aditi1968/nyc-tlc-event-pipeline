"""Kafka -> validate -> PostgreSQL staging / quarantine -> upsert into curated.

Delivery to staging is at-least-once (a killed batch is re-run from the
checkpoint). Curated stays duplicate-free because the upsert keys on stable_id.

    spark-submit --packages <kafka connector>,<postgres jdbc> --py-files scripts/merge.py \
        streaming/stream_job.py --bootstrap kafka:29092 --checkpoint /out/checkpoint
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from merge import init, merge_dates, pg_env
from validate import parse_and_validate

QUARANTINE_COLS = ["stable_id", "raw_value", "rejection_reason", "event_date", "ingested_at"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", default="tlc.trips")
    ap.add_argument("--checkpoint", required=True)
    ap.add_argument("--max-offsets", default="200000", help="max records per micro-batch")
    args = ap.parse_args()

    pg = pg_env()
    jdbc = {
        "url": f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['dbname']}?reWriteBatchedInserts=true",
        "user": pg["user"], "password": pg["password"],
        "driver": "org.postgresql.Driver", "batchsize": "10000",
    }
    init()

    spark = SparkSession.builder.appName("tlc-stream").getOrCreate()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
        .select(F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"))
    )
    parsed = parse_and_validate(raw)

    def sink(batch, _batch_id):
        batch = batch.withColumn("ingested_at", F.current_timestamp()).persist()
        try:
            valid = batch.filter("rejection_reason IS NULL").drop("raw_value", "rejection_reason")
            invalid = batch.filter("rejection_reason IS NOT NULL").select(*QUARANTINE_COLS)
            for df, table in ((invalid, "quarantine_trips"), (valid, "staging_trips")):
                df.write.format("jdbc").options(dbtable=table, **jdbc).mode("append").save()
            dates = [r[0] for r in valid.select("event_date").distinct().collect()]
            if dates:
                merge_dates(dates)
        finally:
            batch.unpersist()

    (
        parsed.writeStream.foreachBatch(sink)
        .option("checkpointLocation", args.checkpoint)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
