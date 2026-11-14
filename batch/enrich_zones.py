"""Batch enrichment: trips + taxi-zone lookup + zone/hour and zone statistics.

The zone-hour window needs rows clustered by (PULocationID, pickup_hour); the
zone window, which reads the zone-hour result and so must run after it, needs
PULocationID. Left alone, Spark shuffles the trip side once for each (two
Exchanges). Repartitioning trips on PULocationID before the join satisfies
both, so the second Exchange disappears.

    spark-submit batch/enrich_zones.py --trips <parquet> --zones <csv> --out <dir> [--repartition]

Compare the printed physical plans and the Spark UI (duration, shuffle read).
"""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def build(spark, trips_path, zones_path, repartition):
    trips = spark.read.parquet(trips_path).withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    zones = (
        spark.read.option("header", True).csv(zones_path)
        .select(F.col("LocationID").cast("int").alias("PULocationID"), "Borough", "Zone")
    )
    if repartition:
        trips = trips.repartition("PULocationID")
    zone_hour = Window.partitionBy("PULocationID", "pickup_hour")
    zone = Window.partitionBy("PULocationID")
    return (
        trips.join(F.broadcast(zones), "PULocationID", "left")
        .withColumn("zone_hour_trips", F.count("*").over(zone_hour))
        .withColumn("zone_hour_avg_fare", F.avg("fare_amount").over(zone_hour))
        .withColumn("zone_avg_fare", F.avg("fare_amount").over(zone))
        # depends on the zone-hour window, so the zone window must run after it
        .withColumn("zone_peak_hour_trips", F.max("zone_hour_trips").over(zone))
        .withColumn("fare_vs_zone_avg", F.col("fare_amount") - F.col("zone_avg_fare"))
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--trips", required=True)
    ap.add_argument("--zones", required=True)
    ap.add_argument("--out", help="omit to only print the plan")
    ap.add_argument("--repartition", action="store_true")
    a = ap.parse_args()

    spark = SparkSession.builder.appName(f"tlc-enrich-repartition-{a.repartition}").getOrCreate()
    df = build(spark, a.trips, a.zones, a.repartition)
    df.explain()
    if a.out:
        import json
        import time
        from urllib.request import urlopen

        t0 = time.time()
        df.write.mode("overwrite").parquet(a.out)
        sc = spark.sparkContext
        stages = json.load(urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages"))
        shuffle_mb = sum(s["shuffleReadBytes"] for s in stages) / 1e6
        print(f"RESULT repartition={a.repartition} seconds={time.time() - t0:.1f} shuffle_read_mb={shuffle_mb:.0f}")
