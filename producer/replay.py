"""Replay NYC TLC Yellow Taxi Parquet files into Kafka as JSON events.

Each event keeps its original pickup/dropoff timestamps and carries a stable ID
derived from (source file, row position), so replaying a file always produces
the same IDs. The stable ID is also the Kafka key.
"""
import argparse
import hashlib
import json
import os
import sys

import pyarrow.parquet as pq

# source column -> event field
COLS = {
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "tip_amount": "tip_amount",
    "total_amount": "total_amount",
}


def stable_id(source_file: str, row_pos: int) -> str:
    return hashlib.sha256(f"{source_file}:{row_pos}".encode()).hexdigest()


def events(path, limit=None):
    name = os.path.basename(path)
    pos = 0
    for batch in pq.ParquetFile(path).iter_batches(batch_size=50_000, columns=list(COLS)):
        for row in batch.to_pylist():
            if limit is not None and pos >= limit:
                return
            yield {
                "stable_id": stable_id(name, pos),
                "source_file": name,
                "row_pos": pos,
                **{COLS[k]: v for k, v in row.items()},
            }
            pos += 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="tlc.trips")
    ap.add_argument("--limit", type=int, help="max rows per file (smoke tests)")
    args = ap.parse_args()

    from confluent_kafka import Producer

    failed = 0

    def on_delivery(err, _msg):
        nonlocal failed
        if err is not None:
            failed += 1

    p = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "lz4",
    })
    sent = 0
    for path in args.files:
        for ev in events(path, args.limit):
            value = json.dumps(ev, default=lambda d: d.isoformat()).encode("utf-8")
            while True:
                try:
                    p.produce(args.topic, key=ev["stable_id"], value=value, on_delivery=on_delivery)
                    break
                except BufferError:  # local queue full, let it drain
                    p.poll(1)
            p.poll(0)
            sent += 1
    p.flush()
    print(f"sent={sent} failed={failed}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
