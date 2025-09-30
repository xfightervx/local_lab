#!/usr/bin/env python3
"""
Kafka Producer for Taxi Trips
-----------------------------
Reads a sample dataset (CSV or Parquet), maps it to the unified schema, and
publishes messages into Kafka topic `rides_raw` with time compression.
"""

import os
import time
import uuid
import json
import argparse
from datetime import datetime
from glob import glob

import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv



def parse_args():
    parser = argparse.ArgumentParser(description="Replay taxi data into Kafka.")
    parser.add_argument("--env", default="kafka/producer/.env", help="Path to .env file")
    return parser.parse_args()


def get_config():
    load_dotenv(".env")
    return {
        "BOOTSTRAP_SERVERS": os.getenv("BOOTSTRAP_SERVERS", "localhost:9092"),
        "TOPIC": os.getenv("TOPIC", "rides_raw"),
        "INPUT_PATH": os.getenv("INPUT_PATH"),
        "TIME_SPEEDUP": float(os.getenv("TIME_SPEEDUP", 1440)),
        "TS_FIELD": os.getenv("TS_FIELD", "tpep_pickup_datetime"),
        "KEY_FIELD": os.getenv("KEY_FIELD", ""),  # blank means auto-generate
        "SLEEP_MIN": float(os.getenv("SLEEP_MIN", 0.0)),
        "MAX_MESSAGES": int(os.getenv("MAX_MESSAGES","")) if os.getenv("MAX_MESSAGES") else None,
        "DRY_RUN": os.getenv("DRY_RUN", "false").lower() == "true",
    }



def make_payload(row, ts_field, key_field):
    """Convert a pandas row into a JSON-serializable dict following schema."""
    ride_id = str(row.get(key_field) or uuid.uuid4())

    event_time = row[ts_field]
    if pd.isna(event_time):
        return None
    if not isinstance(event_time, datetime):
        event_time = pd.to_datetime(event_time)

    payload = {
        "ride_id": ride_id,
        "event_time": event_time.isoformat(),
        "ingest_time": datetime.utcnow().isoformat(),
        "pickup_zone": str(row.get("PULocationID", "")),
        "dropoff_zone": str(row.get("DOLocationID", "")),
        "distance_km": float(row.get("trip_distance", 0.0)) * 1.60934,  # miles → km
        "fare_usd": float(row.get("fare_amount", 0.0)),
        "payment_type": str(row.get("payment_type", "unknown")),
    }
    return payload


def sleep_scaled(curr, prev, speedup, min_sleep):
    """Sleep according to time compression."""
    delta = (curr - prev).total_seconds()
    delay = max(delta / speedup, min_sleep)
    if delay > 0:
        time.sleep(delay)



def main():
    cfg = get_config()
    if not cfg["INPUT_PATH"]:
        raise ValueError("Environment variable INPUT_PATH is not set.")

    parquet_pattern = os.path.join(cfg["INPUT_PATH"], "part*.parquet")
    paths = glob(parquet_pattern)
    if not paths:
        raise FileNotFoundError(f"No parquet files found for pattern: {parquet_pattern}")

    print(f"[INFO] Loading {len(paths)} parquet files from {cfg['INPUT_PATH']}")
    df = pd.concat(pd.read_parquet(p) for p in paths)
    df = df.sort_values(by=cfg["TS_FIELD"]).reset_index(drop=True)

    producer = None
    if not cfg["DRY_RUN"]:
        producer = KafkaProducer(
            bootstrap_servers=cfg["BOOTSTRAP_SERVERS"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )

    prev_time = df.loc[0, cfg["TS_FIELD"]]
    if not isinstance(prev_time, datetime):
        prev_time = pd.to_datetime(str(prev_time))

    sent = 0
    start = time.time()

    for _, row in df.iterrows():
        payload = make_payload(row, cfg["TS_FIELD"], cfg["KEY_FIELD"])
        if not payload:
            continue

        curr_time = pd.to_datetime(row[cfg["TS_FIELD"]])

        # Sleep according to compression
        sleep_scaled(curr_time, prev_time, cfg["TIME_SPEEDUP"], cfg["SLEEP_MIN"])

        if cfg["DRY_RUN"]:
            print(json.dumps(payload))
        else:
            producer.send(cfg["TOPIC"], key=payload["ride_id"], value=payload)

        prev_time = curr_time
        sent += 1
        if cfg["MAX_MESSAGES"] and sent >= cfg["MAX_MESSAGES"]:
            break
        elapsed = time.time() - start
        print(f"[OK] Sent {sent} messages in {elapsed:.2f}s (topic={cfg['TOPIC']})")

    if producer:
        producer.flush()
        producer.close()



if __name__ == "__main__":
    main()
