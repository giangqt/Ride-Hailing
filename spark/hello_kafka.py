#!/usr/bin/env python3
"""
spark/hello_kafka.py — Phase 3 Block 1 hello world.

This is the smallest Spark Structured Streaming job that proves the new
infrastructure works:

    Read from ride-events-raw  →  count messages per micro-batch  →  print

No business logic, no JSON parsing, no joins. The only thing this verifies
is the wiring:

  * Spark cluster talks to Kafka cluster
  * The kafka-sql-spark connector loads correctly
  * Checkpoint directory mounts and writes are working
  * Both host mode and docker mode produce identical behavior

Once this prints non-zero counts when the replay producer is running, every
subsequent Spark job we write can assume the infrastructure is sound.

Run modes
---------

Host mode (fast dev — uses the .venv on your laptop):

    python spark/hello_kafka.py

Docker mode (matches what the thesis demo will look like):

    docker compose exec spark-master spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        /app/spark/hello_kafka.py

Then in another terminal, run the replay producer at any speed:

    python scripts/replay_producer.py \\
        --file data/raw_trips/fhvhv_tripdata_2024-07.parquet \\
        --speed 100 \\
        --max-events 5000

You should see Spark printing batch summaries every 10 seconds with the
message count climbing as the producer feeds the topic.

Exit cleanly with Ctrl-C.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow `python spark/hello_kafka.py` from the repo root to find _session.py
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F  # noqa: E402

from _session import get_session, kafka_brokers, checkpoint_dir  # noqa: E402

TOPIC = "ride-events-raw"
QUERY_NAME = "hello_kafka"


def main() -> int:
    spark = get_session("hello_kafka")
    # Quiet down the worker noise — keep INFO so we still see batch progress.
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print(f"  Spark master:    {spark.sparkContext.master}")
    print(f"  Kafka brokers:   {kafka_brokers()}")
    print(f"  Subscribing to:  {TOPIC}")
    print(f"  Checkpoint:      {checkpoint_dir(QUERY_NAME)}")
    print("=" * 70)

    # readStream returns a DataFrame whose rows look like:
    #   key:bytes, value:bytes, topic:string, partition:int,
    #   offset:long, timestamp:timestamp, timestampType:int
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers())
        .option("subscribe", TOPIC)
        # 'latest' so we only see new messages produced AFTER this job started.
        # Set to 'earliest' if you want to process the full topic history.
        .option("startingOffsets", "latest")
        # Keep going if a topic partition was deleted/recreated. Never set this
        # to true in production — but for our dev cluster where we periodically
        # reset topics, false is the safer default to surface real issues.
        .option("failOnDataLoss", "false")
        .load()
    )

    # The smallest meaningful transform: count messages per micro-batch and
    # show the most recent kafka timestamp seen. We use a tumbling window
    # WITHOUT groupBy so this stays stateless — no watermark needed.
    summary = raw.select(
        F.lit(1).alias("msg"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
    )

    # writeStream with format("console") prints each micro-batch's rows to
    # stdout. With the .trigger(processingTime="10 seconds") below, every
    # 10s Spark prints however many messages it pulled in that batch.
    query = (
        summary.writeStream
        .format("console")
        .option("truncate", "false")
        .option("numRows", "5")             # show first 5 rows per batch
        .option("checkpointLocation", checkpoint_dir(QUERY_NAME))
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName(QUERY_NAME)
        .start()
    )

    print(f"\nStreaming query '{query.name}' started.")
    print("  → Run replay_producer.py in another terminal to see batches arrive.")
    print("  → Spark UI: http://localhost:4040 (host) or :8082 (docker master)")
    print("  → Press Ctrl-C to stop.\n")

    # awaitTermination blocks the main thread until the query stops (Ctrl-C
    # or an exception). Spark continues consuming Kafka in background threads.
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
