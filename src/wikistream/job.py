"""Spark Structured Streaming job: Kafka -> bronze events, silver 1-minute aggregates, dead letters.

Design choices (see README):
* Event-time windows with a watermark, so late events are handled by a rule, not by luck.
* `dropDuplicatesWithinWatermark` on (wiki, event id), bounded by the watermark.
* Records that fail parsing go to a dead-letter folder instead of crashing or vanishing.
* Each file sink has its own checkpoint; the integration test covers checkpoint reuse.
* Bronze follows watermarking/deduplication. Late records are not archived there.
* `failOnDataLoss=false` allows missing Kafka offsets to be skipped; see README for scope.
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from .schema import TOPIC

SCHEMA = StructType([
    StructField("id", LongType()), StructField("wiki", StringType()), StructField("type", StringType()),
    StructField("bot", BooleanType()), StructField("user", StringType()), StructField("title", StringType()),
    StructField("bytes_delta", LongType()), StructField("event_ts", LongType()),
])
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def build_spark(master: str = "local[2]") -> SparkSession:
    return (SparkSession.builder.master(master).appName("wikistream")
            .config("spark.jars.packages", KAFKA_PKG)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false").getOrCreate())


def run(a: argparse.Namespace) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", a.bootstrap).option("subscribe", TOPIC)
           .option("startingOffsets", a.starting_offsets).option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", a.max_offsets).load())

    parsed = (raw.select(F.col("value").cast("string").alias("json"))
              .withColumn("e", F.from_json("json", SCHEMA)))
    ok = F.col("e").isNotNull() & F.col("e.id").isNotNull() & F.col("e.wiki").isNotNull() & F.col("e.event_ts").isNotNull()

    dead = parsed.filter(~ok).select("json", F.current_timestamp().alias("rejected_at"))
    events = (parsed.filter(ok).select("e.*")
              .withColumn("event_time", F.col("event_ts").cast("timestamp"))
              .withWatermark("event_time", a.watermark)
              .dropDuplicatesWithinWatermark(["wiki", "id"]))

    trig = {"availableNow": True} if a.once else {"processingTime": a.trigger}
    out = a.out

    def sink(df, name, partition=None, mode="append"):
        w = df.writeStream.format("parquet").outputMode(mode).option("path", f"{out}/{name}") \
              .option("checkpointLocation", f"{a.checkpoint}/{name}").queryName(name)
        if partition:
            w = w.partitionBy(partition)
        return w.trigger(**trig).start()

    bronze = sink(events.withColumn("dt", F.to_date("event_time")), "bronze_events", "dt")
    silver = sink(
        events.groupBy(F.window("event_time", "1 minute").alias("w"), "wiki")
        .agg(F.count("*").alias("edits"),
             F.sum(F.col("bot").cast("int")).alias("bot_edits"),
             F.sum(F.abs("bytes_delta")).alias("bytes_changed"),
             F.approx_count_distinct("user").alias("editors"))
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "wiki",
                "edits", "bot_edits", "bytes_changed", "editors"),
        "silver_wiki_minute")
    dl = sink(dead, "dead_letters")
    for q in (bronze, silver, dl):
        q.awaitTermination() if a.once else None
    if not a.once:
        spark.streams.awaitAnyTermination()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--out", default=os.getenv("LAKE_PATH", "lake"))
    ap.add_argument("--checkpoint", default=os.getenv("CHECKPOINT_PATH", "checkpoints"))
    ap.add_argument("--watermark", default="2 minutes", help="how late an event may arrive and still count")
    ap.add_argument("--trigger", default="15 seconds")
    ap.add_argument("--starting-offsets", default="earliest")
    ap.add_argument("--max-offsets", type=int, default=50000)
    ap.add_argument("--once", action="store_true", help="process what is available, then stop (used in tests)")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
