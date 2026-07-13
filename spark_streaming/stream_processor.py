"""
Spark Structured Streaming job:
  - reads live events from the 'react-events' Kafka topic
  - writes every raw event into Postgres (feeds Project 1's growth
    accounting views, which read from raw_github_events -> user_activity)
  - also computes a rolling 10-minute window of activity (event count,
    unique active users) for a real-time operational view
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, approx_count_distinct, count
from pyspark.sql.types import StructType, StructField, StringType

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5433"
POSTGRES_DB = os.getenv("POSTGRES_DB", "react_pipeline")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

spark = (
    SparkSession.builder.appName("ReactEventsStreamProcessor")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.7.3",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

event_schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("actor_login", StringType()),
        StructField("repo_name", StringType()),
        StructField("event_created_at", StringType()),
    ]
)

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", "react-events")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = raw_stream.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select("data.*")

parsed = parsed.withColumn(
    "event_created_at", col("event_created_at").cast("timestamp")
)


def write_raw_events(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df = batch_df.dropDuplicates(["event_id"])
    try:
        (
            batch_df.select(
                "event_id", "event_type", "actor_login", "repo_name", "event_created_at"
            )
            .write.jdbc(
                url=JDBC_URL,
                table="raw_github_events",
                mode="append",
                properties=JDBC_PROPERTIES,
            )
        )
        print(f"Batch {batch_id}: wrote {batch_df.count()} raw events")
    except Exception as e:
        print(f"Batch {batch_id}: write failed (likely duplicate events) -- {e}")


raw_query = (
    parsed.writeStream.foreachBatch(write_raw_events)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/tmp/spark-checkpoints/raw_events")
    .start()
)

windowed = (
    parsed.withWatermark("event_created_at", "10 minutes")
    .groupBy(window(col("event_created_at"), "10 minutes"))
    .agg(
        count("*").alias("total_events"),
        approx_count_distinct("actor_login").alias("unique_active_users"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "total_events",
        "unique_active_users",
    )
)


def write_aggregates(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    try:
        batch_df.write.jdbc(
            url=JDBC_URL,
            table="realtime_aggregates",
            mode="append",
            properties=JDBC_PROPERTIES,
        )
        print(f"Batch {batch_id}: wrote {batch_df.count()} aggregate window(s)")
    except Exception as e:
        print(f"Batch {batch_id}: aggregate write failed -- {e}")


agg_query = (
    windowed.writeStream.foreachBatch(write_aggregates)
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/tmp/spark-checkpoints/aggregates")
    .start()
)

print("Streaming job running. Waiting for data...")
raw_query.awaitTermination()
