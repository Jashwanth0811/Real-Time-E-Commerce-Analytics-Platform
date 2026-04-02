"""
PySpark Structured Streaming Job — Kafka → Bronze Layer (PostgreSQL)

Reads from ecommerce.events and ecommerce.orders topics,
applies schema validation, deduplicates on event_id/order_id,
and writes to bronze.raw_events / bronze.raw_orders.

Run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
    org.postgresql:postgresql:42.7.1 \
    spark_streaming.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, ArrayType, MapType
)
import os

# ─── Config ──────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DB_URL          = os.getenv("DB_URL",
    "jdbc:postgresql://localhost:5432/ecommerce_dw")
DB_USER         = os.getenv("DB_USER", "warehouse")
DB_PASSWORD     = os.getenv("DB_PASSWORD", "warehouse123")
DB_DRIVER       = "org.postgresql.Driver"
CHECKPOINT_DIR  = "/tmp/spark_checkpoints"

# ─── Schemas ─────────────────────────────────────────────────

EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),  True),
    StructField("event_type",  StringType(),  True),
    StructField("user_id",     StringType(),  True),
    StructField("session_id",  StringType(),  True),
    StructField("product_id",  StringType(),  True),
    StructField("category",    StringType(),  True),
    StructField("price",       DoubleType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("page_url",    StringType(),  True),
    StructField("referrer",    StringType(),  True),
    StructField("device_type", StringType(),  True),
    StructField("country",     StringType(),  True),
    StructField("city",        StringType(),  True),
    StructField("ts",          StringType(),  True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id",       StringType(),  True),
    StructField("user_id",        StringType(),  True),
    StructField("status",         StringType(),  True),
    StructField("total_amount",   DoubleType(),  True),
    StructField("discount_pct",   DoubleType(),  True),
    StructField("payment_method", StringType(),  True),
    StructField("created_at",     StringType(),  True),
    StructField("updated_at",     StringType(),  True),
])

# ─── JDBC write helper ───────────────────────────────────────

def write_to_postgres(df, table: str, mode: str = "append"):
    """Write a DataFrame to PostgreSQL via JDBC."""
    (df.write
       .format("jdbc")
       .option("url",      DB_URL)
       .option("dbtable",  table)
       .option("user",     DB_USER)
       .option("password", DB_PASSWORD)
       .option("driver",   DB_DRIVER)
       .mode(mode)
       .save())


def write_batch_events(batch_df, epoch_id: int):
    """Micro-batch writer for events — dedup + write."""
    if batch_df.isEmpty():
        return

    cleaned = (batch_df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_type").isin([
            "page_view", "add_to_cart", "remove_from_cart",
            "wishlist", "purchase"
        ]))
        .withColumn("price", F.when(
            F.col("price") < 0, F.lit(None)).otherwise(F.col("price")))
        .dropDuplicates(["event_id"])
        .withColumn("raw_payload", F.to_json(F.struct("*")))
        .withColumn("ingested_at", F.current_timestamp())
    )

    write_to_postgres(cleaned, "bronze.raw_events")
    count = cleaned.count()
    print(f"[Streaming] epoch={epoch_id} | events written={count:,}")


def write_batch_orders(batch_df, epoch_id: int):
    """Micro-batch writer for orders."""
    if batch_df.isEmpty():
        return

    cleaned = (batch_df
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("status").isNotNull())
        .dropDuplicates(["order_id"])
        .withColumn("raw_payload", F.to_json(F.struct("*")))
        .withColumn("ingested_at", F.current_timestamp())
    )

    write_to_postgres(cleaned, "bronze.raw_orders")
    count = cleaned.count()
    print(f"[Streaming] epoch={epoch_id} | orders written={count:,}")


# ─── Spark session ───────────────────────────────────────────

def create_spark():
    return (SparkSession.builder
        .appName("EcommerceStreamingIngestion")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .getOrCreate())


# ─── Main ────────────────────────────────────────────────────

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("[Streaming] Spark session started")

    # ── Events stream ──
    events_raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "ecommerce.events")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 5000)
        .option("failOnDataLoss", "false")
        .load())

    events_df = (events_raw
        .selectExpr("CAST(value AS STRING) AS json_str",
                    "timestamp AS kafka_ts")
        .select(F.from_json("json_str", EVENT_SCHEMA).alias("data"), "kafka_ts")
        .select("data.*", "kafka_ts"))

    events_query = (events_df.writeStream
        .foreachBatch(write_batch_events)
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/events")
        .start())

    # ── Orders stream ──
    orders_raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "ecommerce.orders")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .option("failOnDataLoss", "false")
        .load())

    orders_df = (orders_raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", ORDER_SCHEMA).alias("data"))
        .select("data.*"))

    orders_query = (orders_df.writeStream
        .foreachBatch(write_batch_orders)
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/orders")
        .start())

    print("[Streaming] Both streams running. Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
