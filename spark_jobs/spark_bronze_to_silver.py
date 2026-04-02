"""
PySpark Batch Job — Bronze → Silver Layer

Reads raw Bronze tables, applies:
  - Type casting & null handling
  - Timestamp normalization
  - Outlier filtering (negative prices, impossible quantities)
  - User dimension building
  - Deduplication

Run:
  spark-submit --packages org.postgresql:postgresql:42.7.1 \
    spark_bronze_to_silver.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
import os
from datetime import datetime, timedelta

# ─── Config ──────────────────────────────────────────────────

DB_URL      = os.getenv("DB_URL", "jdbc:postgresql://localhost:5432/ecommerce_dw")
DB_USER     = os.getenv("DB_USER", "warehouse")
DB_PASSWORD = os.getenv("DB_PASSWORD", "warehouse123")
DB_DRIVER   = "org.postgresql.Driver"

JDBC_OPTS = {
    "url":      DB_URL,
    "user":     DB_USER,
    "password": DB_PASSWORD,
    "driver":   DB_DRIVER,
}

# Process last N hours (configurable)
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))


# ─── Spark session ───────────────────────────────────────────

def create_spark():
    return (SparkSession.builder
        .appName("EcommerceBronzeToSilver")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate())


def read_table(spark, table: str):
    return (spark.read.format("jdbc")
        .options(**JDBC_OPTS)
        .option("dbtable", table)
        .load())


def write_table(df, table: str, mode="append"):
    (df.write.format("jdbc")
       .options(**JDBC_OPTS)
       .option("dbtable", table)
       .mode(mode)
       .save())


# ─── Bronze → Silver Events ──────────────────────────────────

def transform_events(spark):
    print("[B→S] Processing events...")
    raw = read_table(spark, "bronze.raw_events")

    # Deduplicate by event_id (keep latest ingestion)
    window = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())
    deduped = (raw
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn"))

    # Parse & clean
    cleaned = (deduped
        .withColumn("ts_parsed",
            F.to_timestamp(F.col("ts")))
        .filter(F.col("ts_parsed").isNotNull())
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_type").isNotNull())
        # Price sanity
        .withColumn("price",
            F.when((F.col("price") < 0) | (F.col("price") > 50000), F.lit(None))
             .otherwise(F.col("price")))
        # Quantity sanity
        .withColumn("quantity",
            F.when((F.col("quantity") < 0) | (F.col("quantity") > 100), F.lit(1))
             .otherwise(F.col("quantity")))
        # Device normalization
        .withColumn("device_type",
            F.lower(F.trim(F.col("device_type"))))
        # Derived columns
        .withColumn("event_date", F.to_date("ts_parsed"))
        .withColumn("event_hour", F.hour("ts_parsed"))
        # Final selection
        .select(
            F.col("event_id"),
            F.col("event_type"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("product_id"),
            F.col("category"),
            F.col("price"),
            F.col("quantity"),
            F.col("device_type"),
            F.col("country"),
            F.col("city"),
            F.col("event_date"),
            F.col("event_hour"),
            F.col("ts_parsed").alias("ts"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    count = cleaned.count()
    print(f"[B→S] Events cleaned: {count:,}")

    # Upsert pattern: delete-then-insert for idempotency
    min_date = cleaned.agg(F.min("event_date")).collect()[0][0]
    max_date = cleaned.agg(F.max("event_date")).collect()[0][0]

    # Write (append; in production use upsert via MERGE)
    write_table(cleaned, "silver.events", mode="append")
    print(f"[B→S] Events written to silver ({min_date} → {max_date})")
    return count


# ─── Bronze → Silver Orders ──────────────────────────────────

def transform_orders(spark):
    print("[B→S] Processing orders...")
    raw = read_table(spark, "bronze.raw_orders")

    window = Window.partitionBy("order_id").orderBy(F.col("ingested_at").desc())
    deduped = (raw
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn"))

    cleaned = (deduped
        .withColumn("created_at_ts",
            F.to_timestamp(F.col("created_at")))
        .withColumn("updated_at_ts",
            F.to_timestamp(F.col("updated_at")))
        .filter(F.col("created_at_ts").isNotNull())
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .withColumn("total_amount",
            F.when(F.col("total_amount") < 0, F.lit(0))
             .otherwise(F.col("total_amount")))
        .withColumn("discount_pct",
            F.when(
                (F.col("discount_pct") < 0) | (F.col("discount_pct") > 100),
                F.lit(0)
            ).otherwise(F.col("discount_pct")))
        .withColumn("item_count",
            F.when(F.col("items").isNotNull(),
                   F.size(F.from_json(F.col("items").cast("string"),
                          "ARRAY<STRING>")))
             .otherwise(F.lit(1)))
        .withColumn("order_date", F.to_date("created_at_ts"))
        .select(
            F.col("order_id"),
            F.col("user_id"),
            F.col("status"),
            F.col("total_amount"),
            F.col("discount_pct"),
            F.col("payment_method"),
            F.lit(1).alias("item_count"),   # simplified
            F.col("order_date"),
            F.col("created_at_ts").alias("created_at"),
            F.col("updated_at_ts").alias("updated_at"),
            F.current_timestamp().alias("processed_at"),
        )
    )

    count = cleaned.count()
    write_table(cleaned, "silver.orders", mode="append")
    print(f"[B→S] Orders written to silver: {count:,}")
    return count


# ─── Build Silver Users dim ──────────────────────────────────

def build_user_dim(spark):
    print("[B→S] Building user dimension...")
    events = read_table(spark, "silver.events")

    users = (events
        .groupBy("user_id")
        .agg(
            F.min("ts").alias("first_seen"),
            F.max("ts").alias("last_seen"),
            F.countDistinct("session_id").alias("total_sessions"),
            F.first("country").alias("country"),
            F.first("city").alias("city"),
            F.first("device_type").alias("device_type"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    count = users.count()
    # Overwrite to rebuild full dim each run
    write_table(users, "silver.users", mode="overwrite")
    print(f"[B→S] User dim written: {count:,} users")


# ─── Main ────────────────────────────────────────────────────

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[B→S] Starting Bronze→Silver batch | lookback={LOOKBACK_HOURS}h")

    event_count = transform_events(spark)
    order_count = transform_orders(spark)
    build_user_dim(spark)

    print(f"\n[B→S] ✓ Complete — events={event_count:,} | orders={order_count:,}")
    spark.stop()


if __name__ == "__main__":
    main()
