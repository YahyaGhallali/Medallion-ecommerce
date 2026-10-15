"""Bronze Layer: Raw Ingestion with explicit schemas and ingestion timestamps."""

from __future__ import annotations

import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Ensure common imports work
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workflow.common import BRONZE_DIR, RAW_DIR, get_spark_session

# Explicit schemas for all 9 raw Olist CSVs
SCHEMAS: dict[str, StructType] = {
    "olist_orders_dataset": StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_approved_at", TimestampType(), True),
        StructField("order_delivered_carrier_date", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
        StructField("order_estimated_delivery_date", TimestampType(), True),
    ]),
    "olist_customers_dataset": StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", IntegerType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
    ]),
    "olist_order_items_dataset": StructType([
        StructField("order_id", StringType(), True),
        StructField("order_item_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("shipping_limit_date", TimestampType(), True),
        StructField("price", DoubleType(), True),
        StructField("freight_value", DoubleType(), True),
    ]),
    "olist_order_payments_dataset": StructType([
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
    ]),
    "olist_order_reviews_dataset": StructType([
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("review_score", IntegerType(), True),
        StructField("review_comment_title", StringType(), True),
        StructField("review_comment_message", StringType(), True),
        StructField("review_creation_date", TimestampType(), True),
        StructField("review_answer_timestamp", TimestampType(), True),
    ]),
    "olist_products_dataset": StructType([
        StructField("product_id", StringType(), True),
        StructField("product_category_name", StringType(), True),
        StructField("product_name_lenght", IntegerType(), True),
        StructField("product_description_lenght", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("product_weight_g", IntegerType(), True),
        StructField("product_length_cm", IntegerType(), True),
        StructField("product_height_cm", IntegerType(), True),
        StructField("product_width_cm", IntegerType(), True),
    ]),
    "olist_sellers_dataset": StructType([
        StructField("seller_id", StringType(), True),
        StructField("seller_zip_code_prefix", IntegerType(), True),
        StructField("seller_city", StringType(), True),
        StructField("seller_state", StringType(), True),
    ]),
    "olist_geolocation_dataset": StructType([
        StructField("geolocation_zip_code_prefix", IntegerType(), True),
        StructField("geolocation_lat", DoubleType(), True),
        StructField("geolocation_lng", DoubleType(), True),
        StructField("geolocation_city", StringType(), True),
        StructField("geolocation_state", StringType(), True),
    ]),
    "product_category_name_translation": StructType([
        StructField("product_category_name", StringType(), True),
        StructField("product_category_name_english", StringType(), True),
    ]),
}


def ingest_table(spark: SparkSession, table_name: str) -> int:
    """Read a raw CSV with its explicit schema, append ingestion timestamp, and write to bronze Parquet."""
    csv_file = RAW_DIR / f"{table_name}.csv"
    if not csv_file.exists():
        raise FileNotFoundError(f"Raw source CSV not found: {csv_file}")

    schema = SCHEMAS[table_name]
    df = (
        spark.read.csv(
            str(csv_file),
            header=True,
            schema=schema,
            timestampFormat="yyyy-MM-dd HH:mm:ss",
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    output_path = BRONZE_DIR / f"{table_name}.parquet"
    df.write.mode("overwrite").parquet(str(output_path))
    count = df.count()
    print(f"  [OK] Bronze: {table_name:35s} -> {count:>8,} rows written")
    return count


def run_bronze(spark: SparkSession | None = None) -> None:
    """Run full bronze layer ingestion."""
    print("=" * 65)
    print("  RUNNING BRONZE LAYER INGESTION (RAW -> BRONZE PARQUET)")
    print("=" * 65)

    spark_instance = spark or get_spark_session("Olist_Bronze_Layer")
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    for table_name in SCHEMAS.keys():
        ingest_table(spark_instance, table_name)

    print("\n[OK] Bronze layer complete.\n")


if __name__ == "__main__":
    run_bronze()
