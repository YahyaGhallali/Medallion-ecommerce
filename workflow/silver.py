"""Silver Layer: Cleaning, Deduplication, String Normalization, and Translations."""

from __future__ import annotations

import sys
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workflow.common import BRONZE_DIR, SILVER_DIR, get_spark_session


def read_bronze(spark: SparkSession, name: str) -> DataFrame:
    """Read a table from the bronze layer."""
    return spark.read.parquet(str(BRONZE_DIR / f"{name}.parquet"))


def write_silver(df: DataFrame, name: str) -> int:
    """Write cleaned DataFrame to the silver layer in Parquet format."""
    output_path = SILVER_DIR / f"{name}.parquet"
    df.write.mode("overwrite").parquet(str(output_path))
    count = df.count()
    print(f"  [OK] Silver: {name:35s} -> {count:>8,} rows written")
    return count


def normalize_string_cols(df: DataFrame) -> DataFrame:
    """Trim whitespace and lowercase all string columns."""
    for field in df.schema.fields:
        if str(field.dataType) == "StringType()":
            df = df.withColumn(field.name, F.lower(F.trim(F.col(field.name))))
    return df


def clean_orders(spark: SparkSession) -> None:
    df = read_bronze(spark, "olist_orders_dataset")
    # Business logic: order_id and customer_id must exist; purchase timestamp is mandatory.
    df = df.dropna(subset=["order_id", "customer_id", "order_purchase_timestamp"])

    # For delivered orders, drop rows missing critical delivery dates
    delivered_with_nulls = (
        (F.col("order_status") == "delivered")
        & (
            F.col("order_approved_at").isNull()
            | F.col("order_delivered_carrier_date").isNull()
            | F.col("order_delivered_customer_date").isNull()
        )
    )
    df = df.filter(~delivered_with_nulls)
    df = df.dropDuplicates(["order_id"])
    df = normalize_string_cols(df)
    write_silver(df, "olist_orders_dataset")


def clean_order_items(spark: SparkSession) -> None:
    df = read_bronze(spark, "olist_order_items_dataset")
    df = df.dropna(subset=["order_id", "order_item_id", "product_id", "seller_id", "price"])
    df = df.dropDuplicates(["order_id", "order_item_id"])
    df = normalize_string_cols(df)
    write_silver(df, "olist_order_items_dataset")


def clean_customers(spark: SparkSession) -> None:
    df = read_bronze(spark, "olist_customers_dataset")
    df = df.dropna(subset=["customer_id", "customer_unique_id"])
    df = df.dropDuplicates(["customer_id"])
    df = normalize_string_cols(df)
    write_silver(df, "olist_customers_dataset")


def clean_products(spark: SparkSession) -> None:
    df_products = read_bronze(spark, "olist_products_dataset")
    df_translation = read_bronze(spark, "product_category_name_translation")

    df_products = df_products.dropna(subset=["product_id"]).dropDuplicates(["product_id"])
    df_products = normalize_string_cols(df_products)
    df_translation = normalize_string_cols(df_translation).select(
        "product_category_name", "product_category_name_english"
    )

    # Join category translations so products have English names
    df = df_products.join(df_translation, on="product_category_name", how="left")
    write_silver(df, "olist_products_dataset")


def clean_sellers(spark: SparkSession) -> None:
    df = read_bronze(spark, "olist_sellers_dataset")
    df = df.dropna(subset=["seller_id"]).dropDuplicates(["seller_id"])
    df = normalize_string_cols(df)
    write_silver(df, "olist_sellers_dataset")


def clean_payments(spark: SparkSession) -> None:
    df = read_bronze(spark, "olist_order_payments_dataset")
    df = df.dropna(subset=["order_id", "payment_sequential", "payment_value"])
    df = df.dropDuplicates(["order_id", "payment_sequential"])
    df = normalize_string_cols(df)
    write_silver(df, "olist_order_payments_dataset")


def clean_reviews(spark: SparkSession) -> None:
    df = read_bronze(spark, "olist_order_reviews_dataset")
    df = df.dropna(subset=["review_id", "order_id", "review_score"])
    df = df.dropDuplicates(["review_id", "order_id"])
    df = normalize_string_cols(df)
    write_silver(df, "olist_order_reviews_dataset")


def clean_geolocation(spark: SparkSession) -> None:
    df = read_bronze(spark, "olist_geolocation_dataset")
    df = df.dropna(subset=["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"])
    # Deduplicate redundant geolocation coordinates
    df = df.dropDuplicates()
    df = normalize_string_cols(df)
    write_silver(df, "olist_geolocation_dataset")


def run_silver(spark: SparkSession | None = None) -> None:
    """Run full silver layer data cleansing & curation."""
    print("=" * 65)
    print("  RUNNING SILVER LAYER (BRONZE -> SILVER CLEANED)")
    print("=" * 65)

    spark_instance = spark or get_spark_session("Olist_Silver_Layer")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    clean_orders(spark_instance)
    clean_order_items(spark_instance)
    clean_customers(spark_instance)
    clean_products(spark_instance)
    clean_sellers(spark_instance)
    clean_payments(spark_instance)
    clean_reviews(spark_instance)
    clean_geolocation(spark_instance)

    print("\n[OK] Silver layer complete.\n")


if __name__ == "__main__":
    run_silver()
