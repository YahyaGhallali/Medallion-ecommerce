"""Gold Layer: Star Schema Modeling & Business KPI Extractions."""

from __future__ import annotations

import sys
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workflow.common import GOLD_DIR, SILVER_DIR, get_spark_session


def read_silver(spark: SparkSession, name: str) -> DataFrame:
    """Read a curated table from the silver layer."""
    return spark.read.parquet(str(SILVER_DIR / f"{name}.parquet"))


def write_gold(df: DataFrame, name: str) -> int:
    """Write star schema table or KPI DataFrame to the gold layer."""
    output_path = GOLD_DIR / f"{name}.parquet"
    df.write.mode("overwrite").parquet(str(output_path))
    count = df.count()
    print(f"  [OK] Gold: {name:35s} -> {count:>8,} rows written")
    return count


def build_star_schema(spark: SparkSession) -> dict[str, DataFrame]:
    """Build dimensions and fact tables."""
    print("\n--- Building Star Schema (Dimensions & Fact Table) ---")

    df_orders = read_silver(spark, "olist_orders_dataset")
    df_customers = read_silver(spark, "olist_customers_dataset")
    df_products = read_silver(spark, "olist_products_dataset")
    df_sellers = read_silver(spark, "olist_sellers_dataset")
    df_order_items = read_silver(spark, "olist_order_items_dataset")

    # 1. dim_customer
    dim_customer = df_customers.select(
        F.monotonically_increasing_id().alias("customer_sk"),
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    )
    write_gold(dim_customer, "dim_customer")

    # 2. dim_product
    dim_product = df_products.select(
        F.monotonically_increasing_id().alias("product_sk"),
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    )
    write_gold(dim_product, "dim_product")

    # 3. dim_seller
    dim_seller = df_sellers.select(
        F.monotonically_increasing_id().alias("seller_sk"),
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    )
    write_gold(dim_seller, "dim_seller")

    # 4. dim_date
    dates = (
        df_orders
        .select(F.col("order_purchase_timestamp").cast("date").alias("date"))
        .filter(F.col("date").isNotNull())
        .distinct()
    )
    dim_date = (
        dates.select(
            F.col("date").alias("date_key"),
            F.year("date").alias("year"),
            F.month("date").alias("month"),
            F.quarter("date").alias("quarter"),
            F.dayofweek("date").alias("day_of_week"),
            F.dayofmonth("date").alias("day_of_month"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
        )
        .orderBy("date_key")
    )
    write_gold(dim_date, "dim_date")

    # 5. fact_order_items
    fact_order_items = (
        df_order_items
        .join(
            df_orders.select(
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ),
            on="order_id",
            how="inner",
        )
        .join(dim_customer.select("customer_sk", "customer_id"), on="customer_id", how="left")
        .join(dim_product.select("product_sk", "product_id"), on="product_id", how="left")
        .join(dim_seller.select("seller_sk", "seller_id"), on="seller_id", how="left")
        .withColumn("purchase_date", F.col("order_purchase_timestamp").cast("date"))
        .join(
            dim_date.select(
                F.col("date_key").alias("purchase_date"),
                F.col("date_key").alias("date_sk"),
            ),
            on="purchase_date",
            how="left",
        )
        .withColumn(
            "delivery_delay_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date"),
            ),
        )
        .select(
            "order_id",
            "order_item_id",
            "customer_sk",
            "product_sk",
            "seller_sk",
            "date_sk",
            "order_status",
            "price",
            "freight_value",
            "delivery_delay_days",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        )
    )
    write_gold(fact_order_items, "fact_order_items")

    return {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_seller": dim_seller,
        "dim_date": dim_date,
        "fact_order_items": fact_order_items,
    }


def compute_kpis(spark: SparkSession, tables: dict[str, DataFrame]) -> None:
    """Compute and store all business KPIs from gold and silver tables."""
    print("\n--- Computing Gold Layer Business KPIs ---")

    df_orders = read_silver(spark, "olist_orders_dataset")
    df_customers = read_silver(spark, "olist_customers_dataset")
    df_payments = read_silver(spark, "olist_order_payments_dataset")
    dim_customer = tables["dim_customer"]
    dim_product = tables["dim_product"]
    dim_seller = tables["dim_seller"]
    fact_order_items = tables["fact_order_items"]

    # ── KPI 1: Monthly Revenue & Month-over-Month Growth ───────────────────
    orders_delivered = (
        df_orders
        .filter(
            (F.col("order_status") == "delivered")
            & F.col("order_purchase_timestamp").isNotNull()
        )
        .select("order_id", "order_purchase_timestamp")
    )
    payments_agg = (
        df_payments
        .groupBy("order_id")
        .agg(F.sum("payment_value").alias("payment_value"))
    )
    orders_with_payments = orders_delivered.join(payments_agg, on="order_id", how="inner")
    w_lag = Window.orderBy("month")

    kpi_monthly_revenue = (
        orders_with_payments
        .withColumn("month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
        .groupBy("month")
        .agg(F.round(F.sum("payment_value"), 2).alias("revenue"))
        .withColumn("prev_revenue", F.lag("revenue").over(w_lag))
        .withColumn(
            "mom_growth_pct",
            F.round((F.col("revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 2),
        )
        .orderBy("month")
    )
    write_gold(kpi_monthly_revenue, "kpi_monthly_revenue")

    # ── KPI 2: Top 3 Product Categories per State ──────────────────────────
    items_with_dims = (
        fact_order_items
        .join(dim_customer.select("customer_sk", "customer_state"), on="customer_sk", how="inner")
        .join(dim_product.select("product_sk", "product_category_name"), on="product_sk", how="inner")
    )
    revenue_by_state_cat = (
        items_with_dims
        .groupBy("customer_state", "product_category_name")
        .agg(F.round(F.sum("price"), 2).alias("total_revenue"))
    )
    w_rank = Window.partitionBy("customer_state").orderBy(F.desc("total_revenue"))
    kpi_top3_categories = (
        revenue_by_state_cat
        .withColumn("rank", F.row_number().over(w_rank))
        .filter(F.col("rank") <= 3)
        .orderBy("customer_state", "rank")
    )
    write_gold(kpi_top3_categories, "kpi_top3_categories_per_state")

    # ── KPI 3: Average Delivery Delay per Seller ───────────────────────────
    kpi_delivery_delay = (
        fact_order_items
        .filter(F.col("delivery_delay_days").isNotNull())
        .join(
            dim_seller.select("seller_sk", "seller_id", "seller_city", "seller_state"),
            on="seller_sk",
            how="inner",
        )
        .groupBy("seller_id", "seller_city", "seller_state")
        .agg(
            F.round(F.avg("delivery_delay_days"), 2).alias("avg_delay_days"),
            F.count("*").alias("total_orders"),
            F.round(F.avg(F.when(F.col("delivery_delay_days") > 0, F.col("delivery_delay_days"))), 2)
            .alias("avg_late_days"),
            F.round(
                F.sum(F.when(F.col("delivery_delay_days") > 0, 1).otherwise(0)) / F.count("*") * 100, 2
            ).alias("late_pct"),
        )
        .orderBy(F.desc("avg_delay_days"))
    )
    write_gold(kpi_delivery_delay, "kpi_delivery_delay_per_seller")

    # ── KPI 4: Repeat-Customer Rate ────────────────────────────────────────
    orders_per_person = (
        df_orders
        .filter(F.col("order_status") == "delivered")
        .join(df_customers.select("customer_id", "customer_unique_id"), on="customer_id", how="inner")
        .groupBy("customer_unique_id")
        .agg(F.count("order_id").alias("order_count"))
    )

    kpi_repeat_rate = (
        orders_per_person
        .select(
            F.count("*").cast("double").alias("total_unique_customers"),
            F.sum(F.when(F.col("order_count") == 1, 1).otherwise(0)).cast("double").alias("one_time_customers"),
            F.sum(F.when(F.col("order_count") > 1, 1).otherwise(0)).cast("double").alias("repeat_customers"),
        )
        .withColumn(
            "repeat_rate_pct",
            F.round(F.col("repeat_customers") / F.col("total_unique_customers") * 100, 2),
        )
        .selectExpr(
            "stack(4, "
            "'total_unique_customers', total_unique_customers, "
            "'one_time_customers', one_time_customers, "
            "'repeat_customers', repeat_customers, "
            "'repeat_rate_pct', repeat_rate_pct) as (metric, value)"
        )
    )
    write_gold(kpi_repeat_rate, "kpi_repeat_customer_rate")

    order_count_dist = (
        orders_per_person
        .groupBy("order_count")
        .agg(F.count("*").alias("num_customers"))
        .orderBy("order_count")
    )
    write_gold(order_count_dist, "kpi_order_count_distribution")

    # ── KPI 5: Cumulative Revenue Over Time ────────────────────────────────
    w_cumulative = Window.orderBy("month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    kpi_cumulative_revenue = (
        kpi_monthly_revenue
        .select("month", "revenue")
        .withColumn("cumulative_revenue", F.round(F.sum("revenue").over(w_cumulative), 2))
    )
    write_gold(kpi_cumulative_revenue, "kpi_cumulative_revenue")

    # ── KPI 6: Revenue by Payment Type ─────────────────────────────────────
    kpi_payment_type = (
        df_payments
        .join(
            df_orders
            .filter(F.col("order_status") == "delivered")
            .select("order_id"),
            on="order_id",
            how="inner",
        )
        .groupBy("payment_type")
        .agg(
            F.round(F.sum("payment_value"), 2).alias("total_revenue"),
            F.count("*").alias("transaction_count"),
            F.round(F.avg("payment_value"), 2).alias("avg_transaction_value"),
            F.round(F.avg("payment_installments"), 1).alias("avg_installments"),
        )
        .withColumn(
            "revenue_share_pct",
            F.round(
                F.col("total_revenue") / F.sum("total_revenue").over(Window.partitionBy()) * 100, 2
            ),
        )
        .orderBy(F.desc("total_revenue"))
    )
    write_gold(kpi_payment_type, "kpi_revenue_by_payment_type")


def run_gold(spark: SparkSession | None = None) -> None:
    """Run full gold layer processing: star schema and KPIs."""
    print("=" * 65)
    print("  RUNNING GOLD LAYER (SILVER -> STAR SCHEMA & KPIS)")
    print("=" * 65)

    spark_instance = spark or get_spark_session("Olist_Gold_Layer")
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    tables = build_star_schema(spark_instance)
    compute_kpis(spark_instance, tables)

    print("\n[OK] Gold layer complete.\n")


if __name__ == "__main__":
    run_gold()
