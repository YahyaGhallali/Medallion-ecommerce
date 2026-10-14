# 🛒 Olist E-Commerce Medallion Data Engineering Pipeline

[![PySpark](https://img.shields.io/badge/PySpark-4.2+-E25A1C.svg?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.14+-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![uv](https://img.shields.io/badge/managed%20by-uv-DE5FE9.svg?logo=astral&logoColor=white)](https://github.com/astral-sh/uv)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%20%E2%86%92%20Silver%20%E2%86%92%20Gold)-00A86B.svg)]()

An end-to-end data engineering pipeline and lakehouse implementation built with **PySpark**, processing ~100k Brazilian e-commerce orders from the [Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). The project implements a **Medallion Architecture** (Bronze ➔ Silver ➔ Gold), cleans relational inconsistencies, models an analytical **Star Schema** (Fact & Dimensions), and extracts core business metrics (KPIs) and visualizations.

---

## 📌 Architecture Overview

The pipeline organizes data into three distinct quality and refinement zones:

```mermaid
flowchart LR
    subgraph Raw["Data Source"]
        CSV["9 Olist CSV Files\n(~100k Orders)"]
    end

    subgraph Bronze["Bronze Layer (Raw Ingestion)"]
        B_P["Parquet Ingestion\n• Explicit StructType Schemas\n• Ingestion Timestamps\n• Preserved Raw State"]
    end

    subgraph Silver["Silver Layer (Cleaned & Curated)"]
        S_P["Enriched Parquet\n• Type Casting & Time Parsing\n• String Normalization\n• Deduplication & Geo Aggregation\n• Category Translations (PT ➔ EN)\n• Business Integrity Checks"]
    end

    subgraph Gold["Gold Layer (Star Schema & Analytics)"]
        direction TB
        F["fact_order_items"]
        D1["dim_customer"]
        D2["dim_product"]
        D3["dim_seller"]
        D4["dim_date"]
        KPI["Analytical Marts & KPIs\n• Monthly Revenue & MoM Growth\n• Top Categories per State\n• Seller Delivery Delays\n• Customer Retention Rate\n• Cumulative Revenue Trends"]
    end

    CSV --> B_P
    B_P --> S_P
    S_P --> F
    S_P --> D1
    S_P --> D2
    S_P --> D3
    S_P --> D4
    F & D1 & D2 & D3 & D4 --> KPI
```

---

## 🏗️ Medallion Layers

| Layer | Path | Description & Transformations |
| :--- | :--- | :--- |
| **Bronze** | [`data/bronze/`](file:///c:/Users/yahya/Desktop/DE/data/bronze) | **Raw Ingestion**: Reads source CSV files using strict `StructType` schemas (avoiding expensive and error-prone `inferSchema`). Appends `ingestion_timestamp` and writes unmodified data into partitioned Parquet format. |
| **Silver** | [`data/silver/`](file:///c:/Users/yahya/Desktop/DE/data/silver) | **Cleaned & Standardized**: Dates converted from strings to timestamps; string columns trimmed and lowercased; duplicates removed (crucial for geolocation zip codes); Portuguese category names joined with translations; invalid/null values audited. |
| **Gold** | [`data/gold/`](file:///c:/Users/yahya/Desktop/DE/data/gold) | **Dimensional Modeling (Star Schema)**: Fact and dimension tables equipped with surrogate keys (`*_sk`), optimized for analytical OLAP queries, plus pre-computed analytical KPI datasets. |

---

## 🌟 Star Schema (Gold Layer)

The gold layer models the e-commerce domain as a Kimball-style dimensional star schema:

```mermaid
erDiagram
    dim_customer ||--o{ fact_order_items : "places"
    dim_product ||--o{ fact_order_items : "contains"
    dim_seller ||--o{ fact_order_items : "fulfills"
    dim_date ||--o{ fact_order_items : "ordered on"

    fact_order_items {
        string order_id
        int order_item_id
        long customer_sk FK
        long product_sk FK
        long seller_sk FK
        date date_sk FK
        string order_status
        double price
        double freight_value
        int delivery_delay_days
        timestamp order_purchase_timestamp
        timestamp order_delivered_customer_date
        timestamp order_estimated_delivery_date
    }

    dim_customer {
        long customer_sk PK
        string customer_id
        string customer_unique_id
        int customer_zip_code_prefix
        string customer_city
        string customer_state
    }

    dim_product {
        long product_sk PK
        string product_id
        string product_category_name
        int product_weight_g
        int product_length_cm
        int product_height_cm
        int product_width_cm
    }

    dim_seller {
        long seller_sk PK
        string seller_id
        int seller_zip_code_prefix
        string seller_city
        string seller_state
    }

    dim_date {
        date date_key PK
        int year
        int month
        int quarter
        int day_of_week
        int day_of_month
        string year_month
        boolean is_weekend
    }
```

### 🔑 Key Data Engineering Design Decisions

1. **Fact Table Grain**:
   - The grain of [`fact_order_items`](file:///c:/Users/yahya/Desktop/DE/gold.ipynb) is **one row per item sold** (`order_id` + `order_item_id`). An order may contain multiple items, potentially from different sellers with different shipping limits and prices. Modeling at the item grain avoids loss of seller- and product-level granularity.
2. **`customer_id` vs. `customer_unique_id`**:
   - In Olist, `customer_id` is an order-scoped token (changes with every purchase).
   - `customer_unique_id` identifies the actual human entity across multiple orders. Retaining both in `dim_customer` allows accurate identification of repeat customers.
3. **Delivery Delay Calculation**:
   - Derived metric: `delivery_delay_days = datediff(order_delivered_customer_date, order_estimated_delivery_date)`.
   - Positive values indicate late deliveries; negative values indicate deliveries that arrived earlier than estimated.
4. **Pre-aggregating Payments**:
   - When calculating revenue from payments, payments are grouped by `order_id` before joining with `orders` to prevent fan-out / Cartesian inflation caused by multi-item or split-installment orders.

---

## 📊 Business Intelligence & KPIs

Computed from the Gold layer tables inside [`gold.ipynb`](file:///c:/Users/yahya/Desktop/DE/gold.ipynb):

| # | Business KPI | Spark Engine Implementation | Output Dataset |
| :---: | :--- | :--- | :--- |
| **1** | **Monthly Revenue & MoM Growth** | `lag("revenue")` window partitioned chronologically to compute percentage change over time. | `kpi_monthly_revenue.parquet` |
| **2** | **Top 3 Product Categories per State** | `row_number()` window partitioned by `customer_state` and ordered by `total_revenue DESC`. | `kpi_top3_categories_per_state.parquet` |
| **3** | **Average Delivery Delay per Seller** | Aggregates actual vs. estimated delivery variance, average delay days, and seller delay rate (`late_pct`). | `kpi_delivery_delay_per_seller.parquet` |
| **4** | **Repeat-Customer Rate** | Grouping by `customer_unique_id` to evaluate retention rate (one-time vs. repeat buyers). | `kpi_repeat_customer_rate.parquet` |
| **5** | **Cumulative Revenue Over Time** | Running total with `sum().over(Window.rowsBetween(unboundedPreceding, currentRow))`. | `kpi_cumulative_revenue.parquet` |
| **6** | **Payment Method Breakdown** | Revenue share %, average transaction size, and installment behavior across credit card, boleto, voucher, and debit. | `kpi_revenue_by_payment_type.parquet` |

### 📈 Revenue Visualization

The project includes an automated visualization pipeline in [`analysis.py`](file:///c:/Users/yahya/Desktop/DE/analysis.py):

![Monthly Revenue Analysis](file:///c:/Users/yahya/Desktop/DE/data/gold/payments_monthly_analysis.png)

---

## 📁 Repository Structure

```text
├── .hadoop/                  # Automatic local Hadoop winutils binaries for Windows
├── data/
│   ├── raw/                  # Downloaded raw Olist CSV files
│   ├── bronze/               # Ingested Parquet files with explicit schema
│   ├── silver/               # Cleaned, typed, and deduplicated Parquet files
│   └── gold/                 # Star schema (fact/dims) and analytical Parquet outputs
├── analysis.py               # Visual analytics script generating matplotlib charts
├── data_design_tables.md     # Detailed data dictionary, ER diagrams, and join paths
├── download_olist.py         # Automated Kaggle dataset downloader script
├── ingestion.ipynb           # Phase 1: Raw ingestion to Bronze layer
├── silver.ipynb              # Phase 2: Cleaning and Silver transformations
├── gold.ipynb                # Phase 3: Star schema modeling and KPI generation
├── plan.md                   # Project architecture blueprint and definition of done
├── pysparkcode.py            # PySpark syntax reference and performance tuning guide
├── pyproject.toml            # Project dependencies and packaging metadata
├── spark_windows.py          # Windows Hadoop / winutils automatic configuration helper
└── uv.lock                   # Deterministic lockfile
```

---

## 🚀 Getting Started

### 1. Prerequisites

- **Python**: 3.11+ (Python 3.14 tested)
- **Java**: JDK 17+ installed and set on `JAVA_HOME`
- **uv**: Recommended package manager ([astral.sh/uv](https://astral.sh/uv))

### 2. Environment Setup

Clone the repository and install dependencies with `uv`:

```bash
# Clone the repository
git clone https://github.com/YahyaGhallali/Medallion-ecommerce.git
cd Medallion-ecommerce

# Install dependencies into virtual environment
uv sync
```

### 3. Windows Hadoop Setup

If running on Windows, PySpark requires Hadoop native binaries (`winutils.exe` and `hadoop.dll`). This repository handles this automatically via [`spark_windows.py`](file:///c:/Users/yahya/Desktop/DE/spark_windows.py):

```python
from spark_windows import configure_windows_hadoop

# Automatically downloads winutils.exe and hadoop.dll to .hadoop/bin and configures HADOOP_HOME
configure_windows_hadoop()
```

### 4. Fetch the Dataset

To fetch the raw dataset directly from Kaggle:

```bash
# Configure your Kaggle credentials (~/.kaggle/kaggle.json or env vars)
# Run the download script
uv run python download_olist.py --output-dir data/raw
```

### 5. Running the Pipeline

Execute the pipeline layers sequentially:

1. **Bronze Layer**: Open and execute [`ingestion.ipynb`](file:///c:/Users/yahya/Desktop/DE/ingestion.ipynb) to ingest raw CSVs to Parquet with explicit schemas.
2. **Silver Layer**: Run [`silver.ipynb`](file:///c:/Users/yahya/Desktop/DE/silver.ipynb) to clean, standardize, and deduplicate tables.
3. **Gold Layer**: Run [`gold.ipynb`](file:///c:/Users/yahya/Desktop/DE/gold.ipynb) to generate the dimensional star schema and compute business KPIs.
4. **Visual Analytics**: Generate revenue and MoM growth charts:
   ```bash
   uv run python analysis.py
   ```

---

## 🛠️ Performance & Engineering Best Practices

- **Explicit Schemas**: Ingestion disables `inferSchema` across all 9 tables, preventing costly full-file scans and type ambiguities.
- **Controlled Partitions**: Set `spark.sql.shuffle.partitions` to `8` for local multi-core execution instead of Spark's default `200`, eliminating excessive task scheduling overhead on laptop hardware.
- **Broadcast Joins**: Used on smaller lookup dimensions (`dim_seller`, `dim_product`, category translations) to eliminate expensive cross-network shuffles during joins.
- **Windowing over Self-Joins**: MoM growth and category ranking utilize native Spark SQL window functions (`lag`, `row_number`) rather than expensive recursive joins.

---

## 📜 License

This project is licensed under the MIT License - see the repository for details.
Dataset provided by [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) via Kaggle.
