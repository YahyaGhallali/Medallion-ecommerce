"""Shared configurations, paths, and SparkSession helpers for the pipeline."""

from __future__ import annotations

import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Safe console output on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from spark_windows import configure_windows_hadoop

# Data layer directory paths
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"


def get_spark_session(app_name: str = "Olist_Medallion_Pipeline") -> SparkSession:
    """Initialize and return a configured SparkSession."""
    configure_windows_hadoop(PROJECT_ROOT)

    # Sanitize app name for Windows command-line launch safety
    clean_app_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in app_name)

    spark = (
        SparkSession.builder.master("local[4]")
        .appName(clean_app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
