"""End-to-end Medallion Pipeline CLI Runner (Bronze -> Silver -> Gold)."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workflow.bronze import run_bronze
from workflow.common import get_spark_session
from workflow.gold import run_gold
from workflow.silver import run_silver


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Olist Medallion Data Engineering Pipeline"
    )
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Pipeline stage to execute (default: all)",
    )
    args = parser.parse_args()

    t_start = time.time()
    spark = get_spark_session(f"Olist_Pipeline_{args.stage.upper()}")

    try:
        if args.stage in ("bronze", "all"):
            t0 = time.time()
            run_bronze(spark)
            print(f"[-] Bronze completed in {time.time() - t0:.2f}s")

        if args.stage in ("silver", "all"):
            t0 = time.time()
            run_silver(spark)
            print(f"[-] Silver completed in {time.time() - t0:.2f}s")

        if args.stage in ("gold", "all"):
            t0 = time.time()
            run_gold(spark)
            print(f"[-] Gold completed in {time.time() - t0:.2f}s")

        total_time = time.time() - t_start
        print("\n" + "=" * 65)
        print(f"[DONE] PIPELINE EXECUTION SUCCESSFUL -- Total time: {total_time:.2f}s")
        print("=" * 65)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
