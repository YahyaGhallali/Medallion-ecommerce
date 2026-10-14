"""
PySpark guided tour: run sections one by one and read the output.

Setup:   pip install pyspark      (and have Java 17+ installed)
Run all: python pyspark_tour.py
Watch the Spark UI while it runs (keeps it open at the end):
         KEEP_UI=1 python pyspark_tour.py      then open http://localhost:4040
"""
import os
import shutil
import tempfile
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# 0. SESSION
# SparkSession is your entry point. master("local[4]") = run on this machine
# using 4 CPU cores as "workers". On a real cluster you drop this line.
# shuffle.partitions = how many partitions a shuffle (groupBy/join) produces.
# Default is 200, far too many for local experiments.
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder.master("local[4]")
    .appName("pyspark_tour")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")  # hide noisy INFO logs


def section(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


# ---------------------------------------------------------------------------
section("1. DATAFRAMES: a distributed table with a schema")
# ---------------------------------------------------------------------------
data = [
    ("France",  "laptop", 1200, 2024),
    ("France",  "phone",   800, 2025),
    ("France",  "laptop", 1250, 2025),
    ("Germany", "laptop", 1100, 2024),
    ("Germany", "laptop", 1300, 2025),
    ("Spain",   "phone",   700, 2025),
    ("Japan",   "phone",   900, 2025),
    ("Japan",   "laptop", 1500, 2024),
]
df = spark.createDataFrame(data, ["country", "product", "revenue", "year"])

df.show()            # ACTION: prints the first 20 rows
df.printSchema()     # column names and types (Spark infers them)
print("rows:", df.count(), "| columns:", df.columns)

# ---------------------------------------------------------------------------
section("2. LAZY EVALUATION: transformations only build a plan")
# ---------------------------------------------------------------------------
t0 = time.time()
plan = df.filter(F.col("year") == 2025).groupBy("country").sum("revenue")
print(f"Building the plan took {time.time() - t0:.4f}s. Nothing has run yet.")
print("Type of result:", type(plan).__name__, "(a plan, not data)")

t0 = time.time()
plan.show()          # ACTION: only now does Spark do the work
print(f"Executing took {time.time() - t0:.2f}s")

# ---------------------------------------------------------------------------
section("3. COLUMN OPERATIONS and IMMUTABILITY")
# ---------------------------------------------------------------------------
# DataFrames are immutable: every operation returns a NEW DataFrame.
df2 = (
    df.withColumn("revenue_k", F.col("revenue") / 1000)
    .withColumn("size", F.when(F.col("revenue") >= 1200, "big").otherwise("small"))
    .withColumn("product_upper", F.upper("product"))
)
df2.show()
print("original df columns unchanged:", df.columns)

# select / filter / drop / rename / sort
(
    df.select("country", "revenue")
    .filter((F.col("revenue") > 800) & (F.col("country") != "Japan"))
    .withColumnRenamed("revenue", "rev")
    .orderBy(F.desc("rev"))
    .show()
)

# ---------------------------------------------------------------------------
section("4. AGGREGATIONS: groupBy + several metrics at once")
# ---------------------------------------------------------------------------
(
    df.groupBy("country")
    .agg(
        F.sum("revenue").alias("total"),
        F.round(F.avg("revenue"), 1).alias("average"),
        F.count("*").alias("n_sales"),
        F.max("revenue").alias("best_sale"),
        F.collect_set("product").alias("products"),
    )
    .orderBy(F.desc("total"))
    .show(truncate=False)
)

# ---------------------------------------------------------------------------
section("5. JOINS")
# ---------------------------------------------------------------------------
# Japan is deliberately missing from this lookup table.
regions = spark.createDataFrame(
    [("France", "EU"), ("Germany", "EU"), ("Spain", "EU")],
    ["country", "region"],
)

print("INNER join: Japan rows disappear")
df.join(regions, on="country", how="inner").show()

print("LEFT join: Japan kept, region is null")
joined = df.join(regions, on="country", how="left")
joined.show()

print("Handling nulls with fillna:")
joined.fillna({"region": "Unknown"}).select("country", "region").distinct().show()

# A normal join shuffles BOTH sides. If one side is small, broadcast() sends
# a copy of it to every machine so the big side never moves.
# Look for BroadcastHashJoin in the plan below.
df.join(F.broadcast(regions), "country").explain()

# ---------------------------------------------------------------------------
section("6. WINDOW FUNCTIONS: calculations across related rows")
# ---------------------------------------------------------------------------
# Unlike groupBy, a window keeps every row and adds a computed column.
w_rank = Window.partitionBy("country").orderBy(F.desc("revenue"))
w_running = (
    Window.partitionBy("country")
    .orderBy("year", "revenue")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
(
    df.withColumn("rank_in_country", F.rank().over(w_rank))
    .withColumn("running_total", F.sum("revenue").over(w_running))
    .orderBy("country", "rank_in_country")
    .show()
)

# ---------------------------------------------------------------------------
section("7. SQL: same engine, same optimizer, different syntax")
# ---------------------------------------------------------------------------
df.createOrReplaceTempView("sales")
spark.sql(
    """
    SELECT country, SUM(revenue) AS total
    FROM sales
    WHERE year = 2025
    GROUP BY country
    HAVING SUM(revenue) > 1000
    ORDER BY total DESC
    """
).show()

# ---------------------------------------------------------------------------
section("8. PARTITIONS: how the data is physically split")
# ---------------------------------------------------------------------------
big = (
    spark.range(0, 2_000_000)  # one column "id", 2 million rows
    .withColumn("country_id", F.col("id") % 5)
    .withColumn("year", 2020 + (F.col("id") % 7))
    .withColumn("amount", F.rand(seed=42) * 100)
)
print("Partitions of 'big':", big.rdd.getNumPartitions())
print("Rows per partition:")
big.groupBy(F.spark_partition_id().alias("partition")).count().orderBy("partition").show()

# repartition(n) reshuffles into n balanced partitions (a shuffle: costly)
# coalesce(n) merges down to fewer partitions WITHOUT a full shuffle (cheap)
print("after repartition(8):", big.repartition(8).rdd.getNumPartitions())
print("after coalesce(2):   ", big.coalesce(2).rdd.getNumPartitions())

# groupBy produces spark.sql.shuffle.partitions partitions (we set 8)
shuffled = big.groupBy("country_id").count()
print("after groupBy (shuffle):", shuffled.rdd.getNumPartitions())

# ---------------------------------------------------------------------------
section("9. explain(): read the optimizer's plan (bottom-up)")
# ---------------------------------------------------------------------------
late = (
    big.groupBy("country_id", "year")
    .agg(F.sum("amount").alias("total"))
    .filter("year = 2025")  # written AFTER the groupBy
)
early = (
    big.filter("year = 2025")  # written BEFORE the groupBy
    .groupBy("country_id", "year")
    .agg(F.sum("amount").alias("total"))
)
print("---- LATE FILTER plan ----")
late.explain()
print("---- EARLY FILTER plan ----")
early.explain()
print(
    "\nCompare: where does Filter sit? How many 'Exchange' lines (= shuffles)?\n"
    "'HashAggregate' twice = partial sums per partition, then final sums."
)
# Full detail: early.explain("extended")  -> parsed / analyzed / optimized / physical

# ---------------------------------------------------------------------------
section("10. PARQUET vs CSV and partitionBy (pushdown to disk)")
# ---------------------------------------------------------------------------
tmp = tempfile.mkdtemp(prefix="pyspark_tour_")
pq_path = os.path.join(tmp, "big_parquet")
csv_path = os.path.join(tmp, "big_csv")

sample = big.limit(500_000)
sample.write.mode("overwrite").partitionBy("year").parquet(pq_path)
sample.write.mode("overwrite").option("header", True).csv(csv_path)

print("Parquet folder layout (one sub-folder per year):")
print(sorted(os.listdir(pq_path)))


def size_mb(path):
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total / 1e6


print(f"Size on disk  parquet: {size_mb(pq_path):.1f} MB | csv: {size_mb(csv_path):.1f} MB")

pq = spark.read.parquet(pq_path)
query = pq.filter("year = 2025").select("amount")
print("\nParquet plan: look for PartitionFilters / PushedFilters / ReadSchema")
query.explain()

csv = spark.read.csv(csv_path, header=True, inferSchema=True)
print("CSV plan: no partition skipping, every file must be parsed")
csv.filter("year = 2025").select("amount").explain()

# ---------------------------------------------------------------------------
section("11. CACHING: keep a reused result in memory")
# ---------------------------------------------------------------------------
# Without cache, every action recomputes the whole plan from scratch.
expensive = big.groupBy("country_id").agg(F.avg("amount").alias("avg_amount"))

t0 = time.time()
expensive.count()
print(f"no cache, 1st action: {time.time() - t0:.2f}s")

expensive.cache()  # lazy: marks it for caching
t0 = time.time()
expensive.count()  # computes AND stores
print(f"cache,    1st action: {time.time() - t0:.2f}s (computes + stores)")
t0 = time.time()
expensive.count()  # served from memory
print(f"cache,    2nd action: {time.time() - t0:.2f}s (from memory)")
expensive.unpersist()
print("(timings are small and noisy on a laptop; the Storage tab in the UI shows it clearly)")

# ---------------------------------------------------------------------------
section("12. BUILT-IN FUNCTIONS vs PYTHON UDFs")
# ---------------------------------------------------------------------------
# Built-in functions run inside Spark's JVM engine and are optimizable.
# A Python UDF ships every row to a Python process and back: much slower.
double_udf = F.udf(lambda x: x * 2.0, "double")

t0 = time.time()
big.select(F.sum(F.col("amount") * 2.0)).collect()
print(f"built-in expression: {time.time() - t0:.2f}s")

t0 = time.time()
big.select(F.sum(double_udf("amount"))).collect()
print(f"python UDF:          {time.time() - t0:.2f}s  (expect noticeably slower)")
print("Rule: use pyspark.sql.functions first, UDFs only as a last resort.")

# ---------------------------------------------------------------------------
section("13. BACK TO PANDAS: only after shrinking the data")
# ---------------------------------------------------------------------------
summary = df.groupBy("country").agg(F.sum("revenue").alias("total"))
pdf = summary.toPandas()  # pulls the result to this machine; keep it small!
print(type(pdf))
print(pdf.sort_values("total", ascending=False))

# ---------------------------------------------------------------------------
if os.environ.get("KEEP_UI"):
    input("\nSpark UI is live at http://localhost:4040 . Press Enter to exit...")

shutil.rmtree(tmp, ignore_errors=True)
spark.stop()
print("\nDone.")