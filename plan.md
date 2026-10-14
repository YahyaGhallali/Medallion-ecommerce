## Project: a mini analytics pipeline on Brazilian e-commerce data

It is built in layers (bronze, silver, gold), so the same project can later grow into a data warehouse workflow without a rewrite.

**Dataset:** the *Olist Brazilian E-Commerce* dataset on Kaggle (free account needed, about 100k orders). It has 9 related CSV files: orders, order items, customers, products, sellers, payments, reviews, geolocation and a category-name translation. It's small enough for a laptop but relational enough to need real joins. *(Alternative: NYC TLC taxi trips plus the zone lookup table.)*

## Phase 1: Bronze (raw ingestion)

- Read each CSV with an **explicit schema** (`StructType`), not `inferSchema`.
- Add an ingestion timestamp column and write each table, unchanged otherwise, as Parquet into a `bronze/` folder.

## Phase 2: Silver (cleaned data)

- Fix types: the date columns arrive as strings.
- Remove duplicates. The geolocation table has a lot of them.
- Join the category translation table so products have English names.
- Find the data quality problems yourself: nulls, impossible dates (delivered before purchased?), orders with no items, and so on. **Count and save the rejected rows** to a separate folder instead of silently dropping them.
- Write the result to `silver/`.

## Phase 3: Gold (star schema)

Build a **fact table** and **dimension tables**:

| Table | Grain / content |
| --- | --- |
| `fact_order_items` | one row per item sold: price, freight, delivery delay in days, keys to the dimensions |
| `dim_customer`, `dim_product`, `dim_seller` | descriptive attributes, each with a surrogate key |
| `dim_date` | one row per day: year, month, quarter, weekday |

Then answer these questions from the gold tables only:

1. Monthly revenue and **month-over-month growth** (hint: `lag`)
2. **Top 3 product categories per state**
3. Average delay between actual and estimated delivery date, per seller
4. **Repeat-customer rate.** Careful: `customer_id` changes with every order, and `customer_unique_id` identifies the actual person. Work out why this matters.
5. Cumulative revenue over time

## Phase 4: Engineering quality

- Run `explain()` on your slowest query and count the `Exchange` lines. Can you reduce them, for example with a broadcast join on the dimensions?
- Write gold partitioned by month and compare the query time against unpartitioned.
- Write at least 5 data quality checks as functions (for example "fact has no null keys", "no duplicate `order_id` + `order_item_id`") and test your transformations with `pytest` on tiny DataFrames.

## Phase 5: Data warehouse extension

Move on once Phases 1 to 4 run end to end and you can explain why each design choice was made. Then:

- **Load the gold tables into a real warehouse.** Free options are PostgreSQL via JDBC, or DuckDB. BigQuery and Snowflake have free trials.
- **Make loads incremental and repeatable.** Load one month at a time, and make re-running a month safe (no duplicates). This is the central skill of warehouse work.
- **Slowly Changing Dimensions (SCD Type 2).** Keep history when a customer changes city.
- **Orchestrate.** Schedule bronze → silver → gold → load with Airflow, or even a plain script plus cron to start.
- **Optionally:** replace plain Parquet with Delta Lake or Iceberg tables, or add dbt for the SQL transformations.

## Definition of done

- One command re-runs the whole pipeline from raw CSVs.
- Folders are separated by layer, and code is in functions, not one giant notebook.
- You can explain why `fact_order_items` has the grain it has.
- You have a short README with your answers to the 5 questions.

## How I can help

Do the phases in order. After each one, paste your code and output and I'll review it, the same way as with the earlier exercises, pointing at problems rather than rewriting. I can also help you decide how to model the star schema before you build it, since that is the hardest design step and the one that matters most for the warehouse phase.

Would you like me to turn this into a checklist document you can keep and tick off as you go?
