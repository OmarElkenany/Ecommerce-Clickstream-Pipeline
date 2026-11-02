#!/usr/bin/env python
# coding: utf-8

# ## Silver_DataQuality
# 
# New notebook

# In[6]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%pyspark

# spark.sql("""
# SHOW TABLES IN Click_stream.clickstream_lakehouse.silver
# """).show(100, truncate=False)


# In[7]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%pyspark

# from delta.tables import DeltaTable

# silver_schema = "Click_stream.clickstream_lakehouse.silver"

# # Discover tables using the namespace that worked in Fabric
# tables_df = spark.sql(f"SHOW TABLES IN {silver_schema}")
# table_names = [
#     row["tableName"]
#     for row in tables_df.collect()
#     if not row["isTemporary"]
# ]

# print(f"Found {len(table_names)} Silver tables")
# print("=" * 60)

# results = []

# for table_name in table_names:
#     full_table_name = f"{silver_schema}.{table_name}"

#     try:
#         print(f"Optimizing: {table_name}")

#         (
#             DeltaTable
#             .forName(spark, full_table_name)
#             .optimize()
#             .executeCompaction()
#         )

#         results.append((table_name, "SUCCESS", ""))
#         print(f"SUCCESS: {table_name}")

#     except Exception as e:
#         results.append((table_name, "FAILED", str(e)[:500]))
#         print(f"FAILED: {table_name}")
#         print(str(e)[:500])

# print("\n" + "=" * 60)
# print("OPTIMIZATION SUMMARY")

# for name, status, error in results:
#     print(f"{status}: {name}")
#     if error:
#         print(f"  Error: {error}")

# print(
#     f"\nTotal: {len(results)} | "
#     f"Success: {sum(1 for r in results if r[1] == 'SUCCESS')} | "
#     f"Failed: {sum(1 for r in results if r[1] == 'FAILED')}"
# )


# In[9]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%pyspark

# from delta.tables import DeltaTable

# silver_schema = "Click_stream.clickstream_lakehouse.silver"
# retention_hours = 168  # 7 days

# tables_df = spark.sql(f"SHOW TABLES IN {silver_schema}")
# table_names = [
#     row["tableName"]
#     for row in tables_df.collect()
#     if not row["isTemporary"]
# ]

# for table_name in table_names:
#     full_table_name = f"{silver_schema}.{table_name}"

#     try:
#         print(f"Vacuuming: {table_name}")

#         DeltaTable.forName(
#             spark, full_table_name
#         ).vacuum(retention_hours)

#         print(f"SUCCESS: {table_name}")

#     except Exception as e:
#         print(f"FAILED: {table_name} - {str(e)[:300]}")


# In[10]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%pyspark

# from datetime import datetime, timezone
# from functools import reduce

# from pyspark.sql import functions as F
# from pyspark.sql.types import (
#     StructType, StructField, StringType, TimestampType
# )

# # ============================================================
# # 1. CONFIGURATION — UPDATED TO THE CURRENT LAKEHOUSE
# # ============================================================

# SILVER_SCHEMA = "Click_stream.clickstream_lakehouse.silver"
# GOLD_SCHEMA   = "Click_stream.clickstream_lakehouse.gold"
# DQ_SCHEMA     = "Click_stream.clickstream_lakehouse.dbo"

# RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
# RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)

# SILVER_TABLES = [
#     "valid_calendar_days",
#     "valid_cart_items",
#     "valid_cart_removals",
#     "valid_categories",
#     "valid_checkouts",
#     "valid_clickstream_events",
#     "valid_customers",
#     "valid_devices",
#     "valid_event_types",
#     "valid_events",
#     "valid_orders",
#     "valid_payment_attempts",
#     "valid_product_prices",
#     "valid_products",
#     "valid_stock_changes",
# ]

# QUARANTINE_TABLES = [
#     "invalid_calendar_days",
#     "invalid_cart_items",
#     "invalid_cart_removals",
#     "invalid_categories",
#     "invalid_checkouts",
#     "invalid_clickstream_events",
#     "invalid_customers",
#     "invalid_devices",
#     "invalid_event_types",
#     "invalid_events",
#     "invalid_orders",
#     "invalid_payment_attempts",
#     "invalid_product_prices",
#     "invalid_products",
#     "invalid_stock_changes",
# ]

# # ============================================================
# # 2. LOAD TABLES USING THEIR FULLY QUALIFIED NAMES
# # ============================================================

# def load_tables(table_names, schema):
#     loaded = {}

#     for name in table_names:
#         try:
#             loaded[name] = spark.table(f"{schema}.{name}")
#             print(f"Loaded: {schema}.{name}")
#         except Exception as e:
#             print(f"SKIPPED: {schema}.{name} | {str(e)[:200]}")

#     return loaded


# T = load_tables(
#     SILVER_TABLES + QUARANTINE_TABLES,
#     SILVER_SCHEMA
# )

# print("\nLoaded valid tables:", sum(t in T for t in SILVER_TABLES))
# print("Loaded invalid tables:", sum(t in T for t in QUARANTINE_TABLES))

# # ============================================================
# # 3. DATA QUALITY RESULT COLLECTION
# # ============================================================

# DQ_RESULTS = []
# DQ_PROFILES = []


# def add_check(table_name, check_name, observed, status, threshold=""):
#     DQ_RESULTS.append({
#         "run_id": RUN_ID,
#         "run_ts": RUN_TS,
#         "layer": "silver",
#         "table_name": table_name,
#         "check_name": check_name,
#         "observed_value": str(observed),
#         "status": status,
#         "threshold": str(threshold),
#     })


# def status_zero(value):
#     return "PASS" if value == 0 else "FAIL"


# def status_percentage(value, pass_limit, warn_limit):
#     if value <= pass_limit:
#         return "PASS"
#     if value <= warn_limit:
#         return "WARN"
#     return "FAIL"


# # ============================================================
# # 4. SILVER DATA QUALITY CHECKS
# # ============================================================

# def silver_checks(T, now_ts=None):
#     if now_ts is None:
#         now_ts = RUN_TS

#     for table_name, df in T.items():
#         is_quarantine = table_name in QUARANTINE_TABLES
#         columns = set(df.columns)

#         # ----------------------------------------------------
#         # A. Table availability and row count
#         # ----------------------------------------------------
#         row_count = df.count()

#         add_check(
#             table_name, "table_available", 1, "PASS",
#             "Table exists and can be read"
#         )

#         add_check(
#             table_name, "row_count", row_count,
#             "PASS" if row_count > 0 else "WARN",
#             "> 0 rows"
#         )

#         # ----------------------------------------------------
#         # B. Null counts and column profiling
#         # ----------------------------------------------------
#         for column_name, dtype in df.dtypes:
#             null_count = df.filter(
#                 F.col(column_name).isNull()
#             ).count()

#             null_pct = (
#                 null_count / row_count * 100
#                 if row_count else 0.0
#             )

#             DQ_PROFILES.append({
#                 "run_id": RUN_ID,
#                 "run_ts": RUN_TS,
#                 "layer": "silver",
#                 "table_name": table_name,
#                 "column_name": column_name,
#                 "data_type": dtype,
#                 "row_count": row_count,
#                 "null_count": null_count,
#                 "null_pct": float(null_pct),
#                 "distinct_count": None,
#             })

#             # Nulls are reported for every column.
#             # Whether they are failures depends on the field's role.
#             add_check(
#                 table_name,
#                 f"null_count:{column_name}",
#                 null_count,
#                 "INFO",
#                 "Review against column requirements"
#             )

#         # ----------------------------------------------------
#         # C. Duplicate event IDs
#         # ----------------------------------------------------
#         key_column = None

#         if "event_id" in columns:
#             key_column = "event_id"
#         elif "order_id" in columns and table_name == "valid_orders":
#             key_column = "order_id"

#         if key_column:
#             non_null = df.filter(F.col(key_column).isNotNull())
#             duplicate_rows = (
#                 non_null.count()
#                 - non_null.select(key_column).distinct().count()
#             )

#             add_check(
#                 table_name,
#                 f"duplicate_key:{key_column}",
#                 duplicate_rows,
#                 status_zero(duplicate_rows),
#                 "0 duplicate key rows"
#             )

#         # ----------------------------------------------------
#         # D. Event timestamp validity
#         # ----------------------------------------------------
#         if "event_ts" in columns:
#             invalid_ts = df.filter(
#                 F.col("event_ts").isNull()
#             ).count()

#             add_check(
#                 table_name,
#                 "event_ts_not_null",
#                 invalid_ts,
#                 status_zero(invalid_ts),
#                 "0 null timestamps"
#             )

#             future_ts = df.filter(
#                 F.col("event_ts") > F.lit(now_ts)
#             ).count()

#             add_check(
#                 table_name,
#                 "future_event_timestamp",
#                 future_ts,
#                 "PASS" if future_ts == 0 else "WARN",
#                 "No timestamps after the data clock"
#             )

#         # ----------------------------------------------------
#         # E. Numeric value checks
#         # ----------------------------------------------------
#         for numeric_col in [
#             "price", "amount", "order_value",
#             "cart_value", "quantity", "list_price"
#         ]:
#             if numeric_col not in columns:
#                 continue

#             if numeric_col == "quantity":
#                 invalid_values = df.filter(
#                     F.col(numeric_col) <= 0
#                 ).count()
#             else:
#                 invalid_values = df.filter(
#                     F.col(numeric_col) < 0
#                 ).count()

#             add_check(
#                 table_name,
#                 f"invalid_numeric:{numeric_col}",
#                 invalid_values,
#                 status_zero(invalid_values),
#                 "Quantity > 0; monetary values >= 0"
#             )

#         # ----------------------------------------------------
#         # F. Event type validity
#         # ----------------------------------------------------
#         if "event_type" in columns:
#             invalid_event_type = df.filter(
#                 F.col("event_type").isNull()
#                 | (F.trim(F.col("event_type")) == "")
#             ).count()

#             add_check(
#                 table_name,
#                 "event_type_not_blank",
#                 invalid_event_type,
#                 status_zero(invalid_event_type),
#                 "0 null or blank event types"
#             )

#         # ----------------------------------------------------
#         # G. Quarantine table checks
#         # ----------------------------------------------------
#         if is_quarantine:
#             if "quarantine_reason" in columns:
#                 missing_reason = df.filter(
#                     F.col("quarantine_reason").isNull()
#                     | (F.trim(F.col("quarantine_reason")) == "")
#                 ).count()

#                 add_check(
#                     table_name,
#                     "quarantine_reason_present",
#                     missing_reason,
#                     status_zero(missing_reason),
#                     "0 missing quarantine reasons"
#                 )

#             add_check(
#                 table_name,
#                 "quarantine_rows",
#                 row_count,
#                 "INFO",
#                 "Monitor rejected rows"
#             )

#     return DQ_RESULTS, DQ_PROFILES


# # ============================================================
# # 5. RUN CHECKS
# # ============================================================

# DQ_RESULTS, DQ_PROFILES = silver_checks(T)

# print("\nSilver DQ checks completed:", len(DQ_RESULTS))
# print("Column profiles generated:", len(DQ_PROFILES))

# # ============================================================
# # 6. CREATE RESULT DATAFRAMES
# # ============================================================

# RESULT_SCHEMA = StructType([
#     StructField("run_id", StringType(), False),
#     StructField("run_ts", TimestampType(), False),
#     StructField("layer", StringType(), False),
#     StructField("table_name", StringType(), False),
#     StructField("check_name", StringType(), False),
#     StructField("observed_value", StringType(), True),
#     StructField("status", StringType(), False),
#     StructField("threshold", StringType(), True),
# ])

# PROFILE_SCHEMA = StructType([
#     StructField("run_id", StringType(), False),
#     StructField("run_ts", TimestampType(), False),
#     StructField("layer", StringType(), False),
#     StructField("table_name", StringType(), False),
#     StructField("column_name", StringType(), False),
#     StructField("data_type", StringType(), True),
#     StructField("row_count", StringType(), True),
#     StructField("null_count", StringType(), True),
#     StructField("null_pct", StringType(), True),
#     StructField("distinct_count", StringType(), True),
# ])

# results_rows = [
#     (
#         r["run_id"], r["run_ts"], r["layer"],
#         r["table_name"], r["check_name"],
#         r["observed_value"], r["status"], r["threshold"]
#     )
#     for r in DQ_RESULTS
# ]

# profile_rows = [
#     (
#         p["run_id"], p["run_ts"], p["layer"],
#         p["table_name"], p["column_name"],
#         p["data_type"], str(p["row_count"]),
#         str(p["null_count"]), str(p["null_pct"]),
#         None if p["distinct_count"] is None
#         else str(p["distinct_count"])
#     )
#     for p in DQ_PROFILES
# ]

# results_df = spark.createDataFrame(results_rows, RESULT_SCHEMA)
# profiles_df = spark.createDataFrame(profile_rows, PROFILE_SCHEMA)

# # ============================================================
# # 7. SCORECARD
# # ============================================================

# scorecard = (
#     results_df
#     .groupBy("status")
#     .count()
#     .orderBy("status")
# )

# print("\n========== SILVER DATA QUALITY SCORECARD ==========")
# scorecard.show(truncate=False)

# results_df.groupBy("table_name", "status").count() \
#     .orderBy("table_name", "status") \
#     .show(200, truncate=False)

# # ============================================================
# # 8. SAVE RESULTS
# # ============================================================

# # Existing results table was found under dbo.
# # Use fully qualified names to avoid writing to the wrong schema.

# RESULTS_TABLE = f"{DQ_SCHEMA}.dq_check_results"
# PROFILE_TABLE = f"{DQ_SCHEMA}.dq_profile_columns"

# def save_results(df, table_name):
#     try:
#         if spark.catalog.tableExists(table_name):
#             df.write.format("delta").mode("append").saveAsTable(table_name)
#         else:
#             df.write.format("delta").mode("overwrite").saveAsTable(table_name)

#         print(f"Saved: {table_name}")

#     except Exception as e:
#         print(f"Could not save {table_name}: {str(e)[:500]}")
#         print("The DQ results are still available in the results_df variable.")


# save_results(results_df, RESULTS_TABLE)
# save_results(profiles_df, PROFILE_TABLE)

# print("\nSilver DQ run finished.")
# print("Run ID:", RUN_ID)
# print("Results table:", RESULTS_TABLE)
# print("Profiles table:", PROFILE_TABLE)


# In[11]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%pyspark

# RESULTS_TABLE = "Click_stream.clickstream_lakehouse.dbo.dq_check_results"

# (
#     results_df.write
#     .format("delta")
#     .mode("append")
#     .option("mergeSchema", "true")
#     .saveAsTable(RESULTS_TABLE)
# )

# print("DQ results saved successfully.")

