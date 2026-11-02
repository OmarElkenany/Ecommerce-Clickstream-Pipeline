#!/usr/bin/env python
# coding: utf-8

# ## Gold_DataQuality
# 
# New notebook

# In[ ]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%pyspark

# # ============================================================
# # GOLD DATA QUALITY FRAMEWORK - MICROSOFT FABRIC
# # Complete standalone script
# # ============================================================
# from datetime import datetime, timezone
# from pyspark.sql import functions as F
# from pyspark.sql.types import *
# from datetime import datetime
# import uuid

# # ============================================================
# # 1. CONFIGURATION
# # ============================================================

# SILVER_SCHEMA = "Click_stream.clickstream_lakehouse.silver"
# GOLD_SCHEMA = "Click_stream.clickstream_lakehouse.gold"
# DBO_SCHEMA = "Click_stream.clickstream_lakehouse.dbo"

# RESULTS_TABLE = f"{DBO_SCHEMA}.gold_dq_check_results"
# PROFILE_TABLE = f"{DBO_SCHEMA}.gold_dq_profile_columns"

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
#     "valid_stock_changes"
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
#     "invalid_stock_changes"
# ]

# GOLD_TABLES = [
#     "dim_customer",
#     "dim_date",
#     "dim_device",
#     "dim_event_type",
#     "dim_product",
#     "fact_events",
#     "fact_order",
#     "fact_session",
#     "gold_cart_additions_daily",
#     "gold_cart_removals_daily",
#     "gold_checkout_duration",
#     "gold_customer_retention_daily",
#     "gold_product_funnel_daily",
#     "gold_revenue_daily",
#     "gold_session_funnel"
# ]

# RUN_ID = str(uuid.uuid4())
# RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)

# # Thresholds are percentages of rows.
# # Critical failures become FAIL; warning failures become WARN.
# NULL_WARN_THRESHOLD = 5.0
# PROFILE_DRIFT_THRESHOLD = 20.0

# # ============================================================
# # 2. LOAD AVAILABLE TABLES
# # ============================================================

# def load_tables(schema, table_names):
#     loaded = {}
#     missing = []

#     for name in table_names:
#         full_name = f"{schema}.{name}"

#         try:
#             if spark.catalog.tableExists(full_name):
#                 loaded[name] = spark.table(full_name)
#             else:
#                 missing.append(full_name)
#         except Exception as e:
#             print(f"Could not load {full_name}: {str(e)[:200]}")
#             missing.append(full_name)

#     return loaded, missing


# SILVER, MISSING_SILVER = load_tables(
#     SILVER_SCHEMA, SILVER_TABLES
# )

# QUARANTINE, MISSING_QUARANTINE = load_tables(
#     SILVER_SCHEMA, QUARANTINE_TABLES
# )

# GOLD, MISSING_GOLD = load_tables(
#     GOLD_SCHEMA, GOLD_TABLES
# )

# print("=" * 75)
# print("GOLD DATA QUALITY FRAMEWORK")
# print("=" * 75)
# print(f"Run ID: {RUN_ID}")
# print(f"Silver tables loaded: {len(SILVER)}/{len(SILVER_TABLES)}")
# print(f"Quarantine tables loaded: {len(QUARANTINE)}/{len(QUARANTINE_TABLES)}")
# print(f"Gold tables loaded: {len(GOLD)}/{len(GOLD_TABLES)}")

# if MISSING_SILVER:
#     print("Missing Silver:", MISSING_SILVER)

# if MISSING_QUARANTINE:
#     print("Missing Quarantine:", MISSING_QUARANTINE)

# if MISSING_GOLD:
#     print("Missing Gold:", MISSING_GOLD)

# # ============================================================
# # 3. RESULT HELPERS
# # ============================================================

# results = []


# def add_result(
#     layer,
#     table,
#     check_name,
#     dimension,
#     severity,
#     description,
#     total_rows,
#     failed_rows,
#     threshold_pct=0.0,
#     details=None
# ):
#     total_rows = int(total_rows or 0)
#     failed_rows = int(failed_rows or 0)

#     fail_pct = (
#         100.0 * failed_rows / total_rows
#         if total_rows > 0 else 0.0
#     )

#     if severity.lower() == "info":
#         status = "INFO"
#     elif total_rows == 0 and severity.lower() == "critical":
#         status = "WARN"
#     elif fail_pct <= threshold_pct:
#         status = "PASS"
#     elif severity.lower() == "critical":
#         status = "FAIL"
#     else:
#         status = "WARN"

#     results.append({
#         "run_id": RUN_ID,
#         "run_ts": RUN_TS,
#         "layer": layer,
#         "table_name": table,
#         "check_name": check_name,
#         "dimension": dimension,
#         "severity": severity.upper(),
#         "description": description,
#         "status": status,
#         "total_rows": total_rows,
#         "failed_rows": failed_rows,
#         "fail_pct": float(fail_pct),
#         "threshold_pct": float(threshold_pct),
#         "details": str(details)[:4000] if details is not None else None
#     })


# def count_rows(df):
#     return df.count()


# def has_column(df, column):
#     return column in df.columns


# def get_numeric_columns(df):
#     numeric_types = (
#         "tinyint", "smallint", "int", "bigint",
#         "float", "double", "decimal", "long", "short"
#     )

#     return [
#         field.name
#         for field in df.schema.fields
#         if any(
#             field.dataType.simpleString().lower().startswith(t)
#             for t in numeric_types
#         )
#     ]


# def get_key_columns(df):
#     # Detect common key names without assuming every table has one.
#     candidates = [
#         "event_id", "order_line_id", "session_id",
#         "customer_key", "date_key", "device_key",
#         "event_type_key", "product_key", "order_id",
#         "customer_id", "product_id"
#     ]

#     return [c for c in candidates if c in df.columns]


# # ============================================================
# # 4. GENERIC GOLD TABLE CHECKS
# # ============================================================

# def check_table_basics(table_name, df):
#     total = count_rows(df)

#     add_result(
#         "GOLD", table_name, "TABLE_NOT_EMPTY",
#         "COMPLETENESS", "critical",
#         "Gold table should contain at least one row.",
#         1, 0 if total > 0 else 1,
#         details=f"row_count={total}"
#     )

#     add_result(
#         "GOLD", table_name, "ROW_COUNT",
#         "VOLUME", "info",
#         "Record the current Gold table row count.",
#         total, 0,
#         details=f"row_count={total}"
#     )

#     # Null checks for columns that look like keys.
#     key_columns = get_key_columns(df)

#     for column in key_columns:
#         null_count = df.filter(
#             F.col(column).isNull()
#         ).count()

#         add_result(
#             "GOLD", table_name,
#             f"NULL_KEY_{column.upper()}",
#             "COMPLETENESS", "critical",
#             f"Key column {column} should not contain nulls.",
#             total, null_count,
#             details=f"column={column}"
#         )

#     # Duplicate checks on table-specific grain.
#     grain_keys = {
#         "dim_customer": ["customer_key"],
#         "dim_date": ["date_key"],
#         "dim_device": ["device_key"],
#         "dim_event_type": ["event_type_key"],
#         "dim_product": ["product_key"],
#         "fact_events": ["event_id"],
#         "fact_order": ["order_line_id"],
#         "fact_session": ["session_id"]
#     }

#     keys = grain_keys.get(table_name, [])

#     if keys and all(c in df.columns for c in keys):
#         distinct_count = df.select(*keys).distinct().count()
#         duplicate_count = max(total - distinct_count, 0)

#         add_result(
#             "GOLD", table_name,
#             "GRAIN_UNIQUENESS",
#             "UNIQUENESS", "critical",
#             f"Expected one row per {', '.join(keys)}.",
#             total, duplicate_count,
#             details=f"grain={keys}; distinct_keys={distinct_count}"
#         )

#     # Numeric columns should not contain unexpected negative values.
#     # This is a warning because negative values may be valid in some datasets.
#     numeric_columns = get_numeric_columns(df)

#     for column in numeric_columns:
#         if any(token in column.lower() for token in [
#             "price", "amount", "revenue", "quantity",
#             "duration", "count", "value", "sales"
#         ]):
#             negative_count = df.filter(
#                 F.col(column).cast("double") < 0
#             ).count()

#             add_result(
#                 "GOLD", table_name,
#                 f"NEGATIVE_VALUES_{column.upper()}",
#                 "VALIDITY", "warning",
#                 f"Review negative values in {column}.",
#                 total, negative_count,
#                 details=f"column={column}"
#             )


# for table_name, df in GOLD.items():
#     check_table_basics(table_name, df)

# # ============================================================
# # 5. FOREIGN KEY / ORPHAN CHECKS
# # ============================================================

# def check_orphans(
#     fact_name,
#     fact_df,
#     fact_key,
#     dimension_name,
#     dimension_df,
#     dimension_key
# ):
#     if (
#         fact_key not in fact_df.columns
#         or dimension_key not in dimension_df.columns
#     ):
#         return

#     # Null fact keys are checked separately.
#     fact_keys = (
#         fact_df
#         .filter(F.col(fact_key).isNotNull())
#         .select(F.col(fact_key).alias("_fk"))
#         .distinct()
#     )

#     dim_keys = (
#         dimension_df
#         .filter(F.col(dimension_key).isNotNull())
#         .select(F.col(dimension_key).alias("_dk"))
#         .distinct()
#     )

#     orphan_count = (
#         fact_keys
#         .join(
#             dim_keys,
#             fact_keys["_fk"] == dim_keys["_dk"],
#             "left_anti"
#         )
#         .count()
#     )

#     total_keys = fact_keys.count()

#     add_result(
#         "GOLD", fact_name,
#         f"FK_TO_{dimension_name.upper()}",
#         "REFERENTIAL_INTEGRITY", "warning",
#         f"Non-null {fact_key} values should exist in "
#         f"{dimension_name}.{dimension_key}.",
#         total_keys, orphan_count,
#         details=(
#             f"fact_key={fact_key}; "
#             f"dimension_key={dimension_key}; "
#             "Null keys excluded; review unknown/sentinel keys if present."
#         )
#     )


# relationships = [
#     ("fact_events", "customer_key", "dim_customer", "customer_key"),
#     ("fact_events", "device_key", "dim_device", "device_key"),
#     ("fact_events", "product_key", "dim_product", "product_key"),
#     ("fact_events", "event_type_key", "dim_event_type", "event_type_key"),
#     ("fact_session", "customer_key", "dim_customer", "customer_key"),
#     ("fact_session", "device_key", "dim_device", "device_key"),
#     ("fact_order", "customer_key", "dim_customer", "customer_key"),
#     ("fact_order", "product_key", "dim_product", "product_key")
# ]

# for fact_name, fk, dim_name, dk in relationships:
#     if fact_name in GOLD and dim_name in GOLD:
#         check_orphans(
#             fact_name, GOLD[fact_name], fk,
#             dim_name, GOLD[dim_name], dk
#         )

# # ============================================================
# # 6. FACT_EVENTS CHECKS
# # ============================================================

# if "fact_events" in GOLD:
#     df = GOLD["fact_events"]
#     total = count_rows(df)

#     if "event_ts" in df.columns:
#         invalid_ts = df.filter(
#             F.col("event_ts").isNotNull()
#             & F.to_timestamp(F.col("event_ts")).isNull()
#         ).count()

#         add_result(
#             "GOLD", "fact_events",
#             "EVENT_TIMESTAMP_VALIDITY",
#             "VALIDITY", "warning",
#             "Non-null event timestamps should be parseable.",
#             total, invalid_ts,
#             details="Column: event_ts"
#         )

#     if "event_type" in df.columns:
#         missing_type = df.filter(
#             F.col("event_type").isNull()
#             | (F.trim(F.col("event_type").cast("string")) == "")
#         ).count()

#         add_result(
#             "GOLD", "fact_events",
#             "EVENT_TYPE_COMPLETENESS",
#             "COMPLETENESS", "critical",
#             "Every event should have an event type.",
#             total, missing_type
#         )

#     if "quantity" in df.columns:
#         bad_quantity = df.filter(
#             F.col("quantity").cast("double") < 0
#         ).count()

#         add_result(
#             "GOLD", "fact_events",
#             "NON_NEGATIVE_QUANTITY",
#             "VALIDITY", "warning",
#             "Review negative event quantities.",
#             total, bad_quantity
#         )

# # ============================================================
# # 7. FACT_SESSION CHECKS
# # ============================================================

# if "fact_session" in GOLD:
#     df = GOLD["fact_session"]
#     total = count_rows(df)

#     if "duration_seconds" in df.columns:
#         bad_duration = df.filter(
#             F.col("duration_seconds").isNotNull()
#             & (F.col("duration_seconds").cast("double") < 0)
#         ).count()

#         add_result(
#             "GOLD", "fact_session",
#             "NON_NEGATIVE_SESSION_DURATION",
#             "VALIDITY", "critical",
#             "Session duration should not be negative.",
#             total, bad_duration
#         )

#     if "event_count" in df.columns:
#         bad_event_count = df.filter(
#             F.col("event_count").isNotNull()
#             & (F.col("event_count").cast("double") < 0)
#         ).count()

#         add_result(
#             "GOLD", "fact_session",
#             "NON_NEGATIVE_EVENT_COUNT",
#             "VALIDITY", "critical",
#             "Session event count should not be negative.",
#             total, bad_event_count
#         )

#     if all(c in df.columns for c in [
#         "converted_flag", "has_order"
#     ]):
#         # This is a consistency warning, not a hard failure:
#         # business definitions of conversion may differ.
#         mismatch = df.filter(
#             F.col("converted_flag").isNotNull()
#             & F.col("has_order").isNotNull()
#             & (
#                 F.col("converted_flag").cast("int")
#                 != F.col("has_order").cast("int")
#             )
#         ).count()

#         add_result(
#             "GOLD", "fact_session",
#             "CONVERSION_ORDER_CONSISTENCY",
#             "CONSISTENCY", "warning",
#             "Review sessions where converted_flag differs from has_order.",
#             total, mismatch,
#             details="Confirm that both fields use the same business definition."
#         )

# # ============================================================
# # 8. FACT_ORDER CHECKS
# # ============================================================

# if "fact_order" in GOLD:
#     df = GOLD["fact_order"]
#     total = count_rows(df)

#     if all(c in df.columns for c in [
#         "unit_price", "line_quantity", "line_amount"
#     ]):
#         # Compare with a small tolerance for floating-point rounding.
#         bad_amount = df.filter(
#             F.col("unit_price").isNotNull()
#             & F.col("line_quantity").isNotNull()
#             & F.col("line_amount").isNotNull()
#             & (
#                 F.abs(
#                     F.col("unit_price").cast("double")
#                     * F.col("line_quantity").cast("double")
#                     - F.col("line_amount").cast("double")
#                 ) > 0.02
#             )
#         ).count()

#         add_result(
#             "GOLD", "fact_order",
#             "LINE_AMOUNT_CALCULATION",
#             "CONSISTENCY", "warning",
#             "line_amount should approximately equal unit_price × line_quantity.",
#             total, bad_amount,
#             details="Tolerance: 0.02; excludes rows with null inputs."
#         )

#     if "line_quantity" in df.columns:
#         bad_quantity = df.filter(
#             F.col("line_quantity").isNotNull()
#             & (F.col("line_quantity").cast("double") < 0)
#         ).count()

#         add_result(
#             "GOLD", "fact_order",
#             "NON_NEGATIVE_LINE_QUANTITY",
#             "VALIDITY", "warning",
#             "Review negative order line quantities.",
#             total, bad_quantity
#         )

# # ============================================================
# # 9. SILVER-TO-GOLD RECONCILIATION
# # ============================================================

# if "valid_clickstream_events" in SILVER and "fact_events" in GOLD:
#     silver_events = SILVER["valid_clickstream_events"]
#     gold_events = GOLD["fact_events"]

#     if "event_id" in silver_events.columns and "event_id" in gold_events.columns:
#         source_ids = (
#             silver_events
#             .filter(F.col("event_id").isNotNull())
#             .select("event_id").distinct()
#         )

#         target_ids = (
#             gold_events
#             .filter(F.col("event_id").isNotNull())
#             .select("event_id").distinct()
#         )

#         missing_in_gold = (
#             source_ids.join(target_ids, "event_id", "left_anti").count()
#         )

#         source_count = source_ids.count()

#         add_result(
#             "RECONCILIATION", "fact_events",
#             "SILVER_EVENTS_MISSING_IN_GOLD",
#             "COMPLETENESS", "warning",
#             "Valid Silver event IDs should be represented in Gold fact_events.",
#             source_count, missing_in_gold,
#             details=(
#                 "This compares distinct event IDs. "
#                 "If Gold intentionally filters events, review the expected exclusions."
#             )
#         )

# if "valid_orders" in SILVER and "fact_order" in GOLD:
#     silver_orders = SILVER["valid_orders"]
#     gold_orders = GOLD["fact_order"]

#     if "order_id" in silver_orders.columns and "order_id" in gold_orders.columns:
#         source_ids = (
#             silver_orders
#             .filter(F.col("order_id").isNotNull())
#             .select("order_id").distinct()
#         )

#         target_ids = (
#             gold_orders
#             .filter(F.col("order_id").isNotNull())
#             .select("order_id").distinct()
#         )

#         missing_in_gold = (
#             source_ids.join(target_ids, "order_id", "left_anti").count()
#         )

#         source_count = source_ids.count()

#         add_result(
#             "RECONCILIATION", "fact_order",
#             "SILVER_ORDERS_MISSING_IN_GOLD",
#             "COMPLETENESS", "warning",
#             "Valid Silver order IDs should be represented in Gold fact_order.",
#             source_count, missing_in_gold,
#             details=(
#                 "Compares distinct order IDs. "
#                 "Review intentional filtering and order-line grain before interpreting warnings."
#             )
#         )
#     else:
#         add_result(
#             "RECONCILIATION", "fact_order",
#             "ORDER_RECONCILIATION_SKIPPED",
#             "COMPLETENESS", "info",
#             "Order reconciliation skipped because order_id is absent.",
#             0, 0
#         )

# # ============================================================
# # 10. QUARANTINE SUMMARY
# # ============================================================

# for table_name, df in QUARANTINE.items():
#     total = count_rows(df)

#     add_result(
#         "QUARANTINE", table_name,
#         "QUARANTINE_ROW_COUNT",
#         "MONITORING", "info",
#         "Track the number of rejected/quarantined rows.",
#         total, 0,
#         details=f"quarantine_rows={total}"
#     )

# # ============================================================
# # 11. PROFILE GOLD TABLE COLUMNS
# # ============================================================

# profile_rows = []

# def safe_stat(df, column, expression):
#     try:
#         return df.agg(expression.alias("value")).first()["value"]
#     except Exception:
#         return None


# for table_name, df in GOLD.items():
#     total = count_rows(df)

#     for field in df.schema.fields:
#         column = field.name

#         try:
#             stats = df.agg(
#                 F.count(
#                     F.when(F.col(column).isNull(), 1)
#                 ).alias("null_count"),
#                 F.approx_count_distinct(
#                     F.col(column)
#                 ).alias("approx_distinct")
#             ).first()

#             null_count = int(stats["null_count"] or 0)
#             distinct_count = int(stats["approx_distinct"] or 0)

#             min_value = None
#             max_value = None
#             mean_value = None
#             top_values = None

#             if field.dataType.simpleString().lower() in [
#                 "string", "date", "timestamp",
#                 "int", "bigint", "long", "double",
#                 "float", "short", "smallint", "tinyint"
#             ] or field.dataType.simpleString().lower().startswith("decimal"):
#                 try:
#                     agg_row = df.agg(
#                         F.min(F.col(column).cast("string")).alias("min_v"),
#                         F.max(F.col(column).cast("string")).alias("max_v")
#                     ).first()

#                     min_value = agg_row["min_v"]
#                     max_value = agg_row["max_v"]
#                 except Exception:
#                     pass

#             if field.dataType.simpleString().lower() in [
#                 "int", "bigint", "long", "double",
#                 "float", "short", "smallint", "tinyint"
#             ] or field.dataType.simpleString().lower().startswith("decimal"):
#                 try:
#                     mean_value = df.agg(
#                         F.avg(F.col(column).cast("double")).alias("mean_v")
#                     ).first()["mean_v"]
#                 except Exception:
#                     pass

#             if field.dataType.simpleString().lower() == "string":
#                 try:
#                     top = (
#                         df.groupBy(column)
#                         .count()
#                         .orderBy(F.desc("count"))
#                         .limit(5)
#                         .collect()
#                     )

#                     top_values = "; ".join(
#                         f"{r[column]} ({r['count']})"
#                         for r in top
#                         if r[column] is not None
#                     )
#                 except Exception:
#                     pass

#             profile_rows.append({
#                 "run_id": RUN_ID,
#                 "run_ts": RUN_TS,
#                 "table_name": table_name,
#                 "column_name": column,
#                 "data_type": field.dataType.simpleString(),
#                 "row_count": int(total),
#                 "null_count": null_count,
#                 "null_pct": (
#                     100.0 * null_count / total if total > 0 else 0.0
#                 ),
#                 "approx_distinct": distinct_count,
#                 "min_value": str(min_value) if min_value is not None else None,
#                 "max_value": str(max_value) if max_value is not None else None,
#                 "mean_value": float(mean_value) if mean_value is not None else None,
#                 "top_values": top_values
#             })

#         except Exception as e:
#             print(
#                 f"Profile warning: {table_name}.{column}: "
#                 f"{str(e)[:150]}"
#             )

# # ============================================================
# # 12. COLUMN NULL-RATE CHECKS
# # ============================================================

# for row in profile_rows:
#     # Only flag columns with actual nulls above the configured threshold.
#     if row["null_pct"] > NULL_WARN_THRESHOLD:
#         add_result(
#             "GOLD", row["table_name"],
#             f"HIGH_NULL_RATE_{row['column_name'].upper()}",
#             "COMPLETENESS", "warning",
#             f"Column null rate exceeds {NULL_WARN_THRESHOLD}%.",
#             row["row_count"], row["null_count"],
#             threshold_pct=NULL_WARN_THRESHOLD,
#             details=(
#                 f"column={row['column_name']}; "
#                 f"null_pct={row['null_pct']:.2f}%"
#             )
#         )

# # ============================================================
# # 13. BUILD RESULTS DATAFRAME
# # ============================================================

# RESULT_SCHEMA = StructType([
#     StructField("run_id", StringType(), True),
#     StructField("run_ts", TimestampType(), True),
#     StructField("layer", StringType(), True),
#     StructField("table_name", StringType(), True),
#     StructField("check_name", StringType(), True),
#     StructField("dimension", StringType(), True),
#     StructField("severity", StringType(), True),
#     StructField("description", StringType(), True),
#     StructField("status", StringType(), True),
#     StructField("total_rows", LongType(), True),
#     StructField("failed_rows", LongType(), True),
#     StructField("fail_pct", DoubleType(), True),
#     StructField("threshold_pct", DoubleType(), True),
#     StructField("details", StringType(), True)
# ])

# results_df = spark.createDataFrame(results, schema=RESULT_SCHEMA)

# PROFILE_SCHEMA = StructType([
#     StructField("run_id", StringType(), True),
#     StructField("run_ts", TimestampType(), True),
#     StructField("table_name", StringType(), True),
#     StructField("column_name", StringType(), True),
#     StructField("data_type", StringType(), True),
#     StructField("row_count", LongType(), True),
#     StructField("null_count", LongType(), True),
#     StructField("null_pct", DoubleType(), True),
#     StructField("approx_distinct", LongType(), True),
#     StructField("min_value", StringType(), True),
#     StructField("max_value", StringType(), True),
#     StructField("mean_value", DoubleType(), True),
#     StructField("top_values", StringType(), True)
# ])

# profile_df = spark.createDataFrame(
#     profile_rows,
#     schema=PROFILE_SCHEMA
# )

# # ============================================================
# # 14. SAVE RESULTS
# # ============================================================

# if results_df.count() > 0:
#     (
#         results_df.write
#         .format("delta")
#         .mode("append")
#         .option("mergeSchema", "true")
#         .saveAsTable(RESULTS_TABLE)
#     )
#     print(f"\nDQ results saved to: {RESULTS_TABLE}")
# else:
#     print("\nNo DQ results were generated.")

# if profile_df.count() > 0:
#     (
#         profile_df.write
#         .format("delta")
#         .mode("append")
#         .option("mergeSchema", "true")
#         .saveAsTable(PROFILE_TABLE)
#     )
#     print(f"Column profiles saved to: {PROFILE_TABLE}")
# else:
#     print("No column profiles were generated.")

# # ============================================================
# # 15. SCORECARD
# # ============================================================

# print("\n" + "=" * 75)
# print("GOLD DATA QUALITY SCORECARD")
# print("=" * 75)

# scorecard_df = (
#     results_df
#     .groupBy("status")
#     .agg(F.count("*").alias("checks"))
#     .orderBy("status")
# )

# scorecard_df.show(truncate=False)

# print("\nResults by layer:")
# (
#     results_df
#     .groupBy("layer", "status")
#     .agg(F.count("*").alias("checks"))
#     .orderBy("layer", "status")
#     .show(100, truncate=False)
# )

# print("\nResults by Gold table:")
# (
#     results_df
#     .filter(F.col("layer") == "GOLD")
#     .groupBy("table_name", "status")
#     .agg(F.count("*").alias("checks"))
#     .orderBy("table_name", "status")
#     .show(200, truncate=False)
# )

# print("\n" + "=" * 75)
# print("FAILED CHECKS")
# print("=" * 75)

# (
#     results_df
#     .filter(F.col("status") == "FAIL")
#     .select(
#         "table_name", "check_name",
#         "failed_rows", "total_rows",
#         "fail_pct", "description"
#     )
#     .show(200, truncate=False)
# )

# print("\n" + "=" * 75)
# print("WARNINGS")
# print("=" * 75)

# (
#     results_df
#     .filter(F.col("status") == "WARN")
#     .select(
#         "table_name", "check_name",
#         "failed_rows", "total_rows",
#         "fail_pct", "description"
#     )
#     .show(200, truncate=False)
# )

# print("\n" + "=" * 75)
# print("GOLD DATA QUALITY RUN COMPLETED")
# print(f"Run ID: {RUN_ID}")
# print("=" * 75)

