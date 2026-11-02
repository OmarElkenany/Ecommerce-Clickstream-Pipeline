#!/usr/bin/env python
# coding: utf-8

# ## Silver_Layer
# 
# New notebook

# # Silver Layer
# 
# Reads **new rows** of each table in the **`raw`** schema (incrementally, via Delta streaming with checkpoints), and appends the result to the **`silver`** schema of the same lakehouse.
# 
# - `silver.valid_<table>`: rows that passed every check
# - `silver.invalid_<table>`: rows that failed at least one check, with `validation_reason`, `source_table`, `processed_ts`

# In[10]:


from pyspark.sql import functions as F

RAW_SCHEMA = "raw"
SILVER_SCHEMA = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

print("Raw schema   :", RAW_SCHEMA)
print("Silver schema:", SILVER_SCHEMA)


# ## 1. Shared validation helpers
# 
# Each cleaner returns the original columns plus `validation_reasons`.

# In[11]:


from pyspark.sql import functions as F

def missing_value(column):
    return F.col(column).isNull()


def missing_text(column):
    return (
        F.col(column).isNull()
        | (F.trim(F.col(column).cast("string")) == "")
    )


def negative_value(column):
    return F.col(column) < 0


def non_positive_value(column):
    return F.col(column) <= 0


def prepare_source(df, source_name, required_columns):
    missing_columns = sorted(set(required_columns) - set(df.columns))
    if missing_columns:
        raise ValueError(
            f"{source_name}: missing required columns: {missing_columns}. "
            f"Available columns: {df.columns}"
        )

    # Trim whitespace from all string columns; leave other data types unchanged.
    for column, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(column, F.trim(F.col(column)))

    return df


def common_event_checks():
    return [
        (missing_text("event_id"), "event_id is missing"),
        (missing_value("event_ts"), "event_ts is missing"),
    ]


def add_reasons(df, checks):
    reason_array = F.array(*[
        F.when(condition, F.lit(reason))
        for condition, reason in checks
    ])
    return (
        df.withColumn("_validation_reasons", reason_array)
          .withColumn(
              "validation_reasons",
              F.filter(F.col("_validation_reasons"), lambda reason: reason.isNotNull())
          )
          .drop("_validation_reasons")
    )


# ## 2. Table-specific cleaning rules
# 

# In[12]:


# 1. raw_cart_items
def clean_cart_items(df):
    df = prepare_source(df, "raw_cart_items", [
        "event_id", "event_ts", "session_id",
        "customer_id", "product_id", "price", "quantity"
    ])
    checks = common_event_checks() + [
        (missing_text("session_id"), "session_id is missing"),
        (missing_text("product_id"), "product_id is missing"),
        (missing_value("price"), "price is missing"),
        (negative_value("price"), "price cannot be negative"),
        (missing_value("quantity"), "quantity is missing"),
        (non_positive_value("quantity"), "quantity must be positive"),
    ]
    return add_reasons(df, checks)


# 2. raw_cart_removals
def clean_cart_removals(df):
    df = prepare_source(df, "raw_cart_removals", [
        "event_id", "event_ts", "session_id",
        "customer_id", "product_id", "price"
    ])
    checks = common_event_checks() + [
        (missing_text("session_id"), "session_id is missing"),
        (missing_text("product_id"), "product_id is missing"),
        (missing_value("price"), "price is missing"),
        (negative_value("price"), "price cannot be negative"),
    ]
    return add_reasons(df, checks)


# 3. categories  (category_id, category_path: both strings)
def clean_categories(df):
    df = prepare_source(df, "categories", ["category_id", "category_path"])
    checks = [
        (missing_text("category_id"), "category_id is missing"),
        (missing_text("category_path"), "category_path is missing"),
    ]
    return add_reasons(df, checks)


# 4. raw_checkouts
def clean_checkouts(df):
    df = prepare_source(df, "raw_checkouts", [
        "event_id", "event_ts", "session_id",
        "customer_id", "cart_value", "quantity", "coupon_code"
    ])
    checks = common_event_checks() + [
        (missing_text("session_id"), "session_id is missing"),
        (missing_value("cart_value"), "cart_value is missing"),
        (negative_value("cart_value"), "cart_value cannot be negative"),
        (missing_value("quantity"), "quantity is missing"),
        (non_positive_value("quantity"), "quantity must be positive"),
    ]
    return add_reasons(df, checks)


# 5. raw_clickstream_events
def clean_clickstream_events(df):
    df = prepare_source(df, "raw_clickstream_events", [
        "event_id", "event_ts", "event_type", "session_id",
        "anonymous_id", "customer_id", "product_id", "price",
        "quantity", "cart_value", "coupon_code", "amount",
        "status", "error_code", "order_id", "order_value",
        "device_type", "referrer", "category", "search_term",
        "payment_method", "shipping_country"
    ])
    checks = common_event_checks() + [
        (missing_text("event_type"), "event_type is missing"),
        (F.col("price").isNotNull() & negative_value("price"),
         "price cannot be negative"),
        (F.col("quantity").isNotNull() & non_positive_value("quantity"),
         "quantity must be positive"),
        (F.col("cart_value").isNotNull() & negative_value("cart_value"),
         "cart_value cannot be negative"),
        (F.col("amount").isNotNull() & negative_value("amount"),
         "amount cannot be negative"),
        (F.col("order_value").isNotNull() & negative_value("order_value"),
         "order_value cannot be negative"),
    ]
    return add_reasons(df, checks)


# 6. raw_customers
def clean_customers(df):
    df = prepare_source(df, "raw_customers", [
        "event_id", "event_ts", "customer_id", "user_id",
        "name", "email", "gender", "signup_date", "country"
    ])
    checks = common_event_checks() + [
        (missing_text("customer_id"), "customer_id is missing"),
        (missing_text("user_id"), "user_id is missing"),
        (missing_value("signup_date"), "signup_date is missing"),
    ]
    return add_reasons(df, checks)


# 7. raw_devices
def clean_devices(df):
    df = prepare_source(df, "raw_devices", [
        "event_id", "event_ts", "anonymous_id",
        "customer_id", "device_type"
    ])
    checks = common_event_checks() + [
        (missing_text("anonymous_id"), "anonymous_id is missing"),
        (missing_text("device_type"), "device_type is missing"),
    ]
    return add_reasons(df, checks)


# 8. raw_events
def clean_events(df):
    df = prepare_source(df, "raw_events", [
        "record_type", "event_id", "event_ts", "payload"
    ])
    checks = [
        (missing_text("record_type"), "record_type is missing"),
        (missing_text("event_id"), "event_id is missing"),
        (missing_value("event_ts"), "event_ts is missing"),
        (missing_text("payload"), "payload is missing"),
    ]
    return add_reasons(df, checks)


# 9. raw_orders
def clean_orders(df):
    df = prepare_source(df, "raw_orders", [
        "order_id", "event_id", "event_ts", "session_id",
        "customer_id", "order_value", "quantity",
        "payment_method", "shipping_country"
    ])
    checks = common_event_checks() + [
        (missing_text("order_id"), "order_id is missing"),
        (missing_text("session_id"), "session_id is missing"),
        (missing_value("order_value"), "order_value is missing"),
        (negative_value("order_value"), "order_value cannot be negative"),
        (missing_value("quantity"), "quantity is missing"),
        (non_positive_value("quantity"), "quantity must be positive"),
    ]
    return add_reasons(df, checks)


# 10. raw_payment_attempts
def clean_payment_attempts(df):
    df = prepare_source(df, "raw_payment_attempts", [
        "event_id", "event_ts", "session_id", "customer_id",
        "amount", "status", "error_code", "payment_method"
    ])
    checks = common_event_checks() + [
        (missing_text("session_id"), "session_id is missing"),
        (missing_value("amount"), "amount is missing"),
        (negative_value("amount"), "amount cannot be negative"),
        (missing_text("status"), "status is missing"),
        (missing_text("payment_method"), "payment_method is missing"),
    ]
    return add_reasons(df, checks)


# 11. raw_product_prices
def clean_product_prices(df):
    df = prepare_source(df, "raw_product_prices", [
        "event_id", "event_ts", "product_id", "list_price"
    ])
    checks = common_event_checks() + [
        (missing_text("product_id"), "product_id is missing"),
        (missing_value("list_price"), "list_price is missing"),
        (negative_value("list_price"), "list_price cannot be negative"),
    ]
    return add_reasons(df, checks)


# 12. raw_products  (category_id is an INTEGER here; categories.category_id is a STRING)
def clean_products(df):
    df = prepare_source(df, "raw_products", [
        "event_id", "event_ts", "product_id", "category_id", "category",
        "brand", "product_name", "list_price", "stock_quantity"
    ])
    checks = common_event_checks() + [
        (missing_text("product_id"), "product_id is missing"),
        (missing_value("category_id"), "category_id is missing"),
        (missing_text("product_name"), "product_name is missing"),
        (missing_value("list_price"), "list_price is missing"),
        (negative_value("list_price"), "list_price cannot be negative"),
        (F.col("stock_quantity").isNotNull() & negative_value("stock_quantity"),
         "stock_quantity cannot be negative"),
    ]
    return add_reasons(df, checks)


# 13. calendar_days  (date_key: timestamp, holiday / weekend / event_flag: boolean)
def clean_calendar_days(df):
    df = prepare_source(df, "calendar_days", [
        "date_key", "holiday", "weekend", "event_flag"
    ])
    checks = [
        (missing_value("date_key"), "date_key is missing"),
        (missing_value("holiday"), "holiday is missing"),
        (missing_value("weekend"), "weekend is missing"),
        (missing_value("event_flag"), "event_flag is missing"),
    ]
    return add_reasons(df, checks)


# 14. event_types  (event_type_id, event_type_name: strings; funnel_order: long)
def clean_event_types(df):
    df = prepare_source(df, "event_types", [
        "event_type_id", "event_type_name", "funnel_order"
    ])
    checks = [
        (missing_text("event_type_id"), "event_type_id is missing"),
        (missing_text("event_type_name"), "event_type_name is missing"),
        (F.col("funnel_order").isNotNull() & negative_value("funnel_order"),
         "funnel_order cannot be negative"),
    ]
    return add_reasons(df, checks)


# 15. raw_stock_changes  (product_id, delta, stock_quantity, reason)
def clean_stock_changes(df):
    df = prepare_source(df, "raw_stock_changes", [
        "event_id", "event_ts", "product_id", "delta", "stock_quantity", "reason"
    ])
    checks = common_event_checks() + [
        (missing_text("product_id"), "product_id is missing"),
        (missing_value("delta"), "delta is missing"),
        (missing_value("stock_quantity"), "stock_quantity is missing"),
        (negative_value("stock_quantity"), "stock_quantity cannot be negative"),
    ]
    return add_reasons(df, checks)


# In[13]:


# Registry: raw table name -> (key column used to de-duplicate within a batch, cleaner function)
# Must run AFTER all cleaner functions above are defined.
TABLES = {
    "raw_cart_items":         ("event_id",      clean_cart_items),
    "raw_cart_removals":      ("event_id",      clean_cart_removals),
    "categories":             ("category_id",   clean_categories),
    "raw_checkouts":          ("event_id",      clean_checkouts),
    "raw_clickstream_events": ("event_id",      clean_clickstream_events),
    "raw_customers":          ("event_id",      clean_customers),
    "raw_devices":            ("event_id",      clean_devices),
    "raw_events":             ("event_id",      clean_events),
    "raw_orders":             ("event_id",      clean_orders),
    "raw_payment_attempts":   ("event_id",      clean_payment_attempts),
    "raw_product_prices":     ("event_id",      clean_product_prices),
    "raw_products":           ("event_id",      clean_products),
    "calendar_days":          ("date_key",      clean_calendar_days),
    "event_types":            ("event_type_id", clean_event_types),
    "raw_stock_changes":      ("event_id",      clean_stock_changes),
}

print(f"Loaded {len(TABLES)} tables")


# ## 3. Duplicate flagging helpers
# 

# In[14]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_SCHEMA = "raw"
SILVER_SCHEMA = "silver"


def silver_name(source):
    """raw_products -> products, calendar_days -> calendar_days"""
    return source.removeprefix("raw_")


def flag_duplicates(cleaned_df, key):
    """Keep the first row per key; later rows get an extra validation reason."""
    w = Window.partitionBy(key).orderBy(F.monotonically_increasing_id())
    return (
        cleaned_df
        .withColumn("_rn", F.row_number().over(w))
        .withColumn(
            "validation_reasons",
            F.concat(
                F.col("validation_reasons"),
                F.when(
                    F.col(key).isNotNull() & (F.col("_rn") > 1),
                    F.array(F.lit(f"duplicate {key}"))
                ).otherwise(F.array().cast("array<string>"))
            )
        )
        .drop("_rn")
    )


# ## 4. Incremental run (self-healing)
# 
# - **Checkpoint and Silver tables are always kept consistent.** For each table, if the checkpoint folder or either Silver table is missing, both are reset so that table is reprocessed from the first raw row. This is what guarantees no raw rows are skipped.
# - **To reprocess everything:** set `FULL_REFRESH = True` for one run, or bump `CHECKPOINT_VERSION` (e.g. `v7` -> `v8`).
# - Valid rows go to `silver.valid_<table>`, invalid rows to `silver.invalid_<table>`. Duplicate keys are flagged within a batch and against keys already accepted.
# - The final cell **fails the notebook** if any table has fewer processed rows than were in `raw` when the run started, so the pipeline follows the red arrow to `Fail_Silver`.

# In[15]:


# ---- Configuration -------------------------------------------------------
CHECKPOINT_VERSION = "v7"      # bump to force a full reprocess
FULL_REFRESH = False           # True = wipe Silver + checkpoints for this run, then reprocess all raw rows
CHECKPOINT_ROOT = f"Files/_checkpoints/silver_tablewise_{CHECKPOINT_VERSION}"


def path_exists(path):
    try:
        notebookutils.fs.ls(path)
        return True
    except Exception:
        return False


def count_table(t):
    return spark.read.table(t).count() if spark.catalog.tableExists(t) else 0


def reset_table(source):
    name = silver_name(source)
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.valid_{name}")
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_SCHEMA}.invalid_{name}")
    if path_exists(f"{CHECKPOINT_ROOT}/{source}"):
        notebookutils.fs.rm(f"{CHECKPOINT_ROOT}/{source}", True)


# Stop leftover streams from earlier cells in this session
for q in spark.streams.active:
    q.stop()

# Keep checkpoint and Silver tables in sync: if either side is missing, reset both.
for source in TABLES:
    name = silver_name(source)
    in_sync = (
        path_exists(f"{CHECKPOINT_ROOT}/{source}")
        and spark.catalog.tableExists(f"{SILVER_SCHEMA}.valid_{name}")
        and spark.catalog.tableExists(f"{SILVER_SCHEMA}.invalid_{name}")
    )
    if FULL_REFRESH or not in_sync:
        reset_table(source)
        print(f"Reset {source}: will be reprocessed from the first raw row")
    else:
        print(f"Kept  {source}: incremental")


# In[16]:


def save_batch(cleaned_df, batch_id, source, target, key):
    """Append one micro-batch to silver.valid_<target> / silver.invalid_<target>."""
    raw_table = f"{RAW_SCHEMA}.{source}"
    valid_target = f"{SILVER_SCHEMA}.valid_{target}"
    invalid_target = f"{SILVER_SCHEMA}.invalid_{target}"

    flagged = flag_duplicates(cleaned_df, key)          # duplicates inside this batch
    columns = flagged.columns

    # Duplicates of keys already accepted in an earlier batch
    if spark.catalog.tableExists(valid_target):
        existing = (
            spark.read.table(valid_target)
                 .select(key).distinct()
                 .withColumn("_already_valid", F.lit(True))
        )
        flagged = (
            flagged.join(existing, on=key, how="left")
                   .withColumn(
                       "validation_reasons",
                       F.concat(
                           F.col("validation_reasons"),
                           F.when(
                               F.col("_already_valid").isNotNull() & F.col(key).isNotNull(),
                               F.array(F.lit(f"duplicate {key}"))
                           ).otherwise(F.array().cast("array<string>"))
                       )
                   )
                   .select(*columns)
        )

    flagged = flagged.persist()
    try:
        valid_df = (
            flagged.filter(F.size("validation_reasons") == 0)
                   .drop("validation_reasons")
        )
        invalid_df = (
            flagged.filter(F.size("validation_reasons") > 0)
                   .withColumn("validation_reason", F.concat_ws("; ", "validation_reasons"))
                   .withColumn("source_table", F.lit(raw_table))
                   .withColumn("processed_ts", F.current_timestamp())
                   .drop("validation_reasons")
        )

        # The txn ids include CHECKPOINT_VERSION, so a new checkpoint never collides with old ones.
        # They make a retried batch idempotent (Delta skips an already-committed batch_id).
        (valid_df.write.format("delta").mode("append")
            .option("txnAppId", f"silver_valid_{target}_{CHECKPOINT_VERSION}")
            .option("txnVersion", batch_id)
            .option("mergeSchema", "true").saveAsTable(valid_target))
        (invalid_df.write.format("delta").mode("append")
            .option("txnAppId", f"silver_invalid_{target}_{CHECKPOINT_VERSION}")
            .option("txnVersion", batch_id)
            .option("mergeSchema", "true").saveAsTable(invalid_target))
    finally:
        flagged.unpersist()


# In[17]:


# Incremental run: process everything new since the last checkpoint, then stop.
active_queries = []

# Raw row counts at the moment the run starts (used by the final check)
RAW_AT_START = {source: count_table(f"{RAW_SCHEMA}.{source}") for source in TABLES}


def make_batch_handler(source, target, key, cleaner):
    def handler(batch_df, batch_id):
        cleaned_df = cleaner(batch_df)
        save_batch(
            cleaned_df,
            batch_id=batch_id,
            source=source,
            target=target,
            key=key
        )
    return handler


for source, (key, cleaner) in TABLES.items():
    target = silver_name(source)
    checkpoint = f"{CHECKPOINT_ROOT}/{source}"

    query = (
        spark.readStream
        .format("delta")
        .table(f"{RAW_SCHEMA}.{source}")
        .writeStream
        .foreachBatch(make_batch_handler(source, target, key, cleaner))
        .option("checkpointLocation", checkpoint)
        .queryName(f"silver_{target}")
        .trigger(availableNow=True)
        .start()
    )

    active_queries.append(query)
    print(f"Started: {query.name}")

# Wait for every query to finish. If any query failed, this raises an error,
# the notebook activity fails, and the pipeline follows the red arrow to Fail_Silver.
for q in active_queries:
    q.awaitTermination()

print("All silver queries finished.")


# In[18]:


# Verify: every raw row that existed when the run started must be in valid or invalid.
print(f"{'source':<26}{'raw_at_start':>13}{'valid':>10}{'invalid':>10}{'missing':>9}  status")
problems = []
for source in TABLES:
    name = silver_name(source)
    start_n = RAW_AT_START[source]
    valid_n = count_table(f"{SILVER_SCHEMA}.valid_{name}")
    invalid_n = count_table(f"{SILVER_SCHEMA}.invalid_{name}")
    missing = max(start_n - valid_n - invalid_n, 0)

    ok = missing == 0
    if not ok:
        problems.append(source)
    print(f"{source:<26}{start_n:>13}{valid_n:>10}{invalid_n:>10}{missing:>9}  {'OK' if ok else 'MISSING ROWS'}")

if problems:
    raise RuntimeError(
        f"Silver is missing raw rows for: {problems}. "
        "Set FULL_REFRESH = True (or bump CHECKPOINT_VERSION) and run again."
    )
print("\nALL RAW ROWS PROCESSED")


# In[ ]:




