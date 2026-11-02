#!/usr/bin/env python
# coding: utf-8

# ## Gold
# 
# New notebook

# In[1]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

GOLD = "Click_stream.clickstream_lakehouse.gold"

SILVER = {
    "events": "Click_stream.clickstream_lakehouse.silver.valid_clickstream_events",
    "customers": "Click_stream.clickstream_lakehouse.silver.valid_customers",
    "devices": "Click_stream.clickstream_lakehouse.silver.valid_devices",
    "products": "Click_stream.clickstream_lakehouse.silver.valid_products",
    "cart_items": "Click_stream.clickstream_lakehouse.silver.valid_cart_items",
    "orders": "Click_stream.clickstream_lakehouse.silver.valid_orders",
    "categories": "Click_stream.clickstream_lakehouse.silver.valid_categories",
    "calendar": "Click_stream.clickstream_lakehouse.silver.valid_calendar_days",
    "event_types": "Click_stream.clickstream_lakehouse.silver.valid_event_types",
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

print("Gold schema:", GOLD)
print("Silver sources configured:", len(SILVER))

for key in ["events", "customers", "devices", "products",
            "cart_items", "orders", "categories",
            "calendar", "event_types"]:
    print(f"\n--- {key} ---")
    spark.table(SILVER[key]).printSchema()


# In[2]:


def read_table(table_name):
    return spark.table(table_name)


def make_key(column_name, prefix):
    return F.when(
        F.col(column_name).isNotNull(),
        F.concat(
            F.lit(prefix + "_"),
            F.sha2(F.col(column_name).cast("string"), 256)
        )
    )


def write_gold(df, table_name):
    full_name = f"{GOLD}.{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    print(f"Written: {full_name}")


def latest_per_key(df, key, timestamp="event_ts"):
    w = Window.partitionBy(key).orderBy(
        F.col(timestamp).desc_nulls_last()
    )
    return (
        df.filter(F.col(key).isNotNull())
          .withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def show_count(name, df):
    print(f"{name}: {df.count():,} rows")


# In[3]:


events_src = read_table(SILVER["events"])
customers_src = read_table(SILVER["customers"])
devices_src = read_table(SILVER["devices"])
products_src = read_table(SILVER["products"])
cart_items_src = read_table(SILVER["cart_items"])
orders_src = read_table(SILVER["orders"])
categories_src = read_table(SILVER["categories"])
calendar_src = read_table(SILVER["calendar"])
event_types_src = read_table(SILVER["event_types"])

for name, df in [
    ("events", events_src),
    ("customers", customers_src),
    ("devices", devices_src),
    ("products", products_src),
    ("cart_items", cart_items_src),
    ("orders", orders_src),
    ("categories", categories_src),
    ("calendar", calendar_src),
    ("event_types", event_types_src),
]:
    show_count(name, df)


# In[4]:


events = latest_per_key(events_src, "event_id")

customers = latest_per_key(customers_src, "customer_id")
devices = latest_per_key(devices_src, "anonymous_id")
products = latest_per_key(products_src, "product_id")

cart_items = (
    cart_items_src
    .filter(F.col("session_id").isNotNull())
    .dropDuplicates(["event_id"])
)

orders = (
    orders_src
    .filter(F.col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
)

categories = (
    categories_src
    .filter(F.col("category_id").isNotNull())
    .dropDuplicates(["category_id"])
)

event_types = (
    event_types_src
    .filter(F.col("event_type_name").isNotNull())
    .withColumn(
        "event_type_norm",
        F.lower(F.trim(F.col("event_type_name")))
    )
    .dropDuplicates(["event_type_norm"])
)

events = events.withColumn(
    "event_type_norm",
    F.lower(F.trim(F.col("event_type")))
)

show_count("Deduplicated events", events)
show_count("Deduplicated orders", orders)


# In[5]:


# DIM_DATE
calendar_dates = (
    calendar_src
    .withColumn("date", F.to_date("date_key"))
    .filter(F.col("date").isNotNull())
    .groupBy("date")
    .agg(
        F.max(F.col("weekend").cast("int")).cast("boolean").alias("weekend"),
        F.max(F.col("holiday").cast("int")).cast("boolean").alias("holiday"),
        F.max(F.col("event_flag").cast("int")).cast("boolean").alias("event_flag")
    )
)

dim_date = calendar_dates.select(
    F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
    "date",
    F.year("date").alias("year"),
    F.quarter("date").alias("quarter"),
    F.month("date").alias("month"),
    F.dayofmonth("date").alias("day"),
    F.dayofweek("date").alias("day_of_week"),
    "weekend",
    "holiday",
    "event_flag"
)

write_gold(dim_date, "dim_date")


# DIM_CUSTOMER
dim_customer = (
    customers
    .select(
        make_key("customer_id", "CUS").alias("customer_key"),
        "customer_id",
        "user_id",
        "name",
        "email",
        "gender",
        "signup_date",
        "country"
    )
)

write_gold(dim_customer, "dim_customer")


# DIM_DEVICE
dim_device = (
    devices
    .select(
        make_key("anonymous_id", "DEV").alias("device_key"),
        "anonymous_id",
        "customer_id",
        "device_type"
    )
)

write_gold(dim_device, "dim_device")


# DIM_PRODUCT
categories_lookup = categories.select(
    F.col("category_id").cast("string").alias("lookup_category_id"),
    F.col("category_path")
)

products_enriched = (
    products.alias("p")
    .join(
        categories_lookup.alias("c"),
        F.col("p.category_id").cast("string")
        == F.col("c.lookup_category_id"),
        "left"
    )
)

dim_product = products_enriched.select(
    make_key("product_id", "PRD").alias("product_key"),
    F.col("p.product_id").alias("product_id"),
    F.col("p.category_id").alias("category_id"),
    F.coalesce(F.col("c.category_path"), F.col("p.category"))
        .alias("category"),
    F.col("p.brand").alias("brand"),
    F.col("p.product_name").alias("product_name"),
    F.col("p.list_price").alias("list_price")
)

write_gold(dim_product, "dim_product")


# DIM_EVENT_TYPE
dim_event_type = (
    event_types
    .select(
        make_key("event_type_norm", "EVT").alias("event_type_key"),
        F.col("event_type_id").alias("source_event_type_id"),
        F.col("event_type_norm").alias("event_type"),
        F.col("funnel_order").cast("long").alias("funnel_order")
    )
)

write_gold(dim_event_type, "dim_event_type")


print("All five dimensions written.")


# In[6]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# FACT_SESSION — FULL BUILD
# Source: Gold fact_events
# Grain: One row per session_id
# ============================================================

GOLD = "Click_stream.clickstream_lakehouse.gold"

# 1. Read the event-level fact table
events = (
    spark.table(f"{GOLD}.fact_events")
    .filter(F.col("session_id").isNotNull())
)

# Normalize event types for consistent comparisons
events = events.withColumn(
    "event_type_norm",
    F.lower(F.trim(F.col("event_type")))
)

# 2. Aggregate events at session level
fact_session_base = (
    events
    .groupBy("session_id")
    .agg(
        F.first("customer_id", ignorenulls=True).alias("customer_id"),
        F.first("anonymous_id", ignorenulls=True).alias("anonymous_id"),
        F.first("device_key", ignorenulls=True).alias("device_key"),

        F.min("event_ts").alias("session_start_ts"),
        F.max("event_ts").alias("session_end_ts"),

        F.count("*").cast("long").alias("event_count"),

        F.countDistinct(
            F.when(
                F.col("product_id").isNotNull(),
                F.col("product_id")
            )
        ).cast("long").alias("distinct_products"),

        F.sum(
            F.when(
                F.col("event_type_norm") == "product_view", 1
            ).otherwise(0)
        ).cast("long").alias("product_views"),

        F.sum(
            F.when(
                F.col("event_type_norm").isin(
                    "add_to_cart",
                    "cart_add",
                    "add_to_cart_item"
                ), 1
            ).otherwise(0)
        ).cast("long").alias("add_to_cart_events"),

        F.sum(
            F.when(
                F.col("event_type_norm") == "checkout_started", 1
            ).otherwise(0)
        ).cast("long").alias("checkout_events"),

        F.max(
            F.when(
                F.col("event_type_norm") == "purchase", 1
            ).otherwise(0)
        ).alias("has_purchase_event"),

        F.max(
            F.when(
                F.col("order_id").isNotNull(), 1
            ).otherwise(0)
        ).alias("has_order"),

        F.min(
            F.when(
                F.col("event_ts").isNotNull(),
                F.struct(
                    F.col("event_ts"),
                    F.col("event_id"),
                    F.col("event_type_key")
                )
            )
        ).alias("_first_event"),

        F.max(
            F.when(
                F.col("event_ts").isNotNull(),
                F.struct(
                    F.col("event_ts"),
                    F.col("event_id"),
                    F.col("event_type_key")
                )
            )
        ).alias("_last_event")
    )
)

# 3. Derive session attributes and flags
fact_session = (
    fact_session_base

    .withColumn(
        "session_date_key",
        F.date_format("session_start_ts", "yyyyMMdd").cast("int")
    )

    .withColumn(
        "duration_seconds",
        F.when(
            F.col("session_start_ts").isNotNull() &
            F.col("session_end_ts").isNotNull(),
            F.col("session_end_ts").cast("long") -
            F.col("session_start_ts").cast("long")
        ).otherwise(None).cast("long")
    )

    .withColumn(
        "first_event_type_key",
        F.col("_first_event.event_type_key")
    )

    .withColumn(
        "last_event_type_key",
        F.col("_last_event.event_type_key")
    )

    .withColumn(
        "converted_flag",
        F.when(
            (F.col("has_purchase_event") == 1) |
            (F.col("has_order") == 1),
            1
        ).otherwise(0)
    )

    .withColumn(
        "bounce_flag",
        F.when(
            (F.col("event_count") == 1) &
            (F.col("product_views") <= 1) &
            (F.col("add_to_cart_events") == 0) &
            (F.col("checkout_events") == 0) &
            (F.col("has_purchase_event") == 0) &
            (F.col("has_order") == 0),
            1
        ).otherwise(0)
    )

    .drop("_first_event", "_last_event")
)

# 4. Add customer dimension key
dim_customer = spark.table(f"{GOLD}.dim_customer")

fact_session = (
    fact_session
    .join(
        dim_customer.select("customer_id", "customer_key"),
        on="customer_id",
        how="left"
    )
)

# 5. Select the exact expected schema
fact_session = fact_session.select(
    "session_id",
    "customer_id",
    "customer_key",
    "anonymous_id",
    "device_key",
    "session_start_ts",
    "session_end_ts",
    "session_date_key",
    "duration_seconds",
    "event_count",
    "distinct_products",
    "product_views",
    "add_to_cart_events",
    "checkout_events",
    "has_purchase_event",
    "has_order",
    "first_event_type_key",
    "last_event_type_key",
    "converted_flag",
    "bounce_flag"
)

# 6. Write Gold table
(
    fact_session.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.fact_session")
)

# 7. Validate
result = spark.table(f"{GOLD}.fact_session")

print("fact_session Full Build completed.")
print(f"Rows: {result.count():,}")
result.printSchema()

# Check session uniqueness
duplicates = (
    result.groupBy("session_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate session IDs:")
duplicates.show(10, truncate=False)

# Check conversion and bounce distributions
result.groupBy(
    "converted_flag", "bounce_flag"
).count().orderBy(
    "converted_flag", "bounce_flag"
).show()


# In[7]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Read Silver source
events = spark.table(
    "Click_stream.clickstream_lakehouse.silver.valid_clickstream_events"
)

# 2. Deduplicate events using event_id
window_spec = Window.partitionBy("event_id").orderBy(
    F.col("event_ts").desc_nulls_last()
)

events_dedup = (
    events
    .withColumn("_rn", F.row_number().over(window_spec))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# 3. Load dimensions
dim_customer = spark.table(
    "Click_stream.clickstream_lakehouse.gold.dim_customer"
)

dim_device = spark.table(
    "Click_stream.clickstream_lakehouse.gold.dim_device"
)

dim_product = spark.table(
    "Click_stream.clickstream_lakehouse.gold.dim_product"
)

dim_event_type = spark.table(
    "Click_stream.clickstream_lakehouse.gold.dim_event_type"
)

# 4. Join dimension keys
fact_events = (
    events_dedup.alias("e")

    .join(
        dim_customer.select("customer_id", "customer_key").alias("cu"),
        F.col("e.customer_id") == F.col("cu.customer_id"),
        "left"
    )

    .join(
        dim_device.select("anonymous_id", "device_key").alias("dv"),
        F.col("e.anonymous_id") == F.col("dv.anonymous_id"),
        "left"
    )

    .join(
        dim_product.select("product_id", "product_key").alias("pr"),
        F.col("e.product_id") == F.col("pr.product_id"),
        "left"
    )

    .join(
        dim_event_type.select("event_type", "event_type_key").alias("et"),
        F.lower(F.trim(F.col("e.event_type"))) ==
        F.lower(F.trim(F.col("et.event_type"))),
        "left"
    )

    .select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.event_ts").alias("event_ts"),
        F.date_format("e.event_ts", "yyyyMMdd")
            .cast("int").alias("event_date_key"),
        F.col("e.session_id").alias("session_id"),
        F.col("e.customer_id").alias("customer_id"),
        F.col("cu.customer_key").alias("customer_key"),
        F.col("e.anonymous_id").alias("anonymous_id"),
        F.col("dv.device_key").alias("device_key"),
        F.col("e.product_id").alias("product_id"),
        F.col("pr.product_key").alias("product_key"),
        F.col("et.event_type_key").alias("event_type_key"),
        F.col("e.event_type").alias("event_type"),
        F.col("e.price").alias("price"),
        F.col("e.quantity").alias("quantity"),
        F.col("e.cart_value").alias("cart_value"),
        F.col("e.amount").alias("amount"),
        F.col("e.order_id").alias("order_id"),
        F.col("e.order_value").alias("order_value"),
        F.col("e.status").alias("status"),
        F.col("e.error_code").alias("error_code"),
        F.col("e.payment_method").alias("payment_method"),
        F.col("e.shipping_country").alias("shipping_country"),
        F.col("e.referrer").alias("referrer"),
        F.col("e.category").alias("category"),
        F.col("e.search_term").alias("search_term")
    )
)

# 5. Full Build: replace the entire Gold table
(
    fact_events.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "Click_stream.clickstream_lakehouse.gold.fact_events"
    )
)

# 6. Validate
print("Full Build completed.")
print("fact_events rows:", spark.table(
    "Click_stream.clickstream_lakehouse.gold.fact_events"
).count())


# In[8]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# FACT_ORDER — FULL BUILD
# ============================================================

# 1. Read Silver sources
orders = spark.table(
    "Click_stream.clickstream_lakehouse.silver.valid_orders"
)

cart_items = spark.table(
    "Click_stream.clickstream_lakehouse.silver.valid_cart_items"
)

dim_customer = spark.table(
    "Click_stream.clickstream_lakehouse.gold.dim_customer"
)

dim_product = spark.table(
    "Click_stream.clickstream_lakehouse.gold.dim_product"
)

# 2. Deduplicate orders
order_window = Window.partitionBy("order_id").orderBy(
    F.col("event_ts").desc_nulls_last()
)

orders_dedup = (
    orders
    .withColumn("_rn", F.row_number().over(order_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# 3. Deduplicate cart items
cart_window = Window.partitionBy("event_id").orderBy(
    F.col("event_ts").desc_nulls_last()
)

cart_dedup = (
    cart_items
    .withColumn("_rn", F.row_number().over(cart_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# 4. Join orders to cart items using session_id
joined = (
    orders_dedup.alias("o")
    .join(
        cart_dedup.alias("c"),
        F.col("o.session_id") == F.col("c.session_id"),
        "left"
    )
)

# 5. Build order lines
order_lines = joined.select(
    F.concat_ws(
        "_",
        F.col("o.order_id").cast("string"),
        F.coalesce(
            F.col("c.event_id").cast("string"),
            F.lit("NO_CART_ITEM")
        )
    ).alias("order_line_id"),

    F.col("o.order_id").alias("order_id"),
    F.col("o.event_id").alias("order_event_id"),
    F.col("o.session_id").alias("session_id"),
    F.col("o.event_ts").alias("order_ts"),
    F.date_format("o.event_ts", "yyyyMMdd")
        .cast("int").alias("order_date_key"),

    F.col("o.customer_id").alias("customer_id"),
    F.col("c.product_id").alias("product_id"),
    F.col("c.event_id").alias("cart_event_id"),
    F.col("c.price").alias("unit_price"),
    F.col("c.quantity").alias("line_quantity"),

    (
        F.col("c.price") * F.col("c.quantity")
    ).alias("line_amount"),

    F.col("o.order_value").alias("order_value"),
    F.col("o.quantity").alias("order_quantity"),

    F.when(F.col("c.product_id").isNotNull(), True)
        .otherwise(False)
        .alias("product_details_available"),

    F.lit(None).cast("boolean").alias("quantity_match"),
    F.lit(None).cast("boolean").alias("value_match"),

    F.col("o.payment_method").alias("payment_method"),
    F.col("o.shipping_country").alias("shipping_country")
)

# 6. Add dimension keys
fact_order = (
    order_lines.alias("f")
    .join(
        dim_customer.select(
            "customer_id", "customer_key"
        ).alias("cu"),
        F.col("f.customer_id") == F.col("cu.customer_id"),
        "left"
    )
    .join(
        dim_product.select(
            "product_id", "product_key"
        ).alias("pr"),
        F.col("f.product_id") == F.col("pr.product_id"),
        "left"
    )
    .select(
        F.col("f.order_line_id"),
        F.col("f.order_id"),
        F.col("f.order_event_id"),
        F.col("f.session_id"),
        F.col("f.order_ts"),
        F.col("f.order_date_key"),
        F.col("f.customer_id"),
        F.col("cu.customer_key"),
        F.col("f.product_id"),
        F.col("pr.product_key"),
        F.col("f.cart_event_id"),
        F.col("f.unit_price"),
        F.col("f.line_quantity"),
        F.col("f.line_amount"),
        F.col("f.order_value"),
        F.col("f.order_quantity"),
        F.col("f.product_details_available"),
        F.col("f.quantity_match"),
        F.col("f.value_match"),
        F.col("f.payment_method"),
        F.col("f.shipping_country")
    )
)

# 7. FULL BUILD — overwrite existing Gold table
(
    fact_order.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "Click_stream.clickstream_lakehouse.gold.fact_order"
    )
)

# 8. Validation
result = spark.table(
    "Click_stream.clickstream_lakehouse.gold.fact_order"
)

print("fact_order Full Build completed.")
print("Rows:", result.count())
result.printSchema()


# In[9]:


gold_events = spark.table(f"{GOLD}.fact_events")
gold_sessions = spark.table(f"{GOLD}.fact_session")
gold_orders = spark.table(f"{GOLD}.fact_order")

# 1. Grain and duplicate checks
print("=== Duplicate key checks ===")

gold_events.groupBy("event_id").count() \
    .filter(F.col("count") > 1).show(10, truncate=False)

gold_sessions.groupBy("session_id").count() \
    .filter(F.col("count") > 1).show(10, truncate=False)

gold_orders.groupBy("order_line_id").count() \
    .filter(F.col("count") > 1).show(10, truncate=False)

# 2. Source vs Gold counts
print("=== Source vs Gold ===")

print("Silver distinct events:", events.select("event_id").distinct().count())
print("Gold distinct events:", gold_events.select("event_id").distinct().count())

print("Silver distinct sessions:",
      events.filter(F.col("session_id").isNotNull())
            .select("session_id").distinct().count())
print("Gold sessions:", gold_sessions.select("session_id").distinct().count())

print("Silver distinct orders:", orders.select("order_id").distinct().count())
print("Gold distinct orders:", gold_orders.select("order_id").distinct().count())

# 3. Order-level revenue: sum order_value only once per order
print("=== Revenue reconciliation ===")

silver_revenue = orders.agg(
    F.sum("order_value").alias("silver_revenue")
).first()["silver_revenue"]

gold_revenue = gold_orders.agg(
    F.sum("order_value").alias("gold_revenue")
).first()["gold_revenue"]

print("Silver order revenue:", silver_revenue)
print("Gold order revenue:", gold_revenue)
print(
    "Revenue difference:",
    None if silver_revenue is None or gold_revenue is None
    else round(silver_revenue - gold_revenue, 2)
)

# 4. Order details and quality flags
print("=== Order detail quality ===")

gold_orders.groupBy(
    "product_details_available",
    "quantity_match",
    "value_match"
).count().orderBy(
    "product_details_available",
    "quantity_match",
    "value_match"
).show(50, truncate=False)

print("=== Orders without product details ===")
gold_orders.filter(
    ~F.col("product_details_available")
).select(
    "order_id", "order_value", "order_quantity"
).dropDuplicates(["order_id"]).count()


# In[10]:


def find_col(df, candidates):
    """
    Return the first candidate column found in the DataFrame.
    Matching is case-insensitive.
    """
    columns_map = {c.lower(): c for c in df.columns}

    for candidate in candidates:
        if candidate.lower() in columns_map:
            return columns_map[candidate.lower()]

    return None


TIME_COLS = [
    "event_ts",
    "event_timestamp",
    "timestamp",
    "event_time",
    "created_at",
    "activity_date",
    "date"
]

GOLD_SCHEMA = "Click_stream.clickstream_lakehouse.gold"

def save_gold(df, table_name):
    full_name = f"{GOLD_SCHEMA}.{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )

    print(f"Saved {full_name}")



# In[15]:


from pyspark.sql import functions as F

def cart_summary(source, target, label):
    # silver is the clickstream events DataFrame
    df = spark.table(
        f"Click_stream.clickstream_lakehouse.silver.{source}"
    )

    # Check the required columns
    required = {"event_ts", "event_type", "quantity"}
    missing = required - set(df.columns)

    if missing:
        print(f"Skipped {target}: missing columns {sorted(missing)}")
        return

    # Select the relevant cart events
    if label == "cart_item":
        df = df.filter(
            F.lower(F.col("event_type")).isin(
                "add_to_cart", "cart_add", "add_to_cart_item"
            )
        )
    elif label == "cart_removal":
        df = df.filter(
            F.lower(F.col("event_type")).isin(
                "remove_from_cart", "cart_remove", "remove_from_cart_item"
            )
        )

    # Aggregate by activity date
    result = (
        df
        .withColumn("activity_date", F.to_date("event_ts"))
        .groupBy("activity_date")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.col("quantity").cast("double"))
                .alias("total_recorded_quantity")
        )
    )

    save_gold(result, target)
    print(f"Saved {target}")


cart_summary(
    "valid_cart_items",
    "gold_cart_additions_daily",
    "cart_item"
)

cart_summary(
    "valid_cart_removals",
    "gold_cart_removals_daily",
    "cart_removal"
)



# In[16]:


SILVER_SCHEMA = "Click_stream.clickstream_lakehouse.silver"

silver = {
    "silver_cart_items":
        spark.table(f"{SILVER_SCHEMA}.valid_cart_items"),

    "silver_cart_removals":
        spark.table(f"{SILVER_SCHEMA}.valid_cart_removals"),
}

print("Silver sources loaded:")
for name, df in silver.items():
    print(f"{name}: {df.count():,} rows")


# In[17]:


from pyspark.sql import functions as F

GOLD = "Click_stream.clickstream_lakehouse.gold"
SILVER = "Click_stream.clickstream_lakehouse.silver"

tables_to_check = [
    f"{GOLD}.fact_events",
    f"{GOLD}.fact_session",
    f"{GOLD}.fact_order",
    f"{GOLD}.dim_customer",
    f"{GOLD}.dim_product",
    f"{GOLD}.dim_date",
    f"{SILVER}.valid_clickstream_events",
    f"{SILVER}.valid_cart_items",
    f"{SILVER}.valid_cart_removals",
    f"{SILVER}.valid_orders",
    f"{SILVER}.valid_customers",
]

for table_name in tables_to_check:
    try:
        df = spark.table(table_name)
        print(f"\n{'=' * 70}")
        print(f"TABLE: {table_name}")
        print("COLUMNS:", df.columns)
        print(f"ROWS: {df.count():,}")
    except Exception as e:
        print(f"\nMISSING: {table_name}")
        print(str(e)[:250])


# In[18]:


from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD = "Click_stream.clickstream_lakehouse.gold"
SILVER = "Click_stream.clickstream_lakehouse.silver"

events = (
    spark.table(f"{GOLD}.fact_events")
    .withColumn("event_type_norm", F.lower(F.trim(F.col("event_type"))))
)

sessions = spark.table(f"{GOLD}.fact_session")
orders = spark.table(f"{GOLD}.fact_order")
products = spark.table(f"{GOLD}.dim_product")
customers = spark.table(f"{GOLD}.dim_customer")

cart_items = spark.table(f"{SILVER}.valid_cart_items")
cart_removals = spark.table(f"{SILVER}.valid_cart_removals")

def save_gold(df, table_name):
    target = f"{GOLD}.{table_name}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    print(f"Saved {target}: {df.count():,} rows")


# In[19]:


session_events = (
    events
    .filter(F.col("session_id").isNotNull())
    .groupBy("session_id")
    .agg(
        F.min("event_ts").alias("first_event_ts"),
        F.max("event_ts").alias("last_event_ts"),
        F.max(F.when(F.col("event_type_norm") == "product_view", 1).otherwise(0)).alias("viewed_product"),
        F.max(F.when(F.col("event_type_norm") == "add_to_cart", 1).otherwise(0)).alias("added_to_cart"),
        F.max(F.when(F.col("event_type_norm") == "checkout_started", 1).otherwise(0)).alias("started_checkout"),
        F.max(F.when(F.col("event_type_norm") == "purchase", 1).otherwise(0)).alias("purchase_event"),
        F.max(F.when(F.col("event_type_norm") == "payment_attempt", 1).otherwise(0)).alias("attempted_payment"),
        F.count("*").alias("event_count")
    )
)

w_last = Window.partitionBy("session_id").orderBy(
    F.col("event_ts").desc_nulls_last(),
    F.col("event_id").desc_nulls_last()
)


last_event = (
    events.filter(F.col("session_id").isNotNull())
    .withColumn("_rn", F.row_number().over(w_last))
    .filter(F.col("_rn") == 1)
    .select(
        "session_id",
        F.col("event_type").alias("final_event_type"),
        F.col("event_ts").alias("final_event_ts"),
        F.col("referrer").alias("final_event_referrer")
    )
)


gold_session_funnel = (
    session_events
    .join(
        sessions.select(
            "session_id", "customer_id", "anonymous_id",
            "duration_seconds", "has_order", "converted_flag", "bounce_flag"
        ),
        "session_id",
        "left"
    )
    .join(last_event, "session_id", "left")
    .withColumn("session_date", F.to_date("first_event_ts"))
    .withColumn(
        "view_to_purchase_flag",
        F.when(
            (F.col("viewed_product") == 1) &
            ((F.col("purchase_event") == 1) | (F.coalesce(F.col("has_order"), F.lit(0)) == 1)),
            1
        ).otherwise(0)
    )
)

save_gold(gold_session_funnel, "gold_session_funnel")


# In[20]:


cart_adds = (
    cart_items
    .filter(F.col("session_id").isNotNull() & F.col("product_id").isNotNull())
    .withColumn("activity_date", F.to_date("event_ts"))
    .groupBy("activity_date", "product_id")
    .agg(
        F.countDistinct("session_id").alias("add_sessions"),
        F.count("*").alias("cart_add_events"),
        F.sum(F.coalesce(F.col("quantity"), F.lit(0))).alias("quantity_added")
    )
)

cart_removes = (
    cart_removals
    .filter(F.col("session_id").isNotNull() & F.col("product_id").isNotNull())
    .withColumn("activity_date", F.to_date("event_ts"))
    .groupBy("activity_date", "product_id")
    .agg(
        F.count("*").alias("cart_remove_events"),
        F.countDistinct("session_id").alias("remove_sessions")
    )
)

product_views = (
    events.filter(
        (F.col("event_type_norm") == "product_view") &
        F.col("product_id").isNotNull()
    )
    .withColumn("activity_date", F.to_date("event_ts"))
    .groupBy("activity_date", "product_id")
    .agg(
        F.count("*").alias("product_view_events"),
        F.countDistinct("session_id").alias("view_sessions")
    )
)

# Purchase events only: do not infer purchased products from session-level cart lines.
purchases = (
    events.filter(
        (F.col("event_type_norm") == "purchase") &
        F.col("product_id").isNotNull()
    )
    .withColumn("activity_date", F.to_date("event_ts"))
    .groupBy("activity_date", "product_id")
    .agg(
        F.countDistinct("session_id").alias("purchase_sessions"),
        F.count("*").alias("purchase_events")
    )
)

keys = (
    cart_adds.select("activity_date", "product_id")
    .union(cart_removes.select("activity_date", "product_id"))
    .union(product_views.select("activity_date", "product_id"))
    .union(purchases.select("activity_date", "product_id"))
    .distinct()
)

gold_product_funnel_daily = (
    keys
    .join(cart_adds, ["activity_date", "product_id"], "left")
    .join(cart_removes, ["activity_date", "product_id"], "left")
    .join(product_views, ["activity_date", "product_id"], "left")
    .join(purchases, ["activity_date", "product_id"], "left")
    .fillna(0, subset=[
        "add_sessions", "cart_add_events", "quantity_added",
        "cart_remove_events", "remove_sessions",
        "product_view_events", "view_sessions",
        "purchase_sessions", "purchase_events"
    ])
    .withColumn(
        "purchase_per_add_session_rate",
        F.when(
            F.col("add_sessions") > 0,
            F.col("purchase_sessions") / F.col("add_sessions")
        )
    )
    .withColumn(
        "unconverted_add_session_rate_proxy",
        F.when(
            F.col("add_sessions") > 0,
            F.greatest(
                F.lit(0),
                F.col("add_sessions") - F.col("purchase_sessions")
            ) / F.col("add_sessions")
        )
    )
    .join(products.select("product_id", "product_name", "category", "brand"),
          "product_id", "left")
)

save_gold(gold_product_funnel_daily, "gold_product_funnel_daily")


# In[21]:


identity = F.coalesce(
    F.col("customer_id"),
    F.concat(F.lit("anon:"), F.col("anonymous_id"))
)

active_days = (
    events
    .filter(F.col("event_ts").isNotNull())
    .withColumn("user_identity", identity)
    .filter(F.col("user_identity").isNotNull())
    .withColumn("activity_date", F.to_date("event_ts"))
    .select("user_identity", "activity_date")
    .distinct()
)

next_day = active_days.select(
    "user_identity",
    F.date_sub("activity_date", 1).alias("activity_date"),
    F.lit(1).alias("returned_next_day")
)

gold_customer_retention_daily = (
    active_days
    .groupBy("activity_date")
    .agg(
        F.countDistinct("user_identity").alias("active_users")
    )
    .join(
        active_days.join(next_day, ["user_identity", "activity_date"], "inner")
        .groupBy("activity_date")
        .agg(F.countDistinct("user_identity").alias("next_day_return_users")),
        "activity_date",
        "left"
    )
    .fillna(0, subset=["next_day_return_users"])
    .withColumn(
        "next_day_return_rate",
        F.when(
            F.col("active_users") > 0,
            F.col("next_day_return_users") / F.col("active_users")
        )
    )
)

save_gold(gold_customer_retention_daily, "gold_customer_retention_daily")


# In[22]:


checkout_events = (
    events.filter(
        F.col("session_id").isNotNull() &
        (F.col("event_type_norm") == "checkout_started")
    )
    .select(
        "session_id",
        F.col("event_ts").alias("checkout_started_ts"),
        F.col("event_id").alias("checkout_event_id")
    )
)

purchase_events = (
    events.filter(
        F.col("session_id").isNotNull() &
        (F.col("event_type_norm") == "purchase")
    )
    .select(
        "session_id",
        F.col("event_ts").alias("purchase_ts"),
        F.col("event_id").alias("purchase_event_id")
    )
)

candidate_pairs = (
    checkout_events.alias("c")
    .join(
        purchase_events.alias("p"),
        (F.col("c.session_id") == F.col("p.session_id")) &
        (F.col("p.purchase_ts") >= F.col("c.checkout_started_ts")),
        "left"
    )
    .select(
        F.col("c.session_id").alias("session_id"),
        F.col("c.checkout_event_id").alias("checkout_event_id"),
        F.col("c.checkout_started_ts").alias("checkout_started_ts"),
        F.col("p.purchase_event_id").alias("purchase_event_id"),
        F.col("p.purchase_ts").alias("purchase_ts")
    )
)

w_checkout = Window.partitionBy(
    "session_id", "checkout_event_id"
).orderBy(F.col("purchase_ts").asc_nulls_last())

gold_checkout_duration = (
    candidate_pairs
    .withColumn("_rn", F.row_number().over(w_checkout))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn(
        "checkout_duration_seconds",
        F.col("purchase_ts").cast("long") -
        F.col("checkout_started_ts").cast("long")
    )
    .withColumn(
        "checkout_completed_flag",
        F.col("purchase_ts").isNotNull().cast("int")
    )
)

save_gold(gold_checkout_duration, "gold_checkout_duration")


# In[23]:


# One record per order for order-level revenue.
# This avoids summing order_value once for every product line.
order_headers = (
    orders
    .filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(
        F.max("order_ts").alias("order_ts"),
        F.max("customer_id").alias("customer_id"),
        F.max("order_value").alias("order_value"),
        F.max("product_details_available").alias("product_details_available")
    )
    .withColumn("order_date", F.to_date("order_ts"))
)

# Category-level allocation is only available when product details exist.
# Use line_amount for category revenue, not repeated order_value.
order_lines = (
    orders
    .filter(
        F.col("order_id").isNotNull() &
        F.col("product_id").isNotNull() &
        F.col("line_amount").isNotNull()
    )
    .join(
        products.select("product_id", "category"),
        "product_id",
        "left"
    )
    .withColumn("order_date", F.to_date("order_ts"))
)

customer_attributes = customers.select(
    "customer_id",
    F.col("gender").alias("customer_gender"),
    F.col("country").alias("customer_country")
).dropDuplicates(["customer_id"])

gold_revenue_daily = (
    order_lines
    .join(customer_attributes, "customer_id", "left")
    .groupBy(
        "order_date",
        "category",
        "customer_gender",
        "customer_country"
    )
    .agg(
        F.sum("line_amount").alias("line_revenue"),
        F.countDistinct("order_id").alias("orders_with_product_details"),
        F.sum("line_quantity").alias("units_ordered")
    )
)

save_gold(gold_revenue_daily, "gold_revenue_daily")


# In[24]:


new_gold_tables = [
    "gold_session_funnel",
    "gold_product_funnel_daily",
    "gold_customer_retention_daily",
    "gold_checkout_duration",
    "gold_revenue_daily"
]

for name in new_gold_tables:
    df = spark.table(f"{GOLD}.{name}")
    print(f"\n{name}: {df.count():,} rows")
    df.show(5, truncate=False)


# In[25]:


print("Type:", type(silver))

if isinstance(silver, dict):
    for key, value in silver.items():
        print(key, type(value).__name__)
else:
    print(silver)


# In[26]:


for table_name in [
    "valid_cart_items",
    "valid_cart_removals",
    "valid_clickstream_events"
]:
    print(f"\n{'=' * 50}\n{table_name}")

    spark.table(
        f"Click_stream.clickstream_lakehouse.silver.{table_name}"
    ).printSchema()

