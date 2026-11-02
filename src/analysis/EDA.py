#!/usr/bin/env python
# coding: utf-8

# ## EDA
# 
# New notebook

# # EDA

# In[7]:


from pyspark.sql import functions as F
import pandas as pd

RAW_SCHEMA = "raw"

TABLES = [
    "raw_cart_items", "raw_cart_removals", "categories", "raw_checkouts",
    "raw_clickstream_events", "raw_customers", "raw_devices", "raw_events",
    "raw_orders", "raw_payment_attempts", "raw_product_prices", "raw_products",
    "calendar_days", "event_types", "raw_stock_changes",
]

# Candidate business key per table (verified in section 3)
KEYS = {t: "event_id" for t in TABLES}
KEYS.update({"categories": "category_id", "calendar_days": "date_key", "event_types": "event_type_id"})

# Relationships to test: (child table, child column, parent table, parent column)
REFERENCES = [
    ("raw_cart_items",      "product_id",  "raw_products",  "product_id"),
    ("raw_cart_removals",   "product_id",  "raw_products",  "product_id"),
    ("raw_product_prices",  "product_id",  "raw_products",  "product_id"),
    ("raw_stock_changes",   "product_id",  "raw_products",  "product_id"),
    ("raw_products",        "category_id", "categories",    "category_id"),
    ("raw_orders",          "customer_id", "raw_customers", "customer_id"),
    ("raw_checkouts",       "customer_id", "raw_customers", "customer_id"),
]

NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "float", "double")
TS_TYPES = ("timestamp", "timestamp_ntz", "date")
FREE_TEXT = {"name", "email", "payload", "product_name", "search_term", "category_path"}
MAX_CATEGORIES = 25     # a string column with at most this many distinct values is treated as categorical
TOP_N = 10

FINDINGS = []           # every issue found, summarised in section 8

def finding(table, column, issue, rows):
    if rows and rows > 0:
        FINDINGS.append({"table": table, "column": column, "issue": issue, "rows": int(rows)})


DFS = {t: spark.read.table(f"{RAW_SCHEMA}.{t}").cache() for t in TABLES}
print(f"Loaded {len(DFS)} raw tables")


# ## 1. Inventory
# 
# Row counts, column counts and the data type of every column.

# In[8]:


ROWS, schema_rows = {}, []
for t, df in DFS.items():
    ROWS[t] = df.count()
    for c, d in df.dtypes:
        schema_rows.append({"table": t, "column": c, "dtype": d})

INVENTORY = pd.DataFrame([
    {"table": t, "rows": ROWS[t], "columns": len(DFS[t].columns), "candidate_key": KEYS[t]} for t in TABLES
]).sort_values("rows", ascending=False)
SCHEMA = pd.DataFrame(schema_rows)

show(INVENTORY)
print(f"Total rows across raw: {sum(ROWS.values()):,}")

try:
    import matplotlib.pyplot as plt
    ax = INVENTORY.set_index("table")["rows"].sort_values().plot.barh(figsize=(9, 5), logx=True)
    ax.set_title("Rows per raw table (log scale)")
    plt.tight_layout(); plt.show()
except Exception as e:
    print("Chart skipped:", e)


# In[9]:


# Full schema of every table
for t in TABLES:
    print(f"\n{t}  ({ROWS[t]:,} rows)")
    print(SCHEMA[SCHEMA["table"] == t][["column", "dtype"]].to_string(index=False))


# ## 2. Null, blank and padded-string profile
# 
# Nulls, empty strings (after trim), strings with leading or trailing spaces, and NaN in floating-point columns.

# In[10]:


NULLS = []
for t, df in DFS.items():
    total = ROWS[t]
    aggs = []
    for c, d in df.dtypes:
        aggs.append(F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__null"))
        if d == "string":
            aggs.append(F.sum((F.trim(F.col(c)) == "").cast("int")).alias(f"{c}__blank"))
            aggs.append(F.sum((F.col(c) != F.trim(F.col(c))).cast("int")).alias(f"{c}__pad"))
        if d in ("float", "double"):
            aggs.append(F.sum(F.isnan(F.col(c)).cast("int")).alias(f"{c}__nan"))
    r = df.agg(*aggs).collect()[0].asDict()

    for c, d in df.dtypes:
        n, b, p, nan = (r.get(f"{c}__null") or 0), (r.get(f"{c}__blank") or 0), (r.get(f"{c}__pad") or 0), (r.get(f"{c}__nan") or 0)
        NULLS.append({"table": t, "column": c, "dtype": d, "nulls": n,
                      "null_pct": round(100 * n / total, 2) if total else 0,
                      "blank_strings": b, "padded_strings": p, "nan": nan})
        if c in (KEYS[t], "event_id", "event_ts"):
            finding(t, c, "null in key / required column", n)
        finding(t, c, "blank strings", b)
        finding(t, c, "leading/trailing whitespace", p)
        finding(t, c, "NaN values", nan)

NULL_DF = pd.DataFrame(NULLS)
issues = NULL_DF[(NULL_DF.nulls > 0) | (NULL_DF.blank_strings > 0) | (NULL_DF.padded_strings > 0) | (NULL_DF.nan > 0)]
print(f"{len(issues)} of {len(NULL_DF)} columns have nulls, blanks, padding or NaN\n")
show(issues.sort_values(["null_pct", "table"], ascending=[False, True]))


# ## 3. Key uniqueness and duplicates
# 
# Checks that the candidate key is unique and not null, and counts exact duplicate rows.

# In[11]:


DUPS, DUP_SAMPLES = [], []
for t, df in DFS.items():
    key, total = KEYS[t], ROWS[t]
    non_null = df.filter(F.col(key).isNotNull())
    nn_rows = non_null.count()
    distinct_keys = non_null.select(key).distinct().count()
    dup_rows = nn_rows - distinct_keys                       # extra rows beyond the first per key
    null_keys = total - nn_rows
    exact_dups = total - df.dropDuplicates().count()         # identical on every column

    DUPS.append({"table": t, "key": key, "rows": total, "distinct_keys": distinct_keys,
                 "null_keys": null_keys, "duplicate_key_rows": dup_rows,
                 "exact_duplicate_rows": exact_dups,
                 "duplicate_pct": round(100 * dup_rows / total, 2) if total else 0})
    finding(t, key, "duplicate key (extra rows)", dup_rows)
    finding(t, "(all columns)", "exact duplicate rows", exact_dups)

    if dup_rows > 0:
        for r in non_null.groupBy(key).count().filter("count > 1").orderBy(F.desc("count")).limit(3).collect():
            DUP_SAMPLES.append({"table": t, "key_value": r[key], "occurrences": r["count"]})

DUP_DF = pd.DataFrame(DUPS)
show(DUP_DF.sort_values("duplicate_key_rows", ascending=False))
if DUP_SAMPLES:
    print("\nMost repeated key values:")
    show(pd.DataFrame(DUP_SAMPLES))


# ## 4. Numeric profile
# 
# Minimum, maximum, mean, negative values and zeros for every numeric column.

# In[12]:


NUM = []
for t, df in DFS.items():
    cols = [c for c, d in df.dtypes if d in NUMERIC_TYPES or d.startswith("decimal")]
    if not cols:
        continue
    aggs = []
    for c in cols:
        aggs += [F.min(c).alias(f"{c}__min"), F.max(c).alias(f"{c}__max"), F.avg(c).alias(f"{c}__avg"),
                 F.sum((F.col(c) < 0).cast("int")).alias(f"{c}__neg"),
                 F.sum((F.col(c) == 0).cast("int")).alias(f"{c}__zero")]
    r = df.agg(*aggs).collect()[0].asDict()
    f = lambda v: None if v is None else round(float(v), 2)
    for c in cols:
        neg, zero = r[f"{c}__neg"] or 0, r[f"{c}__zero"] or 0
        NUM.append({"table": t, "column": c, "min": f(r[f"{c}__min"]), "max": f(r[f"{c}__max"]),
                    "mean": f(r[f"{c}__avg"]), "negatives": neg, "zeros": zero})
        if not c.endswith("_id") and c not in ("delta",):
            finding(t, c, "negative values", neg)
        if c == "quantity":
            finding(t, c, "zero quantity", zero)

show(pd.DataFrame(NUM))


# ## 5. Categorical columns
# 
# String columns with few distinct values: their values, and whether they differ only by case or spacing.

# In[13]:


CATS = []
for t, df in DFS.items():
    cols = [c for c, d in df.dtypes if d == "string" and not c.endswith("_id") and c not in FREE_TEXT]
    if not cols:
        continue
    card = df.agg(*[F.approx_count_distinct(c).alias(c) for c in cols]).collect()[0].asDict()
    for c in cols:
        if card[c] > MAX_CATEGORIES:
            continue
        raw_distinct = df.select(c).distinct().count()
        norm_distinct = df.select(F.lower(F.trim(F.col(c)))).distinct().count()
        finding(t, c, "inconsistent casing/spacing", raw_distinct - norm_distinct)
        for r in df.groupBy(c).count().orderBy(F.desc("count")).limit(TOP_N).collect():
            CATS.append({"table": t, "column": c, "value": r[c], "rows": r["count"],
                         "pct": round(100 * r["count"] / ROWS[t], 2)})

CAT_DF = pd.DataFrame(CATS)
print(f"{CAT_DF[['table','column']].drop_duplicates().shape[0]} categorical columns found\n")
show(CAT_DF, 500)

